# ============================================================
# PLC 数据采集器 - asyncio 调度 + 同步 Modbus（兼容性优先）
# v2.1: 1000台设备单机优化 + 指数退避重试 + 连接池 + 分批调度
# ============================================================
import struct, asyncio, time, logging, os, traceback, math
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pymodbus.client import ModbusTcpClient
import config, database

# 屏蔽 pymodbus 英文日志
logging.getLogger("pymodbus").setLevel(logging.CRITICAL)

# ===== 采集器配置 =====
READ_INTERVAL = config.READ_INTERVAL  # 采集间隔(秒)

# ---- 单机1000台优化参数 ----
# Modbus 线程池：1000台设备，按经验每200台需要~100线程（考虑网络IO等待）
# 使用 512 线程确保1000台设备的Modbus请求不会被线程池阻塞
MODBUS_POOL_SIZE = int(os.environ.get("OIL_MONITOR_MODBUS_POOL", "512"))

# 采集调度并发上限：每次采集周期最多同时进行多少次Modbus通信
# 设 500 可确保所有设备在2个批次内完成，避免asyncio信号量成为瓶颈
MAX_CONCURRENT = int(os.environ.get("OIL_MONITOR_CONCURRENT", "500"))

# 调度批次间隔（秒）：每批之间短暂休息，避免瞬间打满网络
BATCH_INTERVAL = float(os.environ.get("OIL_MONITOR_BATCH_INTERVAL", "0.3"))

# 连接空闲超时（秒）：超过此时间未使用的连接将被关闭回收
CONNECTION_IDLE_TIMEOUT = int(os.environ.get("OIL_MONITOR_CONN_IDLE", "120"))

# ---- 断线重试：指数退避参数 ----
RETRY_BASE_DELAY = 1.0       # 基础退避延迟（秒）
RETRY_MAX_DELAY = 120.0      # 最大退避延迟（秒）
RETRY_BACKOFF_FACTOR = 2.0   # 退避因子
RETRY_JITTER = 0.1           # 随机抖动比例（0.1=±10%），避免惊群效应

_modbus_executor = ThreadPoolExecutor(max_workers=MODBUS_POOL_SIZE, thread_name_prefix="modbus")


def _build_modbus_client(ip, port):
    """创建 Modbus TCP 同步客户端"""
    return ModbusTcpClient(ip, port=port, timeout=2)


class DeviceWorker:
    """单台设备数据对象"""

    __slots__ = ('device_id', 'name', 'ip', 'port', 'client', 'running',
                 'connected', 'count', 'error', 'x_states', 'y_states',
                 'last_connect', 'last_disconnect', 'plc_settings',
                 '_oil_change_confirmed', '_oil_change_time',
                 '_optimize_at_count', '_busy', '_temp_cache',
                 # ---- 断线重试字段 ----
                 '_retry_count',        # 连续失败次数
                 '_next_retry_at',      # 下次允许重试的时间戳
                 '_last_use_time',      # 连接最后使用时间
                 '_consecutive_failures',  # 连续失败计数（用于健康检查）
                 '_last_success_time',     # 最后一次成功采集时间
    )

    def __init__(self, device_id, name, ip, port):
        self.device_id = device_id
        self.name = name
        self.ip = ip
        self.port = port
        self.client = None
        self.running = False
        self.connected = False
        self.count = 0
        self.error = ""
        self.x_states = {}
        self.y_states = {}
        self.last_connect = ""
        self.last_disconnect = ""
        self.plc_settings = {}
        self._oil_change_confirmed = False
        self._oil_change_time = None
        self._optimize_at_count = 0
        self._busy = False
        self._temp_cache = {}
        # 断线重试
        self._retry_count = 0
        self._next_retry_at = 0.0
        self._last_use_time = time.time()
        self._consecutive_failures = 0
        self._last_success_time = time.time()

    def _should_retry(self):
        """检查是否到了重试时间（指数退避）"""
        if self._next_retry_at == 0:
            return True
        return time.time() >= self._next_retry_at

    def _schedule_retry(self):
        """计算下次重试时间（指数退避 + 随机抖动）
        注意：同一采集周期内可能被多次调用（_ensure_connected 失败 + 异常处理），
        使用 _next_retry_at 防重复：若已排期且尚未到达，不重复累加 retry_count"""
        now = time.time()
        if self._next_retry_at > now:
            return  # 已排期等待中，跳过重复调用，防止 retry_count 翻倍增长
        delay = min(RETRY_BASE_DELAY * (RETRY_BACKOFF_FACTOR ** self._retry_count),
                    RETRY_MAX_DELAY)
        # 添加随机抖动，避免所有离线设备同时重连
        jitter = delay * RETRY_JITTER * (2 * (hash(self.device_id) % 1000) / 1000.0 - 1)
        self._next_retry_at = now + delay + jitter
        self._retry_count = min(self._retry_count + 1, 20)  # 上限防止溢出

    def _reset_retry(self):
        """重置重试状态（连接成功后调用）"""
        self._retry_count = 0
        self._next_retry_at = 0.0

    def _ensure_connected(self):
        """同步建立 Modbus 连接（带指数退避重试）"""
        # 检查是否在退避等待期
        if not self._should_retry():
            return False

        if self.client and self.client.connected:
            self._last_use_time = time.time()
            return True

        # 安全关闭旧连接
        old = self.client
        self.client = None
        if old:
            try:
                old.close()
            except Exception:
                pass

        self.client = _build_modbus_client(self.ip, self.port)
        try:
            ok = self.client.connect()
            if ok:
                self.last_connect = datetime.now().strftime("%H:%M:%S")
                self._reset_retry()
                self._last_use_time = time.time()
            else:
                self.last_disconnect = datetime.now().strftime("%H:%M:%S")
                self._schedule_retry()
            return ok
        except Exception:
            self.last_disconnect = datetime.now().strftime("%H:%M:%S")
            self._schedule_retry()
            return False

    def _close_if_idle(self):
        """关闭空闲连接以释放资源（1000台设备不可能全部同时保持连接）"""
        if self.client and self.client.connected:
            if time.time() - self._last_use_time > CONNECTION_IDLE_TIMEOUT:
                try:
                    self.client.close()
                except Exception:
                    pass
                self.client = None

    # ---- 数据读取方法保持不变 ----

    @staticmethod
    def _read_int16(raw):
        return raw - 65536 if raw > 32767 else raw

    def read_oil_quality(self):
        """同步读取油品品质 D0-D61 (Float32)"""
        if not config.OIL_QUALITY:
            return {}
        addrs = set()
        for a, _, _ in config.OIL_QUALITY:
            addrs.add(a)
            addrs.add(a + 1)
        mn, mx = min(addrs), max(addrs)
        try:
            r = self.client.read_holding_registers(mn, count=mx - mn + 1, device_id=1)
        except Exception:
            return {}
        if r.isError():
            return {}
        raw, vals = r.registers, {}
        for a, name, _ in config.OIL_QUALITY:
            lo, hi = a - mn, a + 1 - mn
            if hi < len(raw):
                c = (raw[hi] << 16) | raw[lo]
                try:
                    val = struct.unpack('>f', struct.pack('>I', c))[0]
                    # 过滤 NaN/Inf，防止污染数据库和算法
                    if not math.isfinite(val):
                        vals[name] = 0.0
                    else:
                        vals[name] = round(val, 2)
                except Exception:
                    vals[name] = 0.0
            else:
                vals[name] = 0.0
        return vals

    def read_system_params(self):
        """同步读取系统运行参数 D62-D82 (Int16)"""
        if not config.SYSTEM_PARAMS:
            return {}
        addrs = [a for a, *_ in config.SYSTEM_PARAMS]
        mn, mx = min(addrs), max(addrs)
        try:
            r = self.client.read_holding_registers(mn, count=mx - mn + 1, device_id=1)
        except Exception:
            return {}
        if r.isError():
            return {}
        raw, vals = r.registers, {}
        for addr, key, scale, unit, _ in config.SYSTEM_PARAMS:
            raw_val = raw[addr - mn]
            if addr in (64, 65, 66, 67, 68, 70, 71, 72, 73, 81, 82):
                raw_val = self._read_int16(raw_val)
            val = raw_val * scale
            # 过滤 NaN/Inf
            if not math.isfinite(val):
                vals[key] = 0.0
            else:
                vals[key] = round(val, 2)
        return vals

    def read_x_inputs(self):
        """同步读 D268(K4X000)，PLC MOV 指令打包的 X000-X017"""
        if not config.X_INPUTS:
            return {}
        try:
            r = self.client.read_holding_registers(268, count=1, device_id=1)
        except Exception:
            return {}
        if r.isError() or not r.registers:
            return {}
        val = r.registers[0]
        return {name: (val >> addr) & 1 for addr, name in config.X_INPUTS}

    def read_y_outputs(self):
        """同步读 D269(K2Y000)，PLC MOV 指令打包的 Y000-Y007"""
        if not config.Y_OUTPUTS:
            return {}
        try:
            r = self.client.read_holding_registers(269, count=1, device_id=1)
        except Exception:
            return {}
        if r.isError() or not r.registers:
            return {}
        val = r.registers[0]
        return {name: (val >> addr) & 1 for addr, name in config.Y_OUTPUTS}

    def read_plc_settings(self):
        """同步读取 PLC 设定值 D200-D267"""
        if not config.PLC_SETTINGS:
            return {}
        addrs = [a for a, *_ in config.PLC_SETTINGS]
        mn, mx = min(addrs), max(addrs)
        try:
            r = self.client.read_holding_registers(mn, count=mx - mn + 1, device_id=1)
        except Exception:
            return {}
        if r.isError():
            return {}
        raw, vals = r.registers, {}
        for addr, key, scale, unit, desc, cat in config.PLC_SETTINGS:
            if addr - mn < len(raw):
                raw_val = raw[addr - mn]
                val = raw_val * scale
                if not math.isfinite(val):
                    val = 0
                vals[key] = {"value": round(val, 2),
                             "unit": unit, "desc": desc, "category": cat}
            else:
                vals[key] = {"value": 0, "unit": unit, "desc": desc, "category": cat}

        def _join_ip(*keys):
            parts = [str(int(vals.get(k, {}).get("value", 0))) for k in keys]
            return ".".join(parts) if all(p != "0" for p in parts) else ""

        vals["_ip_display"] = _join_ip("IP地址1", "IP地址2", "IP地址3", "IP地址4")
        vals["_mask_display"] = _join_ip("掩码1", "掩码2", "掩码3", "掩码4")
        vals["_gateway_display"] = _join_ip("网关1", "网关2", "网关3", "网关4")

        # 拼接设备名称 (ASCII)
        name_chars = []
        for i in range(15):
            raw_val = int(vals.get(f"名称位置{i * 2}", {}).get("value", 0))
            if raw_val == 0:
                break
            lo = raw_val & 0xFF
            hi = (raw_val >> 8) & 0xFF
            if lo > 0:
                name_chars.append(chr(lo) if 32 <= lo < 127 else "")
            if hi > 0:
                name_chars.append(chr(hi) if 32 <= hi < 127 else "")
        vals["_name_display"] = "".join(name_chars).strip()
        return vals

    def write_coil(self, addr, state):
        """同步写入线圈"""
        if not self.client or not self.client.connected:
            return False, "PLC未连接"
        try:
            r = self.client.write_coil(addr, bool(state), device_id=1)
            if r.isError():
                return False, str(r)
            self.y_states = self.read_y_outputs() or self.y_states
            return True, "OK"
        except Exception as e:
            return False, str(e)

    def _validate_data(self, sys_vals):
        """校验数据合理性"""
        warnings = []
        all_zero = all(v == 0 for v in sys_vals.values()) if sys_vals else True
        if all_zero:
            warnings.append("所有传感器读数为0，PLC可能已断开")
        # 环境温度传感器未接，不采集不入库
        if "环境温度" in sys_vals:
            sys_vals.pop("环境温度", None)
        # 数字低通滤波
        for k in ("精滤油温", "极化油温"):
            if k in sys_vals:
                sys_vals[k] = self._smooth_temp(k, sys_vals[k])
        return warnings

    def _smooth_temp(self, key, val):
        """EMA平滑 + 尖峰剔除"""
        prev = self._temp_cache.get(key, val)
        # 如果缓存值是异常值（≤0）而当前值正常（>0），信任当前值
        if prev <= 0 and val > 0:
            self._temp_cache[key] = val
            return val
        if abs(val - prev) > 20:
            return prev
        smoothed = 0.3 * val + 0.7 * prev
        self._temp_cache[key] = smoothed
        return smoothed

    def evaluate_alarms(self, sys_vals, oil_vals):
        """告警：读取 PLC 内部 D77-D80 报警位"""
        for key, name, level, desc in config.ALARM_RULES:
            val = sys_vals.get(key, 0) or 0
            if val > 0:
                if not database.check_alarm_recent(name, self.device_id):
                    database.create_alarm(self.device_id, name, level, val, 1)


# ===== 采集主逻辑（同步，在线程池中执行）=====

def _poll_single_device(w: DeviceWorker):
    """采集单台设备（同步函数，在 ThreadPoolExecutor 中执行）
    返回: True=成功, False=失败"""
    try:
        db = database.get_db_for_polling(w.device_id)
        if not w._ensure_connected():
            was_connected = w.connected
            w.connected = False
            w.error = "PLC未连接"
            w._consecutive_failures += 1
            if was_connected:
                # 从已连接变为离线：打印一次
                print(f"  [离线] [{w.name}] PLC未连接，正在重连中...")
            # 定期关闭空闲连接
            w._close_if_idle()
            return False

        was_connected = w.connected
        w.connected = True
        w.error = ""
        if not was_connected:
            print(f"  [在线] [{w.name}] 已恢复连接")

        # 顺序读取四类数据（同步客户端不支持并发，且 PLC 一般也串行处理）
        sys_vals = w.read_system_params()
        oil_vals = w.read_oil_quality()
        x_states = w.read_x_inputs()
        y_states = w.read_y_outputs()

        w.x_states = x_states or {}
        w.y_states = y_states or {}

        if not sys_vals:
            w._consecutive_failures += 1
            return False

        warnings = w._validate_data(sys_vals)
        all_zero = any("所有传感器读数为0" in warn for warn in warnings)

        if warnings and w.count % 30 == 0:
            for warn in warnings:
                print(f"  [!] [{w.name}] 数据校验: {warn}")

        if all_zero:
            w._consecutive_failures += 1
            return False

        if not database.save_data_fast(db, w.device_id, sys_vals, oil_vals, w.x_states, w.y_states):
            # 写入失败，本次数据丢失但不影响后续采集
            w._consecutive_failures += 1
            return False
        w.count += 1
        w._consecutive_failures = 0
        w._last_success_time = time.time()
        w.evaluate_alarms(sys_vals, oil_vals)

        if w.count == 1 or w.count % 60 == 0:
            r = database.calculate_degradation(oil_vals, w.device_id)
            if r["level"] != "no_data":
                database.save_degradation(w.device_id, r)
            try:
                w.plc_settings = w.read_plc_settings()
            except Exception:
                pass

        if w.count == 1:
            raw_vis = oil_vals.get('运动粘度', 0) or 0
            print(f"  [OK] [{w.name}] 首次采集 | 油温={sys_vals.get('精滤油温', 0):.1f}℃  粘度={raw_vis:.1f}  真空={sys_vals.get('真空度', 0):.0f}Pa")
        elif w.count % 1800 == 0:
            print(f"  [OK] [{w.name}] 累计采集{w.count}次 | PLC运行正常")

        return True

    except Exception as e:
        if w.connected:
            print(f"  [X] [{w.name}] 采集异常: {e}")
        w.connected = False
        w.error = str(e)[:100]
        w._consecutive_failures += 1
        try:
            if w.client:
                w.client.close()
        except Exception:
            pass
        w.client = None
        w._schedule_retry()
        return False


class Collector:
    """异步调度管理器：asyncio 协程调度 + 同步 Modbus 在线程池执行
    v2.1: 分批调度避免网络拥塞 + 连接空闲回收 + 健康检查"""

    def __init__(self):
        self.devices = {}           # {device_id: DeviceWorker}
        self.running = False
        self._task = None           # 主调度循环 asyncio Task
        self._optimizer_task = None
        self._health_task = None    # 健康检查任务
        self._semaphore = None      # 并发控制
        self._batch_size = MAX_CONCURRENT  # 每批次最大设备数

    def start(self):
        """在 asyncio event loop 中启动调度"""
        self.running = True
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        devices = database.list_devices()
        active_count = 0
        for dev in devices:
            if dev["status"] == "active":
                w = DeviceWorker(dev["id"], dev["name"], dev["plc_ip"], dev["plc_port"])
                w.running = True
                self.devices[dev["id"]] = w
                active_count += 1
                if active_count <= 5:  # 只打印前5台，1000台全部打印太冗长
                    print(f"  [采集] {dev['name']}({dev['id']}) → {dev['plc_ip']}:{dev['plc_port']}")
        if active_count > 5:
            print(f"  [采集] ... 还有 {active_count - 5} 台设备，共 {active_count} 台")

        # 在已有的 event loop 中创建任务
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._scheduler_loop())
        self._optimizer_task = loop.create_task(self._auto_optimizer_loop())
        self._health_task = loop.create_task(self._health_check_loop())
        print(f"  [采集器] asyncio调度+同步Modbus, 最大并发={MAX_CONCURRENT}, "
              f"Modbus线程池={MODBUS_POOL_SIZE}, 管理 {len(self.devices)} 台设备")

    def stop(self):
        """停止采集"""
        self.running = False
        for w in self.devices.values():
            w.running = False
        for t in (self._task, self._optimizer_task, self._health_task):
            if t:
                t.cancel()
        self._task = None
        self._optimizer_task = None
        self._health_task = None
        # 关闭所有 Modbus 连接
        for w in self.devices.values():
            if w.client:
                try:
                    w.client.close()
                except Exception:
                    pass
        database.close_persistent_connections()
        _modbus_executor.shutdown(wait=False)

    async def _scheduler_loop(self):
        """异步分批调度循环：将设备分批次采集，避免瞬间网络拥塞"""
        # 提取到循环外避免重复创建闭包
        async def _poll_with_limit(w):
            async with self._semaphore:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(_modbus_executor, _poll_single_device, w)

        while self.running:
            cycle_start = time.time()

            # 收集所有活跃设备
            all_devices = [w for w in self.devices.values() if w.running]
            if not all_devices:
                await asyncio.sleep(1)
                continue

            # 分批采集：每批最多 MAX_CONCURRENT 台，批次间间隔 BATCH_INTERVAL 秒
            total_devices = len(all_devices)
            total_batches = math.ceil(total_devices / self._batch_size)

            success_count = 0
            fail_count = 0

            for batch_idx in range(total_batches):
                batch_start = batch_idx * self._batch_size
                batch = all_devices[batch_start:batch_start + self._batch_size]

                # 创建 tasks 并立即用 asyncio.ensure_future 包装，
                # 确保所有 coroutine 都被调度，避免 "was never awaited" 警告
                tasks = [asyncio.ensure_future(_poll_with_limit(w)) for w in batch]

                try:
                    done, pending = await asyncio.wait(
                        tasks, timeout=READ_INTERVAL * 2
                    )
                    for t in done:
                        try:
                            r = t.result()
                            if r is True:
                                success_count += 1
                            elif r is False:
                                fail_count += 1
                        except Exception:
                            fail_count += 1
                    # 取消超时未完成的 pending tasks，释放资源
                    for t in pending:
                        t.cancel()
                    fail_count += len(pending)
                except Exception:
                    fail_count += len(batch)

                # 批次间短暂间隔（最后一批不需要）
                if batch_idx < total_batches - 1:
                    await asyncio.sleep(BATCH_INTERVAL)

            # 周期日志
            cycle_elapsed = time.time() - cycle_start
            if success_count > 0 or fail_count > 0:
                # 每30个周期（约2.5分钟）打印一次汇总
                pass  # 避免日志刷屏，详细状态通过健康检查任务输出

            elapsed = time.time() - cycle_start
            await asyncio.sleep(max(0.5, READ_INTERVAL - elapsed))

    async def _health_check_loop(self):
        """健康检查：状态变化时输出，否则每10分钟汇总一次"""
        await asyncio.sleep(15)  # 启动后等15秒
        while self.running:
            try:
                total = len(self.devices)
                connected = sum(1 for w in self.devices.values() if w.connected)
                offline = total - connected
                problematic = [
                    w for w in self.devices.values()
                    if not w.connected and w._consecutive_failures >= 1
                ]
                stale = [
                    w for w in self.devices.values()
                    if w.connected and time.time() - w._last_success_time > READ_INTERVAL * 3
                ]

                # 数据停滞设备日志
                if stale:
                    stale_names = ", ".join(w.name for w in stale[:5])
                    more = f" ...还有{len(stale)-5}台" if len(stale) > 5 else ""
                    print(f"  [健康检查] 数据停滞设备({len(stale)}台): {stale_names}{more}")

                # 对持续离线超过5分钟的生成告警
                for pw in problematic:
                    if pw._consecutive_failures >= 60:  # ~5分钟无数据
                        if not database.check_alarm_recent("PLC通信中断", pw.device_id, minutes=10):
                            database.create_alarm(
                                pw.device_id, "PLC通信中断", "danger",
                                pw._consecutive_failures,
                                READ_INTERVAL * pw._consecutive_failures
                            )

            except Exception as e:
                print(f"  [健康检查] 异常: {e}")
            await asyncio.sleep(60)

    async def _auto_optimizer_loop(self):
        """异步自动优化：每10分钟检查退化率更新"""
        await asyncio.sleep(30)  # 启动后等30秒
        while self.running:
            try:
                pending = self.check_optimize_needed()
                for item in pending[:20]:
                    result = self.run_optimize(item["did"])
                    rates = result.get("degradation_rates", {})
                    n_rates = len(rates) if rates else 0
                    print(f"  [算法] {item['did']} {item.get('msg', '')} 完成（{n_rates}参数更新退化率）")
                if len(pending) > 20:
                    print(f"  [算法] 本轮跳过 {len(pending) - 20} 台设备，下次继续")
            except Exception as e:
                print(f"  [警告] 自动优化异常: {e}")
                traceback.print_exc()
            await asyncio.sleep(600)

    # ---- 同步兼容接口（供 Flask 线程调用）----

    def status(self):
        return {did: {
            "connected": w.connected,
            "count": w.count,
            "name": w.name,
            "consecutive_failures": w._consecutive_failures,
        } for did, w in self.devices.items()}

    def add_device(self, device_id, name, ip, port):
        if device_id in self.devices:
            self.devices[device_id].running = False
        w = DeviceWorker(device_id, name, ip, port)
        w.running = True
        self.devices[device_id] = w

    def remove_device(self, device_id):
        if device_id in self.devices:
            self.devices[device_id].running = False
            w = self.devices.pop(device_id)
            if w.client:
                try:
                    w.client.close()
                except Exception:
                    pass

    def mark_oil_change(self, device_id):
        if device_id in self.devices:
            w = self.devices[device_id]
            w._oil_change_confirmed = True
            w._oil_change_time = datetime.now()
            w._optimize_at_count = 0
            database.log_operation("system", "oil_change_manual", device_id, "手动换油重置")

    def write_coil(self, device_id, addr, state):
        """同步写线圈（Flask 线程直接调用同步方法）"""
        if device_id not in self.devices:
            return False, "设备不存在"
        w = self.devices[device_id]
        return w.write_coil(addr, state)

    def status_by_device(self, device_id):
        if device_id in self.devices:
            w = self.devices[device_id]
            return {
                "connected": w.connected, "count": w.count, "error": w.error,
                "plc": f"{w.ip}:{w.port}", "x_states": w.x_states,
                "y_states": w.y_states, "plc_settings": w.plc_settings,
                "last_connect": w.last_connect, "last_disconnect": w.last_disconnect,
                "consecutive_failures": w._consecutive_failures,
            }
        return {"connected": False, "error": "设备未配置"}

    def check_optimize_needed(self):
        """检查是否需要更新退化率"""
        pending = []
        now = datetime.now()
        for did, w in self.devices.items():
            cal = database.get_calibration(did)
            if not cal:
                continue
            last_drift = cal.get("drift_updated_at")
            if not last_drift:
                last_drift = cal.get("calibrated_at")
            if not last_drift:
                continue
            try:
                last = datetime.strptime(last_drift, "%Y-%m-%d %H:%M:%S")
                if (now - last).days >= 7:
                    pending.append({
                        "did": did, "total": 0, "type": "stage2",
                        "msg": f"周度重训（上次: {last.strftime('%m-%d')}）"
                    })
            except ValueError:
                pass
        return pending

    def run_optimize(self, device_id):
        """执行优化"""
        import calibration as cal_mod
        result = cal_mod.update_algorithm(device_id)
        if device_id in self.devices:
            cnt = database.get_history_count(device_id=device_id, hours=8760)
            self.devices[device_id]._optimize_at_count = cnt
            self.devices[device_id]._oil_change_time = None
        return result
