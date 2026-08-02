# ============================================================
# 数据库模块 - SQLite（含数据归档策略）
# ============================================================
import sqlite3, hashlib, os, sys, config, math, json, time, threading as _th
from datetime import datetime, timedelta

DB = config.DATABASE_FILE  # 已基于 _DATA_DIR 的绝对路径

# 数据文件目录：data/ 子目录（与系统库文件区分）
_DATA_DIR = os.path.join(os.path.dirname(config.DATABASE_FILE), "data")

# 全局盐：用于 PBKDF2 的额外加盐层，每个密码另有独立盐值
SALT = os.environ.get("OIL_MONITOR_SALT", "oil_monitor_wind_2026")




def _hash(pw):
    """PBKDF2 对每个密码独立加盐，防彩虹表"""
    per_user_salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac('sha256', (SALT + pw).encode(), per_user_salt, 100000)
    return per_user_salt.hex() + ":" + dk.hex()


# ===== 系统库连接池（oil_monitor.db，被所有API共享）=====
# 1000台设备时系统库的写入竞争是瓶颈，使用连接池+WAL优化
_sys_db_pool = []
_sys_db_lock = _th.Lock()
_SYS_POOL_SIZE = int(os.environ.get("OIL_MONITOR_SYS_DB_POOL", "16"))
_SYS_POOL_MAX = int(os.environ.get("OIL_MONITOR_SYS_DB_POOL_MAX", "32"))

# 系统库连接池包装器：让 db.close() 自动归还到池而非真正关闭
class _PooledConnection:
    """包装 sqlite3.Connection，拦截 close() 以归还到连接池"""
    def __init__(self, raw_conn):
        self._raw = raw_conn
        self._closed = False

    def __getattr__(self, name):
        # 代理所有属性到原始连接
        return getattr(self._raw, name)

    def close(self):
        """归还连接到池而非真正关闭"""
        if self._closed:
            return
        self._closed = True
        with _sys_db_lock:
            if len(_sys_db_pool) < _SYS_POOL_MAX:
                try:
                    self._raw.rollback()
                    _sys_db_pool.append(self._raw)
                except sqlite3.Error:
                    try:
                        self._raw.close()
                    except Exception:
                        pass
            else:
                try:
                    self._raw.close()
                except Exception:
                    pass

    def _really_close(self):
        """强制真正关闭连接"""
        try:
            self._raw.close()
        except Exception:
            pass


def connect(_depth=0):
    """从连接池获取系统库连接（复用连接，减少文件打开/关闭开销）
    返回包装对象，.close() 会自动归还到池"""
    # 防止递归过深（池中所有连接都损坏时的极端情况）
    if _depth > 5:
        raise RuntimeError("数据库连接池全部损坏，无法获取有效连接")
    with _sys_db_lock:
        if _sys_db_pool:
            raw = _sys_db_pool.pop()
            # 检查连接是否还活着
            try:
                raw.execute("SELECT 1")
            except sqlite3.Error:
                try:
                    raw.close()
                except Exception:
                    pass
                return connect(_depth + 1)
            return _PooledConnection(raw)
    # 池为空，创建新连接
    raw = sqlite3.connect(DB, check_same_thread=False)
    raw.row_factory = sqlite3.Row
    raw.execute("PRAGMA journal_mode=WAL")
    raw.execute("PRAGMA synchronous=NORMAL")
    raw.execute("PRAGMA foreign_keys=ON")
    raw.execute("PRAGMA busy_timeout=10000")   # 10秒忙等待（1000台设备并发写）
    raw.execute("PRAGMA cache_size=-8000")      # 8MB缓存
    raw.execute("PRAGMA mmap_size=33554432")    # 32MB内存映射（加速读）
    raw.execute("PRAGMA wal_autocheckpoint=1000")  # WAL文件1000页自动checkpoint
    return _PooledConnection(raw)


def _close_sys_pool():
    """关闭系统库连接池中所有连接"""
    with _sys_db_lock:
        for raw in _sys_db_pool:
            try:
                raw.close()
            except Exception:
                pass
        _sys_db_pool.clear()


# ===== 持久化连接池（高频采集复用连接，避免每次开关文件）=====
# 1000台设备优化：使用 LRU 策略，每台设备需要 1 个持久连接
# 默认 1100 确保1000台设备都有连接可用（含余量），超出时淘汰最久未使用
# 若实际设备数 > 默认值，通过环境变量 OIL_MONITOR_MAX_DB_CONNS 调高
_persistent_dbs = {}
_persistent_lock = _th.Lock()
_MAX_PERSISTENT_CONNS = int(os.environ.get("OIL_MONITOR_MAX_DB_CONNS", "1100"))
# 记录每个连接的最后使用时间
_persistent_last_use = {}


def get_db_for_polling(device_id):
    """获取或创建持久化 SQLite 连接（带 LRU 淘汰，防止文件句柄耗尽）"""
    with _persistent_lock:
        now = time.time()
        if device_id in _persistent_dbs:
            _persistent_last_use[device_id] = now
            return _persistent_dbs[device_id]

        # 连接数超限时，淘汰最久未使用的连接
        if len(_persistent_dbs) >= _MAX_PERSISTENT_CONNS:
            oldest_did = min(_persistent_last_use, key=_persistent_last_use.get)
            try:
                _persistent_dbs[oldest_did].close()
            except Exception:
                pass
            del _persistent_dbs[oldest_did]
            del _persistent_last_use[oldest_did]

        _persistent_dbs[device_id] = connect_data(device_id)
        _persistent_last_use[device_id] = now
        return _persistent_dbs[device_id]


def close_persistent_connections():
    """关闭所有持久化连接"""
    with _persistent_lock:
        for did, db in _persistent_dbs.items():
            try:
                db.close()
            except Exception:
                pass
        _persistent_dbs.clear()
        _persistent_last_use.clear()


def connect_data(device_id=None):
    """传感器数据独立存储，按设备分文件防锁定和性能瓶颈"""
    os.makedirs(_DATA_DIR, exist_ok=True)
    db_file = os.path.join(_DATA_DIR, f"oil_data_{device_id}.db") if device_id else os.path.join(_DATA_DIR, "oil_data.db")
    conn = sqlite3.connect(db_file, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA cache_size=-4000")   # 4MB缓存（每个设备独立DB，无需太大）
    conn.execute("PRAGMA wal_autocheckpoint=500")
    return conn


def init():
    db = connect()

    # 用户表
    db.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        role TEXT DEFAULT 'operator',
        created_at DATETIME DEFAULT (datetime('now','localtime'))
    )""")

    # 设备表
    db.execute("""CREATE TABLE IF NOT EXISTS devices (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        plc_ip TEXT DEFAULT '192.168.0.150',
        plc_port INTEGER DEFAULT 3000,
        status TEXT DEFAULT 'active',
        created_at DATETIME DEFAULT (datetime('now','localtime'))
    )""")

    # 数据表 - 系统参数列
    sys = ", ".join(f"\"{key}\" REAL" for addr, key, scale, unit, desc in config.SYSTEM_PARAMS)
    # 数据表 - 油品列
    oil = ", ".join(f"oil_{str(i).zfill(2)} REAL" for i in range(len(config.OIL_QUALITY)))
    # 数据表 - X输入列
    x_cols = ", ".join(f"x_{i} INTEGER DEFAULT 0" for i in range(len(config.X_INPUTS)))
    # 数据表 - Y输出列
    y_cols = ", ".join(f"y_{i} INTEGER DEFAULT 0" for i in range(len(config.Y_OUTPUTS)))

    sql = f"""CREATE TABLE IF NOT EXISTS data_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT DEFAULT 'D001',
        timestamp DATETIME DEFAULT (datetime('now','localtime')),
        {sys}, {oil}, {x_cols}, {y_cols}
    )"""
    # 归档表（结构相同）
    sql_archive = sql.replace("data_log", "data_log_archive")
    degradation_sql = """CREATE TABLE IF NOT EXISTS degradation_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT DEFAULT 'D001',
        timestamp DATETIME DEFAULT (datetime('now','localtime')),
        hi_score REAL, rul_hours REAL, vis_score REAL, water_score REAL,
        metal_score REAL, dielec_score REAL, clean_score REAL,
        level TEXT DEFAULT 'normal'
    )"""

    # 告警记录表
    db.execute("""CREATE TABLE IF NOT EXISTS alarms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT DEFAULT 'D001',
        timestamp DATETIME DEFAULT (datetime('now','localtime')),
        alarm_name TEXT NOT NULL,
        level TEXT DEFAULT 'danger',
        value REAL, threshold REAL, desc TEXT,
        status TEXT DEFAULT 'active',
        handler TEXT, note TEXT,
        resolved_at DATETIME
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_alarm_time ON alarms(timestamp DESC)")

    # 工单表
    db.execute("""CREATE TABLE IF NOT EXISTS work_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL, content TEXT,
        device_id TEXT DEFAULT 'D001',
        status TEXT DEFAULT 'pending',
        priority TEXT DEFAULT 'normal',
        creator TEXT, assignee TEXT, due_date TEXT,
        created_at DATETIME DEFAULT (datetime('now','localtime')),
        resolved_at DATETIME
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_wo_status ON work_orders(status)")

    # 操作日志表
    db.execute("""CREATE TABLE IF NOT EXISTS operation_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT (datetime('now','localtime')),
        operator TEXT, action TEXT,
        device_id TEXT, detail TEXT
    )""")

    # 校准参数表
    db.execute("""CREATE TABLE IF NOT EXISTS calibration (
        device_id TEXT PRIMARY KEY,
        baselines TEXT, drift TEXT, thresholds TEXT,
        calibrated_at TEXT, updated_at TEXT,
        pipeline TEXT, status TEXT DEFAULT 'uncalibrated',
        calibration_start TEXT, drift_updated_at TEXT
    )""")
    # 向前兼容：旧表可能没有新列
    for col_def in [
        "ALTER TABLE calibration ADD COLUMN thresholds TEXT",
        "ALTER TABLE calibration ADD COLUMN calibration_start TEXT",
        "ALTER TABLE calibration ADD COLUMN drift_updated_at TEXT",
    ]:
        try:
            db.execute(col_def)
        except sqlite3.OperationalError:
            pass  # 列已存在

    db.commit()
    db.close()
    _create_defaults()
    # _create_defaults 可能创建了新设备，之后初始化各设备的数据文件
    _init_device_data_files(sql, sql_archive, degradation_sql)


def _init_device_data_files(sql_dl, sql_archive, sql_dg):
    """为每个已存在的设备创建独立数据文件（含归档表）
    1000台设备优化：逐个处理并立即关闭，避免同时打开大量文件句柄"""
    db = connect()
    devices = db.execute("SELECT id FROM devices").fetchall()
    db.close()
    count = 0
    for dev in devices:
        did = dev["id"]
        db_data = None
        try:
            db_data = connect_data(did)
            db_data.execute(sql_dl)
            db_data.execute("CREATE INDEX IF NOT EXISTS idx_time ON data_log(timestamp DESC)")
            db_data.execute("CREATE INDEX IF NOT EXISTS idx_time_device ON data_log(device_id, timestamp DESC)")
            db_data.execute(sql_archive)
            db_data.execute("CREATE INDEX IF NOT EXISTS idx_archive_time ON data_log_archive(timestamp DESC)")
            db_data.execute("CREATE INDEX IF NOT EXISTS idx_archive_time_device ON data_log_archive(device_id, timestamp DESC)")
            db_data.execute(sql_dg)
            db_data.commit()
            count += 1
        except Exception as e:
            print(f"  [数据库] 初始化设备 {did} 数据文件失败: {e}")
        finally:
            if db_data:
                try:
                    db_data.close()
                except Exception:
                    pass
    if count > 0:
        print(f"  [数据库] 已初始化 {count} 台设备数据文件")


def _create_defaults():
    db = connect()
    if not db.execute("SELECT id FROM users WHERE username=?", (config.ADMIN_USER,)).fetchone():
        if not config.ADMIN_PASS:
            print("=" * 60)
            print("  [警告] 管理员密码未配置，未创建管理员账户！")
            print("  请设置系统环境变量 OIL_MONITOR_ADMIN_PASS（加密值）")
            print("=" * 60)
            db.close()
            return
        db.execute("INSERT INTO users (username,password,role) VALUES (?,?,?)",
                   (config.ADMIN_USER, _hash(str(config.ADMIN_PASS)), "admin"))
    if not db.execute("SELECT id FROM devices WHERE id='D001'").fetchone():
        db.execute("INSERT INTO devices (id,name,plc_ip,plc_port) VALUES ('D001','1号风机滤油机','192.168.0.150',3000)")
    db.commit()
    db.close()


# ===== 数据归档 =====
# 注：当前系统仅支持手动清理数据（换油重置），不启用自动归档
# archive_all_devices 函数已移除，如需恢复归档功能请重新实现


# ===== 换油周期管理 =====
def reset_oil_cycle(device_id):
    """换油重置：清除该设备所有旧数据，触发新周期"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 1. 导出最后一条记录到 operation_log
    db = connect_data(device_id)
    last = db.execute("SELECT * FROM data_log ORDER BY id DESC LIMIT 1").fetchone()
    total_active = db.execute("SELECT COUNT(*) FROM data_log").fetchone()[0]
    total_archived = db.execute("SELECT COUNT(*) FROM data_log_archive").fetchone()[0]
    if last:
        log_operation("system", "oil_cycle_end", device_id,
                      f"换油前最后一条: {last['timestamp']} (主表 {total_active} 条 + 归档 {total_archived} 条)")
    deleted = total_active + total_archived
    db.execute("DELETE FROM data_log")
    db.execute("DELETE FROM data_log_archive")
    db.commit()
    # VACUUM 需要独占锁，高频采集时可能冲突。使用 busy_timeout 等待，
    # 若仍失败则跳过（WAL 模式下 DELETE 后空间会被自动复用）
    try:
        db.execute("PRAGMA busy_timeout=30000")  # VACUUM 前设置30秒忙等待
        db.execute("VACUUM")
    except Exception as e:
        print(f"  [数据库] VACUUM data_log 跳过（采集进行中，空间将自动回收）: {e}")
    db.close()

    # 2. 清除劣化历史
    db = connect_data(device_id)
    db.execute("DELETE FROM degradation_log")
    db.commit()
    try:
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("VACUUM")
    except Exception as e:
        print(f"  [数据库] VACUUM degradation_log 跳过: {e}")
    db.close()

    # 3. 标记校准为新周期待启动
    db = connect()
    cur = db.execute(
        "UPDATE calibration SET status='new_cycle_pending', baselines='{}', drift='{}', updated_at=? WHERE device_id=?",
        (now, device_id))
    if cur.rowcount == 0:
        db.execute(
            "INSERT INTO calibration (device_id, baselines, drift, thresholds, pipeline, calibrated_at, updated_at, status) VALUES (?,?,?,?,?,?,?,?)",
            (device_id, '{}', '{}', '{}', '[]', now, now, 'new_cycle_pending'))
    db.commit()
    db.close()

    # 4. 清空 EMA 缓存
    global _ema_cache
    if device_id in _ema_cache:
        del _ema_cache[device_id]

    # 5. 清空告警缓存
    global _alarm_cache
    if device_id in _alarm_cache:
        del _alarm_cache[device_id]

    return deleted


def get_data_stats(device_id):
    """获取数据统计（含主表+归档表）"""
    db = connect_data(device_id)
    cnt = db.execute("SELECT COUNT(*) FROM data_log").fetchone()[0]
    cnt_archive = db.execute("SELECT COUNT(*) FROM data_log_archive").fetchone()[0]
    db.close()
    data_file = os.path.join(_DATA_DIR, f"oil_data_{device_id}.db")
    sz = os.path.getsize(data_file) if os.path.exists(data_file) else 0
    return {"count": cnt + cnt_archive, "count_active": cnt,
            "count_archived": cnt_archive, "size_mb": round(sz / (1024 * 1024), 1)}


# ===== 用户 =====
def verify_user(u, p):
    db = connect()
    r = db.execute("SELECT * FROM users WHERE username=?", (u,)).fetchone()
    db.close()
    if not r:
        return None
    stored = r["password"]
    # 新格式: hex_salt:hex_hash，旧格式: sha256
    if ":" in stored:
        try:
            per_salt = bytes.fromhex(stored.split(":")[0])
            dk = hashlib.pbkdf2_hmac('sha256', (SALT + p).encode(), per_salt, 100000)
            if stored == per_salt.hex() + ":" + dk.hex():
                r = dict(r)
                # 检查 salt 长度是否与当前 _hash() 一致（16字节=32hex字符）
                # 若不一致（如未来修改了 os.urandom 参数），自动升级到新格式
                stored_salt = stored.split(":")[0]
                if len(stored_salt) != 32:  # 16字节 hex = 32字符
                    db2 = connect()
                    db2.execute("UPDATE users SET password=? WHERE username=?", (_hash(p), u))
                    db2.commit()
                    db2.close()
                return r
        except (ValueError, IndexError):
            pass
        return None
    # 兼容旧SHA256格式
    if stored == hashlib.sha256((SALT + p).encode()).hexdigest():
        db2 = connect()
        db2.execute("UPDATE users SET password=? WHERE username=?", (_hash(p), u))
        db2.commit()
        db2.close()
        return dict(r)
    return None


def list_users():
    db = connect()
    rows = db.execute("SELECT id, username, role, created_at FROM users ORDER BY id").fetchall()
    db.close()
    return [dict(r) for r in rows]


def add_user(u, p, role="operator"):
    db = connect()
    try:
        db.execute("INSERT INTO users (username,password,role) VALUES (?,?,?)", (u, _hash(p), role))
        db.commit()
        db.close()
        return True
    except sqlite3.IntegrityError:
        db.close()
        return False


def update_user(uid, **kw):
    db = connect()
    if kw.get("password"):
        kw["password"] = _hash(kw["password"])
    sets = ", ".join(f"{k}=?" for k in kw)
    db.execute(f"UPDATE users SET {sets} WHERE id=?", list(kw.values()) + [uid])
    db.commit()
    db.close()


def delete_user(uid):
    db = connect()
    db.execute("DELETE FROM users WHERE id=? AND username!=?", (uid, config.ADMIN_USER))
    db.commit()
    db.close()


def count_admins():
    db = connect()
    cnt = db.execute("SELECT COUNT(*) FROM users WHERE role='admin'").fetchone()[0]
    db.close()
    return cnt


def is_admin(uid):
    db = connect()
    r = db.execute("SELECT role FROM users WHERE id=?", (uid,)).fetchone()
    db.close()
    return r and r["role"] == "admin"


# ===== 设备 =====
def list_devices():
    db = connect()
    rows = db.execute("SELECT * FROM devices ORDER BY id").fetchall()
    db.close()
    return [dict(r) for r in rows]


def add_device(did, name, ip, port):
    db = connect()
    try:
        db.execute("INSERT INTO devices VALUES (?,?,?,?,'active',datetime('now','localtime'))",
                   (did, name, ip, port))
        db.commit()
        db.close()
        return True
    except sqlite3.IntegrityError:
        db.close()
        return False


def delete_device(did):
    db = connect()
    db.execute("DELETE FROM devices WHERE id=?", (did,))
    db.commit()
    db.close()


# ===== 数据 =====
# 写入失败日志计数器，避免重复日志刷屏
_save_fail_log = {}
_save_fail_lock = _th.Lock()

def save_data_fast(db, device_id, sys_vals, oil_vals, x_vals, y_vals):
    """高频写入：使用持久化连接，不开关文件。带重试机制防止数据静默丢失"""
    sys_keys = [key for addr, key, scale, unit, desc in config.SYSTEM_PARAMS]
    sys_vals_list = [sys_vals.get(k) for k in sys_keys]

    oil_keys = [f"oil_{str(i).zfill(2)}" for i in range(len(config.OIL_QUALITY))]
    oil_vals_list = [oil_vals.get(config.OIL_QUALITY[i][1], 0) for i in range(len(config.OIL_QUALITY))]

    x_keys = [f"x_{i}" for i in range(len(config.X_INPUTS))]
    x_vals_list = [x_vals.get(config.X_INPUTS[i][1], 0) for i in range(len(config.X_INPUTS))]

    y_keys = [f"y_{i}" for i in range(len(config.Y_OUTPUTS))]
    y_vals_list = [y_vals.get(config.Y_OUTPUTS[i][1], 0) for i in range(len(config.Y_OUTPUTS))]

    all_keys = ["device_id"] + sys_keys + oil_keys + x_keys + y_keys
    all_vals = [device_id] + sys_vals_list + oil_vals_list + x_vals_list + y_vals_list

    sql = f"INSERT INTO data_log ({', '.join(all_keys)}) VALUES ({', '.join('?' * len(all_keys))})"

    max_retries = 3
    for attempt in range(max_retries):
        try:
            db.execute(sql, all_vals)
            db.commit()
            # 成功写入，清除失败计数
            with _save_fail_lock:
                _save_fail_log.pop(device_id, None)
            return True
        except sqlite3.OperationalError as e:
            err_msg = str(e)
            # SQLITE_BUSY: 数据库被锁，短暂等待后重试
            if "database is locked" in err_msg.lower() or "busy" in err_msg.lower():
                if attempt < max_retries - 1:
                    import time as _t
                    _t.sleep(0.1 * (attempt + 1))  # 逐步增加等待时间
                    continue
            # 其他错误或重试耗尽
            try:
                db.rollback()
            except Exception:
                pass
            with _save_fail_lock:
                prev = _save_fail_log.get(device_id, 0)
                _save_fail_log[device_id] = prev + 1
                # 每60次失败打印一次，避免日志刷屏
                if _save_fail_log[device_id] % 60 == 0:
                    print(f"  [数据库错误] [{device_id}] 写入失败（累计{_save_fail_log[device_id]}次）: {err_msg[:120]}")
            return False
        except Exception as e:
            try:
                db.rollback()
            except Exception:
                pass
            with _save_fail_lock:
                prev = _save_fail_log.get(device_id, 0)
                _save_fail_log[device_id] = prev + 1
                if _save_fail_log[device_id] % 60 == 0:
                    print(f"  [数据库错误] [{device_id}] 写入失败（累计{_save_fail_log[device_id]}次）: {str(e)[:120]}")
            return False


def get_latest(device_id=None):
    db = connect_data(device_id or "D001")
    r = db.execute("SELECT * FROM data_log ORDER BY id DESC LIMIT 1").fetchone()
    db.close()
    return dict(r) if r else None


# 历史查询最大返回条数（安全上限，内部算法调用可通过 _internal=True 绕过）
_MAX_HISTORY_LIMIT = 50000
# Web API 上限（更严格，防止恶意查询）
_WEB_HISTORY_LIMIT = 5000


def get_history(start=None, end=None, hours=None, device_id=None, limit=5000, offset=0, fields=None, order="DESC", _internal=False):
    """查询历史数据，自动合并主表和归档表
       _internal=True: 内部算法调用，上限放宽到 50000"""
    device_id = device_id or "D001"
    # 安全限制：Web API 5000，内部算法 50000
    cap = _MAX_HISTORY_LIMIT if _internal else _WEB_HISTORY_LIMIT
    limit = min(limit, cap)
    if limit <= 0:
        limit = 500
    order_upper = order.upper()
    if order_upper not in ("ASC", "DESC"):
        order_upper = "DESC"

    cols = ", ".join(f'"{c}"' for c in fields) if fields else "*"

    conds, params = [], []
    if start:
        conds.append("timestamp>=?")
        params.append(start)
    if end:
        conds.append("timestamp<=?")
        params.append(end)
    if not start and not end:
        if hours:
            conds.append("timestamp>=datetime('now','localtime',?)")
            params.append(f'-{hours} hours')
        else:
            conds.append("timestamp>=datetime('now','localtime','-24 hours')")

    w = " WHERE " + " AND ".join(conds) if conds else ""

    db = connect_data(device_id)
    sql = f"SELECT {cols} FROM data_log{w} ORDER BY timestamp {order_upper} LIMIT ? OFFSET ?"
    all_params = params + [limit, offset]

    rows = db.execute(sql, all_params).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_history_count(start=None, end=None, hours=None, device_id=None):
    """获取历史数据总条数（含归档表），用于分页"""
    device_id = device_id or "D001"

    conds, params = [], []
    if start:
        conds.append("timestamp>=?")
        params.append(start)
    if end:
        conds.append("timestamp<=?")
        params.append(end)
    if not start and not end:
        if hours:
            conds.append("timestamp>=datetime('now','localtime',?)")
            params.append(f'-{hours} hours')
        else:
            conds.append("timestamp>=datetime('now','localtime','-24 hours')")

    w = " WHERE " + " AND ".join(conds) if conds else ""

    db = connect_data(device_id)
    cnt = db.execute(f"SELECT COUNT(*) FROM data_log{w}", params).fetchone()[0]
    db.close()
    return cnt


def get_history_page(start=None, end=None, hours=None, device_id=None, limit=100, offset=0):
    """合并查询：一次连接获取 total 和 rows，避免重复开关文件"""
    device_id = device_id or "D001"
    limit = min(limit, 500)
    offset = max(offset, 0)

    conds, params = [], []
    if start:
        conds.append("timestamp>=?")
        params.append(start)
    if end:
        conds.append("timestamp<=?")
        params.append(end)
    if not start and not end:
        if hours:
            conds.append("timestamp>=datetime('now','localtime',?)")
            params.append(f'-{hours} hours')
        else:
            conds.append("timestamp>=datetime('now','localtime','-24 hours')")

    w = " WHERE " + " AND ".join(conds) if conds else ""

    db = connect_data(device_id)
    try:
        total = db.execute(f"SELECT COUNT(*) FROM data_log{w}", params).fetchone()[0]
        rows = db.execute(
            f"SELECT * FROM data_log{w} ORDER BY id DESC LIMIT ? OFFSET ?",
            params + [limit, offset]
        ).fetchall()
        rows = [dict(r) for r in rows]
        return total, rows
    finally:
        db.close()


# ===== 告警（含内存缓存，减少 DB 查询）=====
_alarm_cache = {}   # {device_id: {alarm_name: last_active_time}}
_alarm_cache_lock = _th.Lock()
_ALARM_CACHE_TTL = 60  # 缓存有效期（秒）


def list_alarms(status=None, limit=100, offset=0):
    limit = min(limit, 200)
    db = connect()
    q = "SELECT * FROM alarms"
    params = []
    if status:
        q += " WHERE status=?"
        params.append(status)
    q += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    rows = db.execute(q, params).fetchall()
    db.close()
    return [dict(r) for r in rows]


def count_alarms(status=None):
    db = connect()
    q = "SELECT COUNT(*) FROM alarms"
    params = []
    if status:
        q += " WHERE status=?"
        params.append(status)
    cnt = db.execute(q, params).fetchone()[0]
    db.close()
    return cnt


def resolve_alarm(aid, note=""):
    db = connect()
    db.execute("UPDATE alarms SET status='resolved', resolved_at=datetime('now','localtime'), note=? WHERE id=?",
               (note, aid))
    db.commit()
    db.close()


def process_alarm(aid):
    db = connect()
    db.execute("UPDATE alarms SET status='processing' WHERE id=?", (aid,))
    db.commit()
    db.close()


def check_alarm_recent(name, device_id="D001", minutes=30):
    """检查最近是否已有同类活跃告警（带内存缓存优化）"""
    now = datetime.now()

    # 先查内存缓存
    with _alarm_cache_lock:
        dev_cache = _alarm_cache.get(device_id, {})
        cached_time = dev_cache.get(name)
        if cached_time:
            try:
                if isinstance(cached_time, str):
                    cached_time = datetime.strptime(cached_time, "%Y-%m-%d %H:%M:%S")
                if (now - cached_time).total_seconds() < minutes * 60:
                    return True
            except (ValueError, TypeError):
                pass

    # 缓存未命中，查数据库
    db = connect()
    r = db.execute(
        "SELECT COUNT(*) as cnt FROM alarms WHERE alarm_name=? AND device_id=? "
        "AND status IN ('active','processing') "
        "AND timestamp>=datetime('now','localtime',?)",
        (name, device_id, f'-{minutes} minutes')).fetchone()
    db.close()

    result = r["cnt"] > 0

    # 更新缓存
    if result:
        with _alarm_cache_lock:
            _alarm_cache.setdefault(device_id, {})[name] = now.strftime("%Y-%m-%d %H:%M:%S")

    return result


def _cleanup_alarm_cache():
    """定时清理过期告警缓存"""
    while True:
        import time as _time
        _time.sleep(600)
        now = datetime.now()
        with _alarm_cache_lock:
            for did in list(_alarm_cache.keys()):
                for name in list(_alarm_cache[did].keys()):
                    cached_time = _alarm_cache[did][name]
                    try:
                        if isinstance(cached_time, str):
                            cached_time = datetime.strptime(cached_time, "%Y-%m-%d %H:%M:%S")
                        if (now - cached_time).total_seconds() > _ALARM_CACHE_TTL:
                            del _alarm_cache[did][name]
                    except (ValueError, TypeError):
                        del _alarm_cache[did][name]
                if not _alarm_cache[did]:
                    del _alarm_cache[did]


# 启动告警缓存清理线程
_th.Thread(target=_cleanup_alarm_cache, daemon=True, name="alarm-cache-cleaner").start()


def create_alarm(device_id, name, level, value, threshold):
    """创建告警记录并更新缓存"""
    db = connect()
    desc = next((dsc for _, aname, _, dsc in config.ALARM_RULES if aname == name), "")
    db.execute(
        "INSERT INTO alarms (device_id,alarm_name,level,value,threshold,desc) VALUES (?,?,?,?,?,?)",
        (device_id, name, level, value, threshold, desc))
    db.commit()
    aid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    db.close()

    # 更新缓存
    with _alarm_cache_lock:
        _alarm_cache.setdefault(device_id, {})[name] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return aid


# ===== 操作日志 =====
def log_operation(operator, action, device_id, detail=""):
    db = connect()
    db.execute("INSERT INTO operation_log (operator,action,device_id,detail) VALUES (?,?,?,?)",
               (operator, action, device_id, detail))
    db.commit()
    db.close()


# ===== 校准参数 =====
def save_calibration(device_id, data_dict):
    """存储校准结果，支持新旧两种格式"""
    db = connect()
    baselines_json = json.dumps(data_dict.get("baselines") or {}, ensure_ascii=False)
    drift_json = json.dumps(data_dict.get("degradation_rates") or data_dict.get("drift") or {}, ensure_ascii=False)
    pipeline_json = json.dumps(data_dict.get("pipeline") or [], ensure_ascii=False)
    thresholds_json = json.dumps(data_dict.get("thresholds") or {}, ensure_ascii=False)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = data_dict.get("status", "calibrated")
    calibrated_at = data_dict.get("calibrated_at", now) or now
    cal_start = data_dict.get("calibration_start", "")
    drift_updated_at = data_dict.get("drift_updated_at", "")
    db.execute(
        "INSERT OR REPLACE INTO calibration (device_id, baselines, drift, thresholds, pipeline, calibrated_at, updated_at, status, calibration_start, drift_updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (device_id, baselines_json, drift_json, thresholds_json, pipeline_json, calibrated_at, now, status, cal_start,
         drift_updated_at))
    db.commit()
    db.close()


def get_calibration(device_id="D001"):
    db = connect()
    r = db.execute("SELECT * FROM calibration WHERE device_id=?", (device_id,)).fetchone()
    db.close()
    if not r:
        return None
    d = dict(r)
    for field in ("baselines", "drift", "thresholds"):
        try:
            d[field] = json.loads(d.get(field, "{}"))
        except (json.JSONDecodeError, TypeError):
            d[field] = {}
    return d


# ===== 劣化预测（标准模型）=====

# EMA缓存：按设备隔离，跨调用保留平滑值，减少传感器瞬时噪声
_ema_cache = {}
# EMA 缓存最大设备数（每台5参数×8字节≈40字节，1500台约60KB，可接受）
_EMA_MAX_DEVICES = 1500


def _cleanup_ema_cache():
    """防止 EMA 缓存无限增长"""
    while True:
        import time as _time
        _time.sleep(3600)
        if len(_ema_cache) > _EMA_MAX_DEVICES:
            # 保留最近使用的设备
            excess = len(_ema_cache) - _EMA_MAX_DEVICES
            for key in list(_ema_cache.keys())[:excess]:
                del _ema_cache[key]


_th.Thread(target=_cleanup_ema_cache, daemon=True, name="ema-cache-cleaner").start()


def _normalize(value, new_baseline, threshold, direction='higher-is-worse'):
    """归一化公式：x' = (x - x_new) / (x_threshold - x_new)，输出[0,1]"""
    if abs(threshold - new_baseline) < 1e-10:
        return 0.0
    if direction == 'lower-is-worse':
        result = (new_baseline - value) / (new_baseline - threshold)
    else:
        result = (value - new_baseline) / (threshold - new_baseline)
    return max(0.0, min(1.0, result))


def calculate_degradation(oil_vals, device_id=None):
    """标准健康指数模型（HI 0~1，越高越好）
    参数来源：D0运动粘度、D12含水率、D58污染度、D48介电常数、D26铁磁磨粒"""
    vis = oil_vals.get("运动粘度", 0) or 0
    water = oil_vals.get("含水率", 0) or 0
    iso_code = oil_vals.get("污染度等级2", 0) or 0
    dielec = oil_vals.get("介电常数", 0) or 0
    metal = oil_vals.get("铁磁24H总数", 0) or 0

    # 至少有3个以上传感器有有效读数才计算HI，避免基于不完整数据得出错误结论
    valid_count = sum(1 for v in [vis, water, iso_code, dielec, metal] if v > 0)
    if valid_count < 3:
        if device_id in _ema_cache:
            del _ema_cache[device_id]
        return {"hi": -1, "rul_hours": -1, "level": "no_data",
                "scores": {"vis": 0, "water": 0, "pollution": 0, "dielec": 0, "metal": 0},
                "weights": {"vis": 0.30, "water": 0.25, "pollution": 0.20, "dielec": 0.15, "metal": 0.10},
                "reason": f"有效传感器不足（{valid_count}/5），需≥3个"}

    # 异常值硬过滤 & 重启恢复：超出合理范围拒绝，用EMA前值代替
    cache = _ema_cache.setdefault(device_id, {})
    if not cache:
        cal = get_calibration(device_id)
        if cal and cal.get("baselines", {}).get("values"):
            bl = cal["baselines"]["values"]
            bl_map = {
                "vis": ("oil_00", 0), "water": ("oil_06", 0),
                "dielec": ("oil_24", 0), "metal": ("oil_20", 0)
            }
            for key, (col, default) in bl_map.items():
                val = bl.get(col, {}).get("mean", 0)
                if val > 0:
                    cache[key] = val

    def _clip_outlier(key, val, lo, hi):
        prev = cache.get(key)
        if prev is None:
            return val
        if not (lo <= val <= hi):
            return prev
        return val

    vis = _clip_outlier("vis", vis, 10, 500)
    water = _clip_outlier("water", water, 0, 1000)
    iso_code = _clip_outlier("iso", iso_code, 1, 28)
    dielec = _clip_outlier("dielec", dielec, 1.5, 5.0)
    metal = _clip_outlier("metal", metal, 0, 5000)

    # EMA平滑（α=0.2）
    alpha = 0.2

    def _smooth(key, val):
        prev = cache.get(key, val)
        ema = alpha * val + (1 - alpha) * prev
        cache[key] = ema
        return ema

    vis = _smooth("vis", vis)
    water = _smooth("water", water)
    iso_code = _smooth("iso", iso_code)
    dielec = _smooth("dielec", dielec)
    metal = _smooth("metal", metal)

    # 基线和阈值（只获取一次 calibration，后续退化率复用）
    cal = get_calibration(device_id)
    baselines = cal.get("baselines", {}) if cal else {}
    bl = baselines.get("values", {}) if baselines else {}
    if bl:
        vis_new = bl.get("oil_00", {}).get("mean", 0) or 0
        water_new = bl.get("oil_06", {}).get("mean", 0) or 0
        dielec_new = bl.get("oil_24", {}).get("mean", 0) or 0
        metal_new = bl.get("oil_20", {}).get("mean", 0) or 0
    else:
        vis_new = 320
        water_new = 50
        dielec_new = 2.1
        metal_new = 20

    vis_th = vis_new * 0.92
    water_th = max(150, water_new * 2)
    dielec_th = max(dielec_new + 0.3, dielec_new * 1.3)
    metal_th = max(50, metal_new * 3)

    # 归一化
    vis_norm = _normalize(vis, vis_new, vis_th, direction='lower-is-worse')
    water_norm = _normalize(water, water_new, water_th)
    dielec_norm = _normalize(dielec, dielec_new, dielec_th)
    metal_norm = _normalize(metal, metal_new, metal_th)

    # 污染系数
    cp = 0.0
    if iso_code < 1:
        pollution_norm = 0.0
    else:
        cp = 10 ** ((iso_code - 16) / 3)
        pollution_norm = round(1.0 / (1.0 + math.exp(-0.8 * (cp - 5.5))), 4)

    # 健康指数
    w_vis, w_water, w_poll, w_dielec, w_metal = 0.30, 0.25, 0.20, 0.15, 0.10
    degradation = (w_vis * vis_norm + w_water * water_norm + w_poll * pollution_norm +
                   w_dielec * dielec_norm + w_metal * metal_norm)
    hi = round(max(0.0, min(1.0, 1.0 - degradation)), 4)

    # RUL估算（复用上面已获取的 calibration，不重复查询数据库）
    hi_current, hi_threshold = hi, 0.3
    lam_default = 0.000045
    lam_max = -math.log(1 - 0.15) / 720
    lam = lam_default

    drift = (cal.get("drift", {}) or cal.get("degradation_rates", {})) if cal else {}
    if drift:
        rul_w = {"运动粘度": 0.30, "含水率": 0.25, "污染度等级2": 0.20, "介电常数": 0.15, "铁磁24H总数": 0.10}
        total_rate, total_weight = 0.0, 0.0
        for name, w in rul_w.items():
            info = drift.get(name, {})
            if info and info.get("rate_per_hour"):
                total_rate += abs(info["rate_per_hour"]) * w
                total_weight += w
        if total_weight > 0:
            lam = total_rate / total_weight
            if lam > lam_max:
                lam = lam_default

    lam_best = lam_worst = lam

    if hi_current > hi_threshold and lam > 0:
        rul_hours = round(math.log(hi_current / hi_threshold) / lam)
        rul_min = round(math.log(hi_current / hi_threshold) / lam_worst) if lam_worst > 0 else 0
        rul_max = round(math.log(hi_current / hi_threshold) / lam_best) if lam_best > 0 else 0
    else:
        rul_hours = rul_min = rul_max = 0

    if hi >= 0.5:
        level = "normal"
    elif hi >= 0.3:
        level = "caution"
    elif hi >= 0.2:
        level = "warning"
    else:
        level = "danger"

    is_default = (lam == lam_default)
    return {
        "hi": round(hi * 100, 1), "rul_hours": rul_hours,
        "rul_range": [rul_min, rul_max],
        "level": level, "degradation_rate": round(lam, 8),
        "is_default_rate": is_default,
        "scores": {
            "vis": round(vis_norm * 100, 1), "water": round(water_norm * 100, 1),
            "pollution": round(pollution_norm * 100, 1), "dielec": round(dielec_norm * 100, 1),
            "metal": round(metal_norm * 100, 1)
        },
        "weights": {"vis": 30, "water": 25, "pollution": 20, "dielec": 15, "metal": 10},
        "raw": {
            "vis": round(vis, 1), "water": round(water, 1), "iso": iso_code,
            "dielec": dielec, "metal": round(metal, 1), "cp": round(cp, 2)
        }
    }


def save_degradation(device_id, result):
    db = get_db_for_polling(device_id)
    db.execute(
        "INSERT INTO degradation_log (device_id,hi_score,rul_hours,vis_score,water_score,metal_score,dielec_score,clean_score,level) VALUES (?,?,?,?,?,?,?,?,?)",
        (device_id, result["hi"], result["rul_hours"],
         result["scores"]["vis"], result["scores"]["water"],
         result["scores"]["metal"], result["scores"]["dielec"],
         result["scores"]["pollution"], result["level"]))
    db.commit()
