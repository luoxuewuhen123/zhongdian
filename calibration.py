# ============================================================
# 算法引擎 - 阶段一手动触发，阶段二自动运行
#
# 阶段一（现场注入A品质新油后手动触发）：
#   学习新油基线 + 管路检测 → 生成初始算法
#   条件：≥2000条数据 + 温度稳定段检测通过（标准差<1.5℃）
#
# 阶段二（每7天自动运行一次）：
#   用累计数据 → 退化率（基线锁定不变）
#   条件：≥10000条数据 + 运行时间≥168小时（7天）
# ============================================================
import database, config, math
from datetime import datetime, timedelta

# ---- 校准数据质量阈值 ----
_MIN_BASELINE_ROWS = 3000        # 基线校准最少数据条数（约8.3小时@10秒间隔）
_MIN_RUNTIME_HOURS = 2.0         # 最少运行时长（小时），防止刚启动就用冷启动数据校准
_MIN_DEGRADATION_ROWS = 10000    # 退化率计算最少数据条数
_MIN_DEGRADATION_HOURS = 168     # 退化率最少运行时长（7天），保证头尾时间跨度够大


def _check_data_quality(device_id, min_rows, min_hours):
    """检查数据质量和运行时长是否符合校准条件
    返回: (合格, 原因描述, 实际行数, 实际运行时长)"""
    try:
        db = database.connect_data(device_id)
        count = db.execute("SELECT COUNT(*) FROM data_log").fetchone()[0]
        # 获取最早和最晚时间戳以计算运行时长
        first = db.execute("SELECT timestamp FROM data_log ORDER BY id ASC LIMIT 1").fetchone()
        last = db.execute("SELECT timestamp FROM data_log ORDER BY id DESC LIMIT 1").fetchone()
        db.close()
    except Exception:
        return False, "数据库访问失败", 0, 0

    if count < min_rows:
        remaining = min_rows - count
        wait_minutes = max(0, remaining * config.READ_INTERVAL // 60)
        return False, f"数据不足（当前{count}条，需≥{min_rows}条，约需再等{wait_minutes}分钟）", count, 0

    runtime_hours = 0
    if first and last:
        try:
            t1 = datetime.strptime(first["timestamp"], "%Y-%m-%d %H:%M:%S")
            t2 = datetime.strptime(last["timestamp"], "%Y-%m-%d %H:%M:%S")
            runtime_hours = abs((t2 - t1).total_seconds()) / 3600.0
        except (ValueError, TypeError):
            runtime_hours = 0

    if min_hours > 0 and runtime_hours < min_hours:
        remaining_hours = min_hours - runtime_hours
        return False, f"运行时间不足（当前{runtime_hours:.1f}小时，需≥{min_hours}小时，约需再等{remaining_hours:.1f}小时）", count, runtime_hours

    return True, "ok", count, runtime_hours


def _has_temperature_stability(device_id, rows=None):
    """检查是否存在温度稳定段（精滤油温标准差<1.5℃的连续100条窗口）
    这是判断油液是否循环均匀的关键指标"""
    if rows is None:
        # 查询最近72小时的数据（约25920条，10s/条），足够判断温度稳定性
        rows = database.get_history(device_id=device_id, limit=5000, hours=72,
                                     fields=["timestamp", "精滤油温"], order="ASC", _internal=True)

    if len(rows) < 100:
        return False, "数据量不足以判断温度稳定性"

    window = 100
    for i in range(0, len(rows) - window, 50):
        chunk = rows[i:i + window]
        temps = [r.get("精滤油温", 0) for r in chunk if r.get("精滤油温", 0) > 0]
        if len(temps) < window * 0.8:
            continue
        avg = sum(temps) / len(temps)
        if avg == 0:
            continue
        variance = sum((t - avg) ** 2 for t in temps) / len(temps)
        std = math.sqrt(variance)
        if std < 1.5:
            return True, "ok"

    return False, "未检测到温度稳定段（精滤油温波动>1.5℃），请等待油液循环均匀后再校准"


def update_algorithm(device_id="D001", force_baseline=False):
    """阶段一（force_baseline 或新周期）：全量学习基线
       阶段二（自动每月）：快速查询退化率（仅头尾2万行，24MB）"""
    cal = database.get_calibration(device_id)
    total = database.get_history_count(device_id=device_id, hours=2160)

    result = {
        "device": device_id,
        "data_points": total,
        "pipeline": None,
        "baselines": None,
        "degradation_rates": None,
        "thresholds": None,
    }

    # ---- 管路检测：5000行足够 ----
    pipe_fields = ["timestamp"] + [f"y_{i}" for i in range(len(config.Y_OUTPUTS))]
    pipe_rows = database.get_history(device_id=device_id, limit=5000, hours=2160,
                                      fields=pipe_fields, order="ASC", _internal=True)
    pipeline = detect_pipeline(pipe_rows)
    result["pipeline"] = pipeline

    # ---- 基线学习（手动触发或新周期）----
    is_new_cycle = force_baseline or not cal or cal.get("status") in (None, "", "uncalibrated", "new_cycle_pending")
    if is_new_cycle:
        # 检查数据质量和温度稳定性
        ok, reason, count, runtime = _check_data_quality(device_id, _MIN_BASELINE_ROWS, _MIN_RUNTIME_HOURS)
        if ok:
            # 进一步检查温度稳定段
            temp_ok, temp_reason = _has_temperature_stability(device_id)
            if temp_ok:
                all_fields = pipe_fields + ["精滤油温"] + \
                             [f"oil_{str(i).zfill(2)}" for i in range(len(config.OIL_QUALITY))]
                all_rows = database.get_history(device_id=device_id, limit=20000, hours=2160,
                                                 fields=all_fields, order="ASC", _internal=True)
                if cal and cal.get("calibrated_at") and not force_baseline:
                    all_rows = [r for r in all_rows if r["timestamp"] >= cal["calibrated_at"]]
                result["baselines"] = learn_baselines_from_rows(all_rows)
            else:
                result["_baseline_skip_reason"] = temp_reason
                if cal and cal.get("baselines"):
                    result["baselines"] = cal["baselines"]
        else:
            result["_baseline_skip_reason"] = reason
            if cal and cal.get("baselines"):
                result["baselines"] = cal["baselines"]
    elif cal and cal.get("baselines"):
        result["baselines"] = cal["baselines"]

    # ---- 退化率 + 阈值（每月自动）----
    ok, reason, count, runtime = _check_data_quality(
        device_id, _MIN_DEGRADATION_ROWS, _MIN_DEGRADATION_HOURS
    )
    if ok:
        result["degradation_rates"] = estimate_degradation_fast(device_id)
        result["thresholds"] = adaptive_thresholds_fast(device_id)
    else:
        result["_degradation_skip_reason"] = reason

    # 保存到数据库（退化率更新时独立记录时间戳）
    cal_data = {
        "pipeline": pipeline.get("detected_order", []),
        "baselines": result["baselines"],
        "degradation_rates": result["degradation_rates"],
        "thresholds": result["thresholds"],
    }
    if result["degradation_rates"]:
        cal_data["drift_updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    database.save_calibration(device_id, cal_data)

    result["status"] = "ok"
    return result


# ==============================
# 内部方法
# ==============================

def detect_pipeline(rows):
    """边沿检测：Y输出0→1的跳变顺序 = 管路启动顺序"""
    y_names = {a: n for a, n in config.Y_OUTPUTS}
    pipeline, prev, seen = [], None, set()

    for r in rows[:5000]:  # 只看前5000条
        current = {a: r.get(f"y_{a}", 0) for a in range(6)}
        if prev:
            for a in range(6):
                if current.get(a, 0) == 1 and prev.get(a, 0) == 0 and a not in seen:
                    seen.add(a)
                    pipeline.append({"stage": len(pipeline) + 1, "name": y_names.get(a, f"Y{a}"),
                                     "addr": a, "time": r["timestamp"]})
        prev = current
        if len(pipeline) >= 6: break

    return {
        "detected_order": [p["name"] for p in pipeline],
        "details": pipeline,
        "status": "ok" if pipeline else "no_detection"
    }


def _find_stable_start(rows, window=100, temp_threshold=1.5):
    """找温度稳定点：滑动窗口100条，精滤油温标准差<1.5℃后认为循环均匀"""
    for i in range(0, len(rows) - window, 50):
        chunk = rows[i:i+window]
        temps = [r.get("精滤油温", 0) for r in chunk if r.get("精滤油温", 0) > 0]
        if len(temps) < window * 0.8:
            continue
        avg = sum(temps) / len(temps)
        std = math.sqrt(sum((t - avg) ** 2 for t in temps) / len(temps))
        if std < temp_threshold:
            return i
    return 0  # 找不到稳定点就用全部数据


def learn_baselines_from_rows(rows):
    """温度稳定检测 + 截尾均值法：找到油温稳定的起点，取从此往后的前半段数据，掐头去尾10%算平均"""
    # 找到温度波动小（油液循环均匀）的起始点
    stable_start = _find_stable_start(rows)
    stable_plus = rows[stable_start:]
    # 取稳定后数据的前半段（最接近新油状态）
    calibrate_data = stable_plus[:max(len(stable_plus)//2, 500)]
    data_label = "油温稳定后" if stable_start > 0 else "前半段"

    def stats(key):
        vals = [r.get(key, 0) for r in calibrate_data if r.get(key, 0) not in (0, None)]
        if len(vals) < 50: return None
        vals.sort()
        n = len(vals)
        trimmed = vals[int(n*0.1):int(n*0.9)]
        if not trimmed: return None
        avg = sum(trimmed) / len(trimmed)
        std = math.sqrt(sum((x-avg)**2 for x in trimmed) / len(trimmed))
        return {"mean": round(avg, 4), "std": round(std, 4), "count": len(trimmed)}

    baselines = {}
    for i, (addr, name, desc) in enumerate(config.OIL_QUALITY):
        col = f"oil_{str(i).zfill(2)}"
        s = stats(col)
        if s: baselines[col] = {"name": name, "addr": addr, "mean": s["mean"]}
    for addr, key, scale, unit, desc in config.SYSTEM_PARAMS:
        s = stats(key)
        if s and s["mean"] != 0: baselines[key] = {"name": key, "addr": addr, "mean": s["mean"]}

    return {"count": len(baselines), "values": baselines, "source": data_label, "stable_start": stable_start}


# ===== 快速退化率 / 阈值（每周自动触发，仅查头尾各2万行，~24MB）=====

_SAMPLE_SIZE = 20000   # 头尾各自取2万行（覆盖约28小时，包含完整日温度周期）
_DEG_INDICES = [0, 6, 20, 24, 29]  # 粘度、含水率、铁磁24H总数、介电常数、污染度等级2


def estimate_degradation_fast(device_id):
    """快速退化率：head 取数据库最早20000条（新油基准），tail 取最新20000条
    head 不加 hours 限制，确保对比的是换油后最初的状态"""
    oil_cols = [f"oil_{str(i).zfill(2)}" for i in _DEG_INDICES]
    fields = ["timestamp", "精滤油温"] + oil_cols

    # head: 数据库最早的 20000 条（不加 hours 限制，确保是新油时期数据）
    head = database.get_history(device_id=device_id, limit=_SAMPLE_SIZE, hours=99999,
                                 fields=fields, order="ASC", _internal=True)
    # tail: 最近 90 天最新的 20000 条
    tail = database.get_history(device_id=device_id, limit=_SAMPLE_SIZE, hours=2160,
                                 fields=fields, order="DESC", _internal=True)

    if len(head) < 100 or len(tail) < 100:
        return {}

    # 找到头尾各自的温度稳定起点（tail 是 DESC，先反转为 ASC 再找稳定段）
    h_start = _find_stable_start(head, window=50, temp_threshold=2.0)
    tail_asc = list(reversed(tail))
    t_start = _find_stable_start(tail_asc, window=50, temp_threshold=2.0)
    head_stable = head[h_start:]
    tail_stable = tail_asc[t_start:]

    if len(head_stable) < 100 or len(tail_stable) < 100:
        return {}

    try:
        t1 = datetime.strptime(head_stable[0]["timestamp"], "%Y-%m-%d %H:%M:%S")
        t2 = datetime.strptime(tail_stable[0]["timestamp"], "%Y-%m-%d %H:%M:%S")
        td = abs((t2 - t1).total_seconds()) / 3600.0
    except (ValueError, KeyError, IndexError):
        td = max(len(head_stable) * 2 * config.READ_INTERVAL / 3600.0, 1)

    rates = {}
    for idx in _DEG_INDICES:
        col = f"oil_{str(idx).zfill(2)}"
        name = config.OIL_QUALITY[idx][1]
        vals_h = [r.get(col, 0) for r in head_stable if r.get(col, 0) not in (0, None)]
        vals_t = [r.get(col, 0) for r in tail_stable if r.get(col, 0) not in (0, None)]
        if len(vals_h) < 100 or len(vals_t) < 100:
            continue
        early_mean = sum(vals_h) / len(vals_h)
        late_mean = sum(vals_t) / len(vals_t)
        rate = (late_mean - early_mean) / td
        rates[name] = {"early": round(early_mean, 4), "late": round(late_mean, 4),
                       "rate_per_hour": round(rate, 8), "time_hours": round(td, 1),
                       "stable_source": True}

    return rates


def adaptive_thresholds_fast(device_id):
    """快速阈值：仅查询头尾各2万行系统参数，不加载全量"""
    sys_keys = [key for addr, key, scale, unit, desc in config.SYSTEM_PARAMS]
    fields = ["timestamp"] + sys_keys

    head = database.get_history(device_id=device_id, limit=_SAMPLE_SIZE, hours=2160,
                                 fields=fields, order="ASC", _internal=True)
    tail = database.get_history(device_id=device_id, limit=_SAMPLE_SIZE, hours=2160,
                                 fields=fields, order="DESC", _internal=True)
    all_rows = head + tail
    if len(all_rows) < 500:
        return {}

    thresholds = {}
    for addr, key, scale, unit, desc in config.SYSTEM_PARAMS:
        vals = [r.get(key, 0) for r in all_rows if r.get(key, 0) not in (0, None)]
        if len(vals) < 100:
            continue
        mean = sum(vals) / len(vals)
        if mean == 0:
            continue
        std = math.sqrt(sum((x - mean) ** 2 for x in vals) / len(vals))
        thresholds[key] = {
            "mean": round(mean, 4), "std": round(std, 4),
            "upper": round(mean + 3 * std, 4), "lower": round(mean - 3 * std, 4),
        }

    return {"count": len(thresholds), "values": thresholds}


# ===== 就绪检测 =====

def start_calibration_session(device_id="D001"):
    """触发校准：多维度检查数据质量（数据量 + 运行时长 + 温度稳定性）"""
    # 1. 检查数据量和运行时长
    ok, reason, count, runtime = _check_data_quality(
        device_id, _MIN_BASELINE_ROWS, _MIN_RUNTIME_HOURS
    )
    if not ok:
        return {"started": False, "reason": reason, "count": count, "runtime_hours": round(runtime, 1)}

    # 2. 检查温度稳定性
    temp_ok, temp_reason = _has_temperature_stability(device_id)
    if not temp_ok:
        return {
            "started": False,
            "reason": temp_reason,
            "count": count,
            "runtime_hours": round(runtime, 1),
            "detail": "油液尚未循环均匀，请等待精滤油温波动<1.5℃后再校准"
        }

    # 3. 全部条件满足 → 执行校准
    result = calibrate_baseline(device_id)
    n_baselines = len(result.get("baselines", {}).get("values", {}))
    return {
        "started": True,
        "data_points": n_baselines,
        "msg": f"基线校准完成（{n_baselines}个参数，数据量{count}条，运行{runtime:.1f}小时）",
        "count": count,
        "runtime_hours": round(runtime, 1)
    }


def calibrate_baseline(device_id="D001"):
    """用最早的稳定数据学习新油基线（取前3000条，约8.3小时，asc排序确保取到最早数据）"""
    rows = database.get_history(device_id=device_id, limit=3000, hours=99999, order="ASC", _internal=True)
    ok, reason, count, runtime = _check_data_quality(device_id, _MIN_BASELINE_ROWS, 0)
    if not ok:
        return {"error": reason}
    baselines = learn_baselines_from_rows(rows)
    # 保留已有的退化率和阈值（阶段一校准不应覆盖阶段二数据）
    old_cal = database.get_calibration(device_id)
    cal_data = {
        "baselines": baselines,
        "pipeline": [],
        "status": "calibrated",
        "calibrated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "calibration_start": "",
    }
    if old_cal:
        if old_cal.get("drift"):
            cal_data["degradation_rates"] = old_cal["drift"]
        if old_cal.get("thresholds"):
            cal_data["thresholds"] = old_cal["thresholds"]
    database.save_calibration(device_id, cal_data)
    return {"status": "ok", "data_points": len(rows), "baselines": baselines}


def get_model_status(device_id="D001"):
    cal = database.get_calibration(device_id)
    if not cal:
        return {"status": "uncalibrated", "msg": "尚未校准，使用默认新油基线"}

    baselines = cal.get("baselines", {})
    drift = cal.get("drift", {}) or cal.get("degradation_rates", {})
    thresholds = cal.get("thresholds") or {}
    pipeline = cal.get("pipeline") or []

    # 提取关键基线值
    key_baselines = {}
    for col, info in baselines.get("values", {}).items():
        key_baselines[info.get("name", col)] = info.get("mean", 0)

    # 退化率（转为周度百分比）
    key_drift = {}
    for name, info in drift.items():
        rate_h = info.get("rate_per_hour", 0)
        weekly = round(rate_h * 168 * 100, 4) if rate_h else 0  # %/周
        key_drift[name] = {"weekly_pct": weekly, "raw_hourly": rate_h}

    return {
        "status": "calibrated" if baselines.get("count", 0) > 0 else "partial",
        "calibrated_at": cal.get("calibrated_at"),
        "stage1_done": baselines.get("count", 0) > 0,
        "stage2_done": len(drift) > 0,
        "pipeline": pipeline,
        "key_baselines": key_baselines,
        "key_drift": key_drift,
        "thresholds": thresholds.get("values", {}),
    }
