# ============================================================
# 油液在线监测系统 - Web服务
# ============================================================
import os, sys, functools, io, csv, threading, time as tm, traceback, secrets, asyncio, re
from flask import Flask, jsonify, request, render_template, session, Response
from datetime import timedelta, datetime
import config, database, collector as collector_mod
from calibration import calibrate_baseline, start_calibration_session, get_model_status

# 用户名和设备编号/名称字符集校验（防止 XSS 注入）
_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_\-\u4e00-\u9fa5]+$')
def _is_safe_id(s):
    """检查字符串是否仅含字母数字中文下划线横线"""
    return bool(s) and bool(_ID_PATTERN.match(s)) and len(s) <= 64

# ===== 控制台编码适配：Windows --console 模式下默认 GBK，中文 print 会崩溃 =====
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# PyInstaller 打包后资源路径适配
if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS
    # 可写目录：exe 所在目录（_MEIPASS 是临时解压目录，只读，不能写文件）
    DATA_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = BASE_DIR

app = Flask(__name__, template_folder=os.path.join(BASE_DIR, 'templates'))

# 安全密钥：优先环境变量，否则从文件读取，再否则随机生成并持久化
_SECRET_FILE = os.path.join(DATA_DIR, "oil_monitor.secret")
def _load_secret():
    env_secret = os.environ.get("OIL_MONITOR_SECRET")
    if env_secret: return env_secret
    if os.path.exists(_SECRET_FILE):
        with open(_SECRET_FILE, "r", encoding="utf-8") as f: return f.read().strip()
    s = secrets.token_hex(32)
    with open(_SECRET_FILE, "w", encoding="utf-8") as f: f.write(s)
    return s
app.secret_key = _load_secret()
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)
app.config['SESSION_COOKIE_HTTPONLY'] = True  # JS无法读取，防XSS窃取
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
collector = collector_mod.Collector()

# ===== 异步事件循环（在后台线程运行）=====
_loop = None
_loop_ready = threading.Event()

def _start_event_loop():
    """启动后台 asyncio event loop 供 collector 使用"""
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)

    # 启动采集器
    database.init()
    collector.start()

    _loop_ready.set()  # 通知主线程：event loop 已就绪
    _loop.run_forever()

_loop_thread = threading.Thread(target=_start_event_loop, daemon=True, name="asyncio-loop")
_loop_thread.start()

# 等待 event loop 就绪（最多等30秒，超时则告警并继续启动）
if not _loop_ready.wait(timeout=30):
    print("  [警告] 异步事件循环启动超时（30秒），采集器可能未就绪")

# ===== Watchdog 心跳（每30秒写入，供 watchdog.py 检测进程存活）=====
_HEARTBEAT_FILE = os.path.join(DATA_DIR, "oil_monitor.heartbeat")

def _heartbeat_writer():
    while True:
        try:
            with open(_HEARTBEAT_FILE, "w") as f:
                f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        except Exception:
            pass
        tm.sleep(30)
threading.Thread(target=_heartbeat_writer, daemon=True, name="heartbeat").start()

print(f"[系统] {config.DEVICE_NAME} v2.1")
print(f"  数据永久保留（手动清除）  |  采集间隔 {config.READ_INTERVAL}秒  |  异步架构 最大并发={collector_mod.MAX_CONCURRENT}")

# ===== 首页 =====
@app.route("/")
def index():
    return render_template("index.html", device_name=config.DEVICE_NAME)

# ===== 算法说明 =====
@app.route("/algorithm")
def algorithm_help():
    """油液劣化算法说明页面"""
    import os as _os
    algo_path = _os.path.join(BASE_DIR, "油液劣化算法说明.html")
    if _os.path.exists(algo_path):
        with open(algo_path, "r", encoding="utf-8") as f:
            return f.read()
    return "算法说明文件未找到", 404

# ===== 装饰器 =====
def login_required(f):
    @functools.wraps(f)
    def wrap(*a, **kw):
        if not session.get('user'):
            return jsonify({"code": -1, "msg": "请先登录"}), 401
        return f(*a, **kw)
    return wrap

def admin_required(f):
    @functools.wraps(f)
    def wrap(*a, **kw):
        u = session.get('user')
        if not u or u.get('role') != 'admin':
            return jsonify({"code": -1, "msg": "需要管理员权限"}), 403
        return f(*a, **kw)
    return wrap

def csrf_protect(f):
    """简易 CSRF 保护：检查请求头 X-CSRF-Token 与 session 中的 token 是否一致"""
    @functools.wraps(f)
    def wrap(*a, **kw):
        if request.method in ('GET', 'HEAD', 'OPTIONS'):
            return f(*a, **kw)
        token = request.headers.get('X-CSRF-Token', '')
        if not token or token != session.get('_csrf_token', ''):
            return jsonify({"code": -1, "msg": "CSRF 验证失败，请刷新页面"}), 403
        return f(*a, **kw)
    return wrap

# ===== API 限流（令牌桶算法）=====
_rate_limits = {}           # {key: {"tokens": float, "last": float}}
_rate_limit_lock = threading.Lock()
_RATE_LIMIT_RPS = int(os.environ.get("OIL_MONITOR_RATE_LIMIT", "50"))   # 默认每秒50次
_RATE_BURST = int(os.environ.get("OIL_MONITOR_RATE_BURST", "100"))       # 突发容量
_RATE_LIMIT_MAX_ENTRIES = int(os.environ.get("OIL_MONITOR_RATE_MAX_ENTRIES", "5000"))  # 最大记录数，防止内存泄漏

def _rate_limit_check(key):
    """令牌桶限流：key 通常为 IP 或 IP+endpoint"""
    now = tm.time()
    with _rate_limit_lock:
        entry = _rate_limits.get(key)
        if entry is None:
            # 容量保护：超过最大记录数时，淘汰最旧的条目
            if len(_rate_limits) >= _RATE_LIMIT_MAX_ENTRIES:
                oldest_key = min(_rate_limits, key=lambda k: _rate_limits[k]["last"])
                del _rate_limits[oldest_key]
            entry = {"tokens": float(_RATE_BURST), "last": now}
            _rate_limits[key] = entry
        # 补充令牌
        elapsed = now - entry["last"]
        entry["tokens"] = min(_RATE_BURST, entry["tokens"] + elapsed * _RATE_LIMIT_RPS)
        entry["last"] = now
        if entry["tokens"] < 1.0:
            return False
        entry["tokens"] -= 1.0
        return True

# 定时清理过期限流记录
def _cleanup_rate_limits():
    while True:
        tm.sleep(600)
        now = tm.time()
        with _rate_limit_lock:
            for key in list(_rate_limits.keys()):
                if now - _rate_limits[key]["last"] > 600:
                    del _rate_limits[key]
threading.Thread(target=_cleanup_rate_limits, daemon=True, name="rate-cleaner").start()

def rate_limit(f):
    """API 全局限流装饰器"""
    @functools.wraps(f)
    def wrap(*a, **kw):
        ip = request.remote_addr or "unknown"
        key = f"{ip}:{request.path}"
        if not _rate_limit_check(key):
            return jsonify({"code": -1, "msg": "请求过于频繁，请稍后再试"}), 429
        return f(*a, **kw)
    return wrap

# ===== 认证 =====
@app.route("/api/change-password", methods=["POST"])
@login_required
@csrf_protect
@rate_limit
def api_change_password():
    data = request.json or {}
    old_pw = data.get("old_password", "")
    new_pw = data.get("new_password", "")
    if not new_pw or len(new_pw) < 8:
        return jsonify({"code": -1, "msg": "新密码至少8位"})
    user = database.verify_user(session["user"]["username"], old_pw)
    if not user:
        return jsonify({"code": -1, "msg": "原密码错误"})
    database.update_user(session["user"]["id"], password=new_pw)
    return jsonify({"code": 0, "msg": "密码修改成功"})

# ===== 登录限流：防暴力破解 =====
_login_attempts = {}  # {ip: [timestamps]}
_login_lock = threading.Lock()
def _cleanup_login_attempts():
    """定时清理过期登录记录，防止内存泄漏"""
    while True:
        tm.sleep(300)
        with _login_lock:
            now = tm.time()
            for ip in list(_login_attempts.keys()):
                _login_attempts[ip] = [t for t in _login_attempts[ip] if now - t < 600]
                if not _login_attempts[ip]:
                    del _login_attempts[ip]
threading.Thread(target=_cleanup_login_attempts, daemon=True, name="login-cleaner").start()

def _check_login_limit(ip):
    """5分钟内失败超过5次禁止登录，10分钟后自动解封"""
    now = tm.time()
    with _login_lock:
        attempts = _login_attempts.get(ip, [])
        # 清理超过10分钟的旧记录
        attempts = [t for t in attempts if now - t < 600]
        _login_attempts[ip] = attempts
        # 最近5分钟内的失败次数
        recent = [t for t in attempts if now - t < 300]
        if len(recent) >= 5:
            return False, "登录失败次数过多，请10分钟后再试"
    return True, None

@app.route("/api/login", methods=["POST"])
def api_login():
    ip = request.remote_addr or "unknown"
    ok, msg = _check_login_limit(ip)
    if not ok:
        return jsonify({"code": -1, "msg": msg})
    data = request.json or {}
    user = database.verify_user(data.get("username", ""), data.get("password", ""))
    if user:
        # 登录成功，清除失败记录
        with _login_lock:
            _login_attempts.pop(ip, None)
        session.permanent = True
        session['user'] = {"id": user["id"], "username": user["username"], "role": user["role"]}
        # 生成 CSRF token 并返回给前端
        if '_csrf_token' not in session:
            session['_csrf_token'] = secrets.token_hex(32)
        # 检测是否使用默认密码，强制修改
        need_change = (data.get("password") == config.ADMIN_PASS and
                       data.get("username") == config.ADMIN_USER)
        return jsonify({"code": 0, "data": {
            **session['user'], "csrf_token": session['_csrf_token'],
            "must_change_password": need_change
        }})
    # 登录失败，记录
    with _login_lock:
        _login_attempts.setdefault(ip, []).append(tm.time())
        remaining = 5 - len([t for t in _login_attempts[ip] if tm.time() - t < 300])
    tip = f"，还剩{remaining}次机会" if remaining > 0 else "，已锁定"
    return jsonify({"code": -1, "msg": "账号或密码错误" + tip})

@app.route("/api/logout", methods=["POST"])
@login_required
@csrf_protect
def api_logout():
    session.clear()
    return jsonify({"code": 0})

@app.route("/api/me")
@login_required
def api_me():
    return jsonify({"code": 0, "data": session.get("user")})

# ===== 状态 =====
@app.route("/api/status")
@rate_limit
def api_status():
    did = request.args.get("device") or "D001"
    s = collector.status_by_device(did)
    return jsonify({"code": 0, "data": s})

@app.route("/api/status/all")
@rate_limit
def api_status_all():
    return jsonify({"code": 0, "data": collector.status()})

# ===== 用户管理 =====
@app.route("/api/users")
@login_required
@admin_required
@rate_limit
def api_users():
    return jsonify({"code": 0, "data": database.list_users()})

@app.route("/api/users", methods=["POST"])
@login_required
@admin_required
@csrf_protect
@rate_limit
def api_create_user():
    data = request.json or {}
    username = data.get("username", "")
    if not username or not _is_safe_id(username):
        return jsonify({"code": -1, "msg": "用户名仅支持字母数字中文下划线横线，长度1-64"})
    pw = data.get("password", "")
    if not pw or len(pw) < 8:
        return jsonify({"code": -1, "msg": "密码至少8位"})
    ok = database.add_user(username, pw, data.get("role", "operator"))
    if ok:
        return jsonify({"code": 0, "msg": "创建成功"})
    return jsonify({"code": -1, "msg": "用户名已存在"})

@app.route("/api/users/<int:uid>", methods=["PUT"])
@login_required
@admin_required
@csrf_protect
@rate_limit
def api_update_user(uid):
    data = request.json or {}
    kwargs = {}
    if data.get("role"): kwargs["role"] = data["role"]
    if data.get("password"):
        if len(data["password"]) < 8:
            return jsonify({"code": -1, "msg": "密码至少8位"})
        kwargs["password"] = data["password"]
    if kwargs:
        database.update_user(uid, **kwargs)
        return jsonify({"code": 0, "msg": "更新成功"})
    return jsonify({"code": -1, "msg": "无变更"})

@app.route("/api/users/<int:uid>", methods=["DELETE"])
@login_required
@admin_required
@csrf_protect
@rate_limit
def api_delete_user(uid):
    if database.count_admins() <= 1 and database.is_admin(uid):
        return jsonify({"code": -1, "msg": "不能删除最后一个管理员"})
    database.delete_user(uid)
    return jsonify({"code": 0, "msg": "已删除"})

# ===== 设备管理 =====
@app.route("/api/devices")
@login_required
@rate_limit
def api_devices():
    return jsonify({"code": 0, "data": database.list_devices()})

@app.route("/api/devices", methods=["POST"])
@login_required
@admin_required
@csrf_protect
@rate_limit
def api_create_device():
    data = request.json or {}
    did = data.get("id", "")
    dname = data.get("name", "")
    if not did or not dname:
        return jsonify({"code": -1, "msg": "请填写设备编号和名称"})
    if not _is_safe_id(did) or not _is_safe_id(dname):
        return jsonify({"code": -1, "msg": "设备编号和名称仅支持字母数字中文下划线横线"})
    ip = data.get("plc_ip", "192.168.0.150")
    port = int(data.get("plc_port", 3000))
    ok = database.add_device(did, dname, ip, port)
    if ok:
        collector.add_device(did, dname, ip, port)
        return jsonify({"code": 0})
    return jsonify({"code": -1, "msg": "设备编号已存在"})

@app.route("/api/devices/<did>", methods=["DELETE"])
@login_required
@admin_required
@csrf_protect
@rate_limit
def api_delete_device(did):
    collector.remove_device(did)
    database.delete_device(did)
    return jsonify({"code": 0})

@app.route("/api/oil-reset", methods=["POST"])
@login_required
@admin_required
@csrf_protect
@rate_limit
def api_oil_reset():
    """换油重置：清除旧周期数据，触发新基线校准"""
    did = request.json.get("device") or "D001"
    deleted = database.reset_oil_cycle(did)
    collector.mark_oil_change(did)
    return jsonify({"code": 0, "deleted": deleted})

@app.route("/api/data-stats")
@login_required
@rate_limit
def api_data_stats():
    did = request.args.get("device") or "D001"
    return jsonify({"code": 0, "data": database.get_data_stats(did)})

# ===== 数据接口（含 ETag 条件请求优化）=====
def _etag_from_data(data):
    """对数据计算 ETag（基于数据内容，使用 SHA-256）"""
    import hashlib
    raw = str(data).encode('utf-8') if data else b''
    return hashlib.sha256(raw).hexdigest()

@app.route("/api/latest")
@login_required
@rate_limit
def api_latest():
    did = request.args.get("device") or "D001"
    data = database.get_latest(did)
    if data:
        data.pop("id", None)
        # ETag 条件请求：若数据未变返回 304
        etag = _etag_from_data(data)
        if request.headers.get('If-None-Match') == etag:
            return Response(status=304)
        resp = jsonify({"code": 0, "data": data})
        resp.headers['ETag'] = etag
        resp.headers['Cache-Control'] = 'no-cache'
        return resp
    return jsonify({"code": -1, "msg": "暂无数据"})

@app.route("/api/history")
@login_required
@rate_limit
def api_history():
    did = request.args.get("device")
    start = request.args.get("start")
    end = request.args.get("end")
    hours = request.args.get("hours", type=int)
    page = request.args.get("page", type=int) or 1
    limit = request.args.get("limit", type=int) or 100
    # 安全限制：单次最多 500 条，防止恶意大量查询
    limit = min(limit, 500)
    page = max(page, 1)
    offset = (page - 1) * limit
    total, rows = database.get_history_page(
        start=start, end=end, hours=hours, device_id=did,
        limit=limit, offset=offset
    )
    for r in rows:
        r.pop("id", None)
    return jsonify({"code": 0, "data": rows, "count": len(rows), "total": total, "page": page, "limit": limit})

@app.route("/api/export")
@login_required
@rate_limit
def api_export():
    """CSV导出，流式分批查询，最大50000条，避免一次性加载到内存"""
    start = request.args.get("start")
    end = request.args.get("end")
    hours = request.args.get("hours", type=int)
    did = request.args.get("device")

    # 先获取列名映射（只需查1条获取表头）
    sample = database.get_history(start=start, end=end, hours=hours, device_id=did, limit=1, _internal=True)
    if not sample:
        return jsonify({"code": -1, "msg": "无数据"})

    keys = [k for k in sample[0].keys() if k != 'id']
    name_map = {nm: nm for nm in keys}
    for addr, nm, scale, unit, _ in config.SYSTEM_PARAMS:
        if nm in name_map: name_map[nm] = f"{nm}({unit})"
    for i, (addr, nm, desc) in enumerate(config.OIL_QUALITY):
        col = f"oil_{str(i).zfill(2)}"
        if col in name_map: name_map[col] = nm
    if 'x_states' in name_map: name_map['x_states'] = 'X状态(JSON)'
    if 'y_states' in name_map: name_map['y_states'] = 'Y状态(JSON)'
    header = [name_map.get(k, k) for k in keys]

    # 流式生成器：分批查询，每批1000条
    _BATCH = 1000
    _MAX_TOTAL = 50000

    def generate():
        yield '\ufeff'  # UTF-8 BOM, Excel正确显示中文
        si = io.StringIO()
        writer = csv.writer(si)
        writer.writerow(header)
        yield si.getvalue()
        si.close()

        total_written = 0
        offset = 0
        while total_written < _MAX_TOTAL:
            batch_limit = min(_BATCH, _MAX_TOTAL - total_written)
            rows = database.get_history(
                start=start, end=end, hours=hours, device_id=did,
                limit=batch_limit, offset=offset, _internal=True
            )
            if not rows:
                break
            si = io.StringIO()
            writer = csv.writer(si)
            for r in rows:
                writer.writerow([r.get(k, "") for k in keys])
            yield si.getvalue()
            si.close()
            total_written += len(rows)
            offset += len(rows)
            if len(rows) < batch_limit:
                break

    return Response(generate(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": "attachment;filename=data_export.csv"})

@app.route("/api/params")
@rate_limit
def api_params():
    return jsonify({"code": 0, "data": {
        "system_params": [
            {"addr": addr, "key": key, "name": key, "unit": unit, "desc": desc}
            for addr, key, scale, unit, desc in config.SYSTEM_PARAMS
        ],
        "oil_quality": [
            {"addr": addr, "name": name, "desc": desc, "idx": i}
            for i, (addr, name, desc) in enumerate(config.OIL_QUALITY)
        ],
        "x_inputs": [
            {"addr": addr, "name": name}
            for addr, name in config.X_INPUTS
        ],
        "y_outputs": [
            {"addr": addr, "name": name}
            for addr, name in config.Y_OUTPUTS
        ],
        "alarm_rules": [
            {"key": key, "name": name, "level": level, "desc": desc}
            for key, name, level, desc in config.ALARM_RULES
        ],
        "plc_settings": [
            {"addr": addr, "key": key, "unit": unit, "desc": desc, "category": cat}
            for addr, key, scale, unit, desc, cat in config.PLC_SETTINGS
        ],
        "device_name": config.DEVICE_NAME
    }})

# ===== 告警管理 =====
@app.route("/api/alarms")
@login_required
@rate_limit
def api_alarms():
    status = request.args.get("status")
    page = request.args.get("page", type=int) or 1
    limit = request.args.get("limit", type=int) or 50
    limit = min(limit, 100)
    page = max(page, 1)
    offset = (page - 1) * limit
    total = database.count_alarms(status=status)
    rows = database.list_alarms(status=status, limit=limit, offset=offset)
    return jsonify({"code": 0, "data": rows, "total": total, "page": page})

@app.route("/api/alarms/<int:aid>/resolve", methods=["PUT"])
@login_required
@csrf_protect
@rate_limit
def api_resolve_alarm(aid):
    note = (request.json or {}).get("note", "")
    database.resolve_alarm(aid, note)
    return jsonify({"code": 0})

@app.route("/api/alarms/<int:aid>/process", methods=["PUT"])
@login_required
@csrf_protect
@rate_limit
def api_process_alarm(aid):
    database.process_alarm(aid)
    return jsonify({"code": 0})

# ===== 油液劣化预测 =====
@app.route("/api/degradation")
@login_required
@rate_limit
def api_degradation():
    did = request.args.get("device") or "D001"
    latest = database.get_latest(did)
    if not latest:
        return jsonify({"code": -1, "msg": "暂无数据"})
    oil_vals = {}
    for i, (addr, name, desc) in enumerate(config.OIL_QUALITY):
        oil_vals[name] = latest.get(f"oil_{str(i).zfill(2)}", 0)
    result = database.calculate_degradation(oil_vals, did)
    if result["level"] != "no_data":
        database.save_degradation(did, result)
    return jsonify({"code": 0, "data": result,
        "raw": {"vis": oil_vals.get("运动粘度", 0),
                "water": oil_vals.get("含水率", 0),
                "iso": oil_vals.get("污染度等级2", 0),
                "dielec": oil_vals.get("介电常数", 0),
                "metal": oil_vals.get("铁磁24H总数", 0)}
    })

# ===== 远程控制 =====
@app.route("/api/control", methods=["POST"])
@login_required
@admin_required
@csrf_protect
@rate_limit
def api_control():
    data = request.json or {}
    addr = data.get("addr")
    state = data.get("state", 0)
    did = data.get("device") or "D001"
    if addr is None:
        return jsonify({"code": -1, "msg": "请指定线圈地址"})
    ok, msg = collector.write_coil(did, int(addr), bool(state))
    if ok:
        y_names = {a: n for a, n in config.Y_OUTPUTS}
        action_name = y_names.get(int(addr), f"Y{addr}")
        database.log_operation(session['user']['username'], "control",
                               did, f"{'启动' if state else '停止'} {action_name}")
    return jsonify({"code": 0 if ok else -1, "msg": "控制成功" if ok else msg})

# ===== 校准 & 模型 =====
@app.route("/api/model/status")
@login_required
@rate_limit
def api_model_status():
    did = request.args.get("device", "D001")
    return jsonify({"code": 0, "data": get_model_status(did)})

@app.route("/api/model/start-calibration", methods=["POST"])
@login_required
@admin_required
@csrf_protect
@rate_limit
def api_start_calibration():
    """开始基线校准：检查条件，满足则立即校准"""
    did = (request.json or {}).get("device", "D001")
    result = start_calibration_session(did)
    return jsonify({"code": 0, "data": result})


# ===== 主入口 =====
if __name__ == "__main__":
    from waitress import serve
    # Waitress 线程数：支持高并发 Web 请求
    _waitress_threads = int(os.environ.get("OIL_MONITOR_WEB_THREADS", "128"))
    try:
        print(f"  => http://localhost:{config.SERVER_PORT}")
        print(f"  Web 线程池={_waitress_threads}")
        serve(app, host=config.SERVER_BIND, port=config.SERVER_PORT, threads=_waitress_threads)
    except KeyboardInterrupt:
        print("\n  收到停止信号...")
    except Exception as e:
        print(f"  启动失败: {e}")
        traceback.print_exc()
    finally:
        collector.stop()
        database._close_sys_pool()
        # 停止 asyncio event loop
        if _loop:
            _loop.call_soon_threadsafe(_loop.stop)
        print("  采集器已停止")
