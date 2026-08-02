# ============================================================
# Watchdog 进程守护 - 监控主进程健康，异常退出自动重启
# ============================================================
# 用法（替代直接运行 app.py）:
#   python watchdog.py
# 或打包后:
#   Watchdog守护.exe
#
# 工作原理：
#   - 启动 油液监测系统.exe 作为子进程
#   - 每 15 秒检查子进程是否存活
#   - 子进程异常退出后，等待 5 秒自动重启
#   - 短时间内（5分钟）连续崩溃超过 3 次，停止重启并告警
# ============================================================
import subprocess, sys, os, time, signal, logging
from datetime import datetime, timedelta

# 获取 exe 所在目录（打包后为 dist/，源码运行时为脚本目录）
if getattr(sys, 'frozen', False):
    WORK_DIR = os.path.dirname(sys.executable)
else:
    WORK_DIR = os.path.dirname(os.path.abspath(__file__))

# 配置
CHECK_INTERVAL = 15          # 健康检查间隔（秒）
RESTART_DELAY = 5            # 重启前等待（秒）
MAX_CRASH_WINDOW = 300       # 崩溃窗口（秒）
MAX_CRASH_COUNT = 3          # 窗口内最大崩溃次数
HEARTBEAT_FILE = os.path.join(WORK_DIR, "oil_monitor.heartbeat")  # 心跳文件
HEARTBEAT_STALE_SECONDS = 300  # 心跳过期阈值（秒），需大于心跳写入间隔(30s) * 5，容忍磁盘IO延迟
HEARTBEAT_RESTART_SECONDS = 900  # 心跳过期超过此阈值后，强制杀死并重启进程（900秒=15分钟）
HEARTBEAT_RESTART_WINDOW = 7200  # 心跳重启计数窗口（秒），防止频繁僵死重启
HEARTBEAT_RESTART_MAX = 2        # 窗口内最多心跳重启次数
LOG_FILE = os.path.join(WORK_DIR, "watchdog.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [Watchdog] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("watchdog")


def get_app_cmd():
    """获取主进程启动命令"""
    # 优先使用同目录下的主程序（Windows: oil_monitor.exe, Linux: oil_monitor）
    for name in ["oil_monitor.exe", "oil_monitor"]:
        app_exe = os.path.join(WORK_DIR, name)
        if os.path.exists(app_exe):
            return [app_exe]
    # fallback: 尝试源码运行
    app_py = os.path.join(WORK_DIR, "app.py")
    if os.path.exists(app_py):
        return [sys.executable, app_py]
    raise FileNotFoundError(f"找不到主程序: {app_exe}")


def write_heartbeat():
    """写入心跳时间戳（由主程序调用，Watchdog 只读取）"""
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        pass


def read_heartbeat():
    """读取心跳时间戳"""
    try:
        if os.path.exists(HEARTBEAT_FILE):
            with open(HEARTBEAT_FILE, "r") as f:
                return f.read().strip()
    except Exception:
        pass
    return None


def run():
    """主循环：监控 + 自动重启"""
    log.info("=" * 60)
    log.info("Watchdog 启动，监控油液监测系统主进程")
    log.info(f"  工作目录: {WORK_DIR}")
    log.info(f"  检查间隔: {CHECK_INTERVAL}秒 | 重启延迟: {RESTART_DELAY}秒")
    log.info(f"  崩溃限制: {MAX_CRASH_WINDOW}秒内最多{MAX_CRASH_COUNT}次")
    log.info("=" * 60)

    crash_times = []  # 崩溃时间戳列表
    heartbeat_restart_times = []  # 心跳僵死重启时间戳列表
    proc = None
    restart_count = 0

    try:
        while True:
            # 启动子进程
            try:
                cmd = get_app_cmd()
            except FileNotFoundError as e:
                log.error(str(e))
                log.error("Watchdog 退出")
                return

            log.info(f"启动主进程: {' '.join(cmd)}")
            try:
                # 丢弃子进程输出，避免管道阻塞。主程序日志由自身管理。
                proc = subprocess.Popen(
                    cmd,
                    cwd=WORK_DIR,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                    # 主程序是 --console 打包的，不隐藏其窗口，让用户看到采集日志
                )
            except Exception as e:
                log.error(f"启动主进程失败: {e}")
                log.error("Watchdog 退出")
                return

            log.info(f"主进程已启动 (PID={proc.pid})")

            # 监控循环
            while True:
                time.sleep(CHECK_INTERVAL)

                # 检查子进程是否还活着
                retcode = proc.poll()
                if retcode is not None:
                    # 子进程已退出
                    log.warning(f"主进程退出 (exit code={retcode})")
                    crash_times.append(time.time())
                    restart_count += 1

                    # 清理过期崩溃记录
                    now = time.time()
                    crash_times = [t for t in crash_times if now - t < MAX_CRASH_WINDOW]

                    if len(crash_times) >= MAX_CRASH_COUNT:
                        log.critical(
                            f"主进程在 {MAX_CRASH_WINDOW} 秒内崩溃 {len(crash_times)} 次，"
                            f"超过上限 {MAX_CRASH_COUNT}，停止自动重启！"
                        )
                        log.critical("请检查 watchdog.log 和主程序日志排查原因")
                        try:
                            with open(os.path.join(WORK_DIR, "watchdog_stopped.flag"), "w") as f:
                                f.write(f"停止于 {datetime.now()}, 崩溃{len(crash_times)}次\n")
                        except Exception:
                            pass
                        return

                    log.info(f"等待 {RESTART_DELAY} 秒后重启... (第 {restart_count} 次重启, "
                             f"窗口内崩溃 {len(crash_times)}/{MAX_CRASH_COUNT} 次)")
                    time.sleep(RESTART_DELAY)
                    break  # 跳出内层循环，重新启动进程

                # 检查心跳（主程序应该每 30 秒更新心跳文件）
                heartbeat = read_heartbeat()
                if heartbeat:
                    try:
                        hb_time = datetime.strptime(heartbeat, "%Y-%m-%d %H:%M:%S")
                        stale_seconds = (datetime.now() - hb_time).total_seconds()
                        if stale_seconds > HEARTBEAT_STALE_SECONDS:
                            log.warning(f"主进程心跳过期 {stale_seconds:.0f} 秒，可能已僵死（告警阈值{HEARTBEAT_STALE_SECONDS}秒）")
                        if stale_seconds > HEARTBEAT_RESTART_SECONDS:
                            now = time.time()
                            heartbeat_restart_times = [t for t in heartbeat_restart_times
                                                       if now - t < HEARTBEAT_RESTART_WINDOW]
                            if len(heartbeat_restart_times) >= HEARTBEAT_RESTART_MAX:
                                log.critical(
                                    f"主进程在 {HEARTBEAT_RESTART_WINDOW} 秒内因心跳僵死重启 "
                                    f"{len(heartbeat_restart_times)} 次，超过上限 {HEARTBEAT_RESTART_MAX}，停止自动重启！"
                                )
                                try:
                                    with open(os.path.join(WORK_DIR, "watchdog_stopped.flag"), "w") as f:
                                        f.write(f"心跳僵死停止于 {datetime.now()}\n")
                                except Exception:
                                    pass
                                return
                            log.critical(
                                f"主进程心跳过期 {stale_seconds:.0f} 秒，判定为僵死，"
                                f"强制重启（窗口内僵死重启 {len(heartbeat_restart_times) + 1}/{HEARTBEAT_RESTART_MAX} 次）"
                            )
                            heartbeat_restart_times.append(now)
                            try:
                                if sys.platform == 'win32':
                                    proc.kill()
                                else:
                                    proc.send_signal(signal.SIGKILL)
                                proc.wait(timeout=10)
                            except Exception:
                                pass
                            restart_count += 1
                            break  # 跳出内层循环，重新启动进程
                    except ValueError:
                        pass

    except KeyboardInterrupt:
        log.info("收到停止信号")
    finally:
        # 清理
        if proc and proc.poll() is None:
            log.info("正在终止主进程...")
            try:
                if sys.platform == 'win32':
                    proc.terminate()
                else:
                    proc.send_signal(signal.SIGTERM)
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                log.warning("主进程未响应，强制终止")
                proc.kill()
            except Exception:
                pass
        log.info("Watchdog 已停止")


if __name__ == "__main__":
    run()
