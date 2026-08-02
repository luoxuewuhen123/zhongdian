# ============================================================
# 一键打包: Windows / Linux 双平台支持
# 运行: python build.py
# ============================================================
import subprocess, os, sys, shutil

# GitHub Actions Windows 默认 CP1252 不支持中文，强制 UTF-8
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIST_DIR = os.path.join(BASE_DIR, "dist")
IS_WINDOWS = sys.platform == 'win32'

print("=" * 50)
print("油液监测系统 - 打包工具")
print(f"  目标平台: {'Windows' if IS_WINDOWS else 'Linux'}")
print("=" * 50)

# 安装 PyInstaller
print("\n[1/4] 安装 PyInstaller...")
subprocess.run([sys.executable, "-m", "pip", "install", "pyinstaller", "-q"], check=True)

# 构建 PyInstaller 参数
app_args = [
    sys.executable, "-m", "PyInstaller",
    "--onefile",
    "--name", "oil_monitor",
    "--add-data", f"templates{os.pathsep}templates",
    "--add-data", f"static{os.pathsep}static",
    "--add-data", f"油液劣化算法说明.html{os.pathsep}.",
    "--hidden-import", "pymodbus",
    "--hidden-import", "waitress",
    "--hidden-import", "config",
    "--hidden-import", "database",
    "--hidden-import", "collector",
    "--hidden-import", "calibration",
    "app.py",
]

wd_args = [
    sys.executable, "-m", "PyInstaller",
    "--onefile",
    "--name", "oil_watchdog",
    "watchdog.py",
]

if IS_WINDOWS:
    app_args.insert(app_args.index("--onefile") + 1, "--icon")
    app_args.insert(app_args.index("--icon") + 1, "logo.ico")
    app_args.insert(app_args.index("app.py"), "--console")
    wd_args.insert(wd_args.index("--onefile") + 1, "--icon")
    wd_args.insert(wd_args.index("--icon") + 1, "logo.ico")
    wd_args.insert(wd_args.index("watchdog.py"), "--noconsole")

# 打包主程序
print("\n[2/4] 打包主程序...")
subprocess.run(app_args, check=True)

# 打包 Watchdog
print("\n[3/4] 打包 Watchdog 守护程序...")
subprocess.run(wd_args, check=True)

# 生成启动脚本
print("\n[4/4] 生成启动脚本...")

if IS_WINDOWS:
    start_script = '''@echo off
chcp 65001 >nul
title 油液在线监测系统 v2.1
echo ========================================
echo  油液在线监测系统 v2.1
echo ========================================
echo.
echo 正在启动...
start http://localhost:5000
echo.
"%~dp0oil_monitor.exe"
echo.
echo 程序已停止。
pause
'''
    with open(os.path.join(DIST_DIR, "start.bat"), "w", encoding="utf-8") as f:
        f.write(start_script)

    debug_bat = '''@echo off
chcp 65001 >nul
title 油液监测系统 - 直接启动
echo 直接启动主程序（无Watchdog守护）
echo 访问: http://localhost:5000
echo.
"%~dp0oil_monitor.exe"
pause
'''
    with open(os.path.join(DIST_DIR, "启动(调试模式).bat"), "w", encoding="utf-8") as f:
        f.write(debug_bat)

else:
    # Linux 启动脚本
    start_sh = '''#!/bin/bash
echo "========================================"
echo " 油液在线监测系统 v2.1"
echo "========================================"
echo ""
echo "正在启动..."
xdg-open http://localhost:5000 2>/dev/null || true
echo ""
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
"$SCRIPT_DIR/oil_monitor"
echo ""
echo "程序已停止。"
'''
    sh_path = os.path.join(DIST_DIR, "start.sh")
    with open(sh_path, "w", encoding="utf-8", newline='\n') as f:
        f.write(start_sh)
    os.chmod(sh_path, 0o755)

# 清理临时文件
for tmp_dir in ["build"]:
    tmp_path = os.path.join(BASE_DIR, tmp_dir)
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)

for spec_file in ["oil_monitor.spec", "oil_watchdog.spec"]:
    spec_path = os.path.join(BASE_DIR, spec_file)
    if os.path.exists(spec_path):
        os.remove(spec_path)

def _fmt_size(name):
    path = os.path.join(DIST_DIR, name)
    if os.path.exists(path):
        sz = os.path.getsize(path)
        return f"{sz / (1024 * 1024):.1f} MB"
    return "N/A"

ext = ".exe" if IS_WINDOWS else ""

print("\n" + "=" * 50)
print("  打包完成!")
print("=" * 50)
print(f"  输出目录: {DIST_DIR}")
print()
print(f"  主程序:     oil_monitor{ext} ({_fmt_size('oil_monitor'+ext)})")
print(f"  守护程序:   oil_watchdog{ext} ({_fmt_size('oil_watchdog'+ext)})")

if IS_WINDOWS:
    print(f"  启动脚本:   start.bat")
    print()
    print("  部署方法:")
    print("    1. 将 dist/ 目录下所有文件复制到目标电脑")
    print("    2. 双击 start.bat 启动")
    print("    3. 关闭主程序控制台窗口即可停止")
else:
    print(f"  启动脚本:   start.sh")
    print()
    print("  部署方法:")
    print("    1. 将 dist/ 目录下所有文件复制到目标服务器")
    print("    2. chmod +x start.sh oil_monitor oil_watchdog")
    print("    3. ./start.sh 启动")
    print("    4. Ctrl+C 停止")

print("    5. data/ 目录和数据库文件会在首次运行时自动创建")
print("    6. 升级时保留 data/ 目录即可保留历史数据")
print()
