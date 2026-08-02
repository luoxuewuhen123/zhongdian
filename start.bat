@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ========================================
echo   风电齿轮箱滤油机在线监测系统 v2.1
echo ========================================
echo.

REM 检查 Python 环境
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未找到 Python，请先安装 Python 3.8+
    echo        下载: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [检查] Python 版本:
python --version

REM 检查关键依赖
python -c "import flask; import waitress; import pymodbus" 2>&1 | findstr /i "ModuleNotFoundError" >nul
if %errorlevel% equ 0 (
    echo.
    echo [警告] 缺少依赖，正在自动安装...
    python -m pip install flask waitress pymodbus -q
    if %errorlevel% neq 0 (
        echo [错误] 依赖安装失败，请检查网络连接后重试
        pause
        exit /b 1
    )
    echo [完成] 依赖安装成功
)

echo.
echo [启动] 服务启动中...
echo.
echo.

REM 设置加密的管理员密码（出厂默认，请勿修改此行）
set OIL_MONITOR_ADMIN_PASS=Hx4IZl9fQA==

python app.py
pause
