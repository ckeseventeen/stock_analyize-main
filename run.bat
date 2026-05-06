@echo off
REM ================================================================
REM run.bat — stock_analyize 一键启动脚本（Windows）
REM 使用方法：
REM   run.bat              # 运行全市场分析（默认）
REM   run.bat --market a   # 仅运行 A股
REM   run.bat --market hk  # 仅运行 港股
REM   run.bat --market us  # 仅运行 美股
REM   run.bat --test       # 运行测试套件
REM   run.bat --gui        # 启动 Streamlit Web GUI
REM ================================================================

title 股票量化投研分析平台

echo ╔══════════════════════════════════════════════╗
echo ║    股票量化投研分析平台 v1.0 — 启动中...      ║
echo ╚══════════════════════════════════════════════╝

REM ──── 检查 Python ────
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未找到 Python，请先安装：https://www.python.org/downloads/
    pause
    exit /b 1
)

REM ──── 虚拟环境检测/创建 ────
if not exist ".venv\" (
    echo [信息] 创建虚拟环境...
    python -m venv .venv
)

REM 激活虚拟环境
call .venv\Scripts\activate.bat
echo [信息] 虚拟环境已激活

REM ──── 安装依赖 ────
if not exist ".venv\.deps_installed" (
    echo [信息] 首次运行，安装依赖（约需 2-5 分钟）...
    pip install --upgrade pip -q
    pip install -r requirements.txt -q
    echo. > .venv\.deps_installed
    echo [成功] 依赖安装完成
)

REM ──── 创建必要目录 ────
if not exist "logs" mkdir logs
if not exist "output\data" mkdir output\data
if not exist "output\reports" mkdir output\reports
if not exist "output\backtest" mkdir output\backtest
if not exist "output\plots" mkdir output\plots

REM ──── 解析参数 ────
if "%1"=="--test" (
    echo [信息] 运行测试套件...
    pytest tests\ -v --tb=short
    goto :end
)

if "%1"=="--gui" (
    echo [信息] 启动 Streamlit Web GUI...
    streamlit run src\web\app.py
    goto :end
)

if "%1"=="--lint" (
    echo [信息] 运行代码规范检查...
    ruff check src\ tests\
    goto :end
)

REM 默认：运行市场分析
echo [信息] 启动市场分析...
python main.py %*

:end
echo.
echo [完成] 任务执行完毕
pause
