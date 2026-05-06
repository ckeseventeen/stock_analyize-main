#!/usr/bin/env bash
# ================================================================
# run.sh — stock_analyize 一键启动脚本（Linux/macOS）
# 使用方法：
#   ./run.sh              # 运行全市场分析（默认）
#   ./run.sh --market a   # 仅运行 A股
#   ./run.sh --market hk  # 仅运行 港股
#   ./run.sh --market us  # 仅运行 美股
#   ./run.sh --test       # 运行测试套件
#   ./run.sh --gui        # 启动 Streamlit Web GUI
# ================================================================
set -e

# ──── 颜色输出 ────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║    股票量化投研分析平台 v1.0 — 启动中...      ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════╝${NC}"

# ──── 检查 Python 版本 ────
PYTHON_CMD=""
for cmd in python3.11 python3.10 python3.9 python3 python; do
    if command -v "$cmd" &>/dev/null; then
        VER=$("$cmd" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        MAJOR=$(echo "$VER" | cut -d. -f1)
        MINOR=$(echo "$VER" | cut -d. -f2)
        if [ "$MAJOR" -ge 3 ] && [ "$MINOR" -ge 9 ]; then
            PYTHON_CMD="$cmd"
            echo -e "${GREEN}✓ 使用 Python $VER ($cmd)${NC}"
            break
        fi
    fi
done

if [ -z "$PYTHON_CMD" ]; then
    echo -e "${RED}✗ 未找到 Python 3.9+，请先安装：https://www.python.org/downloads/${NC}"
    exit 1
fi

# ──── 虚拟环境检测/创建 ────
VENV_DIR=".venv"
if [ ! -d "$VENV_DIR" ]; then
    echo -e "${YELLOW}→ 创建虚拟环境 ($VENV_DIR)...${NC}"
    "$PYTHON_CMD" -m venv "$VENV_DIR"
fi

# 激活虚拟环境
source "$VENV_DIR/bin/activate"
echo -e "${GREEN}✓ 虚拟环境已激活${NC}"

# ──── 安装/更新依赖 ────
if [ ! -f ".venv/.deps_installed" ] || [ "requirements.txt" -nt ".venv/.deps_installed" ]; then
    echo -e "${YELLOW}→ 安装依赖（首次或 requirements.txt 有更新）...${NC}"
    pip install --upgrade pip -q
    pip install -r requirements.txt -q
    touch ".venv/.deps_installed"
    echo -e "${GREEN}✓ 依赖安装完成${NC}"
fi

# ──── 创建必要目录 ────
mkdir -p logs output/data output/reports output/backtest output/plots

# ──── 解析参数，执行对应模式 ────
case "${1:-}" in
    --test)
        echo -e "${BLUE}→ 运行测试套件...${NC}"
        pytest tests/ -v --tb=short
        ;;
    --gui)
        echo -e "${BLUE}→ 启动 Streamlit Web GUI...${NC}"
        streamlit run src/web/app.py
        ;;
    --lint)
        echo -e "${BLUE}→ 运行代码规范检查...${NC}"
        ruff check src/ tests/
        ;;
    *)
        echo -e "${BLUE}→ 启动市场分析...${NC}"
        "$PYTHON_CMD" main.py "$@"
        ;;
esac
