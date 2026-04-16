"""
src/visualization/gui/streamlit_app.py — Streamlit 前端入口

启动命令：
    streamlit run src/visualization/gui/streamlit_app.py --server.port=8501

pages/ 目录下的文件会自动出现在侧边栏（按文件名前缀数字排序）。
本文件作为首页，展示系统概览与快速入口。
"""
from __future__ import annotations

# ========================
# matplotlib 后端强制 Agg（必须在任何 matplotlib / backtrader 导入前设置）
# Streamlit 页面在 worker 线程渲染，MacOS/TkAgg GUI 后端会抛
# "Cannot create a GUI FigureManager outside the main thread"
# ========================
import os  # noqa: E402
os.environ.setdefault("MPLBACKEND", "Agg")

import sys
from pathlib import Path

# sys.path 注入：必须在其他项目模块 import 之前
_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st  # noqa: E402

from src.visualization.gui.utils import (  # noqa: E402
    ALERT_STATE_PATH,
    CONFIG_DIR,
    OUTPUT_DIR,
    PATH_A_STOCK,
    PATH_HK_STOCK,
    PATH_PRICE_ALERTS,
    PATH_US_STOCK,
    ensure_project_dirs,
    load_yaml,
)

# 确保 cache/logs/output 目录存在
ensure_project_dirs()

# Streamlit 页面全局设置
st.set_page_config(
    page_title="股票估值分析平台",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ========================
# 首页：系统概览
# ========================

st.title("📊 多市场股票估值分析平台")
st.caption("A 股 · 港股 · 美股  |  估值分析 · 筛选 · 预警 · 财报监控 · 资讯抓取")

st.markdown("---")

# --- 快速指标卡：统计当前配置中的股票数 ---
col1, col2, col3, col4 = st.columns(4)


def _count_stocks(cfg_path: Path) -> int:
    """统计某市场配置中的股票总数（累加所有 category.stocks）"""
    cfg = load_yaml(cfg_path)
    if not cfg:
        return 0
    total = 0
    for cat in (cfg.get("categories") or {}).values():
        if cat and isinstance(cat, dict):
            total += len(cat.get("stocks") or [])
    return total


with col1:
    st.metric("A 股关注股票", _count_stocks(PATH_A_STOCK))
with col2:
    st.metric("港股关注股票", _count_stocks(PATH_HK_STOCK))
with col3:
    st.metric("美股关注股票", _count_stocks(PATH_US_STOCK))
with col4:
    # 价格预警规则条数
    rules = (load_yaml(PATH_PRICE_ALERTS) or {}).get("rules", []) or []
    st.metric("价格预警规则", len(rules))


st.markdown("---")

# --- 功能导航 ---
st.subheader("🗂 功能导航")

left, right = st.columns(2)

with left:
    st.markdown(
        """
        ### 📈 分析与筛选
        - **估值分析**：对单只股票跑 4 格估值图（营收利润、历史分位、目标价、汇总）
        - **股票筛选**：按条件组合（财务、估值、技术面）批量筛选 A 股
        """
    )
    st.markdown(
        """
        ### 🔔 预警与监控
        - **价格预警**：管理价格阈值/涨跌幅/均线突破等规则，推送至手机
        - **财报披露**：跟踪 A/港/美三市场未来 30 天披露日历与业绩预告
        """
    )

with right:
    st.markdown(
        """
        ### 🌐 资讯抓取
        - **资讯抓取**：财经新闻、公司公告、股东持仓、研报评级 4 类数据批量拉取
        """
    )
    st.markdown(
        """
        ### 📜 告警历史
        - **告警历史**：查看历史告警事件、冷却状态、推送日志
        """
    )

st.info("👈 使用左侧 **侧边栏导航** 切换到具体功能页面。")


# ========================
# 系统信息
# ========================

st.markdown("---")
st.subheader("⚙️ 系统信息")

info_col1, info_col2 = st.columns(2)

with info_col1:
    st.markdown("**配置目录**")
    st.code(str(CONFIG_DIR), language="text")

    st.markdown("**告警状态文件**")
    if ALERT_STATE_PATH.exists():
        import json
        try:
            with open(ALERT_STATE_PATH, encoding="utf-8") as f:
                state = json.load(f)
            st.caption(f"当前 {len(state)} 条记录")
        except Exception:
            st.caption("(读取失败)")
    else:
        st.caption("暂无记录")

with info_col2:
    st.markdown("**输出目录**")
    st.code(str(OUTPUT_DIR), language="text")

    # 展示输出目录下的文件数
    if OUTPUT_DIR.exists():
        files = list(OUTPUT_DIR.rglob("*"))
        st.caption(f"共 {len([f for f in files if f.is_file()])} 个文件")

# 页脚
st.markdown("---")
st.caption("💡 数据源：akshare / pytdx / yfinance。推送通道：Server酱 / Bark / PushPlus / Console。")
