"""
src/web/theme.py — 统一主题色 / emoji / 样式常量

集中各页面散落的 emoji + 颜色字面量。新加页面引用本模块，不要硬编码。
"""
from __future__ import annotations

from typing import Final


class Emoji:
    """统一 emoji 常量"""
    SUCCESS: Final[str] = "✅"
    ERROR: Final[str] = "❌"
    WARNING: Final[str] = "⚠️"
    INFO: Final[str] = "💡"
    SAVE: Final[str] = "💾"
    DELETE: Final[str] = "🗑️"
    ADD: Final[str] = "➕"
    EDIT: Final[str] = "✏️"
    RUN: Final[str] = "▶️"
    REFRESH: Final[str] = "🔄"
    SEARCH: Final[str] = "🔍"
    SETTINGS: Final[str] = "⚙️"
    CHART: Final[str] = "📊"
    BACK: Final[str] = "⬅️"
    FORWARD: Final[str] = "➡️"
    FAVORITE: Final[str] = "⭐"
    BELL: Final[str] = "🔔"
    CLOCK: Final[str] = "⏰"
    CALENDAR: Final[str] = "📅"
    GLOBE: Final[str] = "🌐"
    MONEY: Final[str] = "💰"
    GRAPH_UP: Final[str] = "📈"
    DOC: Final[str] = "📋"
    TARGET: Final[str] = "🎯"


class Colors:
    """主题色（Streamlit Markdown 兼容的颜色名 / hex）"""
    PRIMARY: Final[str] = "#1f77b4"
    SUCCESS: Final[str] = "#2ca02c"
    WARNING: Final[str] = "#ff7f0e"
    DANGER: Final[str] = "#d62728"
    NEUTRAL: Final[str] = "#7f7f7f"
    BG_LIGHT: Final[str] = "#f0f2f6"


# 常用类型 → emoji（用于 stats_cards 自动选 icon）
TYPE_ICONS: Final[dict[str, str]] = {
    "price": "💰",
    "pe": "📊",
    "pb": "📊",
    "ps": "📊",
    "market_cap": "💼",
    "volume": "📈",
    "return": "📈",
    "drawdown": "📉",
    "sharpe": "🎯",
    "win_rate": "🏆",
}


def market_flag(market: str) -> str:
    """市场 key → 国旗 emoji"""
    return {
        "a": "🇨🇳",
        "hk": "🇭🇰",
        "us": "🇺🇸",
    }.get(market, "🌐")


# ============================================================================
# 全局 CSS（现代化视觉：由 app.py 路由注入一次，所有页面生效）
# ============================================================================

GLOBAL_CSS: Final[str] = """
<style>
/* ── 布局：收紧默认留白 ── */
.block-container {
    padding-top: 2.2rem;
    padding-bottom: 3rem;
    max-width: 1200px;
}

/* ── 隐藏 Streamlit 默认痕迹 ── */
#MainMenu, footer { visibility: hidden; }
header[data-testid="stHeader"] { background: transparent; }

/* ── 侧边栏 ── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0d1220 0%, #0b0f19 100%);
    border-right: 1px solid rgba(255,255,255,0.06);
}
section[data-testid="stSidebar"] [data-testid="stNavSectionHeader"] {
    color: #64748b;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.12em;
    margin-top: 0.6rem;
}
section[data-testid="stSidebar"] a[data-testid="stSidebarNavLink"] {
    border-radius: 8px;
    padding: 2px 8px;
}
section[data-testid="stSidebar"] a[data-testid="stSidebarNavLink"]:hover {
    background: rgba(99,102,241,0.12);
}

/* ── 标题层级 ── */
h1 {
    font-weight: 800 !important;
    letter-spacing: -0.02em;
    background: linear-gradient(90deg, #f8fafc 30%, #a5b4fc 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    padding-bottom: 0.2rem !important;
}
h2, h3 { font-weight: 700 !important; letter-spacing: -0.01em; }

/* ── Metric 卡片化 ── */
div[data-testid="stMetric"] {
    background: rgba(30, 41, 59, 0.55);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 14px;
    padding: 14px 18px;
    transition: border-color .15s ease;
}
div[data-testid="stMetric"]:hover { border-color: rgba(99,102,241,0.45); }
div[data-testid="stMetric"] label { color: #94a3b8 !important; }

/* ── 按钮 ── */
.stButton button, .stDownloadButton button, .stFormSubmitButton button {
    border-radius: 10px;
    font-weight: 600;
    transition: transform .08s ease, box-shadow .15s ease;
}
.stButton button:hover, .stDownloadButton button:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 14px rgba(99,102,241,0.25);
}
.stButton button[kind="primary"] {
    background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
    border: none;
}

/* ── 表格 / DataFrame ── */
div[data-testid="stDataFrame"] {
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 12px;
    overflow: hidden;
}

/* ── Expander 卡片化 ── */
div[data-testid="stExpander"] {
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 12px;
    background: rgba(30, 41, 59, 0.35);
}
div[data-testid="stExpander"] summary { font-weight: 600; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] { gap: 4px; }
.stTabs [data-baseweb="tab"] {
    border-radius: 10px 10px 0 0;
    padding: 8px 18px;
    font-weight: 600;
}
.stTabs [aria-selected="true"] { background: rgba(99,102,241,0.15); }

/* ── 输入控件圆角 ── */
div[data-baseweb="input"], div[data-baseweb="select"] > div,
.stTextArea textarea, div[data-baseweb="base-input"] {
    border-radius: 10px !important;
}

/* ── 分割线弱化 ── */
hr { border-color: rgba(255,255,255,0.08) !important; margin: 1.2rem 0 !important; }

/* ── 滚动条 ── */
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-thumb { background: rgba(148,163,184,0.35); border-radius: 4px; }
::-webkit-scrollbar-track { background: transparent; }

/* ── 首页导航卡片 ── */
.nav-card {
    background: rgba(30, 41, 59, 0.55);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 14px;
    padding: 18px 20px;
    height: 100%;
    transition: border-color .15s ease, transform .1s ease;
}
.nav-card:hover { border-color: rgba(99,102,241,0.5); transform: translateY(-2px); }
.nav-card .nav-title { font-size: 16px; font-weight: 700; color: #f8fafc; margin-bottom: 6px; }
.nav-card .nav-desc { font-size: 13px; color: #94a3b8; line-height: 1.5; }
</style>
"""


def inject_global_css() -> None:
    """注入全局样式（由 app.py 路由调用一次，所有页面生效）"""
    import streamlit as st
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)
