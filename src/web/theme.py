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
