"""
src/core/cache_policy.py — 缓存 TTL 的单点真相

为什么需要这个：TTL 曾散落在 12+ 个文件里各自硬编码（`ttl_hours=4`、
`ohlcv_ttl_hours * 3`、`24 * 30`…），调一个数据的新鲜度要 grep 全项目，
而且同类数据在不同 provider 里 TTL 不一致（如另类数据 12h vs 基本面 24h
没有依据）。这里按**数据的实际变化频率**统一声明。

用法::
    from src.core.cache_policy import ttl_for, CacheTTL

    ttl_for("kline_daily")        # -> 4（小时）
    CacheTTL.SPOT                 # -> 12

调优：设环境变量 STOCK_ANALYZE_TTL_SCALE=0.5 可把所有 TTL 减半
（开发调试时用；生产保持 1.0）。
"""
from __future__ import annotations

import os
from typing import Final

# 全局 TTL 缩放系数（调试时可整体缩短，不必改代码）
try:
    _SCALE: Final[float] = max(0.01, float(
        os.environ.get("STOCK_ANALYZE_TTL_SCALE", "1.0")))
except ValueError:
    _SCALE = 1.0


class CacheTTL:
    """按数据变化频率分档的 TTL（单位：小时）"""

    # ── 行情类：盘中会变，但本项目以日线决策为主 ──
    SPOT: Final[int] = 12            # 全A实时行情快照（含市值/PE/PB）
    KLINE_MINUTE: Final[int] = 1     # 分钟线：盘中滚动更新
    KLINE_DAILY: Final[int] = 4      # 日线：收盘后才变
    KLINE_WEEKLY: Final[int] = 12    # 周线
    KLINE_MONTHLY: Final[int] = 24   # 月线/年线
    INDEX_SCOPE: Final[int] = 4      # 指数成分股（调仓不频繁，但要跟上）

    # ── 基本面/另类：按报告期或交易日更新 ──
    ALT_INTRADAY: Final[int] = 1     # 北向资金流向汇总等日内数据
    ALT_DAILY: Final[int] = 6        # 龙虎榜等每日盘后数据
    ALT_SLOW: Final[int] = 12        # 大宗交易/融资融券统计
    FUNDAMENTAL: Final[int] = 24     # 三大报表/财务指标（按季度披露）
    EARNINGS: Final[int] = 12        # 财报披露日历

    # ── 元数据：几乎不变 ──
    STOCK_NAMES: Final[int] = 24     # 代码→名称表
    ML_DATASET: Final[int] = 24 * 30  # ML 训练集（月度重训）

    # ── 兜底默认 ──
    DEFAULT: Final[int] = 24


# 语义名 → TTL 常量（供配置/字符串驱动的调用方使用）
_TTL_MAP: Final[dict[str, int]] = {
    "spot": CacheTTL.SPOT,
    "kline_minute": CacheTTL.KLINE_MINUTE,
    "kline_daily": CacheTTL.KLINE_DAILY,
    "kline_weekly": CacheTTL.KLINE_WEEKLY,
    "kline_monthly": CacheTTL.KLINE_MONTHLY,
    "index_scope": CacheTTL.INDEX_SCOPE,
    "alt_intraday": CacheTTL.ALT_INTRADAY,
    "alt_daily": CacheTTL.ALT_DAILY,
    "alt_slow": CacheTTL.ALT_SLOW,
    "fundamental": CacheTTL.FUNDAMENTAL,
    "earnings": CacheTTL.EARNINGS,
    "stock_names": CacheTTL.STOCK_NAMES,
    "ml_dataset": CacheTTL.ML_DATASET,
    "default": CacheTTL.DEFAULT,
}


def ttl_for(kind: str) -> int:
    """
    按数据种类取 TTL（小时），已应用全局缩放系数。

    Args:
        kind: _TTL_MAP 的键；未知种类回退 DEFAULT（不抛异常，避免因
              拼写错误让缓存整个失效）
    """
    base = _TTL_MAP.get(str(kind).lower(), CacheTTL.DEFAULT)
    return max(1, int(round(base * _SCALE)))


def all_policies() -> dict[str, int]:
    """当前生效的全部 TTL（诊断/展示用）"""
    return {k: ttl_for(k) for k in _TTL_MAP}
