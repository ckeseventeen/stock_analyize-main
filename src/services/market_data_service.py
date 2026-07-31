"""
src/services/market_data_service.py — 另类数据 / 基本面深度数据的服务层

存在意义（架构分层）：API 层曾直接 import `src.data.providers.*`，绕过服务层
拿数据。每加一个新数据源就多一处越层调用，久而久之 API 变成"胖控制器"、
数据访问逻辑散落在路由函数里、也无法复用给 CLI/调度器。

本模块是这两类数据的唯一入口，向上只暴露"取清单 / 查数据"两个动作，
向下屏蔽 provider 的构造与单例管理。
"""
from __future__ import annotations

import pandas as pd

from src.data.providers.alternative_data import (
    ALT_DATA_SOURCES,
    get_alt_data_provider,
)
from src.data.providers.fundamental_data import (
    FUNDAMENTAL_SOURCES,
    get_fundamental_provider,
)
from src.utils.logger import get_logger

logger = get_logger("market_data_service")


def _describe(registry: dict) -> list[dict]:
    """数据源注册表 → 前端可渲染的清单"""
    return [
        {"key": k, "label": v["label"], "params": v["params"],
         "optional": v["optional"]}
        for k, v in registry.items()
    ]


# ============================================================================
# 另类数据（龙虎榜/北向/融资融券/大宗/解禁/股东人数/分红/概念成分）
# ============================================================================

def list_alt_sources() -> list[dict]:
    """另类数据源清单"""
    return _describe(ALT_DATA_SOURCES)


def is_alt_source(source: str) -> bool:
    return source in ALT_DATA_SOURCES


def fetch_alt(source: str, **params) -> pd.DataFrame:
    """
    查询另类数据。

    Raises:
        KeyError: 未知数据源
        ValueError: 参数缺失/不支持
        RuntimeError: 上游数据源故障（与"确实无数据"区分，上层可报 502）
    """
    return get_alt_data_provider().fetch(source, **params)


# ============================================================================
# 基本面深度数据（三大报表/财务指标/业绩预告/业绩快报）
# ============================================================================

def list_fundamental_sources() -> list[dict]:
    """基本面数据源清单"""
    return _describe(FUNDAMENTAL_SOURCES)


def is_fundamental_source(source: str) -> bool:
    return source in FUNDAMENTAL_SOURCES


def fetch_fundamental(source: str, **params) -> pd.DataFrame:
    """查询基本面数据（异常语义同 fetch_alt）"""
    return get_fundamental_provider().fetch(source, **params)


# ============================================================================
# 行情元信息（供 API 校验参数，避免 API 直接 import data_provider）
# ============================================================================

def minute_frequencies() -> tuple[str, ...]:
    """支持的分钟线周期（1m/5m/15m/30m/60m）"""
    from src.analysis.screening.data_provider import ScreenerDataProvider

    return tuple(ScreenerDataProvider.MINUTE_FREQ_MAP)


def is_minute_freq(freq: str) -> bool:
    return freq in minute_frequencies()
