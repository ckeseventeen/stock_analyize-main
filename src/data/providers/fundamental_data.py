"""
src/data/providers/fundamental_data.py — 基本面深度数据提供层（Phase 1 路线图 §2.3）

统一封装 akshare 的基本面接口 + 磁盘缓存 + 失败返回空表：

  balance_sheet    资产负债表（按报告期，东财）    stock_balance_sheet_by_report_em
  profit_sheet     利润表（按报告期，东财）        stock_profit_sheet_by_report_em
  cash_flow_sheet  现金流量表（按报告期，东财）    stock_cash_flow_sheet_by_report_em
  indicators       财务指标（ROE/毛利率等 20+）    stock_financial_analysis_indicator
  forecast         业绩预告（按报告期，全市场）    stock_yjyg_em
  express          业绩快报（按报告期，全市场）    stock_yjkb_em

用法::
    provider = FundamentalDataProvider()
    df = provider.fetch("indicators", code="600519")
    df = provider.fetch("forecast")            # 缺省=最近一个季度报告期
"""
from __future__ import annotations

import akshare as ak
import pandas as pd

from src.core.cache_policy import ttl_for
from src.data.providers.cache_manager import CacheManager
from src.utils.logger import get_logger

logger = get_logger("fundamental_data")


# 数据源注册表：key → {label, params(必填), optional, ttl_hours}
FUNDAMENTAL_SOURCES: dict[str, dict] = {
    "balance_sheet": {"label": "资产负债表(按报告期)", "params": ["code"],
                      "optional": [], "ttl_hours": ttl_for("fundamental")},
    "profit_sheet": {"label": "利润表(按报告期)", "params": ["code"],
                     "optional": [], "ttl_hours": ttl_for("fundamental")},
    "cash_flow_sheet": {"label": "现金流量表(按报告期)", "params": ["code"],
                        "optional": [], "ttl_hours": ttl_for("fundamental")},
    "indicators": {"label": "财务指标(ROE/毛利率等)", "params": ["code"],
                   "optional": ["start_year"], "ttl_hours": ttl_for("fundamental")},
    "forecast": {"label": "业绩预告(全市场)", "params": [],
                 "optional": ["period"], "ttl_hours": ttl_for("earnings")},
    "express": {"label": "业绩快报(全市场)", "params": [],
                "optional": ["period"], "ttl_hours": ttl_for("earnings")},
}


def _to_em_symbol(code: str) -> str:
    """6 位代码 → 东财报表接口的 SH/SZ/BJ 前缀格式（如 SH600519）"""
    code = str(code).strip().upper()
    for suffix in (".SS", ".SZ", ".SH", ".BJ"):
        if code.endswith(suffix):
            code = code[: -len(suffix)]
    if code.startswith(("SH", "SZ", "BJ")) and len(code) == 8:
        return code
    if code.startswith(("6", "9")):
        return f"SH{code}"
    if code.startswith(("8", "4")):
        return f"BJ{code}"
    return f"SZ{code}"


def latest_report_period(now: pd.Timestamp | None = None) -> str:
    """最近一个已到期的季度报告期（YYYYMMDD）：0331/0630/0930/1231"""
    now = now or pd.Timestamp.now()
    year = now.year
    for month_day in ("1231", "0930", "0630", "0331"):
        period = pd.Timestamp(f"{year}{month_day}")
        if period <= now:
            return period.strftime("%Y%m%d")
    return f"{year - 1}1231"


class FundamentalDataProvider:
    """基本面深度数据提供层：akshare 封装 + CacheManager 缓存 + 异常吞并"""

    def __init__(self, cache_dir: str = ".cache/fundamental",
                 ttl_hours: int | None = None):
        self._cache = CacheManager(
            cache_dir=cache_dir,
            ttl_hours=ttl_hours if ttl_hours is not None else ttl_for("fundamental"))

    # ------------------------------------------------------------------
    # 统一入口（与 AlternativeDataProvider.fetch 同构）
    # ------------------------------------------------------------------

    def fetch(self, source: str, **params) -> pd.DataFrame:
        """
        按数据源 key 拉数据。

        Raises:
            KeyError: 未知 source
            ValueError: 缺必填参数 / 传入该源不支持的参数
            RuntimeError: 上游数据源异常——与"合法的空结果"（空 DataFrame）
                明确区分，上层可给用户报数据源故障而非"无数据"
        """
        if source not in FUNDAMENTAL_SOURCES:
            raise KeyError(f"未知基本面数据源: {source}，可选 {sorted(FUNDAMENTAL_SOURCES)}")
        spec = FUNDAMENTAL_SOURCES[source]
        missing = [p for p in spec["params"] if not params.get(p)]
        if missing:
            raise ValueError(f"数据源 {source} 缺少必填参数: {missing}")
        unknown = set(params) - set(spec["params"]) - set(spec["optional"])
        if unknown:
            raise ValueError(f"数据源 {source} 不支持参数: {sorted(unknown)}")

        fetcher = getattr(self, f"get_{source}")
        cache_key = "fund_" + source + "_" + "_".join(
            f"{k}={params[k]}" for k in sorted(params) if params[k] is not None)

        def _do():
            try:
                df = fetcher(**params)
                return df if df is not None else pd.DataFrame()
            except Exception as e:
                logger.warning(f"基本面数据 [{source}] 拉取失败: {type(e).__name__}: {e}")
                raise RuntimeError(
                    f"数据源 [{spec['label']}] 拉取失败（{type(e).__name__}），"
                    f"可能是网络/代理不通或接口被限流"
                ) from e

        df = self._cache.get_or_fetch(cache_key, _do, ttl_hours=spec["ttl_hours"])
        return df if df is not None else pd.DataFrame()

    # ------------------------------------------------------------------
    # 各数据源实现
    # ------------------------------------------------------------------

    @staticmethod
    def get_balance_sheet(code: str) -> pd.DataFrame:
        """资产负债表（按报告期，一行一个报告期）"""
        return ak.stock_balance_sheet_by_report_em(symbol=_to_em_symbol(code))

    @staticmethod
    def get_profit_sheet(code: str) -> pd.DataFrame:
        """利润表（按报告期）"""
        return ak.stock_profit_sheet_by_report_em(symbol=_to_em_symbol(code))

    @staticmethod
    def get_cash_flow_sheet(code: str) -> pd.DataFrame:
        """现金流量表（按报告期）"""
        return ak.stock_cash_flow_sheet_by_report_em(symbol=_to_em_symbol(code))

    @staticmethod
    def get_indicators(code: str, start_year: str | None = None) -> pd.DataFrame:
        """财务指标时序（ROE/ROA/毛利率/净利率/资产负债率等 20+ 列）"""
        if not start_year:
            start_year = str(pd.Timestamp.now().year - 5)   # 默认近 5 年
        code = str(code).strip()
        for suffix in (".SS", ".SZ", ".SH", ".BJ"):
            if code.upper().endswith(suffix):
                code = code[: -len(suffix)]
        return ak.stock_financial_analysis_indicator(
            symbol=code, start_year=str(start_year))

    @staticmethod
    def get_forecast(period: str | None = None) -> pd.DataFrame:
        """业绩预告（全市场，按报告期；缺省=最近一个季度报告期）"""
        return ak.stock_yjyg_em(date=str(period or latest_report_period()))

    @staticmethod
    def get_express(period: str | None = None) -> pd.DataFrame:
        """业绩快报（全市场，按报告期；缺省=最近一个季度报告期）"""
        return ak.stock_yjkb_em(date=str(period or latest_report_period()))


# 模块级单例（API 层复用）
_GLOBAL_FUND_PROVIDER: FundamentalDataProvider | None = None


def get_fundamental_provider() -> FundamentalDataProvider:
    global _GLOBAL_FUND_PROVIDER
    if _GLOBAL_FUND_PROVIDER is None:
        _GLOBAL_FUND_PROVIDER = FundamentalDataProvider()
    return _GLOBAL_FUND_PROVIDER
