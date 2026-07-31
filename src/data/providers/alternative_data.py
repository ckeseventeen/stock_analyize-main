"""
src/data/providers/alternative_data.py — 另类数据提供层（Phase 1 路线图 8 项）

统一封装 akshare 的另类数据接口 + 磁盘缓存 + 失败返回空表（不抛异常穿透上层）：

  lhb                 龙虎榜明细（游资/机构动向）        stock_lhb_detail_em
  northbound          北向资金流向汇总                   stock_hsgt_fund_flow_summary_em
  northbound_stock    北向个股持股变动                   stock_hsgt_individual_em
  block_trade         大宗交易市场统计（折溢价）          stock_dzjy_sctj
  margin              融资融券明细（深市，按日）          stock_margin_detail_szse
  restricted_release  个股限售解禁队列（供给冲击）        stock_restricted_release_queue_em
  holder_number       个股股东人数（筹码集中度）          stock_zh_a_gdhs_detail_em
  dividend            个股分红送转历史                   stock_history_dividend_detail
  concept_cons        概念板块成分股（板块轮动）          stock_board_concept_cons_em

用法::
    provider = AlternativeDataProvider()
    df = provider.fetch("lhb", days=5)
    df = provider.fetch("holder_number", code="600519")
"""
from __future__ import annotations

import akshare as ak
import pandas as pd

from src.core.cache_policy import ttl_for
from src.data.providers.cache_manager import CacheManager
from src.utils.logger import get_logger

logger = get_logger("alt_data")


# 数据源注册表：key → {label, params(必填参数名), optional, ttl_hours}
# TTL 取自 src/core/cache_policy 的统一分档（按数据实际更新频率，不再各自拍脑袋）
ALT_DATA_SOURCES: dict[str, dict] = {
    "lhb": {"label": "龙虎榜明细", "params": [], "optional": ["days"],
            "ttl_hours": ttl_for("alt_daily")},
    "northbound": {"label": "北向资金流向汇总", "params": [], "optional": [],
                   "ttl_hours": ttl_for("alt_intraday")},
    "northbound_stock": {"label": "北向个股持股变动", "params": ["code"], "optional": [],
                         "ttl_hours": ttl_for("alt_slow")},
    "block_trade": {"label": "大宗交易市场统计", "params": [], "optional": [],
                    "ttl_hours": ttl_for("alt_slow")},
    "margin": {"label": "融资融券明细(深市)", "params": [], "optional": ["date"],
               "ttl_hours": ttl_for("alt_slow")},
    "restricted_release": {"label": "限售解禁队列", "params": ["code"], "optional": [],
                           "ttl_hours": ttl_for("fundamental")},
    "holder_number": {"label": "股东人数", "params": ["code"], "optional": [],
                      "ttl_hours": ttl_for("fundamental")},
    "dividend": {"label": "分红送转历史", "params": ["code"], "optional": [],
                 "ttl_hours": ttl_for("fundamental")},
    "concept_cons": {"label": "概念板块成分股", "params": ["symbol"], "optional": [],
                     "ttl_hours": ttl_for("index_scope")},
}


class AlternativeDataProvider:
    """另类数据提供层：akshare 封装 + CacheManager 缓存 + 异常吞并"""

    def __init__(self, cache_dir: str = ".cache/altdata",
                 ttl_hours: int | None = None):
        self._cache = CacheManager(
            cache_dir=cache_dir,
            ttl_hours=ttl_hours if ttl_hours is not None else ttl_for("alt_slow"))

    # ------------------------------------------------------------------
    # 统一入口
    # ------------------------------------------------------------------

    def fetch(self, source: str, **params) -> pd.DataFrame:
        """
        按数据源 key 拉数据。

        Raises:
            KeyError: 未知 source
            ValueError: 缺必填参数 / 传入该源不支持的参数
            RuntimeError: 上游数据源异常（网络/反爬/接口变更）——与"合法的
                空结果"（返回空 DataFrame）明确区分，上层可给用户报数据源故障
        """
        if source not in ALT_DATA_SOURCES:
            raise KeyError(f"未知另类数据源: {source}，可选 {sorted(ALT_DATA_SOURCES)}")
        spec = ALT_DATA_SOURCES[source]
        missing = [p for p in spec["params"] if not params.get(p)]
        if missing:
            raise ValueError(f"数据源 {source} 缺少必填参数: {missing}")
        unknown = set(params) - set(spec["params"]) - set(spec["optional"])
        if unknown:
            raise ValueError(f"数据源 {source} 不支持参数: {sorted(unknown)}")

        fetcher = getattr(self, f"get_{source}")
        cache_key = "alt_" + source + "_" + "_".join(
            f"{k}={params[k]}" for k in sorted(params) if params[k] is not None)

        def _do():
            try:
                df = fetcher(**params)
                return df if df is not None else pd.DataFrame()
            except Exception as e:
                logger.warning(f"另类数据 [{source}] 拉取失败: {type(e).__name__}: {e}")
                raise RuntimeError(
                    f"数据源 [{spec['label']}] 拉取失败（{type(e).__name__}），"
                    f"可能是网络/代理不通或接口被限流"
                ) from e

        df = self._cache.get_or_fetch(cache_key, _do, ttl_hours=spec["ttl_hours"])
        return df if df is not None else pd.DataFrame()

    # ------------------------------------------------------------------
    # 各数据源实现（供 fetch 反射调用，也可直接使用）
    # ------------------------------------------------------------------

    @staticmethod
    def get_lhb(days: int = 5) -> pd.DataFrame:
        """近 N 个日历日的龙虎榜明细"""
        end = pd.Timestamp.now()
        start = end - pd.Timedelta(days=int(days))
        return ak.stock_lhb_detail_em(
            start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d"))

    @staticmethod
    def get_northbound() -> pd.DataFrame:
        """北向/南向资金流向汇总（沪股通/深股通当日与历史）"""
        return ak.stock_hsgt_fund_flow_summary_em()

    @staticmethod
    def get_northbound_stock(code: str) -> pd.DataFrame:
        """北向资金对单只股票的持股变动明细"""
        return ak.stock_hsgt_individual_em(symbol=str(code))

    @staticmethod
    def get_block_trade() -> pd.DataFrame:
        """大宗交易市场统计（成交笔数/金额/折溢价率时序）"""
        return ak.stock_dzjy_sctj()

    @staticmethod
    def get_margin(date: str | None = None) -> pd.DataFrame:
        """
        深市融资融券明细。date 缺省时从今天起往前找最近一个有数据的交易日
        （最多回溯 7 天，跳过周末/节假日）。
        """
        if date:
            return ak.stock_margin_detail_szse(date=str(date))
        day = pd.Timestamp.now()
        for _ in range(7):
            try:
                df = ak.stock_margin_detail_szse(date=day.strftime("%Y%m%d"))
                if df is not None and not df.empty:
                    return df
            except Exception:
                pass
            day -= pd.Timedelta(days=1)
        return pd.DataFrame()

    @staticmethod
    def get_restricted_release(code: str) -> pd.DataFrame:
        """个股限售解禁批次队列"""
        return ak.stock_restricted_release_queue_em(symbol=str(code))

    @staticmethod
    def get_holder_number(code: str) -> pd.DataFrame:
        """个股股东人数变动（筹码集中度代理）"""
        return ak.stock_zh_a_gdhs_detail_em(symbol=str(code))

    @staticmethod
    def get_dividend(code: str) -> pd.DataFrame:
        """个股分红送转历史（新浪）"""
        return ak.stock_history_dividend_detail(symbol=str(code), indicator="分红")

    @staticmethod
    def get_concept_cons(symbol: str) -> pd.DataFrame:
        """概念板块成分股（东方财富概念名，如 '融资融券'）"""
        return ak.stock_board_concept_cons_em(symbol=str(symbol))


# 模块级单例（API 层复用，避免每请求重建缓存管理器）
_GLOBAL_ALT_PROVIDER: AlternativeDataProvider | None = None


def get_alt_data_provider() -> AlternativeDataProvider:
    global _GLOBAL_ALT_PROVIDER
    if _GLOBAL_ALT_PROVIDER is None:
        _GLOBAL_ALT_PROVIDER = AlternativeDataProvider()
    return _GLOBAL_ALT_PROVIDER
