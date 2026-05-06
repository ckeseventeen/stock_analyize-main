"""
src/data/scraper/holdings_scraper.py — 股东/机构持仓变动抓取器

数据源：
  - ak.stock_hsgt_hold_stock_em()         : 沪深港通北向资金持股（整体排行）
  - ak.stock_gdfx_top_10_em(symbol, date) : 十大流通股东
  - ak.stock_report_fund_hold()           : 基金持仓（重仓股）

输出：合并后的持仓快照，可与历史快照对比检测增减持。
"""
from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from src.data.scrapers.base import BaseScraper
from src.utils.exception_handler import retry
from src.utils.logger import get_logger

logger = get_logger("scraper")


class HoldingsScraper(BaseScraper):
    """
    股东/机构持仓变动抓取。

    用法：
        scraper = HoldingsScraper(watchlist=["600519"], delta_pct_threshold=5)
        df = scraper.fetch()
    """

    name = "holdings"
    primary_key = "key"   # key = code + holder_name

    def __init__(
        self,
        watchlist: Iterable[str] | None = None,
        delta_pct_threshold: float = 5.0,
        cache_ttl_hours: int = 6,
        **kwargs,
    ):
        super().__init__(cache_ttl_hours=cache_ttl_hours, **kwargs)
        self.watchlist = list(watchlist or [])
        # 北向资金变动阈值（百分比）
        self.delta_pct_threshold = delta_pct_threshold

    @retry(max_attempts=2, delay=2.0)
    def fetch(self, **_) -> pd.DataFrame:
        if not self.watchlist:
            logger.info("持仓抓取：watchlist 为空，跳过")
            return pd.DataFrame()

        # 1. 拉北向资金 + 十大流通股东
        north_df = self._fetch_northbound()
        top10_frames = [self._fetch_top10(code) for code in self.watchlist]

        # 2. 拉取近期增减持公告 (解决用户反馈的"减持字段缺失")
        reduction_df = self._fetch_reduction_notices()

        all_frames: list[pd.DataFrame] = []
        if north_df is not None and not north_df.empty:
            all_frames.append(north_df)
        for df in top10_frames:
            if df is not None and not df.empty:
                all_frames.append(df)
        if reduction_df is not None and not reduction_df.empty:
            all_frames.append(reduction_df)

        if not all_frames:
            return pd.DataFrame(columns=["time", "name", "code", "holder", "shares", "pct", "source", "key"])

        merged = pd.concat(all_frames, ignore_index=True)
        # 注入名称
        merged = self._inject_names(merged)

        # 添加 key 列（用于 fetch_new）
        merged["key"] = (
            merged.get("code", "").astype(str)
            + "|"
            + merged.get("holder", "").astype(str)
            + "|"
            + merged.get("time", "").astype(str)
        )
        return merged

    def _fetch_reduction_notices(self) -> pd.DataFrame | None:
        """从公告中提取近期增减持事件"""
        from src.data.scrapers.announcement_scraper import AnnouncementScraper
        # 抓取最近 30 天关于增减持的公告
        s = AnnouncementScraper(watchlist=self.watchlist, types=["减持", "增持", "增持计划", "减持计划"])
        df = s.fetch()
        if df is None or df.empty:
            return pd.DataFrame()

        # 转换为 holdings 格式
        return pd.DataFrame({
            "time": df["time"],
            "code": df["code"],
            "holder": "增减持公告: " + df["title"].str[:20] + "...",
            "shares": 0,
            "pct": 0,
            "source": "官方披露",
        })

    def _fetch_northbound(self) -> pd.DataFrame | None:
        """北向资金持仓（全市场，按 watchlist 过滤）"""
        cache_key = f"northbound_{pd.Timestamp.now():%Y%m%d}"

        def _fetch():
            try:
                import akshare as ak
                # indicator 支持: 今日持股 / 3日排行 / 5日排行 等
                df = ak.stock_hsgt_hold_stock_em(market="北向", indicator="今日持股")
                if df is None or df.empty:
                    return pd.DataFrame()
                # akshare 字段：代码、名称、今日持股-股数、今日持股-市值、今日持股-占流通股比、今日持股-占总股本比
                out = pd.DataFrame({
                    "time": pd.Timestamp.now(),
                    "code": df.get("代码", "").astype(str),
                    "holder": "北向资金",
                    "shares": df.get("今日持股-股数"),
                    "pct": df.get("今日持股-占流通股比"),
                    "source": "沪深港通",
                })
                # 只保留 watchlist
                if self.watchlist:
                    out = out[out["code"].isin(self.watchlist)]
                return out
            except Exception as e:
                logger.debug(f"北向资金拉取失败: {e}")
                return pd.DataFrame()

        return self._cache.get_or_fetch(cache_key, _fetch, ttl_hours=self._ttl)

    def _fetch_top10(self, code: str) -> pd.DataFrame | None:
        """个股十大流通股东"""
        cache_key = f"top10_{code}_{pd.Timestamp.now():%Y%m}"

        def _fetch():
            try:
                import akshare as ak
                # akshare: date 是报告期（如 '20231231'）
                df = ak.stock_gdfx_free_top_10_em(symbol=f"sh{code}" if code.startswith("6") else f"sz{code}")
                if df is None or df.empty:
                    return pd.DataFrame()
                return pd.DataFrame({
                    "time": pd.to_datetime(df.get("截止日期"), errors="coerce"),
                    "code": code,
                    "holder": df.get("股东名称", ""),
                    "shares": df.get("持股数", df.get("持股数量", 0)),
                    "pct": df.get("占总流通股本持股比例", df.get("持股比例", 0)),
                    "source": "十大流通股东",
                })
            except Exception as e:
                logger.debug(f"十大股东拉取失败 {code}: {e}")
                return pd.DataFrame()

        return self._cache.get_or_fetch(cache_key, _fetch, ttl_hours=self._ttl)
