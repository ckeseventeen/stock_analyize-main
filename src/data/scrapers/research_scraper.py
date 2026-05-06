"""
src/data/scraper/research_scraper.py — 券商研报与机构评级抓取器

数据源：
  - ak.stock_research_report_em(symbol) : 个股研报
输出列：date / code / name / title / rating / target_price / institution / url / [盈利预测 columns]
"""
from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from src.data.scrapers.base import BaseScraper
from src.utils.exception_handler import retry
from src.utils.logger import get_logger

logger = get_logger("scraper")


class ResearchScraper(BaseScraper):
    """
    券商研报抓取。

    用法：
        scraper = ResearchScraper(watchlist=["600519"], rating_filter=["买入", "增持"])
        df = scraper.fetch()
    """

    name = "research"
    primary_key = "title"

    def __init__(
        self,
        watchlist: Iterable[str] | None = None,
        rating_filter: Iterable[str] | None = None,
        cache_ttl_hours: int = 4,
        **kwargs,
    ):
        super().__init__(cache_ttl_hours=cache_ttl_hours, **kwargs)
        self.watchlist = list(watchlist or [])
        self.rating_filter = list(rating_filter) if rating_filter else []

    @retry(max_attempts=2, delay=2.0)
    def fetch(self, **_) -> pd.DataFrame:
        if not self.watchlist:
            logger.info("研报抓取：watchlist 为空，跳过")
            return pd.DataFrame()

        frames = [self._fetch_stock(code) for code in self.watchlist]
        non_empty = [df for df in frames if df is not None and not df.empty]
        if not non_empty:
            return pd.DataFrame(columns=[
                "date", "code", "name", "title", "rating", "target_price", "institution", "url"
            ])

        merged = pd.concat(non_empty, ignore_index=True)

        # 评级过滤
        if self.rating_filter and "rating" in merged.columns:
            mask = merged["rating"].astype(str).apply(
                lambda r: any(k in r for k in self.rating_filter)
            )
            merged = merged[mask]

        merged = merged.drop_duplicates(subset=["title"])

        # 按日期倒序
        if "date" in merged.columns:
            merged = merged.sort_values("date", ascending=False, na_position="last").reset_index(drop=True)

        # 注入名称
        merged = self._inject_names(merged)
        return merged

    def _fetch_stock(self, code: str) -> pd.DataFrame:
        cache_key = f"research_{code}_{pd.Timestamp.now():%Y%m%d}"

        def _fetch():
            try:
                import akshare as ak
                df = ak.stock_research_report_em(symbol=code)
                if df is None or df.empty:
                    return pd.DataFrame()

                # 基础信息映射
                out = pd.DataFrame({
                    "date": pd.to_datetime(df.get("报告日期", df.get("日期")), errors="coerce"),
                    "code": code,
                    "title": df.get("报告名称", ""),
                    "rating": df.get("东财评级", df.get("评级", "")),
                    "target_price": df.get("目标价", ""),
                    "institution": df.get("机构名称", df.get("机构", "")),
                    "url": df.get("报告链接", ""),
                })

                # 动态加入盈利预测列（用户关心的字段补全）
                for col in df.columns:
                    if "盈利预测" in col:
                        out[col] = df[col]
                return out
            except Exception as e:
                logger.debug(f"研报拉取失败 {code}: {e}")
                return pd.DataFrame()

        return self._cache.get_or_fetch(cache_key, _fetch, ttl_hours=self._ttl)
