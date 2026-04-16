"""
src/data/scraper/news_scraper.py — 财经新闻抓取器

数据源：
  - ak.stock_info_global_em()  : 东方财富全球财经快讯（时效性最好）
  - ak.stock_news_em(symbol)   : 个股新闻（按股票代码）

支持关键词过滤，便于从噪声中提取用户关心的事件。
"""
from __future__ import annotations

from typing import Iterable

import pandas as pd

from src.data.scraper.base import BaseScraper
from src.utils.exception_handler import retry
from src.utils.logger import get_logger

logger = get_logger("scraper")


class NewsScraper(BaseScraper):
    """
    财经新闻抓取。

    用法：
        scraper = NewsScraper(keywords=["降息", "加息"])
        df = scraper.fetch()          # 全量（带缓存）
        new_df = scraper.fetch_new()  # 与上次对比的增量
    """

    name = "news"
    primary_key = "title"  # 以标题哈希去重

    def __init__(
        self,
        keywords: Iterable[str] | None = None,
        watchlist_codes: Iterable[str] | None = None,
        cache_ttl_hours: int = 1,
        **kwargs,
    ):
        super().__init__(cache_ttl_hours=cache_ttl_hours, **kwargs)
        self.keywords = list(keywords or [])
        self.watchlist_codes = list(watchlist_codes or [])

    @retry(max_attempts=2, delay=2.0)
    def fetch(self, **_) -> pd.DataFrame:
        """拉取全球财经快讯 + watchlist 个股新闻，合并后过滤关键词"""
        cache_key = f"news_global_{pd.Timestamp.now():%Y%m%d%H}"

        def _fetch_global() -> pd.DataFrame:
            try:
                import akshare as ak
                df = ak.stock_info_global_em()
                if df is None or df.empty:
                    return pd.DataFrame()
                # 规范列名（akshare 返回：'标题', '摘要', '发布时间', '链接'）
                return pd.DataFrame({
                    "time": pd.to_datetime(df.get("发布时间"), errors="coerce"),
                    "title": df.get("标题", ""),
                    "summary": df.get("摘要", ""),
                    "url": df.get("链接", ""),
                    "source": "东方财富全球",
                    "code": "",
                })
            except Exception as e:
                logger.warning(f"全球财经快讯拉取失败: {e}")
                return pd.DataFrame()

        global_df = self._cache.get_or_fetch(cache_key, _fetch_global, ttl_hours=self._ttl)

        # watchlist 个股新闻
        stock_frames: list[pd.DataFrame] = []
        for code in self.watchlist_codes:
            stock_frames.append(self._fetch_stock_news(code))

        frames = [df for df in [global_df, *stock_frames] if df is not None and not df.empty]
        if not frames:
            return pd.DataFrame(columns=["time", "title", "summary", "url", "source", "code", "matched_keyword"])

        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(subset=["title"], keep="first")

        # 关键词过滤
        if self.keywords:
            merged = self.filter_by_keywords(merged, ["title", "summary"], self.keywords)
        else:
            merged["matched_keyword"] = ""

        # 按时间倒序
        if "time" in merged.columns:
            merged = merged.sort_values("time", ascending=False, na_position="last").reset_index(drop=True)
        return merged

    def _fetch_stock_news(self, code: str) -> pd.DataFrame:
        cache_key = f"news_stock_{code}_{pd.Timestamp.now():%Y%m%d%H}"

        def _fetch():
            try:
                import akshare as ak
                df = ak.stock_news_em(symbol=code)
                if df is None or df.empty:
                    return pd.DataFrame()
                return pd.DataFrame({
                    "time": pd.to_datetime(df.get("发布时间"), errors="coerce"),
                    "title": df.get("新闻标题", ""),
                    "summary": df.get("新闻内容", ""),
                    "url": df.get("新闻链接", ""),
                    "source": df.get("文章来源", "个股新闻"),
                    "code": code,
                })
            except Exception as e:
                logger.debug(f"个股新闻 {code} 失败: {e}")
                return pd.DataFrame()

        return self._cache.get_or_fetch(cache_key, _fetch, ttl_hours=self._ttl)
