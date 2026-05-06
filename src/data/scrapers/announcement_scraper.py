"""
src/data/scraper/announcement_scraper.py — 公司公告抓取器

数据源（优先级）：
  - ak.stock_notice_report(symbol, date)  : 东方财富公告中心
  - 备用：ak.stock_zh_a_disclosure_report_cninfo() — 巨潮资讯网

按股票代码 + 公告类型过滤。
"""
from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from src.data.scrapers.base import BaseScraper
from src.utils.exception_handler import retry
from src.utils.logger import get_logger

logger = get_logger("scraper")


# 常见公告类型（可在配置中指定子集）
DEFAULT_TYPES = ["财务报告", "重大事项", "资产重组", "股权激励", "监管问询"]


class AnnouncementScraper(BaseScraper):
    """
    公司公告抓取。

    用法：
        scraper = AnnouncementScraper(watchlist=["600519"], types=["财务报告"])
        new_announcements = scraper.fetch_new()
    """

    name = "announcements"
    primary_key = "title"

    def __init__(
        self,
        watchlist: Iterable[str] | None = None,
        types: Iterable[str] | None = None,
        cache_ttl_hours: int = 2,
        **kwargs,
    ):
        super().__init__(cache_ttl_hours=cache_ttl_hours, **kwargs)
        self.watchlist = list(watchlist or [])
        self.types = list(types) if types else DEFAULT_TYPES.copy()

    @retry(max_attempts=2, delay=2.0)
    def fetch(self, **_) -> pd.DataFrame:
        if not self.watchlist:
            logger.info("公告抓取：watchlist 为空，跳过")
            return pd.DataFrame()

        frames = []
        today = pd.Timestamp.now().strftime("%Y%m%d")
        for code in self.watchlist:
            frames.append(self._fetch_stock(code, today))

        non_empty = [df for df in frames if df is not None and not df.empty]
        if not non_empty:
            return pd.DataFrame(columns=["time", "name", "code", "title", "type", "url"])

        merged = pd.concat(non_empty, ignore_index=True)

        # 按公告类型过滤（模糊匹配，不区分大小写）
        if self.types and "type" in merged.columns:
            lower_types = [str(k).lower().strip() for k in self.types if k]
            mask = merged["type"].astype(str).str.lower().apply(
                lambda t: any(k in t for k in lower_types)
            )
            merged = merged[mask]

        merged = merged.drop_duplicates(subset=["title"])
        if "time" in merged.columns:
            merged = merged.sort_values("time", ascending=False, na_position="last").reset_index(drop=True)

        # 注入名称
        merged = self._inject_names(merged)
        return merged

    def _fetch_stock(self, code: str, today: str) -> pd.DataFrame:
        cache_key = f"notice_{code}_{today}"

        def _fetch():
            try:
                import akshare as ak
                df = ak.stock_notice_report(symbol=code)
                if df is None or df.empty:
                    return pd.DataFrame()
                # akshare 返回列示例：代码、名称、公告标题、公告类型、公告日期、网址
                return pd.DataFrame({
                    "time": pd.to_datetime(df.get("公告日期"), errors="coerce"),
                    "code": df.get("代码", code),
                    "name": df.get("名称", ""),
                    "title": df.get("公告标题", ""),
                    "type": df.get("公告类型", ""),
                    "url": df.get("网址", ""),
                })
            except Exception as e:
                logger.debug(f"公告拉取失败 {code}: {e}")
                return pd.DataFrame()

        return self._cache.get_or_fetch(cache_key, _fetch, ttl_hours=self._ttl)
