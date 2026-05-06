"""
src/data/scraper/ — 数据抓取模块

暴露 4 个抓取器 + 工厂函数：
  - NewsScraper            : 财经新闻 / 实时热点
  - AnnouncementScraper    : 公司公告
  - HoldingsScraper        : 股东/机构持仓变动
  - ResearchScraper        : 研报 / 机构评级
  - build_scrapers(config) : 从 YAML 构建启用的抓取器
  - run_all(config, output_dir) : 一次性跑所有启用的抓取器并保存 CSV
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data.scrapers.announcement_scraper import AnnouncementScraper
from src.data.scrapers.base import BaseScraper
from src.data.scrapers.holdings_scraper import HoldingsScraper
from src.data.scrapers.news_scraper import NewsScraper
from src.data.scrapers.research_scraper import ResearchScraper
from src.utils.logger import get_logger

logger = get_logger("scraper")


SCRAPER_REGISTRY: dict[str, type[BaseScraper]] = {
    "news": NewsScraper,
    "announcements": AnnouncementScraper,
    "holdings": HoldingsScraper,
    "research": ResearchScraper,
}


def build_scrapers(config: dict) -> dict[str, BaseScraper]:
    """
    从 scraper.yaml 解析结果构建启用的抓取器集合。

    Args:
        config: 形如 {"news": {"enable": true, "keywords": [...]}, ...}

    Returns:
        dict[name, scraper_instance]，未启用的跳过
    """
    scrapers: dict[str, BaseScraper] = {}
    for name, cls in SCRAPER_REGISTRY.items():
        section = (config or {}).get(name) or {}
        if not section.get("enable", False):
            continue
        # 过滤掉构造函数不需要的 key
        kwargs = {k: v for k, v in section.items() if k != "enable"}
        try:
            scrapers[name] = cls(**kwargs)
            logger.info(f"抓取器已启用: {name}")
        except Exception as e:
            logger.error(f"抓取器 [{name}] 初始化失败: {e}", exc_info=True)
    return scrapers


def run_all(
    config: dict,
    output_dir: str | Path = "./output",
    incremental: bool = False,
) -> dict[str, pd.DataFrame]:
    """
    运行所有启用的抓取器并落盘。

    Args:
        config: scraper.yaml 解析结果
        output_dir: CSV 输出根目录
        incremental: True 只返回新增条目（fetch_new），False 返回全量（fetch）

    Returns:
        {name: DataFrame}
    """
    results: dict[str, pd.DataFrame] = {}
    scrapers = build_scrapers(config)
    if not scrapers:
        logger.warning("未启用任何抓取器，scraper.yaml 全部 enable=false")
        return results

    for name, scraper in scrapers.items():
        try:
            df = scraper.fetch_new() if incremental else scraper.fetch()
            results[name] = df
            scraper.save_csv(df, output_dir)
        except Exception as e:
            logger.error(f"[{name}] 抓取执行失败: {e}", exc_info=True)
            results[name] = pd.DataFrame()

    return results


__all__ = [
    "BaseScraper",
    "NewsScraper",
    "AnnouncementScraper",
    "HoldingsScraper",
    "ResearchScraper",
    "SCRAPER_REGISTRY",
    "build_scrapers",
    "run_all",
]
