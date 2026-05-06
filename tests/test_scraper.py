"""
tests/test_scraper.py — 抓取器单元测试

通过 monkeypatch akshare 模块，隔离网络依赖。
"""
from __future__ import annotations

import os
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data.scrapers import (
    AnnouncementScraper,
    HoldingsScraper,
    NewsScraper,
    ResearchScraper,
    build_scrapers,
    run_all,
)
from src.data.scrapers.base import BaseScraper


# ========================
# BaseScraper 辅助
# ========================

class _DummyScraper(BaseScraper):
    name = "dummy"
    primary_key = "id"

    def __init__(self, data: pd.DataFrame, **kwargs):
        super().__init__(**kwargs)
        self._data = data

    def fetch(self, **_) -> pd.DataFrame:
        return self._data.copy()


@pytest.mark.unit
class TestBaseScraper:
    def test_fetch_new_returns_all_on_first_call(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        scraper = _DummyScraper(df, cache_dir=str(tmp_path / "c"), seen_path=tmp_path / "seen.json")
        new_df = scraper.fetch_new()
        assert len(new_df) == 3

    def test_fetch_new_detects_increments(self, tmp_path):
        """第二次调用仅返回新增条目"""
        df1 = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        s1 = _DummyScraper(df1, cache_dir=str(tmp_path / "c"), seen_path=tmp_path / "seen.json")
        s1.fetch_new()  # 记录 1, 2

        df2 = pd.DataFrame({"id": [1, 2, 3, 4], "value": ["a", "b", "c", "d"]})
        s2 = _DummyScraper(df2, cache_dir=str(tmp_path / "c"), seen_path=tmp_path / "seen.json")
        new_df = s2.fetch_new()
        assert len(new_df) == 2
        assert set(new_df["id"]) == {3, 4}

    def test_filter_by_keywords(self):
        df = pd.DataFrame({
            "title": ["央行宣布降息 0.25%", "苹果发布新品", "监管部门约谈平台"],
            "summary": ["利好股市", "无", "加强合规"],
        })
        out = BaseScraper.filter_by_keywords(df, ["title", "summary"], ["降息", "监管"])
        assert len(out) == 2
        assert "matched_keyword" in out.columns

    def test_filter_empty_keywords(self):
        df = pd.DataFrame({"title": ["a", "b"]})
        out = BaseScraper.filter_by_keywords(df, ["title"], [])
        # 空关键词不过滤
        assert len(out) == 2

    def test_save_csv(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2], "v": ["a", "b"]})
        scraper = _DummyScraper(df, cache_dir=str(tmp_path / "c"), seen_path=tmp_path / "seen.json")
        path = scraper.save_csv(df, tmp_path)
        assert path is not None
        assert path.exists()


# ========================
# 4 个 Scraper（mock akshare）
# ========================

@pytest.fixture
def fake_akshare(monkeypatch):
    """构造一个假的 akshare 模块，注入 sys.modules"""
    fake = types.ModuleType("akshare")

    # stock_info_global_em: 全球快讯
    fake.stock_info_global_em = lambda: pd.DataFrame({
        "标题": ["央行降息 0.25%", "苹果发布新品", "某股东减持 5%"],
        "摘要": ["对债券利好", "科技产品", "重大事项"],
        "发布时间": ["2026-04-16 09:00", "2026-04-16 10:00", "2026-04-16 11:00"],
        "链接": ["http://a", "http://b", "http://c"],
    })
    fake.stock_news_em = lambda symbol: pd.DataFrame({
        "新闻标题": [f"{symbol} 公司动态 1"],
        "新闻内容": ["内容"],
        "发布时间": ["2026-04-16 12:00"],
        "新闻链接": ["http://x"],
        "文章来源": ["eastmoney"],
    })

    # 公告
    fake.stock_notice_report = lambda symbol: pd.DataFrame({
        "代码": [symbol, symbol],
        "名称": ["测试", "测试"],
        "公告标题": [f"{symbol} 2024 年度报告", f"{symbol} 关于股东减持计划的公告"],
        "公告类型": ["财务报告", "重大事项"],
        "公告日期": ["2026-04-15", "2026-04-16"],
        "网址": ["http://a", "http://b"],
    })

    # 北向资金
    fake.stock_hsgt_hold_stock_em = lambda market, indicator: pd.DataFrame({
        "代码": ["600519", "000001", "002156"],
        "名称": ["贵州茅台", "平安银行", "通富微电"],
        "今日持股-股数": [1000, 2000, 500],
        "今日持股-占流通股比": [3.5, 4.2, 1.1],
    })
    fake.stock_gdfx_free_top_10_em = lambda symbol: pd.DataFrame({
        "股东名称": ["张三", "李四"],
        "持股数": [1000000, 500000],
        "占总流通股本持股比例": [2.5, 1.2],
        "截止日期": ["2026-03-31", "2026-03-31"],
    })

    # 研报
    fake.stock_research_report_em = lambda symbol: pd.DataFrame({
        "报告名称": [f"{symbol} 深度报告", f"{symbol} 跟踪报告"],
        "东财评级": ["买入", "持有"],
        "目标价": [100.0, 80.0],
        "机构名称": ["券商A", "券商B"],
        "报告日期": ["2026-04-15", "2026-04-10"],
        "报告链接": ["http://r1", "http://r2"],
    })

    monkeypatch.setitem(sys.modules, "akshare", fake)
    return fake


@pytest.mark.unit
class TestNewsScraper:
    def test_fetch_with_keyword_filter(self, fake_akshare, tmp_path):
        s = NewsScraper(
            keywords=["降息"],
            cache_dir=str(tmp_path / "c"),
            seen_path=tmp_path / "s.json",
        )
        df = s.fetch()
        assert len(df) >= 1
        assert "matched_keyword" in df.columns
        assert all("降息" in m for m in df["matched_keyword"])

    def test_fetch_no_keyword_returns_all(self, fake_akshare, tmp_path):
        s = NewsScraper(cache_dir=str(tmp_path / "c"), seen_path=tmp_path / "s.json")
        df = s.fetch()
        # 3 条全球 + 0 个股（无 watchlist）
        assert len(df) == 3


@pytest.mark.unit
class TestAnnouncementScraper:
    def test_fetch_filters_by_type(self, fake_akshare, tmp_path):
        s = AnnouncementScraper(
            watchlist=["600519"],
            types=["财务报告"],
            cache_dir=str(tmp_path / "c"),
            seen_path=tmp_path / "s.json",
        )
        df = s.fetch()
        assert len(df) == 1
        assert df.iloc[0]["type"] == "财务报告"

    def test_empty_watchlist(self, fake_akshare, tmp_path):
        s = AnnouncementScraper(cache_dir=str(tmp_path / "c"), seen_path=tmp_path / "s.json")
        df = s.fetch()
        assert df.empty


@pytest.mark.unit
class TestHoldingsScraper:
    def test_fetch_filters_watchlist(self, fake_akshare, tmp_path):
        s = HoldingsScraper(
            watchlist=["600519"],
            cache_dir=str(tmp_path / "c"),
            seen_path=tmp_path / "s.json",
        )
        df = s.fetch()
        # 北向资金 1 条（600519）+ 十大流通股东 2 条
        assert len(df) >= 2
        assert all(df["code"].astype(str).isin(["600519"]))


@pytest.mark.unit
class TestResearchScraper:
    def test_fetch_filters_rating(self, fake_akshare, tmp_path):
        s = ResearchScraper(
            watchlist=["600519"],
            rating_filter=["买入"],
            cache_dir=str(tmp_path / "c"),
            seen_path=tmp_path / "s.json",
        )
        df = s.fetch()
        assert len(df) == 1
        assert "买入" in df.iloc[0]["rating"]

    def test_fetch_no_filter(self, fake_akshare, tmp_path):
        s = ResearchScraper(
            watchlist=["600519"],
            cache_dir=str(tmp_path / "c"),
            seen_path=tmp_path / "s.json",
        )
        df = s.fetch()
        assert len(df) == 2


# ========================
# build_scrapers / run_all
# ========================

@pytest.mark.unit
class TestFactoryAndRunner:
    def test_build_only_enabled(self, fake_akshare, tmp_path):
        cfg = {
            "news": {"enable": True, "cache_dir": str(tmp_path / "n"), "seen_path": str(tmp_path / "ns.json")},
            "announcements": {"enable": False, "watchlist": ["600519"]},
            "holdings": {"enable": True, "watchlist": ["600519"], "cache_dir": str(tmp_path / "h"), "seen_path": str(tmp_path / "hs.json")},
            "research": {"enable": False},
        }
        scrapers = build_scrapers(cfg)
        assert set(scrapers.keys()) == {"news", "holdings"}

    def test_run_all_saves_csv(self, fake_akshare, tmp_path):
        cfg = {
            "news": {"enable": True, "cache_dir": str(tmp_path / "n"), "seen_path": str(tmp_path / "ns.json")},
        }
        results = run_all(cfg, output_dir=tmp_path)
        assert "news" in results
        assert not results["news"].empty
        # CSV 应被保存到 output/scraper/news_*.csv
        scraper_dir = tmp_path / "scraper"
        assert scraper_dir.exists()
        assert any(f.name.startswith("news_") for f in scraper_dir.iterdir())
