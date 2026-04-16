"""
tests/test_earnings_monitor.py — 财报披露监控单元测试

通过 mock EarningsFetcher，避免访问真实网络。
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.automation.alert import AlertStateStore, ConsoleChannel
from src.automation.monitor.earnings_monitor import EarningsMonitor
from src.data.fetcher.earnings_fetcher import COLUMNS


def _make_row(code, name, market, event_type, days_from_today, period="2024-12-31", extra=""):
    return {
        "code": code,
        "name": name,
        "market": market,
        "event_type": event_type,
        "disclose_date": pd.Timestamp(datetime.now().date() + timedelta(days=days_from_today)),
        "report_period": period,
        "extra": extra,
    }


@pytest.fixture
def tmp_out(tmp_path):
    return tmp_path


@pytest.fixture
def mock_fetcher():
    """返回 MagicMock 实例，可为每个方法设置返回值"""
    m = MagicMock()
    m.get_a_share_upcoming.return_value = pd.DataFrame(columns=COLUMNS)
    m.get_hk_upcoming.return_value = pd.DataFrame(columns=COLUMNS)
    m.get_us_upcoming.return_value = pd.DataFrame(columns=COLUMNS)
    return m


@pytest.mark.unit
class TestEarningsMonitor:
    def test_no_events_when_empty(self, mock_fetcher, tmp_out):
        """全部市场无数据 → 无事件"""
        monitor = EarningsMonitor(
            config={"watchlist": {"a": ["600519"]}, "remind_days_ahead": 3},
            channels=[ConsoleChannel({"enable": True})],
            state_store=AlertStateStore(tmp_out / "s.json"),
            output_dir=str(tmp_out),
            fetcher=mock_fetcher,
        )
        result = monitor.run()
        assert result["total"] == 0

    def test_a_share_reminder_triggers(self, mock_fetcher, tmp_out):
        """A 股披露日在 3 天内 → 生成提醒事件"""
        mock_fetcher.get_a_share_upcoming.return_value = pd.DataFrame([
            _make_row("600519", "贵州茅台", "a", "业绩报告", days_from_today=2),
        ])
        monitor = EarningsMonitor(
            config={"watchlist": {"a": ["600519"]}, "remind_days_ahead": 3},
            channels=[ConsoleChannel({"enable": True})],
            state_store=AlertStateStore(tmp_out / "s.json"),
            output_dir=str(tmp_out),
            fetcher=mock_fetcher,
        )
        result = monitor.run()
        assert result["total"] == 1
        # 日历 CSV 应落盘
        assert (tmp_out / "earnings_calendar.csv").exists()

    def test_a_share_too_far_no_reminder(self, mock_fetcher, tmp_out):
        """A 股披露日超 remind_days_ahead，且不是业绩预告 → 不触发"""
        mock_fetcher.get_a_share_upcoming.return_value = pd.DataFrame([
            _make_row("600519", "贵州茅台", "a", "业绩报告", days_from_today=10),
        ])
        monitor = EarningsMonitor(
            config={"watchlist": {"a": ["600519"]}, "remind_days_ahead": 3},
            channels=[ConsoleChannel({"enable": True})],
            state_store=AlertStateStore(tmp_out / "s.json"),
            output_dir=str(tmp_out),
            fetcher=mock_fetcher,
        )
        result = monitor.run()
        assert result["total"] == 0

    def test_forecast_always_triggers(self, mock_fetcher, tmp_out):
        """业绩预告即使远期也应告警（track_forecasts=True）"""
        mock_fetcher.get_a_share_upcoming.return_value = pd.DataFrame([
            _make_row("600519", "贵州茅台", "a", "业绩预告", days_from_today=25),
        ])
        monitor = EarningsMonitor(
            config={"watchlist": {"a": ["600519"]}, "remind_days_ahead": 3, "track_forecasts": True},
            channels=[ConsoleChannel({"enable": True})],
            state_store=AlertStateStore(tmp_out / "s.json"),
            output_dir=str(tmp_out),
            fetcher=mock_fetcher,
        )
        result = monitor.run()
        assert result["total"] == 1

    def test_forecast_disabled(self, mock_fetcher, tmp_out):
        """track_forecasts=False 时，远期业绩预告不触发"""
        mock_fetcher.get_a_share_upcoming.return_value = pd.DataFrame([
            _make_row("600519", "贵州茅台", "a", "业绩预告", days_from_today=25),
        ])
        monitor = EarningsMonitor(
            config={"watchlist": {"a": ["600519"]}, "remind_days_ahead": 3, "track_forecasts": False},
            channels=[ConsoleChannel({"enable": True})],
            state_store=AlertStateStore(tmp_out / "s.json"),
            output_dir=str(tmp_out),
            fetcher=mock_fetcher,
        )
        result = monitor.run()
        assert result["total"] == 0

    def test_filter_by_watchlist(self, mock_fetcher, tmp_out):
        """A 股全市场返回多条，只保留 watchlist 中的"""
        mock_fetcher.get_a_share_upcoming.return_value = pd.DataFrame([
            _make_row("600519", "贵州茅台", "a", "业绩报告", 2),
            _make_row("000002", "万科A", "a", "业绩报告", 2),
        ])
        monitor = EarningsMonitor(
            config={"watchlist": {"a": ["600519"]}, "remind_days_ahead": 3},
            channels=[ConsoleChannel({"enable": True})],
            state_store=AlertStateStore(tmp_out / "s.json"),
            output_dir=str(tmp_out),
            fetcher=mock_fetcher,
        )
        result = monitor.run()
        assert result["total"] == 1
        assert "600519" in result["events"][0]["event_key"]

    def test_multi_market(self, mock_fetcher, tmp_out):
        """三个市场各一条 → 共 3 事件"""
        mock_fetcher.get_a_share_upcoming.return_value = pd.DataFrame([
            _make_row("600519", "贵州茅台", "a", "业绩报告", 2),
        ])
        mock_fetcher.get_hk_upcoming.return_value = pd.DataFrame([
            _make_row("00700", "腾讯", "hk", "推导披露", 2),
        ])
        mock_fetcher.get_us_upcoming.return_value = pd.DataFrame([
            _make_row("AAPL", "AAPL", "us", "财报日", 2),
        ])
        monitor = EarningsMonitor(
            config={
                "watchlist": {"a": ["600519"], "hk": ["00700"], "us": ["AAPL"]},
                "remind_days_ahead": 3,
            },
            channels=[ConsoleChannel({"enable": True})],
            state_store=AlertStateStore(tmp_out / "s.json"),
            output_dir=str(tmp_out),
            fetcher=mock_fetcher,
        )
        result = monitor.run()
        assert result["total"] == 3


# ========================
# EarningsFetcher 辅助函数测试
# ========================

from src.data.fetcher.earnings_fetcher import _parse_ashare_df, _recent_quarter_codes


@pytest.mark.unit
class TestEarningsFetcherHelpers:
    def test_recent_quarter_codes(self):
        """最近 n 个季度代码应以季末日格式返回"""
        codes = _recent_quarter_codes(3)
        assert len(codes) == 3
        for c in codes:
            assert len(c) == 8
            assert c[-4:] in ("0331", "0630", "0930", "1231")

    def test_parse_ashare_yjyg(self):
        """解析业绩预告数据"""
        raw = pd.DataFrame({
            "代码": ["600519", "000001"],
            "名称": ["贵州茅台", "平安银行"],
            "预告日期": ["2026-01-15", "2026-02-01"],
            "预告类型": ["预增", "预减"],
            "预告净利润变动幅度": ["50%", "-20%"],
        })
        out = _parse_ashare_df(raw, "业绩预告", "预告日期", "20251231")
        assert len(out) == 2
        assert "code" in out.columns
        assert out.iloc[0]["code"] == "600519"
        assert out.iloc[0]["event_type"] == "业绩预告"

    def test_parse_empty_df(self):
        """空输入应返回空 DataFrame"""
        out = _parse_ashare_df(pd.DataFrame(), "业绩预告", "预告日期", "20251231")
        assert out.empty
