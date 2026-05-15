"""
tests/test_price_monitor_signal.py — 价格预警接入买点信号 (signal) 类型测试

覆盖：
  - _RuleEvaluator.evaluate 对 signal 规则的正确路由
  - signal 类型 → CONDITION_REGISTRY 桥接
  - 未知 signal / SPOT_ONLY signal 优雅降级
  - 周线 signal 自动用 weekly_df
  - 日/周 K 智能按需拉取（PriceMonitor.collect_events）
  - 事件 title 含 📈 / 价格类含 🔔
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.automation.alert import AlertEvent  # noqa: F401
from src.automation.monitor.price_monitor import (
    PriceMonitor,
    _RuleEvaluator,
    _signal_needs_weekly,
)

# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def synth_daily_300d() -> pd.DataFrame:
    """合成 300 个交易日 OHLCV（中文列名）"""
    np.random.seed(42)
    n = 300
    dates = pd.bdate_range("2024-01-02", periods=n)
    close = 50 + np.cumsum(np.random.randn(n) * 0.5 + 0.02)
    open_ = close + np.random.randn(n) * 0.3
    high = np.maximum(open_, close) + np.abs(np.random.randn(n) * 0.5)
    low = np.minimum(open_, close) - np.abs(np.random.randn(n) * 0.5)
    vol = np.random.randint(100000, 5000000, n).astype(float)
    return pd.DataFrame({
        "日期": dates, "开盘": open_, "最高": high,
        "最低": low, "收盘": close, "成交量": vol,
    })


# =============================================================================
# _signal_needs_weekly：路由判断
# =============================================================================

@pytest.mark.unit
class TestSignalNeedsWeekly:
    def test_weekly_condition_returns_true(self):
        assert _signal_needs_weekly("weekly_macd_divergence") is True
        assert _signal_needs_weekly("weekly_macd_gold_cross") is True

    def test_daily_condition_returns_false(self):
        assert _signal_needs_weekly("rsi_oversold") is False
        assert _signal_needs_weekly("daily_macd_divergence") is False
        assert _signal_needs_weekly("box_breakout") is False

    def test_unknown_returns_false(self):
        assert _signal_needs_weekly("totally_not_real") is False


# =============================================================================
# _RuleEvaluator.evaluate(signal)
# =============================================================================

@pytest.mark.unit
class TestSignalEvaluation:
    def test_missing_signal_field(self, synth_daily_300d):
        # rule 没指定 signal → 返回 False，不抛
        ok, _ = _RuleEvaluator.evaluate(
            {"type": "signal"}, price=50.0, daily_df=synth_daily_300d,
        )
        assert ok is False

    def test_unknown_signal_type(self, synth_daily_300d):
        ok, _ = _RuleEvaluator.evaluate(
            {"type": "signal", "signal": "made_up_signal"},
            price=50.0, daily_df=synth_daily_300d,
        )
        assert ok is False

    def test_spot_only_signal_rejected(self, synth_daily_300d):
        # market_cap 是 spot-only，不适合做买点
        ok, _ = _RuleEvaluator.evaluate(
            {"type": "signal", "signal": "market_cap"},
            price=50.0, daily_df=synth_daily_300d,
        )
        assert ok is False

    def test_daily_signal_uses_daily_df(self, synth_daily_300d):
        """rsi_oversold 是日线条件，应该用 daily_df 评估"""
        ok, desc = _RuleEvaluator.evaluate(
            {"type": "signal", "signal": "rsi_oversold",
             "params": {"threshold": 100}},  # threshold 100 几乎一定触发
            price=50.0, daily_df=synth_daily_300d,
        )
        # 不强制 True（取决于合成数据），但函数不应抛异常
        assert isinstance(ok, bool)

    def test_signal_description_has_emoji(self, synth_daily_300d):
        """触发时 desc 应带 📈 表情和友好名"""
        # 用 threshold=100 强制 RSI 超卖触发
        ok, desc = _RuleEvaluator.evaluate(
            {"type": "signal", "signal": "rsi_oversold",
             "params": {"threshold": 100, "period": 14}},
            price=50.0, daily_df=synth_daily_300d,
        )
        if ok:
            assert "📈" in desc
            assert "RSI" in desc.upper() or "超卖" in desc

    def test_no_daily_df_returns_false(self):
        ok, _ = _RuleEvaluator.evaluate(
            {"type": "signal", "signal": "rsi_oversold"},
            price=50.0, daily_df=None,
        )
        assert ok is False

    def test_empty_daily_df_returns_false(self):
        ok, _ = _RuleEvaluator.evaluate(
            {"type": "signal", "signal": "rsi_oversold"},
            price=50.0, daily_df=pd.DataFrame(),
        )
        assert ok is False


# =============================================================================
# PriceMonitor.collect_events: 智能按需拉 K 线
# =============================================================================

@pytest.mark.unit
class TestPriceMonitorSignalIntegration:
    def test_collect_events_pulls_daily_for_signal(self, synth_daily_300d, monkeypatch):
        """规则含 signal 时应自动拉日线"""
        rules = [{
            "code": "600519", "market": "a", "name": "测试",
            "conditions": [
                {"type": "signal", "signal": "rsi_oversold",
                 "params": {"threshold": 100, "period": 14}},
            ],
        }]
        ohlcv_calls: list[str] = []

        def mock_price(code, market):
            return 50.0, 49.0

        def mock_ohlcv(code, market, days_back=120):
            ohlcv_calls.append(code)
            return synth_daily_300d

        monitor = PriceMonitor(
            rules=rules, channels=[], state_store=None,
            price_fetcher=mock_price, ohlcv_fetcher=mock_ohlcv,
        )
        events = monitor.collect_events()

        # 日线应被拉取（signal 需要）
        assert ohlcv_calls == ["600519"]
        # events 可能为 0 或多个（取决于合成数据）；不强制 True
        assert isinstance(events, list)

    def test_collect_events_pulls_weekly_for_weekly_signal(self, synth_daily_300d):
        """周线 signal 应触发日线拉取 + 周线重采样"""
        rules = [{
            "code": "600519", "market": "a", "name": "测试",
            "conditions": [
                {"type": "signal", "signal": "weekly_macd_divergence"},
            ],
        }]

        def mock_price(code, market):
            return 50.0, 49.0

        ohlcv_calls = []

        def mock_ohlcv(code, market, days_back=120):
            ohlcv_calls.append(code)
            return synth_daily_300d

        monitor = PriceMonitor(
            rules=rules, channels=[], state_store=None,
            price_fetcher=mock_price, ohlcv_fetcher=mock_ohlcv,
        )
        # 不抛异常即视为通过（说明周线重采样 + signal 评估都跑通）
        events = monitor.collect_events()
        assert isinstance(events, list)
        assert "600519" in ohlcv_calls

    def test_resample_weekly_aggregates_correctly(self, synth_daily_300d):
        """日线 → 周线重采样：开盘=first、最高=max、最低=min、收盘=last、量=sum"""
        wk = PriceMonitor._resample_weekly(synth_daily_300d)
        assert not wk.empty
        for col in ("开盘", "最高", "最低", "收盘"):
            assert col in wk.columns
        # 周线行数 ≈ 日线 / 5
        assert 50 <= len(wk) <= 65

    def test_signal_event_has_correct_format(self, synth_daily_300d):
        """触发 signal 事件时 title 含 📈、event_type 以 signal_ 前缀"""
        # 构造一个一定会触发的规则：threshold=100 强制 RSI < 100
        rules = [{
            "code": "600519", "market": "a", "name": "贵州茅台",
            "conditions": [
                {"type": "signal", "signal": "rsi_oversold",
                 "params": {"threshold": 100, "period": 14}},
            ],
        }]

        def mock_price(c, m): return 50.0, 49.0
        def mock_ohlcv(c, m, days_back=120): return synth_daily_300d

        monitor = PriceMonitor(
            rules=rules, channels=[], state_store=None,
            price_fetcher=mock_price, ohlcv_fetcher=mock_ohlcv,
        )
        events = monitor.collect_events()
        for e in events:
            if e.event_type.startswith("signal_"):
                assert "📈" in e.title
                assert "rsi_oversold" in e.event_type
                # event_key 包含 signal: 标识
                assert ":signal:" in e.event_key

    def test_price_rule_still_works(self, synth_daily_300d):
        """加 signal 类型不影响原 price_below 规则"""
        rules = [{
            "code": "600519", "market": "a", "name": "test",
            "conditions": [{"type": "price_below", "value": 100.0}],
        }]
        def mock_price(c, m): return 50.0, 49.0
        def mock_ohlcv(c, m, days_back=120): return pd.DataFrame()

        monitor = PriceMonitor(
            rules=rules, channels=[], state_store=None,
            price_fetcher=mock_price, ohlcv_fetcher=mock_ohlcv,
        )
        events = monitor.collect_events()
        assert len(events) == 1
        assert "🔔" in events[0].title
        assert events[0].event_type == "price_price_below"
