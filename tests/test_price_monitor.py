"""
tests/test_price_monitor.py — 价格预警单元测试

通过 mock price_fetcher / ohlcv_fetcher，避免访问真实网络。
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.automation.alert import AlertStateStore, ConsoleChannel
from src.automation.monitor.price_monitor import PriceMonitor, _RuleEvaluator


# ========================
# 规则求值器测试
# ========================

@pytest.mark.unit
class TestRuleEvaluator:
    def test_price_below_triggers(self):
        ok, _ = _RuleEvaluator.evaluate({"type": "price_below", "value": 1500}, price=1450)
        assert ok is True

    def test_price_below_not_triggers(self):
        ok, _ = _RuleEvaluator.evaluate({"type": "price_below", "value": 1500}, price=1600)
        assert ok is False

    def test_price_above_triggers(self):
        ok, _ = _RuleEvaluator.evaluate({"type": "price_above", "value": 100}, price=120)
        assert ok is True

    def test_pct_change_drop(self):
        """当日跌幅超 5%"""
        ok, _ = _RuleEvaluator.evaluate(
            {"type": "pct_change_daily", "threshold": -5},
            price=95, prev_close=100,
        )
        assert ok is True

    def test_pct_change_rise(self):
        """当日涨幅超 +3%"""
        ok, _ = _RuleEvaluator.evaluate(
            {"type": "pct_change_daily", "threshold": 3},
            price=105, prev_close=100,
        )
        assert ok is True

    def test_pct_change_not_triggered(self):
        ok, _ = _RuleEvaluator.evaluate(
            {"type": "pct_change_daily", "threshold": -5},
            price=98, prev_close=100,
        )
        assert ok is False

    def test_pct_from_cost(self):
        ok, msg = _RuleEvaluator.evaluate(
            {"type": "pct_from_cost", "threshold": 20, "cost": 100},
            price=125, prev_close=None, daily_df=None, cost_basis=100,
        )
        assert ok is True
        assert "涨" in msg

    def test_ma_break_below(self):
        """构造 30 日收盘价均为 100，价格 90 → 跌破"""
        df = pd.DataFrame({"收盘": [100.0] * 30})
        ok, _ = _RuleEvaluator.evaluate(
            {"type": "ma_break", "ma": 20, "direction": "below"},
            price=90, prev_close=None, daily_df=df,
        )
        assert ok is True

    def test_ma_break_above(self):
        df = pd.DataFrame({"收盘": [100.0] * 30})
        ok, _ = _RuleEvaluator.evaluate(
            {"type": "ma_break", "ma": 20, "direction": "above"},
            price=110, prev_close=None, daily_df=df,
        )
        assert ok is True

    def test_ma_break_insufficient_data(self):
        """数据不足时不触发"""
        df = pd.DataFrame({"收盘": [100.0] * 5})
        ok, _ = _RuleEvaluator.evaluate(
            {"type": "ma_break", "ma": 20, "direction": "below"},
            price=90, prev_close=None, daily_df=df,
        )
        assert ok is False

    def test_unknown_rule_type(self):
        ok, _ = _RuleEvaluator.evaluate({"type": "not_exists"}, price=100)
        assert ok is False


# ========================
# PriceMonitor 集成测试
# ========================

@pytest.fixture
def mock_fetchers():
    """返回可覆盖的 fetcher 字典"""
    return {
        "prices": {},          # {code: (price, prev_close)}
        "ohlcv": {},           # {code: DataFrame}
    }


def _make_monitor(rules, mock, tmp_path):
    """辅助构造 PriceMonitor（注入 mock fetcher）"""
    def price_fn(code, market):
        return mock["prices"].get(code, (0.0, None))

    def ohlcv_fn(code, market, days_back=120):
        return mock["ohlcv"].get(code, pd.DataFrame())

    store = AlertStateStore(tmp_path / "state.json")
    console = ConsoleChannel({"enable": True})
    return PriceMonitor(
        rules=rules,
        channels=[console],
        state_store=store,
        cooldown_hours=1,
        output_dir=str(tmp_path),
        price_fetcher=price_fn,
        ohlcv_fetcher=ohlcv_fn,
    )


@pytest.mark.unit
class TestPriceMonitor:
    def test_run_triggers_event(self, mock_fetchers, tmp_path):
        """单一规则触发应生成 1 个事件并推送成功"""
        mock_fetchers["prices"]["600519"] = (1450.0, 1500.0)
        rules = [{
            "code": "600519", "market": "a", "name": "贵州茅台",
            "conditions": [{"type": "price_below", "value": 1500}],
        }]
        monitor = _make_monitor(rules, mock_fetchers, tmp_path)
        result = monitor.run()
        assert result["total"] == 1
        assert result["sent"] == 1
        # CSV 应落盘
        assert (tmp_path / "price_monitor_events.csv").exists()

    def test_run_no_trigger(self, mock_fetchers, tmp_path):
        """条件不满足时不生成事件"""
        mock_fetchers["prices"]["600519"] = (1600.0, 1500.0)
        rules = [{
            "code": "600519", "market": "a", "name": "贵州茅台",
            "conditions": [{"type": "price_below", "value": 1500}],
        }]
        monitor = _make_monitor(rules, mock_fetchers, tmp_path)
        result = monitor.run()
        assert result["total"] == 0

    def test_cooldown_deduplicates(self, mock_fetchers, tmp_path):
        """同事件第二次执行应被冷却跳过"""
        mock_fetchers["prices"]["600519"] = (1450.0, 1500.0)
        rules = [{
            "code": "600519", "market": "a", "name": "贵州茅台",
            "conditions": [{"type": "price_below", "value": 1500}],
        }]
        monitor = _make_monitor(rules, mock_fetchers, tmp_path)
        r1 = monitor.run()
        r2 = monitor.run()
        assert r1["sent"] == 1
        assert r2["skipped"] == 1  # 冷却跳过

    def test_missing_price_skips_stock(self, mock_fetchers, tmp_path):
        """未获取到实时价的股票跳过"""
        # 不注入任何 mock 价
        rules = [{
            "code": "600519", "market": "a", "name": "贵州茅台",
            "conditions": [{"type": "price_below", "value": 1500}],
        }]
        monitor = _make_monitor(rules, mock_fetchers, tmp_path)
        result = monitor.run()
        assert result["total"] == 0

    def test_ma_break_only_fetches_when_needed(self, mock_fetchers, tmp_path):
        """只有配置 ma_break 时才调用 OHLCV fetcher"""
        call_count = {"n": 0}

        def count_ohlcv(code, market, days_back=120):
            call_count["n"] += 1
            return pd.DataFrame({"收盘": [100.0] * 30})

        mock_fetchers["prices"]["000001"] = (120.0, 100.0)
        store = AlertStateStore(tmp_path / "s.json")
        monitor = PriceMonitor(
            rules=[{
                "code": "000001", "market": "a", "name": "平安银行",
                "conditions": [{"type": "price_above", "value": 100}],
            }],
            channels=[ConsoleChannel({"enable": True})],
            state_store=store,
            output_dir=str(tmp_path),
            price_fetcher=lambda c, m: mock_fetchers["prices"].get(c, (0.0, None)),
            ohlcv_fetcher=count_ohlcv,
        )
        monitor.run()
        assert call_count["n"] == 0  # 未调用 OHLCV

    def test_multiple_conditions_either_triggers(self, mock_fetchers, tmp_path):
        """两条条件均满足 → 生成 2 个事件"""
        mock_fetchers["prices"]["600519"] = (1400.0, 1500.0)  # 跌破 1500 + 跌 6.67%
        rules = [{
            "code": "600519", "market": "a", "name": "茅台",
            "conditions": [
                {"type": "price_below", "value": 1500},
                {"type": "pct_change_daily", "threshold": -5},
            ],
        }]
        monitor = _make_monitor(rules, mock_fetchers, tmp_path)
        result = monitor.run()
        assert result["total"] == 2
