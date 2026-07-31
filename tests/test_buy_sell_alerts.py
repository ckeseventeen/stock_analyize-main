"""
tests/test_buy_sell_alerts.py — 买入/卖出信号预警监控器测试

覆盖：
  - resolve_scope_codes（all / 空 / csi300 / watchlist）
  - 单股盯盘模式：填 code 触发，title 含 📈/📉
  - 批量扫描模式：填 scopes 触发，命中聚合到一条事件
  - signal 评估优雅降级（未知类型 / spot-only / 模型缺失）
  - 事件 key 包含 direction（buy/sell）+ rule_id + 日期
  - scheduler / CLI 集成
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.monitors.buy_sell_alerts import (
    BuySellAlertMonitor,
    resolve_scope_codes,
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


@pytest.fixture
def mock_provider(synth_daily_300d):
    """Mock ScreenerDataProvider"""
    class _MockProvider:
        SCOPE_DEFINITIONS = {"沪深300": "csi300", "全部A股": "all"}

        def get_scope_codes(self, scope_keys):
            if "csi300" in scope_keys:
                return {"600519", "000001", "000333"}
            return None

        def get_all_a_shares(self):
            return pd.DataFrame({
                "代码": ["600519", "000001", "000333"],
                "名称": ["贵州茅台", "平安银行", "美的集团"],
                "最新价": [1800.0, 12.5, 75.0],
                "涨跌幅": [1.2, -0.5, 0.8],
                "总市值": [2.26e12, 2.4e11, 5e11],
                "市盈率-动态": [28.5, 5.2, 13.8],
                "市净率": [8.2, 0.6, 3.1],
                "换手率": [0.35, 1.2, 0.8],
            })

        def prefetch_ohlcv_batch(self, codes, period="daily", max_workers=8):
            return {"ok": len(codes), "fail": 0}

        def get_daily_ohlcv(self, code, **kwargs):
            return synth_daily_300d

        def get_weekly_ohlcv(self, code, **kwargs):
            return synth_daily_300d  # 简化：用日线代替

    return _MockProvider()


# =============================================================================
# resolve_scope_codes
# =============================================================================

@pytest.mark.unit
class TestResolveScopeCodes:
    def test_all_returns_none(self, mock_provider):
        assert resolve_scope_codes(["all"], mock_provider) is None

    def test_empty_returns_none(self, mock_provider):
        assert resolve_scope_codes([], mock_provider) is None

    def test_csi300(self, mock_provider):
        codes = resolve_scope_codes(["csi300"], mock_provider)
        assert codes == {"600519", "000001", "000333"}

    def test_watchlist_only(self, mock_provider):
        codes = resolve_scope_codes(["watchlist"], mock_provider)
        assert isinstance(codes, set)


# =============================================================================
# BuySellAlertMonitor — 总流程
# =============================================================================

@pytest.mark.unit
class TestBasicFlow:
    def test_empty_inputs_no_events(self, mock_provider):
        monitor = BuySellAlertMonitor(
            buy_alerts=[], sell_alerts=[], channels=[],
            data_provider=mock_provider,
        )
        assert monitor.collect_events() == []

    def test_missing_signal_type_skips(self, mock_provider):
        monitor = BuySellAlertMonitor(
            buy_alerts=[{"id": "x", "name": "test", "code": "600519"}],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        # 没指定 signal.type → 跳过，不抛
        assert monitor.collect_events() == []

    def test_no_code_no_scopes_skips(self, mock_provider):
        monitor = BuySellAlertMonitor(
            buy_alerts=[{
                "id": "x", "name": "test",
                "signal": {"type": "rsi_oversold"},
            }],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        # 既没 code 也没 scopes → 跳过
        assert monitor.collect_events() == []


# =============================================================================
# 单股盯盘模式
# =============================================================================

@pytest.mark.unit
class TestSingleStockMode:
    def test_single_buy_alert_triggers(self, mock_provider):
        """threshold=100 强制 RSI 超卖触发"""
        monitor = BuySellAlertMonitor(
            buy_alerts=[{
                "id": "gzmt_oversold",
                "name": "贵州茅台 RSI 超卖",
                "code": "600519", "market": "a",
                "signal": {
                    "type": "rsi_oversold",
                    "params": {"threshold": 100, "period": 14},
                },
            }],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        events = monitor.collect_events()
        assert len(events) == 1
        ev = events[0]
        assert "📈" in ev.title
        assert "买入预警" in ev.title
        assert "gzmt_oversold" in ev.event_key
        assert ev.event_key.startswith("buy_alert:")
        assert ev.event_type == "buy_alert_rsi_oversold"

    def test_single_sell_alert_triggers(self, mock_provider):
        """RSI overbought threshold=0 强制超买"""
        monitor = BuySellAlertMonitor(
            buy_alerts=[],
            sell_alerts=[{
                "id": "gzmt_overbought",
                "name": "贵州茅台 RSI 超买",
                "code": "600519", "market": "a",
                "signal": {
                    "type": "rsi_overbought",
                    "params": {"threshold": 0, "period": 14},
                },
            }],
            channels=[], data_provider=mock_provider,
        )
        events = monitor.collect_events()
        assert len(events) == 1
        ev = events[0]
        assert "📉" in ev.title
        assert "卖出预警" in ev.title
        assert ev.event_key.startswith("sell_alert:")

    def test_single_no_trigger(self, mock_provider):
        """threshold=0 时 RSI 永不超卖 → 0 事件"""
        monitor = BuySellAlertMonitor(
            buy_alerts=[{
                "id": "x", "name": "x",
                "code": "600519",
                "signal": {
                    "type": "rsi_oversold",
                    "params": {"threshold": 0, "period": 14},
                },
            }],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        assert monitor.collect_events() == []


# =============================================================================
# 批量扫描模式
# =============================================================================

@pytest.mark.unit
class TestBatchMode:
    def test_batch_rsi_oversold_force_trigger(self, mock_provider):
        monitor = BuySellAlertMonitor(
            buy_alerts=[{
                "id": "csi300_oversold",
                "name": "沪深300 RSI 超卖扫描",
                "scopes": ["csi300"],
                "signal": {
                    "type": "rsi_oversold",
                    "params": {"threshold": 100, "period": 14},
                },
                "max_results": 20, "max_codes": 500,
            }],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        events = monitor.collect_events()
        assert len(events) == 1
        ev = events[0]
        assert "📈" in ev.title
        assert "命中" in ev.title
        assert "csi300_oversold" in ev.event_key
        # body 列出股票
        assert "贵州茅台" in ev.body or "600519" in ev.body

    def test_batch_max_results_caps_body(self, mock_provider):
        monitor = BuySellAlertMonitor(
            buy_alerts=[{
                "id": "cap", "name": "限制",
                "scopes": ["csi300"],
                "signal": {
                    "type": "rsi_oversold",
                    "params": {"threshold": 100},
                },
                "max_results": 1,
            }],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        events = monitor.collect_events()
        if events:
            lines = events[0].body.split("\n")
            stock_lines = [ln for ln in lines if "(" in ln and ")" in ln and "." in ln]
            assert len(stock_lines) <= 1
            assert "还有" in events[0].body or len(stock_lines) == 1

    def test_batch_no_hit_no_event(self, mock_provider):
        monitor = BuySellAlertMonitor(
            buy_alerts=[{
                "id": "no_hit", "name": "no hit",
                "scopes": ["csi300"],
                "signal": {"type": "rsi_overbought",
                          "params": {"threshold": 999}},
            }],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        assert monitor.collect_events() == []


# =============================================================================
# Signal 评估降级
# =============================================================================

@pytest.mark.unit
class TestSignalFallback:
    def test_unknown_signal_skips(self, mock_provider):
        monitor = BuySellAlertMonitor(
            buy_alerts=[{
                "id": "x", "name": "x", "code": "600519",
                "signal": {"type": "made_up_signal"},
            }],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        assert monitor.collect_events() == []

    def test_spot_only_signal_rejected(self, mock_provider):
        """market_cap 是 Spot-only，应跳过"""
        monitor = BuySellAlertMonitor(
            buy_alerts=[{
                "id": "x", "name": "x", "scopes": ["csi300"],
                "signal": {"type": "market_cap"},
            }],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        assert monitor.collect_events() == []


# =============================================================================
# scheduler / CLI 集成
# =============================================================================

@pytest.mark.unit
class TestSchedulerIntegration:
    def test_buy_sell_alerts_in_job_builders(self):
        from src.automation.scheduler import JOB_BUILDERS
        assert "buy_sell_alerts" in JOB_BUILDERS

    def test_old_builders_removed(self):
        from src.automation.scheduler import JOB_BUILDERS
        assert "price_monitor" not in JOB_BUILDERS
        assert "batch_signal" not in JOB_BUILDERS

    def test_builder_returns_callable(self):
        from src.automation.scheduler import JOB_BUILDERS
        callable_fn = JOB_BUILDERS["buy_sell_alerts"]({
            "rules_config": "./config/price_alerts.yaml",
            "alerts_config": "./config/alerts.yaml",
        })
        assert callable(callable_fn)


# =============================================================================
# enabled 字段
# =============================================================================

@pytest.mark.unit
class TestEnabledField:
    def test_disabled_rule_skipped(self, mock_provider):
        """enabled=False 的规则应被跳过"""
        monitor = BuySellAlertMonitor(
            buy_alerts=[{
                "id": "disabled_one", "name": "disabled",
                "code": "600519", "enabled": False,
                "signal": {"type": "rsi_oversold",
                           "params": {"threshold": 100}},
            }],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        # 即便 threshold=100 会强制触发，enabled=False 也不应产生事件
        assert monitor.collect_events() == []

    def test_enabled_default_true(self, mock_provider):
        """没指定 enabled 字段时默认启用"""
        monitor = BuySellAlertMonitor(
            buy_alerts=[{
                "id": "default", "name": "default",
                "code": "600519",
                "signal": {"type": "rsi_oversold",
                           "params": {"threshold": 100}},
                # 不写 enabled 字段
            }],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        assert len(monitor.collect_events()) == 1

    def test_mixed_enabled_disabled(self, mock_provider):
        """混合启用/禁用：只有启用的产生事件"""
        monitor = BuySellAlertMonitor(
            buy_alerts=[
                {"id": "on", "name": "on", "code": "600519", "enabled": True,
                 "signal": {"type": "rsi_oversold", "params": {"threshold": 100}}},
                {"id": "off", "name": "off", "code": "000001", "enabled": False,
                 "signal": {"type": "rsi_oversold", "params": {"threshold": 100}}},
            ],
            sell_alerts=[], channels=[], data_provider=mock_provider,
        )
        events = monitor.collect_events()
        assert len(events) == 1
        assert "on" in events[0].event_key


@pytest.mark.unit
class TestSingleRuleTest:
    def test_test_single_rule_ignores_enabled(self, mock_provider):
        """test_single_rule() 用于前端「🧪 测试」按钮，应该无视 enabled"""
        monitor = BuySellAlertMonitor(
            buy_alerts=[], sell_alerts=[], channels=[],
            data_provider=mock_provider,
        )
        disabled_rule = {
            "id": "x", "name": "x", "code": "600519",
            "enabled": False,    # 即便禁用
            "signal": {"type": "rsi_oversold", "params": {"threshold": 100}},
        }
        event = monitor.test_single_rule(disabled_rule, direction="buy")
        # 测试模式应能命中（无视 enabled）
        assert event is not None
        assert "📈" in event.title

    def test_test_single_rule_no_hit(self, mock_provider):
        monitor = BuySellAlertMonitor(
            buy_alerts=[], sell_alerts=[], channels=[],
            data_provider=mock_provider,
        )
        rule = {
            "id": "x", "name": "x", "code": "600519",
            "signal": {"type": "rsi_oversold", "params": {"threshold": 0}},
        }
        # threshold=0 → RSI 必然 > 0 → 不命中
        assert monitor.test_single_rule(rule, direction="buy") is None
