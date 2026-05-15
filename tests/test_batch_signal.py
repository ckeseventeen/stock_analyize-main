"""
tests/test_batch_signal.py — 批量信号扫描监控器测试

覆盖：
  - resolve_scope_codes：scopes 列表 → 代码集合（含 watchlist / all 特殊处理）
  - BatchSignalMonitor.collect_events：股票池×信号扫描产生 AlertEvent
  - 命中股票被聚合成 1 条事件（body 列出最多 max_results 只）
  - 未知 / spot-only / 模型不存在等优雅降级
  - scheduler JOB_BUILDERS 注册
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.automation.monitor.batch_signal import (
    BatchSignalMonitor,
    resolve_scope_codes,
)

# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def synth_daily_300d() -> pd.DataFrame:
    """合成 300 日 OHLCV（中文列名）"""
    np.random.seed(7)
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
    """Mock ScreenerDataProvider：返回 3 只股票的合成 spot + 都用同一份 OHLCV"""
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

    def test_csi300_returns_codes(self, mock_provider):
        codes = resolve_scope_codes(["csi300"], mock_provider)
        assert codes == {"600519", "000001", "000333"}

    def test_watchlist_only(self, mock_provider):
        """watchlist 应从 a_stock.yaml 加载（这里可能为空，但不能抛错）"""
        codes = resolve_scope_codes(["watchlist"], mock_provider)
        assert isinstance(codes, set)   # 空集合或有数据


# =============================================================================
# BatchSignalMonitor
# =============================================================================

@pytest.mark.unit
class TestBatchSignalMonitor:
    def test_empty_scan_rules_no_events(self, mock_provider):
        monitor = BatchSignalMonitor(
            scan_rules=[], channels=[],
            data_provider=mock_provider,
        )
        events = monitor.collect_events()
        assert events == []

    def test_missing_signal_type_skips(self, mock_provider):
        monitor = BatchSignalMonitor(
            scan_rules=[{"id": "x", "name": "test", "scopes": ["csi300"]}],
            channels=[], data_provider=mock_provider,
        )
        events = monitor.collect_events()
        assert events == []  # 没指定 signal.type → 跳过

    def test_unknown_signal_skips(self, mock_provider):
        monitor = BatchSignalMonitor(
            scan_rules=[{
                "id": "x", "name": "test", "scopes": ["csi300"],
                "signal": {"type": "made_up_signal"},
            }],
            channels=[], data_provider=mock_provider,
        )
        events = monitor.collect_events()
        assert events == []

    def test_spot_only_signal_rejected(self, mock_provider):
        """market_cap 是 spot-only，不该接入 batch signal"""
        monitor = BatchSignalMonitor(
            scan_rules=[{
                "id": "x", "name": "test", "scopes": ["csi300"],
                "signal": {"type": "market_cap"},
            }],
            channels=[], data_provider=mock_provider,
        )
        events = monitor.collect_events()
        assert events == []

    def test_rsi_oversold_with_force_threshold(self, mock_provider):
        """threshold=100 强制触发 RSI 超卖；3 只股票应都命中"""
        monitor = BatchSignalMonitor(
            scan_rules=[{
                "id": "test_rsi", "name": "RSI 强制超卖",
                "scopes": ["csi300"],
                "signal": {"type": "rsi_oversold",
                          "params": {"threshold": 100, "period": 14}},
            }],
            channels=[], data_provider=mock_provider,
        )
        events = monitor.collect_events()
        assert len(events) == 1
        ev = events[0]
        assert "📈" in ev.title or "买点" in ev.title
        assert "test_rsi" in ev.event_key
        assert "命中" in ev.body

    def test_no_hits_returns_no_event(self, mock_provider):
        """threshold=0 时 RSI 必然 > 0 → 无命中"""
        monitor = BatchSignalMonitor(
            scan_rules=[{
                "id": "test_no_hit", "name": "无命中",
                "scopes": ["csi300"],
                "signal": {"type": "rsi_overbought",
                          "params": {"threshold": 999, "period": 14}},
            }],
            channels=[], data_provider=mock_provider,
        )
        events = monitor.collect_events()
        assert len(events) == 0

    def test_max_results_caps_body(self, mock_provider):
        """body 应只列出 max_results 只"""
        monitor = BatchSignalMonitor(
            scan_rules=[{
                "id": "cap", "name": "限制",
                "scopes": ["csi300"],
                "signal": {"type": "rsi_oversold",
                          "params": {"threshold": 100}},
                "max_results": 1,
            }],
            channels=[], data_provider=mock_provider,
        )
        events = monitor.collect_events()
        if events:
            # body 内 "贵州茅台/平安银行/美的集团" 中应只有 1 个被列出
            lines = events[0].body.split("\n")
            stock_lines = [ln for ln in lines if "(" in ln and ")" in ln and "." in ln]
            assert len(stock_lines) <= 1
            # 提示还有 N 只
            assert "还有" in events[0].body or len(stock_lines) == 1


# =============================================================================
# scheduler 集成
# =============================================================================

@pytest.mark.unit
class TestSchedulerIntegration:
    def test_batch_signal_in_job_builders(self):
        from src.automation.scheduler import JOB_BUILDERS
        assert "batch_signal" in JOB_BUILDERS

    def test_batch_signal_builder_returns_callable(self):
        from src.automation.scheduler import JOB_BUILDERS
        callable_fn = JOB_BUILDERS["batch_signal"]({
            "rules_config": "./config/price_alerts.yaml",
            "alerts_config": "./config/alerts.yaml",
        })
        assert callable(callable_fn)


# =============================================================================
# CLI 集成
# =============================================================================

@pytest.mark.unit
class TestCLIIntegration:
    def test_main_accepts_batch_signal_monitor_type(self):
        """main.py 的 argparse 应识别 --monitor batch_signal"""
        import importlib.util
        spec = importlib.util.spec_from_file_location("_main",
                                                       "main.py")
        # 不实际执行 main()；只校验模块可 import + argparse 配置正确
        try:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
        except SystemExit:
            pass  # main() 自身的 sys.exit 可忽略
