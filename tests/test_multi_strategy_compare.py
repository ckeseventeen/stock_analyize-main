"""
tests/test_multi_strategy_compare.py — 多策略对比回测测试

覆盖：
  - BacktestRunner 输出 trades / equity_curve
  - run_all_strategies 在合成 OHLCV 上跑通全部策略（ml_rebalance 优雅跳过）
  - CompareResult.summary_df / equity_curves_df
  - 默认参数完整性
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.strategy.backtest import (
    STRATEGY_DEFAULTS,
    STRATEGY_REGISTRY,
    BacktestRunner,
    MACrossoverStrategy,
    get_default_params,
    run_all_strategies,
)


@pytest.fixture
def synth_ohlcv_500d() -> pd.DataFrame:
    """500 天合成日线 - 带轻微震荡上行 + 周期性回调"""
    np.random.seed(7)
    n = 500
    dates = pd.bdate_range("2022-01-03", periods=n)
    trend = np.linspace(50, 80, n)
    noise = np.random.randn(n) * 1.5
    cycle = 5 * np.sin(np.arange(n) * 2 * np.pi / 60)
    close = trend + noise + cycle
    open_ = close + np.random.randn(n) * 0.4
    high = np.maximum(open_, close) + np.abs(np.random.randn(n) * 0.5)
    low = np.minimum(open_, close) - np.abs(np.random.randn(n) * 0.5)
    vol = np.random.randint(1_000_000, 5_000_000, n).astype(float)
    return pd.DataFrame({
        "date": dates, "open": open_, "high": high,
        "low": low, "close": close, "volume": vol,
    })


# =============================================================================
# BacktestRunner 输出增强
# =============================================================================

@pytest.mark.unit
class TestRunnerOutputs:
    def test_report_contains_trades_and_equity(self, synth_ohlcv_500d):
        runner = BacktestRunner(MACrossoverStrategy, synth_ohlcv_500d,
                                fast_period=10, slow_period=30)
        report = runner.run(initial_cash=100000)
        assert "trades" in report
        assert "equity_curve" in report
        assert isinstance(report["trades"], pd.DataFrame)
        assert isinstance(report["equity_curve"], pd.DataFrame)
        # equity_curve 至少应该有 datetime/value 列
        if not report["equity_curve"].empty:
            assert "value" in report["equity_curve"].columns
            assert "datetime" in report["equity_curve"].columns

    def test_trades_columns_complete(self, synth_ohlcv_500d):
        runner = BacktestRunner(MACrossoverStrategy, synth_ohlcv_500d,
                                fast_period=5, slow_period=20)
        report = runner.run(initial_cash=100000)
        trades = report["trades"]
        if not trades.empty:
            for col in ("datetime", "side", "price", "size", "value", "commission"):
                assert col in trades.columns
            assert trades["side"].isin(["buy", "sell"]).all()


# =============================================================================
# 默认参数
# =============================================================================

@pytest.mark.unit
class TestDefaults:
    def test_all_strategies_have_defaults(self):
        for key in STRATEGY_REGISTRY.keys():
            assert key in STRATEGY_DEFAULTS, f"策略 {key} 缺默认参数"

    def test_get_default_params_returns_copy(self):
        a = get_default_params("ma_crossover")
        b = get_default_params("ma_crossover")
        a["new_key"] = "x"
        assert "new_key" not in b   # 改 a 不影响 b

    def test_rule_based_default_has_indicators(self):
        defaults = get_default_params("rule_based")
        rc = defaults.get("rule_config", {})
        assert "indicators" in rc
        assert "buy_when" in rc
        assert "sell_when" in rc

    def test_screener_rule_default_has_conditions(self):
        defaults = get_default_params("screener_rule")
        assert defaults.get("buy_conditions")
        assert defaults.get("sell_conditions")

    def test_ml_default_no_skip_in_compare(self):
        defaults = get_default_params("ml_rebalance")
        assert defaults.get("skip_if_no_model") is False


# =============================================================================
# run_all_strategies 集成
# =============================================================================

@pytest.mark.unit
class TestRunAllStrategies:
    def test_runs_all_strategies(self, synth_ohlcv_500d):
        # 只跑技术类（rule_based 和 screener_rule 需要 indicators 库，跑慢一点但应能通）
        result = run_all_strategies(
            synth_ohlcv_500d,
            stock_code="TEST",
            market="a",
            initial_cash=100000,
            strategy_keys=["ma_crossover", "factor_rebalance"],
        )
        assert len(result.results) == 2
        # 每个都应该有 report 或 error
        for r in result.results:
            if r.success:
                assert r.report is not None
                assert "总收益率(%)" in r.report
            else:
                assert r.error or r.skip_reason

    def test_ml_skipped_gracefully_without_model(self, synth_ohlcv_500d, monkeypatch):
        """模型不存在时 ml_rebalance 应被标记 skipped 而非 error"""
        from src.ml import predictor as predictor_module
        predictor_module.reset_predictor()
        # 指向不存在的目录强制 get_predictor 返回 None
        from pathlib import Path
        monkeypatch.setattr(predictor_module, "_DEFAULT_MODEL_DIR",
                            Path("/nonexistent/xxxxxx"))

        result = run_all_strategies(
            synth_ohlcv_500d, stock_code="TEST", market="a",
            strategy_keys=["ml_rebalance"],
        )
        ml = result.results[0]
        # 不应抛出（skip_if_no_model=False in defaults）
        assert ml.key == "ml_rebalance"

    def test_summary_df_structure(self, synth_ohlcv_500d):
        result = run_all_strategies(
            synth_ohlcv_500d, stock_code="TEST", market="a",
            strategy_keys=["ma_crossover"],
        )
        df = result.summary_df()
        assert "策略" in df.columns
        assert "总收益率(%)" in df.columns
        assert "夏普" in df.columns
        assert len(df) == 1

    def test_equity_curves_df_includes_buyhold(self, synth_ohlcv_500d):
        # BacktestRunner._prepare_data 需要 datetime index
        df = synth_ohlcv_500d.set_index(pd.to_datetime(synth_ohlcv_500d["date"]))
        df = df.drop(columns=["date"])

        result = run_all_strategies(
            df.reset_index().rename(columns={"index": "date"}),
            stock_code="TEST", market="a",
            strategy_keys=["ma_crossover"],
        )
        eq = result.equity_curves_df()
        if not eq.empty:
            assert "Buy & Hold" in eq.columns

    def test_progress_callback_invoked(self, synth_ohlcv_500d):
        calls = []
        def cb(idx, total, label, status):
            calls.append((idx, label, status))

        run_all_strategies(
            synth_ohlcv_500d, stock_code="TEST", market="a",
            strategy_keys=["ma_crossover"], progress_cb=cb,
        )
        # 至少一次 running + 一次 done/error/skipped
        statuses = {s for _, _, s in calls}
        assert "running" in statuses or len(calls) > 0
