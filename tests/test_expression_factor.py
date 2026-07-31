"""
tests/test_expression_factor.py — 表达式因子引擎 + Alpha 158 + IC 检验框架

覆盖：
  - 表达式求值正确性（KMID / delay / mean / RSV / 比较运算）
  - AST 白名单安全校验（拒绝 import/属性访问/下标/未知名字）
  - ExpressionFactor 接入 FactorEngine（type: expression）
  - Alpha 158 配置加载（31 因子全部可实例化、可求值）
  - IC 分析：完美预测因子 IC≈1、分层单调、衰减曲线
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.analysis.factor import (
    FACTOR_REGISTRY,
    ExpressionError,
    ExpressionFactor,
    FactorEngine,
    build_alpha158_factors,
    evaluate_expression,
    load_alpha158_config,
)
from src.analysis.factor.ic_analysis import (
    build_close_panel,
    build_factor_panel,
    factor_diagnostics,
    forward_returns,
    ic_series,
    ic_summary,
    layered_returns,
)


def _ohlcv(n: int = 100, seed: int = 7, start: float = 50.0) -> pd.DataFrame:
    """合成中文列名日线数据"""
    rng = np.random.default_rng(seed)
    close = start + np.cumsum(rng.normal(0.05, 0.8, n))
    open_ = close + rng.normal(0, 0.3, n)
    high = np.maximum(open_, close) + np.abs(rng.normal(0, 0.4, n))
    low = np.minimum(open_, close) - np.abs(rng.normal(0, 0.4, n))
    return pd.DataFrame({
        "日期": pd.bdate_range("2025-01-02", periods=n),
        "开盘": open_, "最高": high, "最低": low, "收盘": close,
        "成交量": rng.integers(1e5, 5e6, n).astype(float),
    })


@pytest.mark.unit
class TestEvaluateExpression:
    def test_kmid_matches_manual(self):
        df = _ohlcv(30)
        out = evaluate_expression(df, "(close - open) / (open + 1e-12)")
        manual = (df["收盘"] - df["开盘"]) / (df["开盘"] + 1e-12)
        np.testing.assert_allclose(out.values, manual.values, rtol=1e-9)

    def test_delay_and_mean(self):
        df = _ohlcv(30)
        out = evaluate_expression(df, "delay(close, 5) / (close + 1e-12)")
        manual = df["收盘"].shift(5) / (df["收盘"] + 1e-12)
        np.testing.assert_allclose(
            out.dropna().values, manual.dropna().values, rtol=1e-9)

        ma = evaluate_expression(df, "mean(close, 10)")
        np.testing.assert_allclose(
            ma.dropna().values, df["收盘"].rolling(10).mean().dropna().values,
            rtol=1e-9)

    def test_rsv_bounded_01(self):
        df = _ohlcv(60)
        out = evaluate_expression(
            df,
            "(close - ts_min(low, 10)) / (ts_max(high, 10) - ts_min(low, 10) + 1e-12)",
        ).dropna()
        assert not out.empty
        assert (out >= 0).all() and (out <= 1).all()

    def test_comparison_returns_float(self):
        df = _ohlcv(30)
        out = evaluate_expression(df, "mean(close > delay(close, 1), 5)")
        valid = out.dropna()
        assert not valid.empty
        assert (valid >= 0).all() and (valid <= 1).all()

    def test_scalar_expression_broadcast(self):
        df = _ohlcv(10)
        out = evaluate_expression(df, "1 + 2")
        assert len(out) == 10
        assert (out == 3.0).all()

    def test_english_columns_also_work(self, synthetic_ohlcv_en):
        out = evaluate_expression(synthetic_ohlcv_en, "(close - open) / (open + 1e-12)")
        assert out.notna().sum() > 200


@pytest.mark.unit
class TestExpressionSecurity:
    @pytest.mark.parametrize("bad", [
        "__import__('os').system('id')",     # import
        "close.__class__",                    # 属性访问
        "close[0]",                           # 下标
        "open3 + 1",                          # 未知名字
        "foo(close)",                         # 未知函数
        "lambda x: x",                        # lambda
        "'abc'",                              # 字符串常量
        "delay(close, n=1)",                  # 关键字参数
        "close if 1 else open",               # 条件表达式
    ])
    def test_rejects_illegal(self, bad):
        with pytest.raises(ExpressionError):
            evaluate_expression(_ohlcv(10), bad)


@pytest.mark.unit
class TestExpressionFactor:
    def test_registered_in_registry(self):
        assert FACTOR_REGISTRY["expression"] is ExpressionFactor

    def test_calculate_latest_value(self):
        df = _ohlcv(50)
        f = ExpressionFactor(expr="mean(close, 5) / (close + 1e-12)", name="MA5")
        val = f.safe_calculate({"daily_df": df})
        manual = (df["收盘"].rolling(5).mean() / (df["收盘"] + 1e-12)).iloc[-1]
        assert val == pytest.approx(float(manual))

    def test_invalid_expr_fails_at_construction(self):
        with pytest.raises(ExpressionError):
            ExpressionFactor(expr="__import__('os')", name="BAD")

    def test_too_short_data_returns_nan(self):
        f = ExpressionFactor(expr="mean(close, 5)", name="MA5", min_bars=30)
        assert np.isnan(f.safe_calculate({"daily_df": _ohlcv(10)}))

    def test_engine_integration(self):
        engine = FactorEngine()
        engine.add_factor(ExpressionFactor(expr="(close - open) / (open + 1e-12)",
                                           name="KMID"))
        out = engine.compute({"600519": {"daily_df": _ohlcv(50)}})
        assert "KMID" in out.columns
        assert not np.isnan(out.loc["600519", "KMID"])


@pytest.mark.unit
class TestAlpha158Config:
    def test_config_loads_31_factors(self):
        entries = load_alpha158_config()
        assert len(entries) == 31
        names = [e["name"] for e in entries]
        assert len(names) == len(set(names)), "因子名不能重复"
        for expected in ("KMID", "KSFT2", "OPEN0", "ROC20", "MA20", "STD20",
                         "RSV10", "BETA20", "CORR20", "VMA20"):
            assert expected in names

    def test_all_factors_instantiable_and_computable(self):
        factors = build_alpha158_factors()
        assert len(factors) == 31
        df = _ohlcv(120)
        data = {"daily_df": df}
        computed = {f.name: f.safe_calculate(data) for f in factors}
        nan_names = [k for k, v in computed.items() if np.isnan(v)]
        assert not nan_names, f"以下因子在 120 根 bar 上算出 NaN: {nan_names}"


@pytest.mark.unit
class TestICFramework:
    @staticmethod
    def _universe(n_codes: int = 8, n_days: int = 80) -> dict[str, pd.DataFrame]:
        return {f"{600000 + i}": _ohlcv(n_days, seed=i, start=30.0 + 5 * i)
                for i in range(n_codes)}

    def test_panels_shape(self):
        uni = self._universe()
        fp = build_factor_panel(uni, "(close - open) / (open + 1e-12)")
        cp = build_close_panel(uni)
        assert fp.shape == cp.shape == (80, 8)
        assert isinstance(fp.index, pd.DatetimeIndex)

    def test_perfect_factor_ic_one(self):
        """因子 = 未来收益本身 → Spearman IC 应为 1"""
        uni = self._universe()
        cp = build_close_panel(uni)
        fwd = forward_returns(cp, horizon=5)
        ic = ic_series(fwd, fwd)  # 用前瞻收益自身当因子
        assert not ic.empty
        assert ic.min() > 0.999

    def test_ic_summary_fields(self):
        ic = pd.Series([0.05, 0.10, -0.02, 0.08, 0.03])
        s = ic_summary(ic)
        assert s["n_periods"] == 5
        assert s["ic_mean"] == pytest.approx(0.048, abs=1e-3)
        assert s["ic_win_rate"] == pytest.approx(0.8)
        assert s["icir"] > 0

    def test_layered_monotonic_for_perfect_factor(self):
        uni = self._universe(n_codes=10)
        cp = build_close_panel(uni)
        fwd = forward_returns(cp, horizon=5)
        layers = layered_returns(fwd, fwd, n_layers=5)
        assert list(layers.index) == ["L1", "L2", "L3", "L4", "L5", "L5-L1"]
        vals = layers[["L1", "L2", "L3", "L4", "L5"]].values
        assert (np.diff(vals) >= 0).all(), "完美因子的分层收益应单调递增"
        assert layers["L5-L1"] > 0

    def test_factor_diagnostics_end_to_end(self):
        uni = self._universe()
        report = factor_diagnostics(
            uni, "mean(close, 5) / (close + 1e-12)",
            horizon=5, decay_horizons=(1, 5, 10))
        assert set(report) == {"ic", "ic_series", "layers", "decay"}
        assert report["ic"]["n_periods"] > 0
        assert set(report["decay"]) == {1, 5, 10}

    def test_empty_input_graceful(self):
        report = factor_diagnostics({}, "close")
        assert report["ic"]["n_periods"] == 0
        assert report["layers"].empty


@pytest.mark.unit
class TestAlpha158Api:
    def test_endpoint_lists_factors(self):
        from fastapi.testclient import TestClient

        import src.api.main as api_main
        r = TestClient(api_main.app).get("/api/factors/alpha158")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 31
        assert {"name", "expr"} <= set(data[0])
