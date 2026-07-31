"""
tests/test_optimizer.py — 参数寻优框架（grid/random/bayesian + Walk-Forward）

覆盖：
  - 网格候选生成（int 区间取样/list 全枚举/组合数上限抽稀）
  - 随机搜索可复现（seed）+ 边界合规
  - 贝叶斯（optuna TPE）跑通且参数在空间内
  - 约束过滤（fast<slow 违例组合不可能成为最优）
  - best_params 与 trials 一致性
  - Walk-Forward：折数/IS-OOS 字段/数据太短报错
  - /api/backtest/optimize 与 /optimizable 端点
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.strategy.backtest.optimizer import (
    OPTIMIZABLE_STRATEGIES,
    ParamOptimizer,
)
from src.strategy.backtest.vector_engine import ma_crossover_signals


def _trend_df(n: int = 300, seed: int = 3) -> pd.DataFrame:
    """带趋势和回撤的合成日线（保证均线交叉有信号可产生）"""
    rng = np.random.default_rng(seed)
    close = 50 + np.cumsum(rng.normal(0.08, 1.0, n))
    close = np.maximum(close, 5.0)
    return pd.DataFrame({
        "日期": pd.bdate_range("2024-01-02", periods=n),
        "开盘": close, "最高": close * 1.01, "最低": close * 0.99,
        "收盘": close, "成交量": np.full(n, 1e6),
    })


def _mk_optimizer(**kw) -> ParamOptimizer:
    return ParamOptimizer(
        ma_crossover_signals,
        {"fast": (3, 10), "slow": (15, 40)},
        constraint=lambda p: p["fast"] < p["slow"],
        **kw,
    )


@pytest.mark.unit
class TestCandidates:
    def test_grid_int_sampling_and_cap(self):
        opt = _mk_optimizer()
        combos = opt._grid_candidates(n_cap=1000)
        assert 0 < len(combos) <= 100  # 每维 ≤10 点
        for c in combos:
            assert 3 <= c["fast"] <= 10 and 15 <= c["slow"] <= 40
            assert isinstance(c["fast"], int)
        capped = opt._grid_candidates(n_cap=7)
        assert len(capped) == 7

    def test_grid_list_spec(self):
        opt = ParamOptimizer(ma_crossover_signals,
                             {"fast": [3, 5], "slow": [20, 30, 40]})
        combos = opt._grid_candidates(n_cap=100)
        assert len(combos) == 6

    def test_random_reproducible(self):
        opt = _mk_optimizer()
        a = opt._random_candidates(20, seed=1)
        b = opt._random_candidates(20, seed=1)
        c = opt._random_candidates(20, seed=2)
        assert a == b and a != c
        for p in a:
            assert 3 <= p["fast"] <= 10 and 15 <= p["slow"] <= 40

    def test_empty_space_raises(self):
        with pytest.raises(ValueError, match="param_space"):
            ParamOptimizer(ma_crossover_signals, {})


@pytest.mark.unit
class TestOptimize:
    def test_grid_best_matches_trials_argmax(self):
        opt = _mk_optimizer()
        r = opt.optimize(_trend_df(), method="grid", n_trials=60)
        assert r.method == "grid" and r.n_trials == 60
        assert np.isfinite(r.best_score)
        top = r.trials.iloc[0]
        assert r.best_score == pytest.approx(float(top["score"]))
        assert r.best_params == {"fast": top["fast"], "slow": top["slow"]}
        # trials 按 score 降序
        assert r.trials["score"].is_monotonic_decreasing

    def test_constraint_never_wins(self):
        # 空间刻意重叠：fast (5,30) / slow (10,20)，违例组合必须拿 -inf
        opt = ParamOptimizer(ma_crossover_signals,
                             {"fast": (5, 30), "slow": (10, 20)},
                             constraint=lambda p: p["fast"] < p["slow"])
        r = opt.optimize(_trend_df(), method="random", n_trials=50)
        assert r.best_params["fast"] < r.best_params["slow"]

    def test_random_method(self):
        r = _mk_optimizer().optimize(_trend_df(), method="random", n_trials=30)
        assert r.n_trials == 30 and np.isfinite(r.best_score)

    def test_bayesian_optuna(self):
        r = _mk_optimizer().optimize(_trend_df(), method="bayesian", n_trials=25)
        assert r.method == "bayesian" and r.n_trials == 25
        assert 3 <= r.best_params["fast"] <= 10
        assert 15 <= r.best_params["slow"] <= 40
        assert r.best_params["fast"] < r.best_params["slow"]

    def test_unknown_method_raises(self):
        with pytest.raises(ValueError, match="未知寻优方法"):
            _mk_optimizer().optimize(_trend_df(), method="magic")

    def test_metric_sharpe(self):
        r = _mk_optimizer(metric="夏普比率").optimize(
            _trend_df(), method="grid", n_trials=30)
        assert r.metric == "夏普比率"
        assert np.isfinite(r.best_score)


@pytest.mark.unit
class TestWalkForward:
    def test_folds_and_summary(self):
        opt = _mk_optimizer()
        wf = opt.walk_forward(_trend_df(400), n_splits=3, method="grid",
                              n_trials=20)
        assert len(wf.folds) == 3
        for col in ("fold", "train_bars", "test_bars",
                    "param_fast", "param_slow", "is_score", "oos_score"):
            assert col in wf.folds.columns
        assert wf.summary["n_folds"] == 3
        assert wf.overfit_gap == pytest.approx(wf.is_mean - wf.oos_mean)

    def test_too_short_raises(self):
        with pytest.raises(ValueError, match="数据太短"):
            _mk_optimizer().walk_forward(_trend_df(50), n_splits=4)


@pytest.mark.unit
class TestRegistry:
    def test_ma_crossover_registered(self):
        spec = OPTIMIZABLE_STRATEGIES["ma_crossover"]
        assert callable(spec["func"]) and callable(spec["constraint"])
        assert spec["constraint"]({"fast": 5, "slow": 20}) is True
        assert spec["constraint"]({"fast": 30, "slow": 20}) is False


@pytest.mark.unit
class TestOptimizeApi:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient

        import src.api.main as api_main
        return TestClient(api_main.app)

    def test_optimizable_endpoint(self, client):
        r = client.get("/api/backtest/optimizable")
        assert r.status_code == 200
        data = r.json()
        assert data[0]["key"] == "ma_crossover"
        assert "space" in data[0]

    def test_optimize_endpoint(self, client, monkeypatch):
        from src.services import backtest_service as btsvc

        monkeypatch.setattr(btsvc, "fetch_ohlcv",
                            lambda code, market, days, frequency="d": _trend_df())
        r = client.post("/api/backtest/optimize", json={
            "code": "600519", "method": "grid", "n_trials": 30})
        assert r.status_code == 200
        data = r.json()
        assert set(data["best_params"]) == {"fast", "slow"}
        assert data["method"] == "grid"
        assert len(data["top_trials"]) <= 10
        assert data["walk_forward"] is None

    def test_optimize_with_walk_forward(self, client, monkeypatch):
        from src.services import backtest_service as btsvc

        monkeypatch.setattr(btsvc, "fetch_ohlcv",
                            lambda code, market, days, frequency="d": _trend_df(400))
        r = client.post("/api/backtest/optimize", json={
            "code": "600519", "method": "grid", "n_trials": 20,
            "walk_forward": True})
        assert r.status_code == 200
        wf = r.json()["walk_forward"]
        assert wf and wf["summary"]["n_folds"] >= 3

    def test_unknown_strategy_404(self, client):
        r = client.post("/api/backtest/optimize", json={
            "code": "600519", "strategy": "nope"})
        assert r.status_code == 404

    def test_unknown_method_422(self, client):
        r = client.post("/api/backtest/optimize", json={
            "code": "600519", "method": "magic"})
        assert r.status_code == 422


@pytest.mark.unit
class TestWalkForwardEmptyFolds:
    def test_all_folds_skipped_raises_clear_error(self):
        """极端 train_ratio 导致所有折被跳过 → ValueError 而非 KeyError"""
        opt = _mk_optimizer()
        with pytest.raises(ValueError, match="无有效折"):
            opt.walk_forward(_trend_df(400), n_splits=4, train_ratio=0.05,
                             n_trials=10)
