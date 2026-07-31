"""
tests/test_new_conditions.py — 新增筛选条件（低波动 / 股息率）

这两个条件是为「低波动质优」「红利价值」两个主流策略补的。
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.analysis.screening.conditions import (
    CONDITION_REGISTRY,
    DividendYieldCondition,
    LowVolatilityCondition,
)


def _ohlcv(n: int = 120, daily_sigma: float = 0.01, seed: int = 1) -> pd.DataFrame:
    """按目标日波动率生成价格序列（年化 ≈ daily_sigma*sqrt(252)*100）"""
    rng = np.random.default_rng(seed)
    rets = rng.normal(0, daily_sigma, n)
    close = 50 * np.exp(np.cumsum(rets))
    return pd.DataFrame({
        "日期": pd.bdate_range("2025-01-02", periods=n),
        "开盘": close, "最高": close * 1.01, "最低": close * 0.99,
        "收盘": close, "成交量": np.full(n, 1e6),
    })


@pytest.mark.unit
class TestLowVolatility:
    def test_registered(self):
        assert CONDITION_REGISTRY["low_volatility"] is LowVolatilityCondition

    def test_low_vol_passes(self):
        """日波动 0.5% → 年化 ≈ 8%，落在 [8, 28] 区间内"""
        cond = LowVolatilityCondition(period=60, min_vol=5, max_vol=28)
        assert cond.evaluate_full(pd.Series(dtype=object), _ohlcv(daily_sigma=0.005))

    def test_high_vol_rejected(self):
        """日波动 4% → 年化 ≈ 63%，超出上限"""
        cond = LowVolatilityCondition(period=60, min_vol=5, max_vol=28)
        assert not cond.evaluate_full(pd.Series(dtype=object), _ohlcv(daily_sigma=0.04))

    def test_too_quiet_rejected(self):
        """波动极低通常是僵尸股/长期停牌，min_vol 下限要挡住"""
        cond = LowVolatilityCondition(period=60, min_vol=10, max_vol=28)
        assert not cond.evaluate_full(pd.Series(dtype=object),
                                      _ohlcv(daily_sigma=0.0005))

    def test_insufficient_data(self):
        cond = LowVolatilityCondition(period=60)
        assert not cond.evaluate_full(pd.Series(dtype=object), _ohlcv(n=20))
        assert not cond.evaluate_full(pd.Series(dtype=object), pd.DataFrame())

    def test_required_bars_protocol(self):
        """数据深度协议：拉取深度要覆盖统计窗口 + 热身"""
        assert LowVolatilityCondition(period=60).required_bars() == 80
        assert LowVolatilityCondition(period=250).required_bars() == 270

    def test_spot_stage_passes_through(self):
        assert LowVolatilityCondition().evaluate_spot(pd.Series(dtype=object)) is True


@pytest.mark.unit
class TestDividendYield:
    def test_registered(self):
        assert CONDITION_REGISTRY["dividend_yield"] is DividendYieldCondition

    def test_uses_real_column_when_available(self):
        cond = DividendYieldCondition(min_yield=2, max_yield=10)
        assert cond.evaluate_spot(pd.Series({"股息率": 5.0}))
        assert not cond.evaluate_spot(pd.Series({"股息率": 0.5}))
        assert not cond.evaluate_spot(pd.Series({"股息率": 20.0}))

    def test_estimates_from_pe_when_column_missing(self):
        """数据源无股息率列时用 派息率/PE 估算：0.3/10*100 = 3%"""
        cond = DividendYieldCondition(min_yield=2, max_yield=10,
                                      assumed_payout_ratio=0.3)
        assert cond.evaluate_spot(pd.Series({"市盈率-动态": 10.0}))
        # PE=60 → 估算 0.5%，低于下限
        assert not cond.evaluate_spot(pd.Series({"市盈率-动态": 60.0}))

    def test_negative_pe_rejected(self):
        """亏损股 PE 为负，不能估出股息率"""
        cond = DividendYieldCondition(min_yield=2)
        assert not cond.evaluate_spot(pd.Series({"市盈率-动态": -15.0}))

    def test_vectorized_matches_row_wise(self):
        df = pd.DataFrame({
            "代码": ["600000", "600001", "600002"],
            "股息率": [5.0, 0.5, 20.0],
        })
        cond = DividendYieldCondition(min_yield=2, max_yield=10)
        vec = cond.evaluate_vectorized(df).tolist()
        row = [cond.evaluate_spot(df.iloc[i]) for i in range(len(df))]
        assert vec == row == [True, False, False]

    def test_vectorized_estimate_path(self):
        df = pd.DataFrame({"市盈率-动态": [10.0, 60.0, -5.0]})
        cond = DividendYieldCondition(min_yield=2, max_yield=10,
                                      assumed_payout_ratio=0.3)
        assert cond.evaluate_vectorized(df).tolist() == [True, False, False]

    def test_is_spot_condition(self):
        """股息率只需快照数据，应在第一轮向量化阶段完成（快）"""
        assert DividendYieldCondition().requires_ohlcv is False


@pytest.mark.unit
class TestNewStrategiesConfig:
    """新增的 6 个主流策略必须能被解析成条件对象"""

    EXPECTED = ("ml_enhanced", "dual_ma_trend", "mean_reversion",
                "low_vol_quality", "dividend_value", "daily_divergence_rebound")

    def test_all_present_and_parsable(self):
        from src.analysis.screening.config_schema import parse_screen_config

        for sid in self.EXPECTED:
            conds, _ = parse_screen_config("config/screen_config.yaml",
                                           strategy_ids=[sid])
            assert conds, f"策略 {sid} 解析出 0 个条件"

    def test_all_have_sell_conditions(self):
        """每个策略都要能回测——必须配卖出条件"""
        import yaml

        cfg = yaml.safe_load(open("config/screen_config.yaml", encoding="utf-8"))
        for sid in self.EXPECTED:
            bt = cfg["strategies"][sid].get("backtest") or {}
            assert bt.get("sell_conditions"), f"策略 {sid} 缺少卖出条件，无法回测"

    def test_ml_strategy_uses_ml_top_k(self):
        """ml_top_k 条件此前无人使用；ML 策略是它的第一个消费者"""
        import yaml

        cfg = yaml.safe_load(open("config/screen_config.yaml", encoding="utf-8"))
        types = [c["type"] for c in cfg["strategies"]["ml_enhanced"]["conditions"]]
        assert "ml_top_k" in types
