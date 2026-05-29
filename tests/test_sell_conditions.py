"""
tests/test_sell_conditions.py — Phase 5 卖出 / 回调预警 condition 单元测试

覆盖：
  - WeeklyMACDTopDivergenceCondition / DailyMACDTopDivergenceCondition
  - KDJDeathCrossCondition
  - BIASCondition
  - BreakBelowMACondition
  - VolumeBlowoffCondition
"""
import numpy as np
import pandas as pd
import pytest

from src.analysis.screening.conditions import (
    BIASCondition,
    BreakBelowMACondition,
    DailyMACDTopDivergenceCondition,
    KDJDeathCrossCondition,
    VolumeBlowoffCondition,
    WeeklyMACDTopDivergenceCondition,
    CONDITION_REGISTRY,
    SIGNAL_DIRECTION,
    get_signal_direction,
)


@pytest.fixture
def bias_high_df():
    """收盘价远高于 MA20 (~12%)，触发 BIAS 卖出信号"""
    n = 60
    close = np.linspace(10, 11, n - 5).tolist() + [11.5, 11.8, 12.1, 12.5, 13.0]
    return _make_ohlcv(close)


@pytest.fixture
def bias_normal_df():
    """收盘价贴近 MA20，BIAS 不触发"""
    n = 60
    close = np.linspace(10, 10.2, n)
    return _make_ohlcv(close)


@pytest.fixture
def break_below_ma60_df():
    """收盘价从 MA60 上方跌到下方（典型破位）"""
    n = 100
    # 前 90 根震荡向上至 100，最后 10 根快速下跌
    up = np.linspace(80, 100, 90)
    down = np.linspace(100, 60, 10)
    close = np.concatenate([up, down])
    return _make_ohlcv(close)


@pytest.fixture
def above_ma60_df():
    """收盘价持续在 MA60 之上，不破位"""
    close = np.linspace(50, 80, 100)
    return _make_ohlcv(close)


@pytest.fixture
def volume_blowoff_df():
    """最后一根：单日量 = 60 日均量 × 5，涨幅 8%"""
    n = 80
    close = np.full(n, 10.0)
    close[-1] = 10.8  # 涨 8%
    vol = np.full(n, 100000.0)
    vol[-1] = 500000.0  # 5 倍量
    return _make_ohlcv(close.tolist(), vol.tolist())


@pytest.fixture
def normal_volume_df():
    """量价平稳，不触发 blowoff"""
    n = 80
    close = np.linspace(10, 10.3, n)
    vol = np.full(n, 100000.0)
    return _make_ohlcv(close.tolist(), vol.tolist())


@pytest.fixture
def kdj_high_death_df():
    """构造 KDJ 高位死叉：先涨到高位（K/D/J 都很高），然后回落"""
    # 前 20 根快速拉升把 K/D 推到高位，后几根回落
    up = np.linspace(10, 20, 25)
    down = np.linspace(20, 18, 10)
    close = np.concatenate([up, down])
    return _make_ohlcv(close.tolist())


def _make_ohlcv(close_list, vol_list=None) -> pd.DataFrame:
    """根据收盘价列表构造一个最小合规 OHLCV DataFrame（中文列名）"""
    n = len(close_list)
    close = np.array(close_list, dtype=float)
    dates = pd.bdate_range("2024-01-02", periods=n)
    return pd.DataFrame({
        "日期": dates,
        "开盘": close - 0.01,
        "最高": close + 0.02,
        "最低": close - 0.02,
        "收盘": close,
        "成交量": np.array(vol_list if vol_list else [100000.0] * n),
    })


@pytest.mark.unit
class TestBIASCondition:
    def test_high_bias_above_triggers(self, bias_high_df):
        cond = BIASCondition(ma_period=20, threshold=8.0, direction="above")
        assert cond.evaluate_full(pd.Series(), bias_high_df) is True

    def test_low_bias_not_triggered(self, bias_normal_df):
        cond = BIASCondition(ma_period=20, threshold=8.0, direction="above")
        assert cond.evaluate_full(pd.Series(), bias_normal_df) is False

    def test_direction_below_does_not_match_overbought(self, bias_high_df):
        """direction=below 时高乖离向上不应触发"""
        cond = BIASCondition(ma_period=20, threshold=8.0, direction="below")
        assert cond.evaluate_full(pd.Series(), bias_high_df) is False

    def test_direction_both_triggers_either_side(self, bias_high_df):
        cond = BIASCondition(ma_period=20, threshold=8.0, direction="both")
        assert cond.evaluate_full(pd.Series(), bias_high_df) is True

    def test_short_data_returns_false(self):
        cond = BIASCondition(ma_period=20)
        short_df = _make_ohlcv([10.0] * 10)
        assert cond.evaluate_full(pd.Series(), short_df) is False


@pytest.mark.unit
class TestBreakBelowMACondition:
    def test_break_triggered(self, break_below_ma60_df):
        cond = BreakBelowMACondition(ma_period=60, lookback=10)
        assert cond.evaluate_full(pd.Series(), break_below_ma60_df) is True

    def test_above_not_triggered(self, above_ma60_df):
        cond = BreakBelowMACondition(ma_period=60, lookback=3)
        assert cond.evaluate_full(pd.Series(), above_ma60_df) is False


@pytest.mark.unit
class TestVolumeBlowoffCondition:
    def test_blowoff_triggered(self, volume_blowoff_df):
        cond = VolumeBlowoffCondition(lookback_bars=60, vol_multiple=3.0,
                                       min_price_change_pct=5.0)
        assert cond.evaluate_full(pd.Series(), volume_blowoff_df) is True

    def test_normal_not_triggered(self, normal_volume_df):
        cond = VolumeBlowoffCondition()
        assert cond.evaluate_full(pd.Series(), normal_volume_df) is False

    def test_high_volume_but_no_price_change(self):
        """放量但不涨：不算 blowoff"""
        n = 80
        close = np.full(n, 10.0)
        vol = np.full(n, 100000.0)
        vol[-1] = 500000.0
        df = _make_ohlcv(close.tolist(), vol.tolist())
        cond = VolumeBlowoffCondition()
        assert cond.evaluate_full(pd.Series(), df) is False


@pytest.mark.unit
class TestKDJDeathCross:
    def test_does_not_crash_on_normal_data(self, kdj_high_death_df):
        """正常数据不应抛异常；返回 True/False 都算成功"""
        cond = KDJDeathCrossCondition(n=9, m1=3, m2=3, j_threshold=70)
        result = cond.evaluate_full(pd.Series(), kdj_high_death_df)
        assert isinstance(result, bool)

    def test_short_data_returns_false(self):
        cond = KDJDeathCrossCondition()
        short_df = _make_ohlcv([10.0] * 5)
        assert cond.evaluate_full(pd.Series(), short_df) is False


@pytest.mark.unit
class TestMACDTopDivergence:
    """MACD 顶背离条件 — 主要测试不挂 + 返回 bool"""

    def test_weekly_returns_bool(self, synthetic_ohlcv_df):
        cond = WeeklyMACDTopDivergenceCondition(lookback_bars=60)
        result = cond.evaluate_full(pd.Series(), synthetic_ohlcv_df)
        assert isinstance(result, bool)

    def test_daily_returns_bool(self, synthetic_ohlcv_df):
        cond = DailyMACDTopDivergenceCondition(lookback_bars=120)
        result = cond.evaluate_full(pd.Series(), synthetic_ohlcv_df)
        assert isinstance(result, bool)

    def test_short_data_false(self):
        cond = DailyMACDTopDivergenceCondition()
        short_df = _make_ohlcv([10.0] * 10)
        assert cond.evaluate_full(pd.Series(), short_df) is False


@pytest.mark.unit
class TestRegistryIntegrity:
    """新 condition 是否注册到 registry 和元数据"""

    NEW_NAMES = (
        "weekly_macd_top_divergence",
        "daily_macd_top_divergence",
        "kdj_death_cross",
        "bias",
        "break_below_ma",
        "volume_blowoff",
    )

    def test_all_registered(self):
        for name in self.NEW_NAMES:
            assert name in CONDITION_REGISTRY, f"{name} 未注册到 CONDITION_REGISTRY"

    def test_signal_direction_set(self):
        """非中性的 5 个都应标 sell；bias 是 neutral"""
        assert SIGNAL_DIRECTION["weekly_macd_top_divergence"] == "sell"
        assert SIGNAL_DIRECTION["daily_macd_top_divergence"] == "sell"
        assert SIGNAL_DIRECTION["kdj_death_cross"] == "sell"
        assert SIGNAL_DIRECTION["break_below_ma"] == "sell"
        assert SIGNAL_DIRECTION["volume_blowoff"] == "sell"
        assert SIGNAL_DIRECTION["bias"] == "neutral"

    def test_bias_direction_resolution(self):
        """bias 根据 direction 参数细分 buy/sell"""
        assert get_signal_direction("bias", {"direction": "above"}) == "sell"
        assert get_signal_direction("bias", {"direction": "below"}) == "buy"
        assert get_signal_direction("bias", {}) == "neutral"
