"""
tests/test_backtest_service.py — 回测服务层单元测试（纯计算部分，无网络）
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.services import backtest_service as btsvc


def _synthetic_ohlcv(n: int = 120) -> pd.DataFrame:
    """确定性正弦波 K 线，保证 MACD 有金叉死叉"""
    idx = pd.date_range("2025-01-01", periods=n, freq="D")
    base = 100 + 10 * np.sin(np.linspace(0, 6 * np.pi, n))
    close = pd.Series(base, index=idx)
    return pd.DataFrame({
        "open": close.shift(1).fillna(close.iloc[0]),
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": 1_000_000,
    }, index=idx)


@pytest.mark.unit
class TestBuildChartFrames:
    def test_macd_columns_present(self):
        frames = btsvc.build_chart_frames(_synthetic_ohlcv())
        for col in ("macd", "macd_signal", "macd_hist"):
            assert col in frames.macd_df.columns

    def test_crosses_detected_on_oscillating_series(self):
        frames = btsvc.build_chart_frames(_synthetic_ohlcv())
        # 正弦波必然产生多次金叉/死叉
        assert len(frames.golden_cross_dates) >= 1
        assert len(frames.death_cross_dates) >= 1

    def test_divergences_are_lists(self):
        frames = btsvc.build_chart_frames(_synthetic_ohlcv())
        assert isinstance(frames.bottom_divergences, list)
        assert isinstance(frames.top_divergences, list)


@pytest.mark.unit
class TestBestOf:
    def test_picks_highest_return(self):
        class R:
            def __init__(self, success, ret, label=""):
                self.success = success
                self.report = {"总收益率(%)": ret} if success else None
                self.label = label

        class CR:
            results = [R(True, 5.0, "a"), R(True, 12.0, "b"), R(False, 99.0, "c")]

        best = btsvc.best_of(CR())
        assert best.label == "b"

    def test_no_success_returns_none(self):
        class CR:
            results = []

        assert btsvc.best_of(CR()) is None
