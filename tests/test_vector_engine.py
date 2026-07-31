"""
tests/test_vector_engine.py — 向量化回测引擎 + 筛选 Pass2 并行化

覆盖：
  - run_vector_backtest：手工可验算场景（零成本平进平出=0收益）、买入持有等价、
    成本计入、交易明细还原、输入校验
  - ma_crossover_signals 信号正确性
  - batch_vector_backtest 批量汇总
  - _evaluate_row 模块级评估函数（线程/进程共用路径）
  - Pass2 进程池评估（磁盘缓存预填充，全程离线）
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.strategy.backtest.vector_engine import (
    batch_vector_backtest,
    ma_crossover_signals,
    run_vector_backtest,
)


def _df_from_closes(closes: list[float]) -> pd.DataFrame:
    n = len(closes)
    return pd.DataFrame({
        "日期": pd.bdate_range("2025-01-02", periods=n),
        "开盘": closes, "最高": [c * 1.01 for c in closes],
        "最低": [c * 0.99 for c in closes], "收盘": closes,
        "成交量": [1e6] * n,
    })


@pytest.mark.unit
class TestVectorBacktest:
    def test_zero_cost_round_trip_is_flat(self):
        """t=1 买入信号 → t=2 收盘 11 成交；t=3 卖出信号 → t=4 收盘 11 成交 → 0 收益"""
        df = _df_from_closes([10, 10, 11, 12, 11, 10])
        signals = [0, 1, 0, -1, 0, 0]
        r = run_vector_backtest(df, signals, initial_cash=100_000,
                                commission=0.0, stamp_tax=0.0)
        assert r.report["总收益率(%)"] == pytest.approx(0.0, abs=1e-9)
        assert r.report["交易次数"] == 1
        t = r.trades.iloc[0]
        assert t["entry_price"] == 11 and t["exit_price"] == 11
        assert bool(t["closed"])

    def test_buy_and_hold_equivalence(self):
        """首 bar 买入 + 零成本 → 总收益 = close[-1]/close[1] - 1（次 bar 成交）"""
        closes = [10, 10.5, 11, 12, 13, 12.5, 14]
        df = _df_from_closes(closes)
        signals = [1, 0, 0, 0, 0, 0, 0]
        r = run_vector_backtest(df, signals, commission=0.0, stamp_tax=0.0)
        expected = closes[-1] / closes[1] - 1
        # report 四舍五入到 2 位小数
        assert r.report["总收益率(%)"] == pytest.approx(expected * 100, abs=0.01)
        # 期末未平仓：交易表有 1 条 open 记录，不计入 胜率/交易次数
        assert r.report["交易次数"] == 0
        assert not bool(r.trades.iloc[0]["closed"])

    def test_costs_reduce_equity(self):
        df = _df_from_closes([10, 10, 11, 12, 11, 10])
        signals = [0, 1, 0, -1, 0, 0]
        free = run_vector_backtest(df, signals, commission=0.0, stamp_tax=0.0)
        costly = run_vector_backtest(df, signals,
                                     commission=0.00025, stamp_tax=0.001)
        assert costly.report["最终资金"] < free.report["最终资金"]
        # 逐笔收益也应含成本
        assert costly.trades.iloc[0]["return_pct"] < free.trades.iloc[0]["return_pct"]

    def test_no_lookahead(self):
        """信号当 bar 的涨幅不应计入收益（次 bar 成交）"""
        # t=1 发信号，t=1→t=2 大涨 50%；t=2 成交后 t=3 起才有收益
        df = _df_from_closes([10, 10, 15, 15, 15])
        r = run_vector_backtest(df, [0, 1, 0, 0, 0], commission=0.0, stamp_tax=0.0)
        assert r.report["总收益率(%)"] == pytest.approx(0.0, abs=1e-9)

    def test_signal_length_mismatch_raises(self):
        with pytest.raises(ValueError, match="长度"):
            run_vector_backtest(_df_from_closes([10, 11, 12]), [1, 0])

    def test_empty_df_graceful(self):
        r = run_vector_backtest(pd.DataFrame(), [])
        assert r.report == {} and r.equity.empty

    def test_report_keys_align_with_backtrader_runner(self):
        df = _df_from_closes(list(np.linspace(10, 20, 60)))
        r = run_vector_backtest(df, ma_crossover_signals(df["收盘"], 5, 20))
        for key in ("总收益率(%)", "年化收益率(%)", "最大回撤(%)",
                    "夏普比率", "交易次数", "胜率(%)"):
            assert key in r.report


@pytest.mark.unit
class TestNextOpenExecution:
    """execution='next_open'：次 bar 开盘成交（与 Backtrader 口径一致）"""

    @staticmethod
    def _df(closes, opens):
        n = len(closes)
        return pd.DataFrame({
            "日期": pd.bdate_range("2025-01-02", periods=n),
            "开盘": opens, "最高": [max(o, c) * 1.01 for o, c in zip(opens, closes)],
            "最低": [min(o, c) * 0.99 for o, c in zip(opens, closes)],
            "收盘": closes, "成交量": [1e6] * n,
        })

    def test_round_trip_priced_at_opens(self):
        """t=1 买信号 → t=2 开盘价成交；t=3 卖信号 → t=4 开盘价成交
        零成本总收益 = open[4]/open[2] - 1"""
        closes = [10, 10, 11, 12, 11, 10]
        opens = [9.8, 9.9, 10.5, 11.5, 11.8, 10.2]
        r = run_vector_backtest(
            self._df(closes, opens), [0, 1, 0, -1, 0, 0],
            commission=0.0, stamp_tax=0.0, execution="next_open")
        expected = opens[4] / opens[2] - 1
        assert r.report["总收益率(%)"] == pytest.approx(expected * 100, abs=0.01)
        t = r.trades.iloc[0]
        assert t["entry_price"] == pytest.approx(opens[2])
        assert t["exit_price"] == pytest.approx(opens[4])

    def test_differs_from_next_close_when_gap(self):
        closes = [10, 10, 11, 12, 11, 10]
        opens = [10, 10, 10.2, 11.5, 11.9, 10.1]   # 有跳空
        sig = [0, 1, 0, -1, 0, 0]
        df = self._df(closes, opens)
        r_open = run_vector_backtest(df, sig, commission=0, stamp_tax=0,
                                     execution="next_open")
        r_close = run_vector_backtest(df, sig, commission=0, stamp_tax=0)
        assert r_open.report["总收益率(%)"] != r_close.report["总收益率(%)"]

    def test_unknown_execution_raises(self):
        with pytest.raises(ValueError, match="成交模式"):
            run_vector_backtest(_df_from_closes([10, 11]), [0, 0],
                                execution="magic")


@pytest.mark.unit
class TestMaCrossoverSignals:
    def test_v_shape_generates_death_then_golden_cross(self):
        # 先涨（快线上穿）→ 再跌（死叉）→ 再涨（金叉）
        close = pd.Series(
            list(np.linspace(60, 120, 40)) + list(np.linspace(120, 60, 40))
            + list(np.linspace(60, 130, 60)))
        sig = ma_crossover_signals(close, fast=5, slow=20)
        assert (sig == -1).any(), "下跌段应有死叉"
        assert (sig == 1).any(), "回升段应有金叉"
        # 金叉应出现在死叉之后
        assert np.flatnonzero(sig == 1).max() > np.flatnonzero(sig == -1).min()


@pytest.mark.unit
class TestBatchVectorBacktest:
    def test_batch_sorted_by_return(self):
        up = _df_from_closes(list(np.linspace(10, 30, 80)))     # 强趋势
        flat = _df_from_closes([10 + (i % 3) * 0.1 for i in range(80)])
        out = batch_vector_backtest(
            {"UP": up, "FLAT": flat, "BAD": pd.DataFrame()},
            ma_crossover_signals, fast=5, slow=20)
        assert list(out.index)[0] == "UP"
        assert "BAD" not in out.index
        assert out.loc["UP", "总收益率(%)"] >= out.loc["FLAT", "总收益率(%)"]


# ============================================================================
# 筛选 Pass2 并行化
# ============================================================================

def _uptrend_daily(n: int = 60) -> pd.DataFrame:
    return _df_from_closes(list(np.linspace(10, 20, n)))


def _downtrend_daily(n: int = 60) -> pd.DataFrame:
    return _df_from_closes(list(np.linspace(20, 10, n)))


@pytest.mark.unit
class TestEvaluateRow:
    def test_pass_and_fail_paths(self):
        from src.analysis.screening.conditions import PriceAboveMACondition
        from src.analysis.screening.screener import _evaluate_row

        provider = SimpleNamespace(
            get_weekly_ohlcv=lambda code: pd.DataFrame(),
            get_daily_ohlcv=lambda code, days_back=120: (
                _uptrend_daily() if code == "600000" else _downtrend_daily()),
        )
        cond = PriceAboveMACondition(ma_period=5)

        ok, name, code, reason, _ = _evaluate_row(
            provider, pd.Series({"代码": "600000", "名称": "上升股"}),
            [cond], need_weekly=False, need_daily=True)
        assert ok and reason == "ok"

        ok2, _, _, reason2, _ = _evaluate_row(
            provider, pd.Series({"代码": "600001", "名称": "下降股"}),
            [cond], need_weekly=False, need_daily=True)
        assert not ok2 and reason2 == "cond_failed:price_above_ma"

    def test_no_data_reason(self):
        from src.analysis.screening.conditions import PriceAboveMACondition
        from src.analysis.screening.screener import _evaluate_row

        provider = SimpleNamespace(
            get_weekly_ohlcv=lambda code: pd.DataFrame(),
            get_daily_ohlcv=lambda code, days_back=120: pd.DataFrame(),
        )
        ok, _, _, reason, _ = _evaluate_row(
            provider, pd.Series({"代码": "600000", "名称": "无数据"}),
            [PriceAboveMACondition(5)], need_weekly=False, need_daily=True)
        assert not ok and reason == "no_data"


@pytest.mark.unit
@pytest.mark.slow
class TestPass2ProcessPool:
    def test_process_pool_evaluation_offline(self, tmp_path):
        """预填磁盘缓存 → 子进程全部缓存命中，离线跑进程池评估"""
        from src.analysis.screening.conditions import PriceAboveMACondition
        from src.analysis.screening.data_provider import ScreenerDataProvider
        from src.analysis.screening.screener import StockScreener

        cache_dir = str(tmp_path / "screener_cache")
        provider = ScreenerDataProvider(cache_dir=cache_dir)

        codes = [f"6000{i:02d}" for i in range(16)]
        # 偶数序号=上升趋势（应通过 MA 条件），奇数=下降趋势（应剔除）
        for i, code in enumerate(codes):
            df = _uptrend_daily() if i % 2 == 0 else _downtrend_daily()
            provider._cache.set(f"daily_a_{code}_120", df)

        candidates = pd.DataFrame({
            "代码": codes,
            "名称": [f"测试{i}" for i in range(16)],
        })
        screener = StockScreener(data_provider=provider,
                                 max_workers=2, use_processes=True)
        passed, reasons = screener._pass2_eval_processes(
            candidates, [PriceAboveMACondition(ma_period=5)],
            need_weekly=False, need_daily=True, workers=2)

        assert len(passed) == 8
        passed_codes = set(candidates.loc[passed, "代码"])
        assert passed_codes == {c for i, c in enumerate(codes) if i % 2 == 0}
        assert reasons.get("ok") == 8
        assert reasons.get("cond_failed:price_above_ma") == 8


@pytest.mark.unit
class TestScreeningRequestModel:
    def test_use_processes_field(self):
        from src.api.main import ScreeningRequest

        req = ScreeningRequest(strategy_ids=["s1"], use_processes=True)
        assert req.use_processes is True
        assert ScreeningRequest(strategy_ids=["s1"]).use_processes is False
