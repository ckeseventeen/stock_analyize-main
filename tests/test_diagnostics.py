"""
tests/test_diagnostics.py — 组合诊断单元测试

数据全部注入（无网络）：price_map + 空 K 线 fetcher + 手工 MarketRegime。
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.portfolio.diagnostics import diagnose
from src.portfolio.market_regime import MarketRegime
from src.portfolio.models import Holding, Portfolio


def _make_pf(holdings: list[Holding], cash: float = 0.0) -> Portfolio:
    return Portfolio(holdings=holdings, cash=cash, default_alerts={})


def _h(code: str, name: str, qty: int, cost: float, tag: str = "") -> Holding:
    return Holding(code=code, name=name, market="a", qty=qty,
                   avg_cost=cost, buy_date="2026-01-01", tag=tag)


_EMPTY_KLINE = lambda code, period: pd.DataFrame()  # noqa: E731


@pytest.mark.unit
class TestDiagnose:
    def test_balanced_portfolio_scores_high(self):
        """分散、无亏损、无信号 → 高分 A/B"""
        pf = _make_pf([
            _h("000001", "股A", 100, 10.0, "银行-银行-国有行"),
            _h("000002", "股B", 100, 10.0, "地产-住宅-开发"),
            _h("000003", "股C", 100, 10.0, "电子-半导体-封测"),
            _h("000004", "股D", 100, 10.0, "医药-中药-品牌"),
            _h("000005", "股E", 100, 10.0, "食品-白酒-高端"),
        ])
        price_map = {c: 11.0 for c in ("000001", "000002", "000003", "000004", "000005")}
        diag = diagnose(pf, price_map, kline_fetcher=_EMPTY_KLINE)
        assert diag.score >= 80
        assert diag.grade in ("A", "B")
        assert diag.position_count == 5
        assert diag.total_pnl_pct == pytest.approx(10.0)

    def test_single_stock_concentration_penalized(self):
        """一只票 100% 仓位 → 集中度维度重扣"""
        pf = _make_pf([_h("600519", "茅台", 100, 1500.0, "食品-白酒-高端")])
        diag = diagnose(pf, {"600519": 1600.0}, kline_fetcher=_EMPTY_KLINE)
        conc = next(d for d in diag.dimensions if d.key == "concentration")
        # 单票 >40% 扣 40 + 行业 >60% 扣 30 + 少于 3 只扣 10
        assert conc.score <= 30
        assert any("单票" in i or "集中" in i or "偏弱" in i for i in diag.issues)

    def test_deep_loss_pnl_penalized(self):
        """全仓浮亏 25% → 盈亏结构低分"""
        pf = _make_pf([
            _h("000001", "套牢股", 100, 20.0, "电子-半导体-封测"),
        ])
        diag = diagnose(pf, {"000001": 15.0}, kline_fetcher=_EMPTY_KLINE)
        pnl_dim = next(d for d in diag.dimensions if d.key == "pnl")
        assert pnl_dim.score == 0  # 100% 仓位深套，100 - 100*1.5 → 0
        assert diag.total_pnl_pct == pytest.approx(-25.0)

    def test_bear_regime_lowers_score(self):
        """空头大盘 → 大盘维度 40 分"""
        pf = _make_pf([_h("000001", "股A", 100, 10.0)])
        bear = MarketRegime(regime="bear", weight_multiplier=1.5, summary="空头")
        diag_bear = diagnose(pf, {"000001": 11.0}, kline_fetcher=_EMPTY_KLINE, regime=bear)
        diag_none = diagnose(pf, {"000001": 11.0}, kline_fetcher=_EMPTY_KLINE)
        r_bear = next(d for d in diag_bear.dimensions if d.key == "regime")
        r_none = next(d for d in diag_none.dimensions if d.key == "regime")
        assert r_bear.score == 40.0
        assert r_none.score == 70.0
        assert diag_bear.score < diag_none.score

    def test_missing_price_flagged(self):
        """无行情的持仓应出现在问题清单，且不参与信号评估"""
        pf = _make_pf([
            _h("000001", "有行情", 100, 10.0),
            _h("999999", "无行情", 100, 10.0),
        ])
        diag = diagnose(pf, {"000001": 11.0}, kline_fetcher=_EMPTY_KLINE)
        assert any("无行情" in i for i in diag.issues)
        no_price = next(e for e in diag.holdings if e.code == "999999")
        assert no_price.verdict is None
        assert no_price.weight_pct == 0

    def test_no_kline_fetcher_neutral_signal_score(self):
        """不传 kline_fetcher → 信号维度中性 70 分"""
        pf = _make_pf([_h("000001", "股A", 100, 10.0)])
        diag = diagnose(pf, {"000001": 11.0}, kline_fetcher=None)
        sig = next(d for d in diag.dimensions if d.key == "signal")
        assert sig.score == 70.0

    def test_stop_loss_holding_in_issues(self):
        """浮亏超过止损线的持仓 → L1 触发 → 问题清单"""
        pf = _make_pf([_h("000001", "止损股", 100, 20.0)])
        # 亏 30%，默认 stop_loss_pct=8 → L1 触发
        diag = diagnose(pf, {"000001": 14.0}, kline_fetcher=_EMPTY_KLINE)
        e = diag.holdings[0]
        assert e.verdict is not None
        assert e.verdict.action == "stop_loss"
        assert any("止损" in i for i in diag.issues)

    def test_report_dict_serializable(self):
        """to_report_dict 输出应可 JSON 序列化"""
        import json
        pf = _make_pf([_h("000001", "股A", 100, 10.0, "电子-半导体")])
        diag = diagnose(pf, {"000001": 11.0}, kline_fetcher=_EMPTY_KLINE)
        d = diag.to_report_dict()
        text = json.dumps(d, ensure_ascii=False)
        assert "000001" in text
        assert d["grade"] in ("A", "B", "C", "D")

    def test_empty_portfolio(self):
        """空组合不崩溃"""
        diag = diagnose(_make_pf([]), {}, kline_fetcher=_EMPTY_KLINE)
        assert diag.position_count == 0
        assert diag.grade in ("A", "B", "C", "D")
