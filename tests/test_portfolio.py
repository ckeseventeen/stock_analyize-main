"""
tests/test_portfolio.py — 持仓管理 + L1+L2 卖出引擎测试
"""
import numpy as np
import pandas as pd
import pytest

from src.portfolio import (
    Holding,
    Portfolio,
    PortfolioManager,
    SellEngine,
    Transaction,
)


# ========================
# 数据模型
# ========================

@pytest.mark.unit
class TestHoldingModel:
    def test_market_value_and_pnl(self):
        h = Holding(code="600519", name="贵州茅台", qty=100, avg_cost=1500.0,
                    buy_date="2025-01-01")
        assert h.cost_basis() == 150000.0
        assert h.market_value(1800.0) == 180000.0
        assert h.unrealized_pnl(1800.0) == 30000.0
        assert h.unrealized_pnl_pct(1800.0) == pytest.approx(20.0)

    def test_loss_pnl(self):
        h = Holding(code="600519", name="贵州茅台", qty=100, avg_cost=1500.0)
        assert h.unrealized_pnl(1200.0) == -30000.0
        assert h.unrealized_pnl_pct(1200.0) == pytest.approx(-20.0)

    def test_zero_cost_safe(self):
        h = Holding(code="x", name="x", qty=100, avg_cost=0)
        # 不应除以零
        assert h.unrealized_pnl_pct(10) == 0.0

    def test_recompute_from_transactions(self):
        h = Holding(code="600519", name="贵州茅台")
        h.transactions = [
            Transaction(action="buy", date="2025-01-01", qty=100, price=1500),
            Transaction(action="buy", date="2025-02-01", qty=100, price=1700),
            Transaction(action="sell", date="2025-03-01", qty=50, price=1900),
        ]
        h.recompute_from_transactions()
        # 总买入 200 股，金额 320000，均价 1600
        assert h.avg_cost == pytest.approx(1600.0)
        # 剩余持仓 = 200 - 50 = 150
        assert h.qty == 150


# ========================
# 持仓 CRUD
# ========================

@pytest.mark.unit
class TestPortfolioManager:
    def test_save_load_roundtrip(self, tmp_path):
        path = tmp_path / "holdings.yaml"
        mgr = PortfolioManager(path)
        pf = Portfolio(
            holdings=[
                Holding(code="603005", name="晶方", qty=1000, avg_cost=28.5,
                        buy_date="2025-06-15", tag="科技"),
                Holding(code="600519", name="茅台", qty=100, avg_cost=1500),
            ],
            cash=10000,
            default_alerts={"stop_loss_pct": 8},
        )
        assert mgr.save(pf) is True

        loaded = mgr.load()
        assert len(loaded.holdings) == 2
        assert loaded.find("603005").name == "晶方"
        assert loaded.find("603005").tag == "科技"
        assert loaded.find("600519").avg_cost == 1500
        assert loaded.cash == 10000
        assert loaded.default_alerts["stop_loss_pct"] == 8

    def test_add_holding(self, tmp_path):
        mgr = PortfolioManager(tmp_path / "h.yaml")
        h = Holding(code="603005", name="晶方", qty=1000, avg_cost=28.5)
        ok, msg = mgr.add_holding(h)
        assert ok is True
        pf = mgr.load()
        assert pf.find("603005") is not None

    def test_add_duplicate_rejected(self, tmp_path):
        mgr = PortfolioManager(tmp_path / "h.yaml")
        mgr.add_holding(Holding(code="603005", name="晶方", qty=1000, avg_cost=28.5))
        ok, msg = mgr.add_holding(Holding(code="603005", name="晶方", qty=500, avg_cost=30))
        assert ok is False
        assert "已存在" in msg

    def test_update_holding(self, tmp_path):
        mgr = PortfolioManager(tmp_path / "h.yaml")
        mgr.add_holding(Holding(code="603005", name="晶方", qty=1000, avg_cost=28.5))
        ok, _ = mgr.update_holding("603005", {"qty": 1500, "tag": "新标签"})
        assert ok is True
        pf = mgr.load()
        h = pf.find("603005")
        assert h.qty == 1500
        assert h.tag == "新标签"

    def test_remove_holding(self, tmp_path):
        mgr = PortfolioManager(tmp_path / "h.yaml")
        mgr.add_holding(Holding(code="603005", name="晶方", qty=1000, avg_cost=28.5))
        ok, _ = mgr.remove_holding("603005")
        assert ok is True
        assert mgr.load().find("603005") is None

    def test_add_transaction_with_recompute(self, tmp_path):
        mgr = PortfolioManager(tmp_path / "h.yaml")
        mgr.add_holding(Holding(code="603005", name="晶方", qty=1000, avg_cost=28.5,
                                 transactions=[
                                     Transaction("buy", "2025-06-15", 1000, 28.5)
                                 ]))
        # 加一笔加仓
        ok, _ = mgr.add_transaction(
            "603005",
            Transaction("buy", "2025-07-01", 500, 30.0),
            recompute=True,
        )
        assert ok is True
        h = mgr.load().find("603005")
        assert h.qty == 1500
        # 加权均价 = (1000*28.5 + 500*30) / 1500 = 29.0
        assert h.avg_cost == pytest.approx(29.0)


# ========================
# 卖出引擎
# ========================

@pytest.fixture
def synthetic_uptrend_daily():
    """模拟 250 天平稳上涨数据"""
    n = 250
    dates = pd.bdate_range("2024-01-02", periods=n)
    close = np.linspace(20, 30, n) + np.random.RandomState(42).randn(n) * 0.2
    return pd.DataFrame({
        "日期": dates,
        "开盘": close - 0.05,
        "最高": close + 0.1,
        "最低": close - 0.1,
        "收盘": close,
        "成交量": np.full(n, 1000000.0),
    })


@pytest.fixture
def synthetic_weekly():
    """周线 50 根"""
    n = 50
    dates = pd.bdate_range("2024-01-02", periods=n, freq="W-FRI")
    close = np.linspace(20, 30, n) + np.random.RandomState(42).randn(n) * 0.3
    return pd.DataFrame({
        "日期": dates,
        "开盘": close - 0.1,
        "最高": close + 0.2,
        "最低": close - 0.2,
        "收盘": close,
        "成交量": np.full(n, 5000000.0),
    })


@pytest.mark.unit
class TestSellEngine:
    def test_stop_loss_triggers_when_pnl_below_threshold(self, synthetic_uptrend_daily, synthetic_weekly):
        engine = SellEngine({"stop_loss_pct": 8.0})
        h = Holding(code="603005", name="x", qty=1000, avg_cost=100.0,
                    buy_date="2025-01-01")
        # 当前价 90 → 浮亏 10% → 触发 8% 止损
        v = engine.evaluate(h, current_price=90.0,
                             daily_df=synthetic_uptrend_daily,
                             weekly_df=synthetic_weekly)
        assert v.action == "stop_loss"
        assert "stop_loss" in v.l1_triggered

    def test_no_stop_loss_when_profit(self, synthetic_uptrend_daily, synthetic_weekly):
        engine = SellEngine({"stop_loss_pct": 8.0})
        h = Holding(code="603005", name="x", qty=1000, avg_cost=20.0)
        v = engine.evaluate(h, current_price=28.0,
                             daily_df=synthetic_uptrend_daily,
                             weekly_df=synthetic_weekly)
        assert "stop_loss" not in v.l1_triggered

    def test_trailing_stop_triggers(self, synthetic_uptrend_daily, synthetic_weekly):
        engine = SellEngine({"trailing_pct": 10.0})
        h = Holding(code="603005", name="x", qty=1000, avg_cost=20.0)
        # recent_high=100, current=85 → 回落 15% → 触发 10% 移动止盈
        v = engine.evaluate(h, current_price=85.0,
                             daily_df=synthetic_uptrend_daily,
                             weekly_df=synthetic_weekly,
                             recent_high=100.0)
        assert "trailing_stop" in v.l1_triggered

    def test_l2_signals_returns_bool_evaluation(self, synthetic_uptrend_daily, synthetic_weekly):
        """L2 信号评估不应抛异常，且应返回 0-100 风险分"""
        engine = SellEngine()
        h = Holding(code="603005", name="x", qty=1000, avg_cost=20.0)
        v = engine.evaluate(h, current_price=28.0,
                             daily_df=synthetic_uptrend_daily,
                             weekly_df=synthetic_weekly)
        assert 0 <= v.risk_pct <= 100
        assert isinstance(v.signals, list)

    def test_signal_alert_disabled(self, synthetic_uptrend_daily, synthetic_weekly):
        engine = SellEngine({"enable_signal_alert": False})
        h = Holding(code="603005", name="x", qty=1000, avg_cost=20.0)
        v = engine.evaluate(h, current_price=28.0,
                             daily_df=synthetic_uptrend_daily,
                             weekly_df=synthetic_weekly)
        assert v.risk_pct == 0
        assert v.signals == []

    def test_per_holding_override_takes_precedence(self, synthetic_uptrend_daily, synthetic_weekly):
        # 全局 8%，单只覆盖为 20%
        engine = SellEngine({"stop_loss_pct": 8.0})
        h = Holding(code="603005", name="x", qty=1000, avg_cost=100.0,
                    alerts={"stop_loss_pct": 20.0})
        # 浮亏 10%，全局会触发但单只覆盖为 20% 不触发
        v = engine.evaluate(h, current_price=90.0,
                             daily_df=synthetic_uptrend_daily,
                             weekly_df=synthetic_weekly)
        assert "stop_loss" not in v.l1_triggered

    def test_to_signals_l1_critical(self, synthetic_uptrend_daily, synthetic_weekly):
        engine = SellEngine({"stop_loss_pct": 8.0})
        h = Holding(code="603005", name="x", qty=1000, avg_cost=100.0)
        v = engine.evaluate(h, current_price=80.0,  # 浮亏 20%
                             daily_df=synthetic_uptrend_daily,
                             weekly_df=synthetic_weekly)
        sigs = SellEngine.to_signals(h, v)
        assert any(s.level == "L1" and s.severity == "critical" for s in sigs)

    def test_to_signals_no_signal_when_safe(self, synthetic_uptrend_daily, synthetic_weekly):
        engine = SellEngine()
        h = Holding(code="603005", name="x", qty=1000, avg_cost=20.0)
        v = engine.evaluate(h, current_price=28.0,  # 浮盈
                             daily_df=synthetic_uptrend_daily,
                             weekly_df=synthetic_weekly)
        sigs = SellEngine.to_signals(h, v)
        # 无 L1 触发，L2 平稳数据下应低 risk → 无推送
        critical_l1 = [s for s in sigs if s.level == "L1"]
        assert len(critical_l1) == 0
