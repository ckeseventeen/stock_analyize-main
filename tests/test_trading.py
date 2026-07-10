"""
tests/test_trading.py — 交易层单元测试

PaperBroker 注入固定价格 fetcher，全程无网络；状态文件用 tmp 目录隔离。
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.trading import (
    BROKER_REGISTRY,
    OrderSide,
    OrderStatus,
    PaperBroker,
    available_brokers,
    create_broker,
)


PRICES = {"600519": 1600.0, "000001": 10.0}


def _fetcher(code: str) -> float:
    return PRICES.get(code, 0.0)


@pytest.fixture
def broker(tmp_path) -> PaperBroker:
    return PaperBroker(
        state_path=tmp_path / "paper.json",
        initial_cash=1_000_000.0,
        price_fetcher=_fetcher,
    )


@pytest.mark.unit
class TestRegistry:
    def test_paper_registered(self):
        assert "paper" in BROKER_REGISTRY
        assert "paper" in available_brokers()

    def test_live_stubs_registered_but_unavailable(self):
        assert "futu" in BROKER_REGISTRY
        assert "tiger" in BROKER_REGISTRY
        futu = create_broker("futu")
        with pytest.raises(NotImplementedError):
            futu.connect()

    def test_unknown_broker_raises(self):
        with pytest.raises(KeyError):
            create_broker("nonexistent")


@pytest.mark.unit
class TestMarketOrders:
    def test_market_buy_fills_immediately(self, broker):
        order = broker.place_order("000001", OrderSide.BUY, 1000, name="平安银行")
        assert order.status == OrderStatus.FILLED
        assert order.filled_price == 10.0
        # 现金减少 = 金额 + 费用
        amount = 1000 * 10.0
        fee = PaperBroker.calc_commission(OrderSide.BUY, amount)
        assert broker.get_cash() == pytest.approx(1_000_000 - amount - fee)
        pos = broker.get_positions()
        assert len(pos) == 1
        assert pos[0].qty == 1000
        # 成本含费用摊薄
        assert pos[0].avg_cost == pytest.approx((amount + fee) / 1000)

    def test_market_sell_roundtrip(self, broker):
        broker.place_order("000001", OrderSide.BUY, 1000)
        order = broker.place_order("000001", OrderSide.SELL, 1000)
        assert order.status == OrderStatus.FILLED
        assert broker.get_positions() == []
        # 一来一回亏掉双边费用
        assert broker.get_cash() < 1_000_000
        assert broker.get_cash() > 1_000_000 - 100  # 费用应远小于 100 元

    def test_sell_commission_includes_stamp_tax(self):
        buy_fee = PaperBroker.calc_commission(OrderSide.BUY, 10000)
        sell_fee = PaperBroker.calc_commission(OrderSide.SELL, 10000)
        assert sell_fee > buy_fee
        assert sell_fee - buy_fee == pytest.approx(10000 * 0.0005)

    def test_min_commission(self):
        # 小额订单佣金按最低 5 元
        fee = PaperBroker.calc_commission(OrderSide.BUY, 1000)
        assert fee >= 5.0

    def test_insufficient_cash_rejected(self, broker):
        order = broker.place_order("600519", OrderSide.BUY, 100_000)  # 1.6 亿
        assert order.status == OrderStatus.REJECTED
        assert "资金不足" in order.note
        assert broker.get_cash() == 1_000_000

    def test_sell_without_position_rejected(self, broker):
        order = broker.place_order("000001", OrderSide.SELL, 100)
        assert order.status == OrderStatus.REJECTED
        assert "持仓不足" in order.note

    def test_odd_lot_rejected(self, broker):
        order = broker.place_order("000001", OrderSide.BUY, 150)
        assert order.status == OrderStatus.REJECTED

    def test_unknown_code_rejected(self, broker):
        order = broker.place_order("999999", OrderSide.BUY, 100)
        assert order.status == OrderStatus.REJECTED
        assert "行情" in order.note


@pytest.mark.unit
class TestLimitOrders:
    def test_limit_buy_pends_then_fills(self, broker):
        # 挂低于现价的买单 → 挂单
        order = broker.place_order("000001", OrderSide.BUY, 1000, price=9.5)
        assert order.status == OrderStatus.PENDING

        # 价格未触及 → 不成交
        assert broker.check_pending_fills() == 0

        # 价格跌到 9.4 → 成交
        PRICES["000001"] = 9.4
        try:
            assert broker.check_pending_fills() == 1
            orders = broker.list_orders()
            assert orders[0].status == OrderStatus.FILLED
            assert orders[0].filled_price == 9.5  # 按委托价成交
            assert broker.get_positions()[0].qty == 1000
        finally:
            PRICES["000001"] = 10.0

    def test_cancel_pending_order(self, broker):
        order = broker.place_order("000001", OrderSide.BUY, 1000, price=9.0)
        assert order.status == OrderStatus.PENDING
        assert broker.cancel_order(order.order_id) is True
        assert broker.list_orders()[0].status == OrderStatus.CANCELLED
        # 已撤订单不能再撤
        assert broker.cancel_order(order.order_id) is False

    def test_limit_sell_requires_position(self, broker):
        order = broker.place_order("000001", OrderSide.SELL, 100, price=11.0)
        assert order.status == OrderStatus.REJECTED


@pytest.mark.unit
class TestPersistence:
    def test_state_survives_reload(self, tmp_path):
        path = tmp_path / "paper.json"
        b1 = PaperBroker(state_path=path, price_fetcher=_fetcher)
        b1.place_order("000001", OrderSide.BUY, 500)

        b2 = PaperBroker(state_path=path, price_fetcher=_fetcher)
        assert b2.get_positions()[0].qty == 500
        assert len(b2.list_orders()) == 1
        assert b2.get_cash() == pytest.approx(b1.get_cash())

    def test_reset(self, broker):
        broker.place_order("000001", OrderSide.BUY, 500)
        broker.reset(initial_cash=500_000)
        assert broker.get_cash() == 500_000
        assert broker.get_positions() == []
        assert broker.list_orders() == []
        assert broker.summary()["initial_cash"] == 500_000

    def test_summary(self, broker):
        broker.place_order("000001", OrderSide.BUY, 1000)
        s = broker.summary()
        assert s["position_count"] == 1
        assert s["market_value"] == pytest.approx(10_000)
        assert s["total_assets"] == pytest.approx(s["cash"] + 10_000)
        # 总资产 ≈ 初始资金 - 费用
        assert s["total_pnl"] == pytest.approx(-PaperBroker.calc_commission(
            OrderSide.BUY, 10_000), abs=0.01)
