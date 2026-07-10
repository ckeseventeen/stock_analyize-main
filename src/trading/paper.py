"""
src/trading/paper.py — 模拟盘（Paper Trading）

全功能可用的本地模拟账户：
  - 市价单按最新价立即成交；限价单挂单，价格触及后由 check_pending_fills() 撮合
  - A 股费用模型：佣金 万2.5（最低 5 元）+ 卖出印花税 0.05% + 过户费 十万分之一
  - 状态持久化 cache/paper_broker.json（原子写）
  - 行情通过 price_fetcher 注入，便于测试与替换数据源

简化约定（与真实交易的差异）：
  - 不模拟 T+1、涨跌停、滑点与部分成交
  - 限价单撮合按「最新价穿越委托价」判断
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from src.trading.base import (
    BrokerBase,
    Order,
    OrderSide,
    OrderStatus,
    PositionInfo,
    register_broker,
)
from src.utils.logger import get_logger
from src.core.config_io import CACHE_DIR

logger = get_logger("paper_broker")

DEFAULT_STATE_PATH = CACHE_DIR / "paper_broker.json"
DEFAULT_INITIAL_CASH = 1_000_000.0

# A 股费用参数
COMMISSION_RATE = 0.00025      # 佣金 万2.5
COMMISSION_MIN = 5.0           # 最低佣金
STAMP_TAX_RATE = 0.0005        # 印花税（仅卖出）
TRANSFER_FEE_RATE = 0.00001    # 过户费


def _default_price_fetcher(code: str) -> float:
    """默认行情源：全市场 spot 中查最新价；失败返回 0"""
    try:
        from src.analysis.screening.data_provider import ScreenerDataProvider
        spot = ScreenerDataProvider().get_all_a_shares()
        if spot is None or spot.empty:
            return 0.0
        row = spot[spot["代码"].astype(str) == str(code).strip()]
        if row.empty:
            return 0.0
        return float(row.iloc[0].get("最新价", 0) or 0)
    except Exception as e:
        logger.warning(f"行情获取失败 {code}: {e}")
        return 0.0


@register_broker
class PaperBroker(BrokerBase):
    """本地模拟盘"""

    name = "paper"
    label = "📝 模拟盘（本地）"
    is_live = False

    def __init__(
        self,
        state_path: Path | str = DEFAULT_STATE_PATH,
        initial_cash: float = DEFAULT_INITIAL_CASH,
        price_fetcher: Optional[Callable[[str], float]] = None,
    ):
        self._path = Path(state_path)
        self._initial_cash = float(initial_cash)
        self._price_fetcher = price_fetcher or _default_price_fetcher
        self._state = self._load()

    # ─────────────── 持久化 ───────────────

    def _load(self) -> dict:
        if self._path.exists():
            try:
                return json.loads(self._path.read_text(encoding="utf-8"))
            except Exception as e:
                logger.error(f"模拟盘状态读取失败，重置账户: {e}")
        return {
            "cash": self._initial_cash,
            "initial_cash": self._initial_cash,
            "positions": {},   # code -> {name, qty, avg_cost}
            "orders": [],      # Order dict 列表（新的在前）
            "created_at": datetime.now().isoformat(timespec="seconds"),
        }

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(self._state, ensure_ascii=False, indent=1),
            encoding="utf-8",
        )
        os.replace(tmp, self._path)

    def reset(self, initial_cash: Optional[float] = None) -> None:
        """清空账户重新开始"""
        if initial_cash is not None:
            self._initial_cash = float(initial_cash)
        self._path.unlink(missing_ok=True)
        self._state = self._load()
        self._save()
        logger.info(f"模拟盘已重置，初始资金 {self._initial_cash:,.0f}")

    # ─────────────── 查询 ───────────────

    def connect(self) -> bool:
        return True

    def get_cash(self) -> float:
        return float(self._state.get("cash", 0))

    def get_positions(self) -> list[PositionInfo]:
        out = []
        for code, p in (self._state.get("positions") or {}).items():
            out.append(PositionInfo(
                code=code,
                name=str(p.get("name", "")),
                qty=int(p.get("qty", 0)),
                avg_cost=float(p.get("avg_cost", 0)),
                market_price=self._price_fetcher(code),
            ))
        return out

    def list_orders(self, limit: int = 100) -> list[Order]:
        return [Order.from_dict(d) for d in (self._state.get("orders") or [])[:limit]]

    # ─────────────── 费用 ───────────────

    @staticmethod
    def calc_commission(side: OrderSide, amount: float) -> float:
        """按 A 股费用模型计算总费用"""
        fee = max(amount * COMMISSION_RATE, COMMISSION_MIN)
        fee += amount * TRANSFER_FEE_RATE
        if side == OrderSide.SELL:
            fee += amount * STAMP_TAX_RATE
        return round(fee, 2)

    # ─────────────── 下单 ───────────────

    def place_order(self, code: str, side: OrderSide, qty: int,
                    price: float = 0.0, name: str = "", note: str = "") -> Order:
        code = str(code).strip()
        qty = int(qty)
        order = Order.new(code, name, side, qty, price, note=note)

        if qty <= 0 or qty % 100 != 0:
            order.status = OrderStatus.REJECTED
            order.note = (order.note + " | " if order.note else "") + "数量必须为 100 的整数倍"
            self._append_order(order)
            return order

        if price <= 0:
            # 市价单：立即按最新价成交
            market_price = self._price_fetcher(code)
            if market_price <= 0:
                order.status = OrderStatus.REJECTED
                order.note = (order.note + " | " if order.note else "") + "无法获取行情"
                self._append_order(order)
                return order
            self._try_fill(order, market_price)
        else:
            # 限价单：先校验可行性，挂单等待撮合
            if side == OrderSide.BUY:
                need = qty * price + self.calc_commission(side, qty * price)
                if need > self.get_cash():
                    order.status = OrderStatus.REJECTED
                    order.note = (order.note + " | " if order.note else "") + "可用资金不足"
            else:
                held = int((self._state["positions"].get(code) or {}).get("qty", 0))
                if held < qty:
                    order.status = OrderStatus.REJECTED
                    order.note = (order.note + " | " if order.note else "") + "持仓不足"
        self._append_order(order)
        return order

    def cancel_order(self, order_id: str) -> bool:
        for d in self._state.get("orders", []):
            if d.get("order_id") == order_id and d.get("status") == OrderStatus.PENDING.value:
                d["status"] = OrderStatus.CANCELLED.value
                self._save()
                return True
        return False

    def check_pending_fills(self) -> int:
        """
        撮合所有挂单：最新价触及委托价即成交。

        Returns:
            本次成交单数
        """
        filled = 0
        for d in self._state.get("orders", []):
            if d.get("status") != OrderStatus.PENDING.value:
                continue
            order = Order.from_dict(d)
            market_price = self._price_fetcher(order.code)
            if market_price <= 0:
                continue
            hit = (market_price <= order.price if order.side == OrderSide.BUY
                   else market_price >= order.price)
            if hit:
                ok = self._try_fill(order, order.price, persist_existing=d)
                if ok:
                    filled += 1
        if filled:
            self._save()
        return filled

    # ─────────────── 内部 ───────────────

    def _try_fill(self, order: Order, fill_price: float,
                  persist_existing: Optional[dict] = None) -> bool:
        """执行成交：更新现金/持仓；资金或持仓不足则 REJECTED"""
        amount = order.qty * fill_price
        fee = self.calc_commission(order.side, amount)
        positions = self._state.setdefault("positions", {})

        if order.side == OrderSide.BUY:
            total = amount + fee
            if total > self.get_cash():
                order.status = OrderStatus.REJECTED
                order.note = (order.note + " | " if order.note else "") + "可用资金不足"
                self._sync_order(order, persist_existing)
                return False
            self._state["cash"] = self.get_cash() - total
            pos = positions.get(order.code) or {"name": order.name, "qty": 0, "avg_cost": 0.0}
            old_qty, old_cost = int(pos["qty"]), float(pos["avg_cost"])
            new_qty = old_qty + order.qty
            pos["qty"] = new_qty
            # 摊薄成本把费用计入
            pos["avg_cost"] = (old_qty * old_cost + amount + fee) / new_qty
            if order.name:
                pos["name"] = order.name
            positions[order.code] = pos
        else:
            pos = positions.get(order.code)
            if not pos or int(pos.get("qty", 0)) < order.qty:
                order.status = OrderStatus.REJECTED
                order.note = (order.note + " | " if order.note else "") + "持仓不足"
                self._sync_order(order, persist_existing)
                return False
            self._state["cash"] = self.get_cash() + amount - fee
            pos["qty"] = int(pos["qty"]) - order.qty
            if pos["qty"] <= 0:
                del positions[order.code]
            else:
                positions[order.code] = pos

        order.status = OrderStatus.FILLED
        order.filled_price = fill_price
        order.commission = fee
        order.filled_at = datetime.now().isoformat(timespec="seconds")
        self._sync_order(order, persist_existing)
        return True

    def _sync_order(self, order: Order, persist_existing: Optional[dict]) -> None:
        """把 order 的最新字段同步回持久化 dict（已在列表中的挂单）"""
        if persist_existing is not None:
            persist_existing.update(order.to_dict())

    def _append_order(self, order: Order) -> None:
        self._state.setdefault("orders", []).insert(0, order.to_dict())
        self._save()

    # ─────────────── 账户总览 ───────────────

    def summary(self) -> dict:
        """账户总览（页面用）"""
        positions = self.get_positions()
        mv = sum(p.market_value for p in positions)
        cash = self.get_cash()
        total = cash + mv
        initial = float(self._state.get("initial_cash", self._initial_cash) or 0)
        return {
            "cash": cash,
            "market_value": mv,
            "total_assets": total,
            "initial_cash": initial,
            "total_pnl": total - initial,
            "total_pnl_pct": (total / initial - 1) * 100 if initial > 0 else 0.0,
            "position_count": len(positions),
        }
