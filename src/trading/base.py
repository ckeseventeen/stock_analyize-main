"""
src/trading/base.py — 券商适配层抽象

统一接口，让上层（交易台页面 / 预警联动）不感知具体券商：
  broker = create_broker("paper")
  broker.connect()
  order = broker.place_order("600519", OrderSide.BUY, qty=100, price=1600.0)
  broker.get_positions()
"""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from src.utils.logger import get_logger

logger = get_logger("trading")


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    PENDING = "pending"        # 已提交未成交（限价单挂单）
    FILLED = "filled"          # 全部成交
    CANCELLED = "cancelled"    # 已撤单
    REJECTED = "rejected"      # 被拒（资金/持仓不足等）


@dataclass
class Order:
    """统一订单结构"""
    order_id: str
    code: str
    name: str
    side: OrderSide
    qty: int
    price: float               # 委托价；0 = 市价
    status: OrderStatus
    filled_price: float = 0.0  # 成交均价
    commission: float = 0.0    # 手续费（含佣金/印花税）
    created_at: str = ""
    filled_at: str = ""
    note: str = ""             # 来源备注（如 "sell_engine:reduce_half"）

    @staticmethod
    def new(code: str, name: str, side: OrderSide, qty: int, price: float,
            note: str = "") -> "Order":
        return Order(
            order_id=uuid.uuid4().hex[:12],
            code=code, name=name, side=side, qty=int(qty), price=float(price),
            status=OrderStatus.PENDING,
            created_at=datetime.now().isoformat(timespec="seconds"),
            note=note,
        )

    def to_dict(self) -> dict:
        d = dict(self.__dict__)
        d["side"] = self.side.value
        d["status"] = self.status.value
        return d

    @staticmethod
    def from_dict(d: dict) -> "Order":
        return Order(
            order_id=str(d.get("order_id", "")),
            code=str(d.get("code", "")),
            name=str(d.get("name", "")),
            side=OrderSide(d.get("side", "buy")),
            qty=int(d.get("qty", 0)),
            price=float(d.get("price", 0)),
            status=OrderStatus(d.get("status", "pending")),
            filled_price=float(d.get("filled_price", 0)),
            commission=float(d.get("commission", 0)),
            created_at=str(d.get("created_at", "")),
            filled_at=str(d.get("filled_at", "")),
            note=str(d.get("note", "")),
        )


@dataclass
class PositionInfo:
    """统一持仓结构（券商侧视角）"""
    code: str
    name: str
    qty: int
    avg_cost: float
    market_price: float = 0.0

    @property
    def market_value(self) -> float:
        return self.qty * self.market_price

    @property
    def pnl_pct(self) -> float:
        if self.avg_cost <= 0:
            return 0.0
        return (self.market_price - self.avg_cost) / self.avg_cost * 100


class BrokerBase(ABC):
    """券商适配基类"""

    #: 注册名（子类覆盖）
    name: str = "base"
    #: 是否真实资金账户（页面据此加确认提示）
    is_live: bool = False

    @abstractmethod
    def connect(self) -> bool:
        """建立连接；模拟盘直接返回 True"""

    def disconnect(self) -> None:
        """断开连接（默认无操作）"""

    @abstractmethod
    def get_cash(self) -> float:
        """可用资金"""

    @abstractmethod
    def get_positions(self) -> list[PositionInfo]:
        """当前持仓"""

    @abstractmethod
    def place_order(self, code: str, side: OrderSide, qty: int,
                    price: float = 0.0, name: str = "", note: str = "") -> Order:
        """
        下单。price=0 表示市价单（模拟盘按最新价立即成交）。

        Returns:
            Order（status 反映结果：FILLED / PENDING / REJECTED）
        """

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """撤单（仅 PENDING 可撤）"""

    @abstractmethod
    def list_orders(self, limit: int = 100) -> list[Order]:
        """订单历史（新的在前）"""


# ============================================================================
# 注册表
# ============================================================================

# {name: (类, 可用性检查函数)}；真实券商适配依赖可选 SDK
BROKER_REGISTRY: dict[str, type[BrokerBase]] = {}


def register_broker(cls: type[BrokerBase]) -> type[BrokerBase]:
    """装饰器：注册券商适配"""
    BROKER_REGISTRY[cls.name] = cls
    return cls


def available_brokers() -> dict[str, str]:
    """{name: 显示标签}，含可用性提示"""
    labels = {}
    for name, cls in BROKER_REGISTRY.items():
        label = getattr(cls, "label", name)
        labels[name] = label
    return labels


def create_broker(name: str, **kwargs) -> BrokerBase:
    """按名称实例化券商适配；未注册抛 KeyError"""
    if name not in BROKER_REGISTRY:
        available = ", ".join(sorted(BROKER_REGISTRY)) or "(空)"
        raise KeyError(f"未知券商适配 '{name}'。可用: {available}")
    return BROKER_REGISTRY[name](**kwargs)
