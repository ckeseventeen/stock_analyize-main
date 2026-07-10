"""
src/trading/ — 交易执行层（模拟盘 + 券商适配）

设计：
  - BrokerBase 定义统一下单/持仓/订单接口
  - PaperBroker 模拟盘：全功能可用（下单/成交/持仓/手续费/持久化）
  - FutuBroker / TigerBroker：真实券商适配桩（可选依赖，未安装 SDK 时不可用）

⚠️ 合规提示：对接真实券商前请确认账户开通了对应 API 权限；
本模块所有自动化信号仅生成「建议单」，实际下单必须人工确认。
"""
from src.trading.base import (
    BROKER_REGISTRY,
    BrokerBase,
    Order,
    OrderSide,
    OrderStatus,
    PositionInfo,
    available_brokers,
    create_broker,
)
from src.trading.paper import PaperBroker
from src.trading import live_brokers  # noqa: F401  # 触发 futu/tiger 桩注册

__all__ = [
    "BROKER_REGISTRY",
    "BrokerBase",
    "Order",
    "OrderSide",
    "OrderStatus",
    "PositionInfo",
    "PaperBroker",
    "available_brokers",
    "create_broker",
]
