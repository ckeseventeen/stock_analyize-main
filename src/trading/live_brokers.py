"""
src/trading/live_brokers.py — 真实券商适配（可选依赖）

⚠️ 合规与风险提示：
  - 真实账户下单涉及真实资金，务必先在模拟盘充分验证策略
  - 使用前确认券商账户已开通 API 权限（富途需本地运行 OpenD 网关）
  - 本项目的自动化信号只生成「建议单」，实际提交必须人工确认

当前状态：**适配桩（stub）**。统一接口已定义好（见 base.py），
接入时只需实现各 broker 的 5 个抽象方法，上层页面无需改动。
"""
from __future__ import annotations

from src.trading.base import (
    BrokerBase,
    Order,
    OrderSide,
    PositionInfo,
    register_broker,
)
from src.utils.logger import get_logger

logger = get_logger("trading")

_FUTU_GUIDE = (
    "富途适配未启用。接入步骤：\n"
    "  1. pip install futu-api\n"
    "  2. 本地启动 OpenD 网关（默认 127.0.0.1:11111）\n"
    "  3. 在 live_brokers.py 中按 TODO 实现 FutuBroker 的接口映射\n"
    "文档: https://openapi.futunn.com/"
)

_TIGER_GUIDE = (
    "老虎适配未启用。接入步骤：\n"
    "  1. pip install tigeropen\n"
    "  2. 在老虎开发者平台申请 API 并配置私钥\n"
    "  3. 在 live_brokers.py 中按 TODO 实现 TigerBroker 的接口映射\n"
    "文档: https://quant.itigerup.com/openapi/"
)


class _StubLiveBroker(BrokerBase):
    """未实现的真实券商公共桩：所有操作抛出带接入指引的异常"""

    is_live = True
    _guide: str = "未实现"

    def _unavailable(self):
        raise NotImplementedError(self._guide)

    def connect(self) -> bool:
        self._unavailable()

    def get_cash(self) -> float:
        self._unavailable()

    def get_positions(self) -> list[PositionInfo]:
        self._unavailable()

    def place_order(self, code: str, side: OrderSide, qty: int,
                    price: float = 0.0, name: str = "", note: str = "") -> Order:
        self._unavailable()

    def cancel_order(self, order_id: str) -> bool:
        self._unavailable()

    def list_orders(self, limit: int = 100) -> list[Order]:
        self._unavailable()


@register_broker
class FutuBroker(_StubLiveBroker):
    """
    富途 OpenAPI 适配（桩）。

    TODO 接入映射：
      connect        → futu.OpenSecTradeContext(host, port) + unlock_trade(pwd)
      get_cash       → accinfo_query().power
      get_positions  → position_list_query()
      place_order    → place_order(price, qty, code="SH.600519", trd_side=...)
      cancel_order   → modify_order(ModifyOrderOp.CANCEL, order_id)
      list_orders    → order_list_query()
    """
    name = "futu"
    label = "🐮 富途 OpenAPI（未启用）"
    _guide = _FUTU_GUIDE


@register_broker
class TigerBroker(_StubLiveBroker):
    """
    老虎证券 OpenAPI 适配（桩）。

    TODO 接入映射：
      connect        → TradeClient(TigerOpenClientConfig(私钥/账户))
      get_cash       → get_assets()
      get_positions  → get_positions()
      place_order    → create_order + place_order
      cancel_order   → cancel_order(order_id)
      list_orders    → get_orders()
    """
    name = "tiger"
    label = "🐯 老虎证券（未启用）"
    _guide = _TIGER_GUIDE
