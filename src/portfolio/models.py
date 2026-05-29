"""
src/portfolio/models.py — 持仓相关数据结构

无业务逻辑，纯数据载体。manager.py 负责持久化与查询，sell_engine.py 负责决策。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional


# ========================
# 单笔交易记录
# ========================

@dataclass
class Transaction:
    """
    一笔买入或卖出（用于分批建仓 / 分批止盈）

    Attributes:
        action: "buy" | "sell"
        date: 交易日期 (YYYY-MM-DD)
        qty: 数量（股）
        price: 成交价（元）
        note: 备注（如"突破颈线买入"）
    """
    action: str
    date: str
    qty: int
    price: float
    note: str = ""

    @property
    def amount(self) -> float:
        """成交金额（元）"""
        return self.qty * self.price


# ========================
# 卖点信号（用于推送 / UI 展示）
# ========================

@dataclass
class SellSignal:
    """
    一条卖出预警信号

    Attributes:
        code: 股票代码
        name: 股票名称
        level: L1-L5（机构 5 层卖出体系层级）
        rule: 规则名（如 "stop_loss_8pct" / "macd_top_div + bias_high"）
        severity: "critical" | "warning" | "info"
        message: 中文描述（推送正文用）
        triggered_at: 触发时间
        meta: 额外字段（命中信号详情 / 当前盈亏等）
    """
    code: str
    name: str
    level: str           # L1 / L2 / L3 / L4 / L5
    rule: str
    severity: str        # critical / warning / info
    message: str
    triggered_at: datetime = field(default_factory=datetime.now)
    meta: dict = field(default_factory=dict)


# ========================
# 持仓
# ========================

@dataclass
class Holding:
    """
    单只持仓

    Attributes:
        code: 6 位股票代码
        name: 股票名称
        market: "a" | "hk" | "us"，默认 a 股
        qty: 当前持有数量（手动维护 / 由 transactions 推算）
        avg_cost: 加权平均成本（元）
        buy_date: 首次买入日期 (YYYY-MM-DD)
        notes: 自由备注
        transactions: 交易明细列表（可选；如有则 qty/avg_cost 可自动推算）
        alerts: 单只标的的提醒阈值覆盖（覆盖全局默认）
                例如 {"stop_loss_pct": 10, "trailing_pct": 8, "enable_signal_alert": true}
        tag: 自定义标签（如 "科技股"/"白马"），用于组合视图分组
    """
    code: str
    name: str
    market: str = "a"
    qty: int = 0
    avg_cost: float = 0.0
    buy_date: str = ""
    notes: str = ""
    transactions: list[Transaction] = field(default_factory=list)
    alerts: dict = field(default_factory=dict)
    tag: str = ""

    # ── 计算属性（需要外部传入最新价才能算）──

    def market_value(self, current_price: float) -> float:
        """当前市值 = 当前价 × 持仓数量"""
        return current_price * self.qty

    def cost_basis(self) -> float:
        """总成本 = 均价 × 数量"""
        return self.avg_cost * self.qty

    def unrealized_pnl(self, current_price: float) -> float:
        """浮动盈亏（元）"""
        return self.market_value(current_price) - self.cost_basis()

    def unrealized_pnl_pct(self, current_price: float) -> float:
        """浮动盈亏百分比"""
        if self.avg_cost <= 0:
            return 0.0
        return (current_price - self.avg_cost) / self.avg_cost * 100

    def holding_days(self, today: Optional[date] = None) -> int:
        """持有天数（自然日）"""
        if not self.buy_date:
            return 0
        try:
            buy = datetime.strptime(self.buy_date, "%Y-%m-%d").date()
        except ValueError:
            return 0
        today = today or date.today()
        return (today - buy).days

    # ── 辅助：从 transactions 重算 qty/avg_cost ──

    def recompute_from_transactions(self) -> None:
        """
        如果 transactions 不为空，根据全部买入/卖出记录重算当前持仓数量和加权均价。

        简化模型：用 FIFO 不准确，这里用"总买入金额 / 总买入数量"作为 avg_cost
        （即把每次买入的均价混合）。卖出只扣减 qty，不调整 cost basis 中的成本。
        """
        if not self.transactions:
            return
        total_buy_qty = 0
        total_buy_amount = 0.0
        total_sell_qty = 0
        for tx in self.transactions:
            if tx.action == "buy":
                total_buy_qty += tx.qty
                total_buy_amount += tx.amount
            elif tx.action == "sell":
                total_sell_qty += tx.qty
        if total_buy_qty > 0:
            self.avg_cost = total_buy_amount / total_buy_qty
        self.qty = total_buy_qty - total_sell_qty


# ========================
# 持仓组合
# ========================

@dataclass
class Portfolio:
    """
    持仓组合（一个用户/账户的所有持仓集合）

    Attributes:
        holdings: 持仓列表
        cash: 可用现金（元）— 用于组合层风控（如总仓位比例）
        default_alerts: 全局默认提醒规则
                例如:
                  {"stop_loss_pct": 8.0,  # L1 固定止损
                   "trailing_pct": 10.0,  # L1 移动止盈
                   "enable_signal_alert": True}
    """
    holdings: list[Holding] = field(default_factory=list)
    cash: float = 0.0
    default_alerts: dict = field(default_factory=dict)

    def total_market_value(self, price_map: dict[str, float]) -> float:
        """所有持仓的当前总市值（不含现金）"""
        return sum(h.market_value(price_map.get(h.code, 0.0)) for h in self.holdings)

    def total_cost_basis(self) -> float:
        return sum(h.cost_basis() for h in self.holdings)

    def total_unrealized_pnl(self, price_map: dict[str, float]) -> float:
        return sum(h.unrealized_pnl(price_map.get(h.code, 0.0)) for h in self.holdings)

    def find(self, code: str) -> Optional[Holding]:
        for h in self.holdings:
            if h.code == code:
                return h
        return None
