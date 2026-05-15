"""
src/strategy/backtest/base_strategy.py — 回测策略基类

封装 backtrader.Strategy，提供：
  - 通用日志、订单/交易通知
  - S2 warmup 支持：params.warmup_bars 之前不交易
  - B3 防同 bar 成交：建议子类使用 self.buy_target_percent() 而非手动算 size
"""
import backtrader as bt

from src.utils.logger import get_logger

logger = get_logger("backtest")


class BaseStrategy(bt.Strategy):
    """
    回测策略基类

    Params:
        warmup_bars: 预热 K 线数，前 N 根禁止交易（用于 MA250 之类长指标热身）
        log_level: "info" | "debug"，控制日志详细度

    子类需实现:
        __init__: 定义指标（必要时调用 super().__init__()）
        next: 定义交易逻辑（建议先 if not self.is_warmup_done(): return）
    """

    params = (
        ("warmup_bars", 0),
        ("log_level", "info"),
    )

    def is_warmup_done(self) -> bool:
        """S2：判断是否过了预热期，未过则禁止交易"""
        return len(self) > self.p.warmup_bars

    def buy_target_percent(self, target: float = 0.95) -> bt.Order | None:
        """
        B3 推荐用法：用百分比下单，让 Backtrader 用真实成交价（下一 bar 开盘）算 size，
        避免 self.buy(size=close[0]*0.95/price) 这种"用当 bar 收盘价估 size 但实际成交在下 bar 开盘"。

        Args:
            target: 目标仓位占总资产比例
        """
        if not self.is_warmup_done():
            return None
        return self.order_target_percent(target=target)

    def close_all(self) -> bt.Order | None:
        """全部平仓的语义糖（同样走下一 bar 开盘成交）"""
        if self.position:
            return self.close()
        return None

    def log(self, txt: str) -> None:
        """带日期的策略日志"""
        dt = self.datas[0].datetime.date(0)
        if self.p.log_level == "debug":
            logger.debug(f"[{dt.isoformat()}] {txt}")
        else:
            logger.info(f"[{dt.isoformat()}] {txt}")

    def notify_order(self, order: bt.Order) -> None:
        """订单状态通知"""
        if order.status in (order.Submitted, order.Accepted):
            return

        if order.status == order.Completed:
            if order.isbuy():
                self.log(f"买入成交: 价格={order.executed.price:.2f}, 数量={order.executed.size:.0f}, "
                         f"手续费={order.executed.comm:.2f}")
            else:
                self.log(f"卖出成交: 价格={order.executed.price:.2f}, 数量={order.executed.size:.0f}, "
                         f"手续费={order.executed.comm:.2f}")
        elif order.status in (order.Canceled, order.Margin, order.Rejected):
            self.log(f"订单未成交: 状态={order.getstatusname()}")

    def notify_trade(self, trade: bt.Trade) -> None:
        """交易盈亏通知"""
        if trade.isclosed:
            self.log(f"平仓: 毛利润={trade.pnl:.2f}, 净利润={trade.pnlcomm:.2f}")
