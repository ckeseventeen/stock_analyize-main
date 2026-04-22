"""
src/strategy/backtest/base_strategy.py — 回测策略基类

封装 backtrader.Strategy，提供通用日志、订单通知、交易通知。
"""
import backtrader as bt

from src.utils.logger import get_logger

logger = get_logger("backtest")


class BaseStrategy(bt.Strategy):
    """
    回测策略基类

    子类需实现:
        __init__: 定义指标
        next: 定义交易逻辑
    """

    def log(self, txt: str) -> None:
        """带日期的策略日志"""
        dt = self.datas[0].datetime.date(0)
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
