"""
src/strategy/backtest/ma_crossover.py — 双均线交叉策略

金叉买入，死叉卖出。
"""
import backtrader as bt

from src.strategy.backtest.base_strategy import BaseStrategy


class MACrossoverStrategy(BaseStrategy):
    """
    双均线交叉策略

    Params:
        fast_period: 快速均线周期（默认10）
        slow_period: 慢速均线周期（默认30）
    """

    params = (
        ("fast_period", 10),
        ("slow_period", 30),
    )

    def __init__(self):
        self.fast_ma = bt.indicators.SMA(self.data.close, period=self.params.fast_period)
        self.slow_ma = bt.indicators.SMA(self.data.close, period=self.params.slow_period)
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

    def next(self):
        if not self.position:
            # 无持仓：金叉买入
            if self.crossover > 0:
                size = int(self.broker.getcash() * 0.95 / self.data.close[0])
                if size > 0:
                    self.buy(size=size)
                    self.log(f"金叉信号 -> 买入 {size} 股 @ {self.data.close[0]:.2f}")
        else:
            # 有持仓：死叉卖出
            if self.crossover < 0:
                self.close()
                self.log(f"死叉信号 -> 全部卖出 @ {self.data.close[0]:.2f}")
