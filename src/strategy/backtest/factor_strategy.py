"""
src/strategy/backtest/factor_strategy.py — 因子轮动策略

基于周期性再平衡，按因子阈值决定买卖。
"""
import backtrader as bt

from src.strategy.backtest.base_strategy import BaseStrategy


class FactorRebalanceStrategy(BaseStrategy):
    """
    因子再平衡策略

    每隔 rebalance_days 个交易日检查因子值，根据阈值决定持仓。
    因子值通过 data feed 中的额外列传入。

    Params:
        rebalance_days: 再平衡周期（交易日）
        buy_threshold: 低于此值时买入（如低PE）
        sell_threshold: 高于此值时卖出（如高PE）
        factor_line: 因子数据在 data feed 中的行索引（默认使用 close 作为演示）
    """

    params = (
        ("rebalance_days", 20),
        ("buy_threshold", 15.0),
        ("sell_threshold", 30.0),
    )

    def __init__(self):
        self.bar_count = 0
        self.pe_sma = bt.indicators.SMA(self.data.close, period=self.params.rebalance_days)

    def next(self):
        self.bar_count += 1
        if self.bar_count % self.params.rebalance_days != 0:
            return

        current_pe = self.data.close[0]  # 实际使用时替换为因子数据列

        if not self.position:
            if current_pe < self.params.buy_threshold:
                size = int(self.broker.getcash() * 0.95 / self.data.close[0])
                if size > 0:
                    self.buy(size=size)
                    self.log(f"因子信号 -> 买入 {size} 股 (PE={current_pe:.1f} < {self.params.buy_threshold})")
        else:
            if current_pe > self.params.sell_threshold:
                self.close()
                self.log(f"因子信号 -> 卖出 (PE={current_pe:.1f} > {self.params.sell_threshold})")
