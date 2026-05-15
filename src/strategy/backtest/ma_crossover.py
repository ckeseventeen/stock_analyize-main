"""
src/strategy/backtest/ma_crossover.py — 双均线交叉策略

金叉买入，死叉卖出。
"""
import math

import backtrader as bt

from src.strategy.backtest.base_strategy import BaseStrategy


class MACrossoverStrategy(BaseStrategy):
    """
    双均线交叉策略

    Params:
        fast_period: 快速均线周期（默认10）
        slow_period: 慢速均线周期（默认30）
        position_size: 仓位比例（默认 0.95）
        warmup_bars: 预热期，建议设为 slow_period
    """

    params = (
        ("fast_period", 10),
        ("slow_period", 30),
        ("position_size", 0.95),
    )

    def __init__(self):
        self.fast_ma = bt.indicators.SMA(self.data.close, period=self.params.fast_period)
        self.slow_ma = bt.indicators.SMA(self.data.close, period=self.params.slow_period)
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

    def next(self):
        # S2：预热期内不交易
        if not self.is_warmup_done():
            return

        if not self.position:
            # 无持仓：金叉买入
            if self.crossover > 0:
                price = self.data.close[0]
                # B20 修复：停牌/缺数据时 close[0] = NaN
                if not math.isfinite(price) or price <= 0:
                    return
                # B3：用 percent 下单（Backtrader 用下一 bar 开盘价算实际 size）
                order = self.buy_target_percent(target=self.params.position_size)
                if order is not None:
                    self.log(f"金叉信号 -> 目标仓位 {self.params.position_size:.0%}")
        else:
            # 有持仓：死叉卖出
            if self.crossover < 0:
                self.close_all()
                self.log("死叉信号 -> 全部卖出（下一 bar 开盘成交）")
