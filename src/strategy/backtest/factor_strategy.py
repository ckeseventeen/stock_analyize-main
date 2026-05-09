"""
src/strategy/backtest/factor_strategy.py — 因子轮动策略

基于周期性再平衡，按因子阈值决定买卖。
支持通过 Backtrader GenericCSVData 扩展列传入因子数据，
若未提供因子列则退化为基于收盘价与阈值的简单策略（仅作演示）。
"""
import backtrader as bt

from src.strategy.backtest.base_strategy import BaseStrategy
from src.utils.logger import get_logger

logger = get_logger("backtest_factor")


class FactorRebalanceStrategy(BaseStrategy):
    """
    因子再平衡策略

    每隔 rebalance_days 个交易日检查因子值，根据阈值决定持仓。

    因子数据传入方式（二选一）：
      1. 通过 Backtrader GenericCSVData 的额外列传入（推荐）：
         在数据加载时添加 PE 列，策略通过 self.data.pe 访问
      2. 无因子数据时退化为收盘价 vs 阈值（仅作演示，不具实战意义）

    Params:
        rebalance_days: 再平衡周期（交易日）
        buy_threshold: 低于此值时买入（如低PE=15）
        sell_threshold: 高于此值时卖出（如高PE=30）
        factor_line: 因子数据在 data feed 中的列名，默认 "pe"
                     若 data feed 中不存在该列，则退化为使用 close
    """

    params = (
        ("rebalance_days", 20),
        ("buy_threshold", 15.0),
        ("sell_threshold", 30.0),
        ("factor_line", "pe"),
    )

    def __init__(self):
        self.bar_count = 0
        self.pe_sma = bt.indicators.SMA(self.data.close, period=self.params.rebalance_days)

        # 检测 data feed 是否包含因子列
        self._use_factor = hasattr(self.data, self.params.factor_line)
        if not self._use_factor:
            logger.warning(
                f"因子策略: data feed 不含 '{self.params.factor_line}' 列，"
                f"退化为收盘价 vs 阈值模式（仅供演示）"
            )

    def _get_factor_value(self) -> float:
        """获取当前因子值，优先使用因子列，否则退化为收盘价"""
        if self._use_factor:
            return getattr(self.data, self.params.factor_line)[0]
        return self.data.close[0]

    def next(self):
        self.bar_count += 1
        if self.bar_count % self.params.rebalance_days != 0:
            return

        current_factor = self._get_factor_value()
        factor_name = self.params.factor_line if self._use_factor else "close"

        if not self.position:
            if current_factor < self.params.buy_threshold:
                price = self.data.close[0]
                if price <= 0:
                    return
                size = int(self.broker.getcash() * 0.95 / price)
                if size > 0:
                    self.buy(size=size)
                    self.log(
                        f"因子信号 -> 买入 {size} 股 "
                        f"({factor_name}={current_factor:.1f} < {self.params.buy_threshold})"
                    )
        else:
            if current_factor > self.params.sell_threshold:
                self.close()
                self.log(
                    f"因子信号 -> 卖出 "
                    f"({factor_name}={current_factor:.1f} > {self.params.sell_threshold})"
                )
