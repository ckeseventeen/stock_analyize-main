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
        ("strict_factor", False),  # B11：True 时缺因子列直接抛错，禁止退化为收盘价
    )

    def __init__(self):
        self.bar_count = 0
        self.pe_sma = bt.indicators.SMA(self.data.close, period=self.params.rebalance_days)

        # 检测 data feed 是否包含因子列
        self._use_factor = hasattr(self.data, self.params.factor_line)
        if not self._use_factor:
            # B11 修复：strict_factor=True 时缺因子直接报错，避免"看似在跑因子回测，实则用收盘价"
            msg = (
                f"因子策略: data feed 不含 '{self.params.factor_line}' 列。"
                f"若需真实因子回测，请在 GenericCSVData 中添加该列。"
            )
            if self.params.strict_factor:
                raise ValueError(msg + " (strict_factor=True 时禁止退化)")
            logger.warning(
                f"{msg} 当前 strict_factor=False，退化为收盘价 vs 阈值模式（仅供演示）。"
                f"⚠️ 该模式下回测结果与真实因子策略无关，请勿用于实盘决策。"
            )

    def _get_factor_value(self) -> float:
        """获取当前因子值，优先使用因子列，否则退化为收盘价"""
        if self._use_factor:
            return getattr(self.data, self.params.factor_line)[0]
        return self.data.close[0]

    def next(self):
        # S2：预热期内不交易
        if not self.is_warmup_done():
            return

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
                # B3：percent 下单，避免同 bar close 估算偏差
                order = self.buy_target_percent(target=0.95)
                if order is not None:
                    self.log(
                        f"因子信号 -> 目标仓位 95% "
                        f"({factor_name}={current_factor:.1f} < {self.params.buy_threshold})"
                    )
        else:
            if current_factor > self.params.sell_threshold:
                self.close_all()
                self.log(
                    f"因子信号 -> 卖出 "
                    f"({factor_name}={current_factor:.1f} > {self.params.sell_threshold})"
                )
