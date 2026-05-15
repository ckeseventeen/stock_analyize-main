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

    两种工作模式（自动检测）：
      1. **真因子模式**（data feed 有 factor_line 列）：
         比较 `factor_value < buy_threshold` / `> sell_threshold`
      2. **退化/演示模式**（factor 列不存在）：
         比较 `close 相对 MA20 的偏离百分比`：
           - 偏离 ≤ buy_below_ma_pct 时买入（默认 -5%，价跌破 MA20 5%）
           - 偏离 ≥ sell_above_ma_pct 时卖出（默认 +8%，价超 MA20 8%）
         这样无论标的价位多少都能产生信号（取代原"close < 15"几乎不可能触发的问题）。

    Params 全部 slider 可调，不调用默认值。
    """

    params = (
        # 真因子模式参数
        ("rebalance_days", 20),
        ("buy_threshold", 15.0),
        ("sell_threshold", 30.0),
        ("factor_line", "pe"),
        ("strict_factor", False),
        # 退化模式专用参数（基于 close 与 MA20 的偏离百分比）
        ("ma_period", 20),
        ("buy_below_ma_pct", -5.0),    # close 跌破 MA20 N% 买入（负数）
        ("sell_above_ma_pct", 8.0),     # close 超出 MA20 N% 卖出（正数）
        # 仓位
        ("position_size", 0.95),
    )

    def __init__(self):
        self.bar_count = 0

        # 检测 data feed 是否包含因子列
        self._use_factor = hasattr(self.data, self.params.factor_line)
        if not self._use_factor:
            msg = (
                f"因子策略: data feed 不含 '{self.params.factor_line}' 列。"
                f"若需真实因子回测，请在 GenericCSVData 中添加该列。"
            )
            if self.params.strict_factor:
                raise ValueError(msg + " (strict_factor=True 时禁止退化)")
            logger.warning(
                f"{msg} 当前 strict_factor=False，已切换到"
                f"「close vs MA{self.params.ma_period}」偏离%模式 "
                f"(买入 ≤{self.params.buy_below_ma_pct:+.1f}%, 卖出 ≥{self.params.sell_above_ma_pct:+.1f}%)。"
            )

        # 退化模式需要 MA 参考线
        self.ma_ref = bt.indicators.SMA(self.data.close, period=int(self.params.ma_period))

    def _get_factor_signal_value(self) -> tuple[float, float, float]:
        """
        返回 (current_value, buy_thresh, sell_thresh)：
          - 真因子模式：直接返回因子值与阈值
          - 退化模式：返回 (close 相对 MA 偏离百分比, buy_below_ma_pct, sell_above_ma_pct)
        """
        if self._use_factor:
            return (float(getattr(self.data, self.params.factor_line)[0]),
                    float(self.params.buy_threshold),
                    float(self.params.sell_threshold))
        # 退化模式
        close = float(self.data.close[0])
        ma = float(self.ma_ref[0])
        if ma <= 0:
            return (0.0, float(self.params.buy_below_ma_pct), float(self.params.sell_above_ma_pct))
        dev_pct = (close / ma - 1.0) * 100.0
        return (dev_pct,
                float(self.params.buy_below_ma_pct),
                float(self.params.sell_above_ma_pct))

    def next(self):
        if not self.is_warmup_done():
            return

        self.bar_count += 1
        if self.bar_count % int(self.params.rebalance_days) != 0:
            return

        value, buy_th, sell_th = self._get_factor_signal_value()
        mode_tag = self.params.factor_line if self._use_factor else f"close-vs-MA{self.params.ma_period}%"

        if not self.position:
            # 真因子：value < buy_th 买入；退化：dev_pct ≤ buy_below_ma_pct 买入
            triggered = (value < buy_th) if self._use_factor else (value <= buy_th)
            if triggered:
                price = self.data.close[0]
                if price <= 0:
                    return
                order = self.buy_target_percent(target=float(self.params.position_size))
                if order is not None:
                    self.log(
                        f"因子信号 -> 买入 仓位 {self.params.position_size:.0%} "
                        f"({mode_tag}={value:+.2f} {'<' if self._use_factor else '≤'} {buy_th:+.2f})"
                    )
        else:
            triggered = (value > sell_th) if self._use_factor else (value >= sell_th)
            if triggered:
                self.close_all()
                self.log(
                    f"因子信号 -> 卖出 "
                    f"({mode_tag}={value:+.2f} {'>' if self._use_factor else '≥'} {sell_th:+.2f})"
                )
