"""
src/strategy/backtest/screener_rule.py — 桥接筛选器条件的策略

允许将 screen_config.yaml 中的筛选条件直接作为回测信号。
"""
import backtrader as bt
import pandas as pd
from src.strategy.backtest.base_strategy import BaseStrategy
from src.screener.conditions import CONDITION_REGISTRY, BaseCondition
from src.utils.logger import get_logger

logger = get_logger("backtest_screener_rule")

class ScreenerRuleStrategy(BaseStrategy):
    """
    桥接策略：使用筛选器中的逻辑组件（如背离、突破等）进行回测。

    Params:
        buy_conditions: list[dict] — 买入条件配置（YAML格式）
        sell_conditions: list[dict] — 卖出条件配置
        buy_logic: str — "any" 或 "all"
        sell_logic: str — "any" 或 "all"
        position_size: float — 仓位比例
    """
    params = (
        ("buy_conditions", []),
        ("sell_conditions", []),
        ("buy_logic", "all"),
        ("sell_logic", "any"),
        ("position_size", 0.95),
    )

    def __init__(self):
        self._buy_objs = self._init_conditions(self.params.buy_conditions)
        self._sell_objs = self._init_conditions(self.params.sell_conditions)
        
        # 预计算整个数据集的信号（为了性能，避免在 next() 里反复切片 DataFrame）
        # 警告：在大数据集上可能耗时，但比在 next() 里逐 bar 构造 DataFrame 快
        self._buy_signals = self._precompute_signals(self._buy_objs, self.params.buy_logic)
        self._sell_signals = self._precompute_signals(self._sell_objs, self.params.sell_logic)
        
        self._bar_idx = 0

    def _init_conditions(self, configs: list[dict]) -> list[BaseCondition]:
        objs = []
        from src.screener.config_schema import _PARAM_MAP
        for cfg in configs:
            ctype = cfg.get("type")
            if ctype in CONDITION_REGISTRY:
                cls = CONDITION_REGISTRY[ctype]
                param_map = _PARAM_MAP.get(ctype, {})
                kwargs = {}
                for yaml_key, init_key in param_map.items():
                    if yaml_key in cfg:
                        kwargs[init_key] = cfg[yaml_key]
                try:
                    objs.append(cls(**kwargs))
                except Exception as e:
                    logger.error(f"构建条件 {ctype} 失败: {e}")
        return objs

    def _precompute_signals(self, condition_objs: list[BaseCondition], logic: str) -> list[bool]:
        if not condition_objs:
            return [False] * len(self.data)
        
        # 将 Backtrader 数据馈送转回 DataFrame 供筛选器组件使用
        # 仅取必要列
        df = pd.DataFrame({
            "open": self.data.open.array,
            "high": self.data.high.array,
            "low": self.data.low.array,
            "close": self.data.close.array,
            "volume": self.data.volume.array,
        })
        
        signals = []
        total = len(df)
        
        # 逐 bar 预计算
        for i in range(total):
            bar_spot = df.iloc[i]
            # 筛选器条件通常需要一段历史。我们提供到当前 bar 为止的所有数据。
            # 注意：这保证了不会“偷看”未来数据，因为 iloc[:i+1] 只含当前及之前。
            bar_history = df.iloc[:i+1]
            
            cond_results = []
            for obj in condition_objs:
                try:
                    res = obj.evaluate_full(bar_spot, bar_history)
                    cond_results.append(res)
                except Exception:
                    cond_results.append(False)
            
            if logic == "all":
                signals.append(all(cond_results) if cond_results else False)
            else:
                signals.append(any(cond_results) if cond_results else False)
        
        return signals

    def next(self):
        if self._bar_idx >= len(self._buy_signals):
            return
            
        buy_sig = self._buy_signals[self._bar_idx]
        sell_sig = self._sell_signals[self._bar_idx]
        self._bar_idx += 1
        
        if not self.position:
            if buy_sig:
                price = self.data.close[0]
                if price <= 0: return
                size = int(self.broker.getcash() * self.params.position_size / price)
                if size > 0:
                    self.buy(size=size)
                    self.log(f"筛选器买入信号 -> {size} 股 @ {price:.2f}")
        else:
            if sell_sig:
                self.close()
                self.log(f"筛选器卖出信号 -> 全部卖出 @ {self.data.close[0]:.2f}")
