"""
src/strategy/backtest/strategies/ — 自动发现的策略子包

把新策略 drop 到这个目录下，文件顶部装饰 `@STRATEGY_REGISTRY.register("key")` 即可。
src/strategy/backtest/__init__.py 末尾的 autodiscover 会自动 import 所有 .py。

示例：
    # src/strategy/backtest/strategies/triple_ma.py
    from src.strategy.backtest import STRATEGY_REGISTRY
    from src.strategy.backtest.base_strategy import BaseStrategy

    @STRATEGY_REGISTRY.register("triple_ma", label="三均线")
    class TripleMA(BaseStrategy):
        params = (("fast", 5), ("mid", 20), ("slow", 60))
        ...
"""
