from src.strategy.backtest.base_strategy import BaseStrategy
from src.strategy.backtest.factor_strategy import FactorRebalanceStrategy
from src.strategy.backtest.ma_crossover import MACrossoverStrategy
from src.strategy.backtest.report import BacktestReport
from src.strategy.backtest.rule_based import RuleBasedStrategy
from src.strategy.backtest.runner import BacktestRunner

# ========================
# 策略注册表（YAML 预设 → 策略类）
# 新增策略时：
#   1. 在此 dict 注册
#   2. 在 STRATEGY_PARAM_SCHEMAS 中描述参数
#   3. 前端自动渲染对应表单，无需改页面代码
# ========================

STRATEGY_REGISTRY: dict[str, type[BaseStrategy]] = {
    "ma_crossover": MACrossoverStrategy,
    "factor_rebalance": FactorRebalanceStrategy,
    "rule_based": RuleBasedStrategy,
}

STRATEGY_LABELS: dict[str, str] = {
    "ma_crossover": "双均线交叉 (MA Crossover)",
    "factor_rebalance": "因子再平衡 (Factor Rebalance)",
    "rule_based": "自定义规则 (YAML DSL)",
}

# 每项 schema: {key, label, type, default, min?, max?, step?, help?}
# 特殊 type "yaml" 由前端单独渲染为 text_area，用于嵌套结构参数
STRATEGY_PARAM_SCHEMAS: dict[str, list[dict]] = {
    "ma_crossover": [
        {"key": "fast_period", "label": "短期均线 (Fast MA)", "type": "int",
         "default": 10, "min": 3, "max": 60, "step": 1,
         "help": "金叉快线周期"},
        {"key": "slow_period", "label": "长期均线 (Slow MA)", "type": "int",
         "default": 30, "min": 10, "max": 250, "step": 1,
         "help": "死叉慢线周期"},
    ],
    "factor_rebalance": [
        {"key": "rebalance_days", "label": "调仓周期 (天)", "type": "int",
         "default": 20, "min": 1, "max": 250, "step": 1},
        {"key": "buy_threshold", "label": "买入阈值", "type": "float",
         "default": 15.0, "step": 0.5},
        {"key": "sell_threshold", "label": "卖出阈值", "type": "float",
         "default": 30.0, "step": 0.5},
    ],
    "rule_based": [
        {"key": "rule_config", "label": "规则 YAML", "type": "yaml",
         "default": {
             "indicators": {
                 "rsi_14": {"type": "rsi", "period": 14},
                 "ma_20": {"type": "sma", "period": 20},
                 "ma_60": {"type": "sma", "period": 60},
             },
             "buy_when": [
                 "rsi_14 < 30",
                 "ma_20 cross_up ma_60",
             ],
             "sell_when": [
                 "rsi_14 > 70",
                 "ma_20 cross_down ma_60",
             ],
             "buy_logic": "any",
             "sell_logic": "any",
             "position_size": 0.95,
         },
         "help": "在此 YAML 中自定义指标与买卖规则，无需编辑 Python"},
    ],
}


__all__ = [
    "BaseStrategy",
    "MACrossoverStrategy",
    "FactorRebalanceStrategy",
    "RuleBasedStrategy",
    "BacktestRunner",
    "BacktestReport",
    "STRATEGY_REGISTRY",
    "STRATEGY_LABELS",
    "STRATEGY_PARAM_SCHEMAS",
]
