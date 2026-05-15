"""
src/strategy/backtest/__init__.py — 策略注册中心（PR 2 重构）

PR 2 改动：
  - STRATEGY_REGISTRY 从手写 dict 改为 PluginRegistry，支持装饰器注册
  - STRATEGY_PARAM_SCHEMAS 改为按需自省（registry.schema_for(key)）
  - autodiscover 触发 strategies/ 子包下所有 .py 的 @register 装饰器

向后兼容：
  - STRATEGY_REGISTRY 仍可像 dict 一样用（提供 __getitem__）
  - STRATEGY_LABELS / STRATEGY_PARAM_SCHEMAS 仍可像 dict 一样用
"""
from __future__ import annotations

from src.core.plugin import PluginRegistry, autodiscover
from src.strategy.backtest.base_strategy import BaseStrategy
from src.strategy.backtest.factor_strategy import FactorRebalanceStrategy
from src.strategy.backtest.ma_crossover import MACrossoverStrategy
from src.strategy.backtest.ml_strategy import MLRebalanceStrategy
from src.strategy.backtest.report import BacktestReport
from src.strategy.backtest.rule_based import RuleBasedStrategy
from src.strategy.backtest.runner import BacktestRunner
from src.strategy.backtest.screener_rule import ScreenerRuleStrategy
# 多策略对比器（4_策略回测.py 多策略模式用）
from src.strategy.backtest.compare import CompareResult, StrategyResult, run_all_strategies
from src.strategy.backtest.defaults import STRATEGY_DEFAULTS, get_default_params

# ========================
# PluginRegistry：策略注册中心
# ========================

STRATEGY_REGISTRY: PluginRegistry[BaseStrategy] = PluginRegistry("strategy")

# 注册既有策略（命令式，保留旧 key + 显式标签）
STRATEGY_REGISTRY.register_class("ma_crossover", MACrossoverStrategy,
                                 label="双均线交叉 (MA Crossover)")
STRATEGY_REGISTRY.register_class("factor_rebalance", FactorRebalanceStrategy,
                                 label="因子再平衡 (Factor Rebalance)")
STRATEGY_REGISTRY.register_class("rule_based", RuleBasedStrategy,
                                 label="自定义规则 (YAML DSL)")
STRATEGY_REGISTRY.register_class("screener_rule", ScreenerRuleStrategy,
                                 label="筛选器组件桥接 (Screener Logic)")
STRATEGY_REGISTRY.register_class("ml_rebalance", MLRebalanceStrategy,
                                 label="ML 预测轮动 (B 路径自学习)")


# ========================
# 向后兼容代理：让 STRATEGY_REGISTRY / STRATEGY_LABELS / STRATEGY_PARAM_SCHEMAS
# 能像旧 dict 一样使用（避免 import 端大改）
# ========================

class _LegacyRegistryProxy:
    """让 PluginRegistry 看起来像旧 dict[str, type]"""
    def __init__(self, registry: PluginRegistry):
        self._r = registry

    def __getitem__(self, key: str) -> type:
        return self._r.get(key)

    def get(self, key: str, default=None):
        return self._r.get(key) if self._r.has(key) else default

    def __contains__(self, key: str) -> bool:
        return self._r.has(key)

    def __iter__(self):
        return iter(self._r.keys())

    def __len__(self) -> int:
        return len(self._r)

    def keys(self):
        return self._r.keys()

    def items(self):
        return self._r.items()


class _LegacyLabelsProxy:
    """让 STRATEGY_LABELS 仍是 dict[str, str] 形式"""
    def __init__(self, registry: PluginRegistry):
        self._r = registry

    def __getitem__(self, key: str) -> str:
        return self._r.labels()[key]

    def get(self, key: str, default=None):
        return self._r.labels().get(key, default)

    def __contains__(self, key: str) -> bool:
        return key in self._r.labels()

    def __iter__(self):
        return iter(self._r.labels())

    def keys(self):
        return self._r.labels().keys()

    def items(self):
        return self._r.labels().items()


class _LegacySchemasProxy:
    """让 STRATEGY_PARAM_SCHEMAS[key] 触发 registry.schema_for(key)"""
    def __init__(self, registry: PluginRegistry):
        self._r = registry
        # 同时保留显式手写的 schema 覆盖，让 PR 2 期间已有的手写参数表保持有效
        self._overrides: dict[str, list[dict]] = _LEGACY_OVERRIDES

    def __getitem__(self, key: str) -> list[dict]:
        if key in self._overrides:
            return self._overrides[key]
        return self._r.schema_for(key)

    def get(self, key: str, default=None):
        try:
            return self[key]
        except (KeyError, ValueError):
            return default

    def __contains__(self, key: str) -> bool:
        return key in self._overrides or self._r.has(key)


# 保留手写 schema 覆盖（PR 5 后会逐步迁移到 dataclass metadata）。
# 现在保留是为了让现有 4_策略回测 页 + 测试不破坏。
_LEGACY_OVERRIDES: dict[str, list[dict]] = {
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
             "buy_when": ["rsi_14 < 30", "ma_20 cross_up ma_60"],
             "sell_when": ["rsi_14 > 70", "ma_20 cross_down ma_60"],
             "buy_logic": "any",
             "sell_logic": "any",
             "position_size": 0.95,
         },
         "help": "在此 YAML 中自定义指标与买卖规则，无需编辑 Python"},
    ],
    "screener_rule": [
        {"key": "buy_conditions", "label": "买入条件 (筛选器格式)", "type": "yaml",
         "default": [
             {"type": "rsi_oversold", "threshold": 30, "period": 14},
             {"type": "price_above_ma", "ma_period": 20},
         ],
         "help": "直接使用 screen_config.yaml 中的条件定义"},
        {"key": "sell_conditions", "label": "卖出条件 (筛选器格式)", "type": "yaml",
         "default": [{"type": "rsi_oversold", "threshold": 70, "period": 14}],
         "help": "例如 RSI 超过 70 卖出"},
        {"key": "buy_logic", "label": "买入逻辑", "type": "str", "default": "all",
         "help": "all=全部满足时买入; any=任一满足时买入"},
        {"key": "sell_logic", "label": "卖出逻辑", "type": "str", "default": "any"},
        {"key": "position_size", "label": "仓位比例", "type": "float", "default": 0.95},
    ],
    "ml_rebalance": [
        {"key": "rebalance_days", "label": "调仓周期 (天)", "type": "int",
         "default": 20, "min": 5, "max": 60, "step": 1,
         "help": "ML 模型预测的标签是未来 20 日超额收益，建议保持一致"},
        {"key": "buy_threshold", "label": "买入阈值 (预测超额% ≥)", "type": "float",
         "default": 0.0, "step": 0.1,
         "help": "预测未来超额收益 ≥ 此值时建仓。多数模型 IC 约 0.05，预测分常在 ±0.5% 范围；过高会导致 0 交易"},
        {"key": "sell_threshold", "label": "卖出阈值 (预测超额% ≤)", "type": "float",
         "default": -0.5, "step": 0.1,
         "help": "预测未来超额收益 ≤ 此值时清仓"},
        {"key": "position_size", "label": "仓位比例", "type": "float", "default": 0.95},
        {"key": "warmup_bars", "label": "预热 K 线数", "type": "int",
         "default": 250, "min": 30, "max": 500, "step": 10,
         "help": "前 N 根 bar 不交易，让 MA250 等长指标先填满；至少 250"},
    ],
}

STRATEGY_LABELS = _LegacyLabelsProxy(STRATEGY_REGISTRY)
STRATEGY_PARAM_SCHEMAS = _LegacySchemasProxy(STRATEGY_REGISTRY)


# ========================
# 自动发现：触发 strategies/ 子包下所有 @STRATEGY_REGISTRY.register
# 用户新策略 drop 到 strategies/ 即生效
# ========================
autodiscover("src.strategy.backtest.strategies")


__all__ = [
    "BaseStrategy",
    "MACrossoverStrategy",
    "FactorRebalanceStrategy",
    "RuleBasedStrategy",
    "ScreenerRuleStrategy",
    "MLRebalanceStrategy",
    "BacktestRunner",
    "BacktestReport",
    "STRATEGY_REGISTRY",
    "STRATEGY_LABELS",
    "STRATEGY_PARAM_SCHEMAS",
    # 多策略对比
    "run_all_strategies",
    "CompareResult",
    "StrategyResult",
    "STRATEGY_DEFAULTS",
    "get_default_params",
]
