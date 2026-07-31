"""
src/strategy/backtest/defaults.py — 各策略的"开箱即用"默认参数

多策略对比页需要给每个策略喂一份合理的默认配置（用户不能也不应该
为 5 个策略各调一遍参）。这里集中维护：

  - 技术参数策略（ma/factor/ml）：从 STRATEGY_PARAM_SCHEMAS 取 default 即可
  - YAML 参数策略（rule_based/screener_rule）：人工挑一份"通用"配置

调用方：`compare.py` + 4_策略回测.py 多策略模式
"""
from __future__ import annotations

from typing import Any

# rule_based 通用默认：RSI 极值 + 双均线交叉
_RULE_BASED_DEFAULT: dict[str, Any] = {
    "indicators": {
        "rsi_14": {"type": "rsi", "period": 14},
        "ma_20": {"type": "sma", "period": 20},
        "ma_60": {"type": "sma", "period": 60},
    },
    "buy_when": [
        "rsi_14 < 30",                # RSI 超卖
        "ma_20 cross_up ma_60",        # 金叉
    ],
    "sell_when": [
        "rsi_14 > 70",                # RSI 超买
        "ma_20 cross_down ma_60",      # 死叉
    ],
    "buy_logic": "any",
    "sell_logic": "any",
    "position_size": 0.95,
}

# screener_rule 通用默认：RSI 超卖 或 均线突破 → 买入；RSI 超买 → 卖出
# 关键：buy_logic 用 "any" 而非 "all"，因为 rsi_oversold (RSI<30) 与 price_above_ma
# (close > MA20) 几乎互斥（超卖时通常已跌破均线），用 all 几乎永不触发买入。
_SCREENER_RULE_DEFAULT: dict[str, Any] = {
    "buy_conditions": [
        {"type": "rsi_oversold", "threshold": 30, "period": 14},
        {"type": "price_above_ma", "ma_period": 20},
    ],
    "sell_conditions": [
        {"type": "rsi_overbought", "threshold": 70, "period": 14},
    ],
    "buy_logic": "any",       # ← 修复：原 "all" 几乎互斥不触发
    "sell_logic": "any",
    "position_size": 0.95,
}


# 策略 key → 默认参数 dict
STRATEGY_DEFAULTS: dict[str, dict[str, Any]] = {
    "ma_crossover": {
        "fast_period": 10,
        "slow_period": 30,
        "position_size": 0.95,
    },
    "factor_rebalance": {
        "rebalance_days": 20,
        "buy_threshold": 15.0,
        "sell_threshold": 30.0,
        # data feed 不含 pe 列时退化到 close vs threshold，本默认配置仅作演示
    },
    "rule_based": {
        "rule_config": _RULE_BASED_DEFAULT,
    },
    "screener_rule": {
        "buy_conditions": _SCREENER_RULE_DEFAULT["buy_conditions"],
        "sell_conditions": _SCREENER_RULE_DEFAULT["sell_conditions"],
        "buy_logic": _SCREENER_RULE_DEFAULT["buy_logic"],
        "sell_logic": _SCREENER_RULE_DEFAULT["sell_logic"],
        "position_size": _SCREENER_RULE_DEFAULT["position_size"],
    },
    "ml_rebalance": {
        "rebalance_days": 20,
        "buy_threshold": 0.0,
        "sell_threshold": -0.5,
        "position_size": 0.95,
        "warmup_bars": 250,
        "skip_if_no_model": True,     # 模型未训练时直接跳过，在对比结果中显示明确提示
    },
}


def get_default_params(strategy_key: str) -> dict:
    """返回某策略的默认参数（不存在则空 dict）"""
    return dict(STRATEGY_DEFAULTS.get(strategy_key, {}))


def get_all_default_strategies() -> dict[str, dict]:
    """返回 {key: params} 字典副本（多策略对比起点）"""
    return {k: dict(v) for k, v in STRATEGY_DEFAULTS.items()}


# =============================================================================
# 策略分类（多策略对比页用于分组展示）
# =============================================================================

# 分类约定：
#   - core    : 参数化策略，开箱即用、信号稳定（ma_crossover/ml_rebalance）
#   - bridge  : DSL/桥接策略，行为高度依赖 YAML 配置（rule_based/screener_rule）
#   - demo    : 演示性策略，数据不足或参数特殊时退化（factor_rebalance 缺因子数据时）
STRATEGY_CATEGORY: dict[str, str] = {
    "ma_crossover": "core",
    "ml_rebalance": "core",
    "rule_based": "bridge",
    "screener_rule": "bridge",
    "factor_rebalance": "demo",
}

CATEGORY_LABEL: dict[str, str] = {
    "core": "📊 核心策略",
    "bridge": "🔌 桥接策略 (DSL/Conditions)",
    "demo": "🧪 演示策略",
}

# 渲染顺序
CATEGORY_ORDER: list[str] = ["core", "bridge", "demo"]


def get_strategy_category(strategy_key: str) -> str:
    """返回某策略的分类（默认 core）"""
    return STRATEGY_CATEGORY.get(strategy_key, "core")
