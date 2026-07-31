"""
src/screener/config_schema.py — 筛选配置YAML解析

将 YAML 配置文件解析为条件对象列表。
支持筛选 + 回测一体化配置。
"""
import yaml

from src.analysis.screening.conditions import CONDITION_REGISTRY, BaseCondition
from src.utils.logger import get_logger

logger = get_logger("screener_config")

# 各条件类型支持的参数映射
_PARAM_MAP = {
    "market_cap": {"min": "min_cap", "max": "max_cap"},
    "pe_range": {"min": "min_pe", "max": "max_pe"},
    "pb_range": {"min": "min_pb", "max": "max_pb"},
    "price_range": {"min": "min_price", "max": "max_price"},
    "turnover_rate": {"min": "min_rate", "max": "max_rate"},
    "price_change": {"min": "min_change", "max": "max_change"},
    "weekly_macd_divergence": {
        "lookback_bars": "lookback_bars",
        "zero_axis_filter": "zero_axis_filter",
        "multi_level_check": "multi_level_check",
        "order": "order",
        "max_bars_since_trough": "max_bars_since_trough",
    },
    "daily_macd_divergence": {
        "lookback_bars": "lookback_bars",
        "zero_axis_filter": "zero_axis_filter",
        "multi_level_check": "multi_level_check",
        "order": "order",
        "max_bars_since_trough": "max_bars_since_trough",
    },
    "rsi_oversold": {"threshold": "threshold", "period": "period"},
    "rsi_overbought": {"threshold": "threshold", "period": "period"},
    "price_above_ma": {"ma_period": "ma_period"},
    "box_breakout": {
        "lookback_bars": "lookback_bars",
        "breakout_pct": "breakout_pct",
        "consolidation_pct": "consolidation_pct",
    },
    "downtrend_breakout": {
        "lookback_bars": "lookback_bars",
        "min_touches": "min_touches",
        "breakout_pct": "breakout_pct",
    },
    "exclude_st": {},
    "exclude_delisting_risk": {},
    "exclude_risk": {"strict": "strict"},
    "roe_filter": {"min_roe": "min_roe"},
    "multi_ma_bull": {
        "ma_list": "ma_list",
        "require_all_above": "require_all_above",
        "require_up_trend": "require_up_trend",
        "tolerance": "tolerance",
    },
    "volume_break": {
        "lookback_bars": "lookback_bars",
        "vol_multiple": "vol_multiple",
    },
    "weekly_macd_gold_cross": {"require_zero_near": "require_zero_near"},
    "volume_shrink": {
        "lookback_bars": "lookback_bars",
        "shrink_ratio": "shrink_ratio",
    },
    "support_ma": {
        "ma_period": "ma_period",
        "close_touch": "close_touch",
    },
    "bollinger_breakout": {
        "period": "period",
        "std_dev": "std_dev",
        "direction": "direction",
    },
    "kdj_gold_cross": {
        "n": "n",
        "m1": "m1",
        "m2": "m2",
        "j_threshold": "j_threshold",
    },
    "ma_gold_cross": {
        "fast_period": "fast_period",
        "slow_period": "slow_period",
    },
    "ma_death_cross": {
        "fast_period": "fast_period",
        "slow_period": "slow_period",
    },
    "macd_hist_positive": {
        "consecutive": "consecutive",
    },
    "box_breakout_volume": {
        "lookback_bars": "lookback_bars",
        "breakout_pct": "breakout_pct",
        "consolidation_pct": "consolidation_pct",
        "vol_lookback": "vol_lookback",
        "vol_multiple": "vol_multiple",
    },
    "stop_loss": {
        "max_loss_pct": "max_loss_pct",
    },
    "trailing_stop": {
        "callback_pct": "callback_pct",
        "lookback": "lookback",
    },
    "volume_price_divergence": {
        "lookback_bars": "lookback_bars",
        "direction": "direction",
    },
    "northbound_flow": {
        "lookback_days": "lookback_days",
        "min_net_buy": "min_net_buy",
    },
    "ml_top_k": {
        "top_k": "top_k",
        "min_score": "min_score",
    },
    # Phase 5 卖出 / 回调预警
    "weekly_macd_top_divergence": {
        "lookback_bars": "lookback_bars",
    },
    "daily_macd_top_divergence": {
        "lookback_bars": "lookback_bars",
    },
    "kdj_death_cross": {
        "n": "n",
        "m1": "m1",
        "m2": "m2",
        "j_threshold": "j_threshold",
    },
    "bias": {
        "ma_period": "ma_period",
        "threshold": "threshold",
        "direction": "direction",
    },
    "break_below_ma": {
        "ma_period": "ma_period",
        "lookback": "lookback",
    },
    "volume_blowoff": {
        "lookback_bars": "lookback_bars",
        "vol_multiple": "vol_multiple",
        "min_price_change_pct": "min_price_change_pct",
    },
    "atr_trailing_stop": {
        "atr_period": "atr_period",
        "multiplier": "multiplier",
        "lookback": "lookback",
    },
}

# 不适合回测的基本面/排除类条件（仅在实时 Spot 筛选时有效）
SPOT_ONLY_TYPES = {
    "exclude_risk", "exclude_st", "exclude_delisting_risk",
    "market_cap", "pe_range", "pb_range",
    "price_range", "turnover_rate", "roe_filter",
    "price_change", "stop_loss", "northbound_flow",
}


def list_strategies(config_path: str) -> dict[str, str]:
    """
    列出配置文件中所有的策略 ID 与 名称。

    Returns:
        { "strategy_id": "策略显示名称", ... }
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception:
        return {}

    strategies_cfg = config.get("strategies", {})
    if not strategies_cfg:
        # 兼容旧格式
        if "screen" in config:
            return {"default": "默认策略"}
        return {}

    return {sid: cfg.get("name", sid) for sid, cfg in strategies_cfg.items()}


def _build_conditions(conditions_config: list[dict], sid: str = "",
                       strict: bool | None = None) -> list[BaseCondition]:
    """
    从条件配置列表构建条件对象列表。

    B21 修复：未知类型默认仍然 warning + skip（保留向后兼容），
    但若环境变量 SCREENER_STRICT=1 或 strict=True 则抛错，便于 CI 检测。

    Args:
        strict: True 抛 ValueError；False/None 警告并跳过；
                None 时读 SCREENER_STRICT 环境变量
    """
    if strict is None:
        from src.core.settings import settings
        strict = settings.screener_strict

    result = []
    unknown_types: list[str] = []
    for cond_dict in conditions_config:
        if not isinstance(cond_dict, dict):
            logger.warning(f"[{sid}] 条件项不是字典，跳过: {cond_dict!r}")
            continue

        # 组合节点（含 logic + conditions）→ 递归构建 CompositeCondition，
        # 支持 "A 且 (B 或 C)" 这类嵌套表达
        if "logic" in cond_dict and "conditions" in cond_dict:
            from src.analysis.screening.conditions import CompositeCondition

            children = _build_conditions(
                cond_dict.get("conditions") or [], sid=sid, strict=strict)
            if not children:
                logger.warning(f"[{sid}] 组合条件无有效子条件，跳过")
                continue
            try:
                result.append(CompositeCondition(
                    logic=cond_dict.get("logic", "all"), conditions=children))
                logger.debug(
                    f"解析组合条件 [{sid}]: {cond_dict.get('logic')} "
                    f"× {len(children)} 个子条件")
            except Exception as e:
                logger.error(f"构建组合条件 [{sid}] 失败: {e}")
                if strict:
                    raise
            continue

        cond_type = cond_dict.get("type", "")
        if cond_type not in CONDITION_REGISTRY:
            unknown_types.append(cond_type)
            logger.warning(f"[{sid}] 未知筛选条件类型: {cond_type}，跳过")
            continue

        cond_class = CONDITION_REGISTRY[cond_type]
        param_map = _PARAM_MAP.get(cond_type, {})

        kwargs = {}
        for yaml_key, init_key in param_map.items():
            if yaml_key in cond_dict:
                kwargs[init_key] = cond_dict[yaml_key]

        try:
            condition = cond_class(**kwargs)
            result.append(condition)
            logger.debug(f"解析筛选条件 [{sid}]: {cond_type} -> {condition}")
        except Exception as e:
            logger.error(f"构建筛选条件 [{sid}:{cond_type}] 失败: {e}")
            if strict:
                raise

    if unknown_types and strict:
        raise ValueError(
            f"[{sid}] 配置包含未知条件类型: {unknown_types}。"
            f"可用类型: {sorted(CONDITION_REGISTRY.keys())}"
        )
    return result


def parse_screen_config(config_path: str, strategy_ids: list[str] | None = None) -> tuple[list[BaseCondition], dict]:
    """
    解析筛选配置文件中的指定策略。

    Args:
        config_path: YAML配置文件路径
        strategy_ids: 要加载的策略 ID 列表。None 表示加载第一个策略（兼容旧版）。

    Returns:
        (合并后的条件对象列表, 最后一个策略的输出配置)
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"筛选配置文件加载失败 [{config_path}]: {e}")
        return [], {}

    # 获取所有策略配置
    strategies_cfg = config.get("strategies", {})

    # 兼容性处理：如果没用新格式，尝试读取旧的 "screen" key
    if not strategies_cfg and "screen" in config:
        strategies_cfg = {"default": config["screen"]}
        strategy_ids = ["default"]

    if not strategies_cfg:
        logger.warning("配置文件中未找到策略定义")
        return [], {}

    # 如果没指定 ID，默认取第一个
    if not strategy_ids:
        strategy_ids = [list(strategies_cfg.keys())[0]]

    all_conditions = []
    final_output_config = {}

    for sid in strategy_ids:
        if sid not in strategies_cfg:
            logger.warning(f"跳过不存在的策略: {sid}")
            continue

        s_cfg = strategies_cfg[sid]
        conditions_config = s_cfg.get("conditions", [])
        output_config = s_cfg.get("output", {})

        # 记录最后一个选中的输出配置作为最终配置（也可按需合并）
        final_output_config.update(output_config)

        all_conditions.extend(_build_conditions(conditions_config, sid))

    logger.info(f"从策略 {strategy_ids} 中解析出 {len(all_conditions)} 个筛选条件")
    return all_conditions, final_output_config


def parse_backtest_from_strategy(config_path: str, strategy_id: str) -> dict:
    """
    从筛选策略配置中提取回测参数。

    策略可包含可选的 backtest 段：
        strategies:
          my_strategy:
            conditions: [...]        # 技术类条件自动用作买入信号
            backtest:
              sell_conditions: [...]  # 卖出条件
              buy_logic: all
              sell_logic: any
              position_size: 0.95
              default_stock: "600519"
              days_back: 1000

    Returns:
        {
            "buy_conditions": list[dict],    # 技术类买入条件配置
            "sell_conditions": list[dict],    # 卖出条件配置
            "buy_logic": str,
            "sell_logic": str,
            "position_size": float,
            "default_stock": str,
            "days_back": int,
            "has_backtest": bool,
        }
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception:
        return {"has_backtest": False}

    strategies_cfg = config.get("strategies", {})
    if strategy_id not in strategies_cfg:
        return {"has_backtest": False}

    s_cfg = strategies_cfg[strategy_id]
    conditions_config = s_cfg.get("conditions", [])
    backtest_cfg = s_cfg.get("backtest", {})

    # 从 conditions 中提取技术类条件作为买入信号
    tech_buy_conditions = [
        c for c in conditions_config
        if c.get("type") not in SPOT_ONLY_TYPES
    ]

    sell_conditions = backtest_cfg.get("sell_conditions", []) if backtest_cfg else []

    return {
        "buy_conditions": tech_buy_conditions,
        "sell_conditions": sell_conditions,
        "buy_logic": backtest_cfg.get("buy_logic", "all") if backtest_cfg else "all",
        "sell_logic": backtest_cfg.get("sell_logic", "any") if backtest_cfg else "any",
        "position_size": float(backtest_cfg.get("position_size", 0.95)) if backtest_cfg else 0.95,
        "default_stock": backtest_cfg.get("default_stock", "") if backtest_cfg else "",
        "days_back": int(backtest_cfg.get("days_back", 1000)) if backtest_cfg else 1000,
        "has_backtest": bool(backtest_cfg),
    }


def get_strategy_full_config(config_path: str, strategy_id: str) -> dict:
    """
    获取策略的完整配置（包含条件列表和回测配置），供前端编辑器使用。

    Returns:
        { "name": str, "conditions": list[dict], "backtest": dict, "output": dict }
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception:
        return {}

    strategies_cfg = config.get("strategies", {})
    return dict(strategies_cfg.get(strategy_id, {}))
