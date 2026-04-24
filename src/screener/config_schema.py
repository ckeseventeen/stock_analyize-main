"""
src/screener/config_schema.py — 筛选配置YAML解析

将 YAML 配置文件解析为条件对象列表。
"""
import yaml

from src.screener.conditions import CONDITION_REGISTRY, BaseCondition
from src.utils.logger import get_logger

logger = get_logger("screener_config")

# 各条件类型支持的参数映射
_PARAM_MAP = {
    "market_cap": {"min": "min_cap", "max": "max_cap"},
    "pe_range": {"min": "min_pe", "max": "max_pe"},
    "pb_range": {"min": "min_pb", "max": "max_pb"},
    "price_range": {"min": "min_price", "max": "max_price"},
    "turnover_rate": {"min": "min_rate", "max": "max_rate"},
    "weekly_macd_divergence": {
        "lookback_bars": "lookback_bars",
        "zero_axis_filter": "zero_axis_filter",
        "multi_level_check": "multi_level_check",
    },
    "daily_macd_divergence": {
        "lookback_bars": "lookback_bars",
        "zero_axis_filter": "zero_axis_filter",
        "multi_level_check": "multi_level_check",
    },
    "rsi_oversold": {"threshold": "threshold", "period": "period"},
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
    "exclude_recent_unlock": {},
    "exclude_recent_large_unlock": {},
    "roe_filter": {"min_roe": "min_roe"},
    "multi_ma_bull": {
        "ma_list": "ma_list",
        "require_all_above": "require_all_above",
        "require_up_trend": "require_up_trend",
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

        for cond_dict in conditions_config:
            cond_type = cond_dict.get("type", "")
            if cond_type not in CONDITION_REGISTRY:
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
                all_conditions.append(condition)
                logger.debug(f"解析筛选条件 [{sid}]: {cond_type} -> {condition}")
            except Exception as e:
                logger.error(f"构建筛选条件 [{sid}:{cond_type}] 失败: {e}")

    logger.info(f"从策略 {strategy_ids} 中解析出 {len(all_conditions)} 个筛选条件")
    return all_conditions, final_output_config
