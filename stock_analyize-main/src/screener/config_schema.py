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
    "weekly_macd_divergence": {"lookback_bars": "lookback_bars"},
    "daily_macd_divergence": {"lookback_bars": "lookback_bars"},
    "rsi_oversold": {"threshold": "threshold", "period": "period"},
    "price_above_ma": {"ma_period": "ma_period"},
}


def parse_screen_config(config_path: str) -> tuple[list[BaseCondition], dict]:
    """
    解析筛选配置文件

    Args:
        config_path: YAML配置文件路径

    Returns:
        (条件对象列表, 输出配置字典)

    配置示例:
        screen:
          conditions:
            - type: market_cap
              min: 50
              max: 2000
            - type: pe_range
              min: 0
              max: 30
            - type: weekly_macd_divergence
              lookback_bars: 60
          output:
            sort_by: 总市值(亿)
            limit: 50
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"筛选配置文件加载失败 [{config_path}]: {e}")
        return [], {}

    screen_config = config.get("screen", {})
    conditions_config = screen_config.get("conditions", [])
    output_config = screen_config.get("output", {})

    conditions = []
    for cond_dict in conditions_config:
        cond_type = cond_dict.get("type", "")
        if cond_type not in CONDITION_REGISTRY:
            logger.warning(f"未知筛选条件类型: {cond_type}，跳过")
            continue

        cond_class = CONDITION_REGISTRY[cond_type]
        param_map = _PARAM_MAP.get(cond_type, {})

        # 将YAML参数名映射为构造函数参数名
        kwargs = {}
        for yaml_key, init_key in param_map.items():
            if yaml_key in cond_dict:
                kwargs[init_key] = cond_dict[yaml_key]

        try:
            condition = cond_class(**kwargs)
            conditions.append(condition)
            logger.debug(f"解析筛选条件: {cond_type} -> {condition}")
        except Exception as e:
            logger.error(f"构建筛选条件 [{cond_type}] 失败: {e}")

    logger.info(f"从配置文件解析出 {len(conditions)} 个筛选条件")
    return conditions, output_config
