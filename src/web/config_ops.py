"""
src/web/config_ops.py — 兼容 shim

实现已迁移到 src/core/config_io.py（核心层）。
web 层旧 import 继续可用；新代码请直接 import src.core.config_io。
"""
from src.core.config_io import *  # noqa: F401,F403
from src.core.config_io import (  # noqa: F401
    ALERT_LOG_PATH,
    ALERT_STATE_PATH,
    CACHE_DIR,
    CONFIG_DIR,
    LOGS_DIR,
    MARKET_CONFIG_PATHS,
    MARKET_LABELS,
    OUTPUT_DIR,
    PROJECT_ROOT,
    _cached_load_yaml,
    _read_yaml_file,
    _resolve_dotted,
)
