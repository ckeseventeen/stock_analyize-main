"""
src/utils/logger.py — 统一日志管理模块

使用 TimedRotatingFileHandler 按天轮转，保留30天日志。

架构设计：
  - Handler（控制台 + 文件）只绑定在根 Logger "stock_analyzer" 上，全局仅一套。
  - 各模块通过 get_logger("screener") 获取子 Logger "stock_analyzer.screener"，
    日志沿 propagate 链传递到根 Logger 的 handler，避免重复写入。
  - setup_logger() 仅在首次调用时初始化 handler，后续调用幂等。
"""
import logging
import logging.handlers
import os
import sys
import threading

# 根 Logger 名称，所有子 Logger 均以此为前缀
_ROOT_LOGGER_NAME = "stock_analyzer"

# 日志保留天数
_LOG_RETENTION_DAYS = 30

# 初始化锁：保证多线程环境下 handler 只创建一次
_init_lock = threading.Lock()
_initialized = False


def setup_logger(
    name: str = _ROOT_LOGGER_NAME,
    log_dir: str = "logs",
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
) -> logging.Logger:
    """
    初始化并返回 Logger。

    首次调用时在根 Logger "stock_analyzer" 上创建 Console + File handler；
    后续调用直接返回已有 Logger（线程安全、幂等）。

    Args:
        name: Logger 名称。传入模块名（如 "screener"）会自动映射为
              "stock_analyzer.screener" 子 Logger。
        log_dir: 日志文件存放目录（仅首次初始化时生效）
        console_level: 控制台输出级别（仅首次初始化时生效）
        file_level: 文件输出级别（仅首次初始化时生效）

    Returns:
        logging.Logger: 配置好的日志记录器
    """
    global _initialized

    # 1. 确保根 Logger 的 handler 只初始化一次
    if not _initialized:
        with _init_lock:
            if not _initialized:
                _setup_root_handlers(log_dir, console_level, file_level)
                _initialized = True

    # 2. 根据传入的 name 获取对应 Logger
    #    - name == _ROOT_LOGGER_NAME → 直接返回根 Logger
    #    - name 已经是 "stock_analyzer.xxx" → 直接用
    #    - name 是其他字符串（如 "screener"）→ 转为 "stock_analyzer.screener"
    if name == _ROOT_LOGGER_NAME:
        return logging.getLogger(_ROOT_LOGGER_NAME)

    if name.startswith(_ROOT_LOGGER_NAME + "."):
        full_name = name
    else:
        full_name = f"{_ROOT_LOGGER_NAME}.{name}"

    child_logger = logging.getLogger(full_name)
    # 子 Logger 不需要自己的 handler，通过 propagate 传递到根 Logger
    # 但要确保不重复添加 handler（某些场景可能被外部代码修改）
    child_logger.setLevel(logging.DEBUG)
    return child_logger


def _setup_root_handlers(
    log_dir: str,
    console_level: int,
    file_level: int,
) -> None:
    """在根 Logger 上绑定 Console + TimedRotatingFile handler（仅调用一次）。"""
    root_logger = logging.getLogger(_ROOT_LOGGER_NAME)
    root_logger.setLevel(logging.DEBUG)

    # 防御：如果已有 handler（比如被 pytest 注入），不重复添加
    if root_logger.handlers:
        return

    # ── 控制台 handler ──
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_fmt)

    # ── 文件 handler：按天轮转，保留30天 ──
    os.makedirs(log_dir, exist_ok=True)
    base_filename = os.path.join(log_dir, "stock_analyzer.log")
    file_handler = logging.handlers.TimedRotatingFileHandler(
        base_filename,
        when="midnight",
        interval=1,
        backupCount=_LOG_RETENTION_DAYS,
        encoding="utf-8",
    )
    file_handler.setLevel(file_level)
    file_fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-7s [%(name)s] [%(filename)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_fmt)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # 阻止日志继续向 Python 默认 root logger 传播（避免第三方库的 handler 拿到）
    root_logger.propagate = False

    root_logger.debug("日志模块初始化完成（根 Logger handler 已就绪）")


def get_logger(name: str = _ROOT_LOGGER_NAME) -> logging.Logger:
    """
    获取 Logger，供各模块使用。

    推荐用法::

        from src.utils.logger import get_logger
        logger = get_logger(__name__)   # 或 get_logger("screener")

    首次调用时自动初始化根 Logger 的 handler。
    """
    return setup_logger(name)
