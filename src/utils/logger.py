"""
src/utils/logger.py — 统一日志管理模块（修复单例问题）

修复说明：
  原 logger.py 每次调用 setup_logger() 都以当前秒命名新日志文件，
  在一个进程内多次调用时会创建冗余 handler 和文件。
  本版本用模块级字典管理已初始化的 Logger，确保全进程单例。
"""
import logging
import os
import sys
from datetime import datetime

# 模块级单例存储：logger_name -> Logger 实例
_loggers: dict[str, logging.Logger] = {}

# 进程级别的日志文件名（进程启动时固定，不随调用时间变化）
_LOG_SESSION_TIME = datetime.now().strftime("%Y%m%d_%H%M%S")


def setup_logger(
    name: str = "stock_analyzer",
    log_dir: str = "logs",
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
) -> logging.Logger:
    """
    获取或创建具名 Logger（进程级单例，多次调用返回同一实例）。

    Args:
        name: Logger 名称，同名调用返回已有实例
        log_dir: 日志文件存放目录（相对路径以 cwd 为基准）
        console_level: 控制台输出级别（默认 INFO）
        file_level: 文件输出级别（默认 DEBUG）

    Returns:
        logging.Logger: 配置好的日志记录器
    """
    global _loggers

    # 单例检查：已存在直接返回
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)

    # 防止重复添加 handler（跨模块重复调用时的额外保护）
    if logger.handlers:
        _loggers[name] = logger
        return logger

    logger.setLevel(logging.DEBUG)

    # ── 控制台 handler ──
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_fmt)

    # ── 文件 handler（使用进程启动时间，不随每次调用变化）──
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{name}_{_LOG_SESSION_TIME}.log")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(file_level)
    file_fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-7s [%(filename)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_fmt)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # 缓存单例
    _loggers[name] = logger

    logger.info(f"日志初始化完成 → {log_file}")
    return logger


def get_logger(name: str = "stock_analyzer") -> logging.Logger:
    """
    获取已存在的 Logger（不自动创建），供模块内部使用。
    如果 Logger 不存在则创建并使用默认配置。
    """
    return setup_logger(name)
