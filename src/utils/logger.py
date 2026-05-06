"""
src/utils/logger.py — 统一日志管理模块

使用 TimedRotatingFileHandler 按天轮转，保留30天日志。
修复：原版每次进程启动创建独立文件，500个文件累积到17MB。
"""
import logging
import logging.handlers
import os
import sys
from datetime import datetime

# 模块级单例存储：logger_name -> Logger 实例
_loggers: dict[str, logging.Logger] = {}

# 日志保留天数
_LOG_RETENTION_DAYS = 30


def setup_logger(
    name: str = "stock_analyzer",
    log_dir: str = "logs",
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
) -> logging.Logger:
    """
    获取或创建具名 Logger（进程级单例，多次调用返回同一实例）。

    日志文件按天轮转（stock_analyzer.log.YYYY-MM-DD），
    自动保留最近 _LOG_RETENTION_DAYS 天。

    Args:
        name: Logger 名称，同名调用返回已有实例
        log_dir: 日志文件存放目录
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

    # 防止重复添加 handler
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

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # 缓存单例
    _loggers[name] = logger

    logger.debug(f"日志模块初始化: {name}")
    return logger


def get_logger(name: str = "stock_analyzer") -> logging.Logger:
    """
    获取已存在的 Logger（不自动创建），供模块内部使用。
    如果 Logger 不存在则创建并使用默认配置。
    """
    return setup_logger(name)
