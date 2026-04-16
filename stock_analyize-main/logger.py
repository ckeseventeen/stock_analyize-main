import logging
import sys
import os
from datetime import datetime

_LOG_SESSION_TIME = datetime.now().strftime("%Y%m%d_%H%M%S")


def setup_logger(name="stock_analyzer", log_dir="logs"):
    """配置并返回日志记录器，同时输出到控制台和文件"""
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # 创建日志目录
    os.makedirs(log_dir, exist_ok=True)

    # 日志文件按会话时间命名（模块级时间戳，避免重复调用产生多个文件）
    log_file = os.path.join(log_dir, f"{_LOG_SESSION_TIME}.log")

    # 控制台 handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_fmt = logging.Formatter("[%(asctime)s] %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    console_handler.setFormatter(console_fmt)

    # 文件 handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_fmt = logging.Formatter("[%(asctime)s] %(levelname)-7s [%(filename)s:%(lineno)d] %(message)s",
                                 datefmt="%Y-%m-%d %H:%M:%S")
    file_handler.setFormatter(file_fmt)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info(f"日志文件: {log_file}")
    return logger
