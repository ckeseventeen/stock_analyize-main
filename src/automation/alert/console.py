"""
src/automation/alert/console.py — 控制台 / 日志文件告警通道

兜底通道：无需任何第三方配置，推送到 stdout + logs/alerts.log。
用于本地开发、集成测试、Docker 容器日志查看。
"""
from __future__ import annotations

import logging
from pathlib import Path

from src.automation.alert.base import AlertChannel, AlertEvent
from src.utils.logger import get_logger

logger = get_logger("alert")

# 告警独立日志文件（与 stock_analyzer 主日志分离，便于前端读取）
_ALERT_LOG_PATH = Path("logs/alerts.log")
_ALERT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# 专用文件 handler（单例）
_file_logger: logging.Logger | None = None


def _get_alert_file_logger() -> logging.Logger:
    """懒加载独立的 alerts.log 文件 logger（供前端读取解析）"""
    global _file_logger
    if _file_logger is not None:
        return _file_logger

    fl = logging.getLogger("alert.file")
    fl.setLevel(logging.INFO)
    # 防重复添加 handler
    if not fl.handlers:
        handler = logging.FileHandler(_ALERT_LOG_PATH, encoding="utf-8")
        handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        fl.addHandler(handler)
        # 禁用向上传播，避免重复输出到主 logger 控制台
        fl.propagate = False

    _file_logger = fl
    return fl


class ConsoleChannel(AlertChannel):
    """
    控制台 + 本地日志文件告警通道（零配置兜底）。

    推送格式：
      [时间] 标题 | 股票代码/名称 | 事件类型
      正文...
    """

    name = "console"

    def _send_impl(self, event: AlertEvent) -> bool:
        # 1. 主 logger 输出到控制台
        header = f"{event.title}"
        meta = " | ".join(filter(None, [event.stock_code, event.stock_name, event.event_type]))
        if meta:
            header = f"{header} [{meta}]"

        logger.info(f"📣 {header}")
        # 分行打印正文（控制台友好）
        for line in event.body.splitlines() or [event.body]:
            logger.info(f"   {line}")

        # 2. 写入独立的 alerts.log（前端告警历史页读取）
        fl = _get_alert_file_logger()
        fl.info(
            f"{event.event_type}\t{event.stock_code}\t{event.stock_name}\t"
            f"{event.title}\t{event.body.replace(chr(10), ' / ')}"
        )

        return True
