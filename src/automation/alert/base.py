"""
src/automation/alert/base.py — 告警通道抽象基类

所有推送通道（Server酱、Bark、PushPlus、Console 等）均继承 AlertChannel，
统一 send() 接口，便于上层（价格预警、财报监控）透明调用多个通道。
"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

import requests

from src.utils.logger import get_logger

logger = get_logger("alert")


# ========================
# 告警事件数据结构
# ========================

@dataclass
class AlertEvent:
    """
    单次告警事件的数据载体。

    Attributes:
        title: 推送标题（通常为"股票名 + 事件类型"）
        body: 推送正文（Markdown/HTML 格式，具体通道会自行处理）
        event_key: 去重用的事件唯一键（格式如 "600519:price_below_1500:2026-04-15"）
        stock_code: 股票代码（可选，便于前端/日志过滤）
        stock_name: 股票名称（可选）
        event_type: 告警类型（如 price_below / earnings_forecast / new_announcement）
        timestamp: 事件生成时间
        extras: 通道专属扩展字段（如 Bark 的 group、level）
    """
    title: str
    body: str
    event_key: str
    stock_code: str = ""
    stock_name: str = ""
    event_type: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    extras: dict = field(default_factory=dict)


# ========================
# 通道 ABC
# ========================

class AlertChannel(ABC):
    """
    告警推送通道抽象基类。

    子类只需：
      1. 设置类属性 `name`（小写标识符）
      2. 实现 `_send_impl(title, body, **kwargs) -> bool`

    基类统一处理：
      - 超时 / 重试
      - 日志（成功/失败）
      - 异常不抛出（单通道失败不影响其他通道）
    """

    # 通道名（子类必须覆盖，对应 config/alerts.yaml 里的 key）
    name: str = "base"

    # 默认 HTTP 超时（秒）
    timeout: int = 10

    # 最大重试次数（含首次）
    max_retries: int = 3

    def __init__(self, config: dict | None = None):
        """
        Args:
            config: 通道配置字典（从 alerts.yaml 传入），各通道按需解析
        """
        self.config = config or {}
        self.enabled = bool(self.config.get("enable", False))

    def send(self, event: AlertEvent) -> bool:
        """
        统一入口：发送告警事件。

        Args:
            event: AlertEvent 实例

        Returns:
            True 表示发送成功，False 表示失败（异常已捕获，不抛出）
        """
        if not self.enabled:
            logger.debug(f"通道 [{self.name}] 未启用，跳过")
            return False

        try:
            ok = self._send_with_retry(event)
            if ok:
                logger.info(f"[{self.name}] 告警推送成功: {event.title}")
            else:
                logger.error(f"[{self.name}] 告警推送失败: {event.title}")
            return ok
        except Exception as e:
            # 捕获所有异常，确保不影响其他通道
            logger.error(f"[{self.name}] 推送异常: {e}", exc_info=True)
            return False

    def _send_with_retry(self, event: AlertEvent) -> bool:
        """内部重试封装：子类不需关心重试细节"""
        last_err: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return self._send_impl(event)
            except Exception as e:
                last_err = e
                logger.warning(
                    f"[{self.name}] 第 {attempt}/{self.max_retries} 次发送失败: {e}"
                )
        if last_err is not None:
            logger.error(f"[{self.name}] 达到最大重试次数，放弃推送: {last_err}")
        return False

    @abstractmethod
    def _send_impl(self, event: AlertEvent) -> bool:
        """
        子类实现：实际调用推送 API。

        返回 True/False；遇到可重试错误应抛异常（由基类统一重试）。
        """
        raise NotImplementedError

    # ── 公共工具 ──

    @staticmethod
    def _resolve_secret(cfg_value: str | None, env_var: str) -> str:
        """
        敏感字段取值顺序：环境变量 > YAML 配置。

        Args:
            cfg_value: YAML 中配置的值
            env_var: 对应的环境变量名

        Returns:
            解析后的密钥；都没有时返回空字符串
        """
        env_val = os.environ.get(env_var, "").strip()
        if env_val:
            return env_val
        return (cfg_value or "").strip()

    @staticmethod
    def _http_post(url: str, **kwargs) -> requests.Response:
        """封装 POST + 默认超时"""
        kwargs.setdefault("timeout", 10)
        return requests.post(url, **kwargs)

    @staticmethod
    def _http_get(url: str, **kwargs) -> requests.Response:
        """封装 GET + 默认超时"""
        kwargs.setdefault("timeout", 10)
        return requests.get(url, **kwargs)
