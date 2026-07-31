"""
src/automation/alert/base.py — 告警通道抽象基类

所有推送通道（Server酱、Bark、PushPlus、Console 等）均继承 AlertChannel，
统一 send() 接口，便于上层（价格预警、财报监控）透明调用多个通道。
"""
from __future__ import annotations

import ipaddress
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urlparse

import requests

from src.utils.logger import get_logger

logger = get_logger("alert")


# SEC3 修复：SSRF 防护白名单
# 已知的合法推送服务域名后缀，其他域名要么明确放行（user override）要么拒绝
_ALERT_DOMAIN_WHITELIST = (
    "sct.ftqq.com",        # Server酱
    "sctapi.ftqq.com",
    "day.app",             # Bark 官方
    "api.day.app",
    "pushplus.plus",       # PushPlus
    "www.pushplus.plus",
)

_BLOCKED_HOSTS = (
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "169.254.169.254",    # AWS/GCP metadata endpoint
    "metadata.google.internal",
    "metadata",
)


def _validate_url(url: str, allow_private: bool = False) -> bool:
    """
    SEC3 修复：校验告警 URL，防止 SSRF。

    Args:
        url: 待校验的 URL
        allow_private: True 时允许私网 IP（用于自建 Bark/Gotify 服务器，需用户显式开启）

    Returns:
        True = 合法可访问
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    # 阻止云元数据端点（即使 allow_private 也禁止）
    if host in _BLOCKED_HOSTS:
        logger.warning(f"[alert SSRF] 拒绝访问元数据/本地地址: {host}")
        return False
    # 私网 IP 校验
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_loopback or ip.is_link_local:
            logger.warning(f"[alert SSRF] 拒绝访问环回/链路本地: {host}")
            return False
        if (ip.is_private or ip.is_reserved) and not allow_private:
            logger.warning(f"[alert SSRF] 拒绝访问私网/保留地址（如需启用置 allow_private=True）: {host}")
            return False
    except ValueError:
        # 不是 IP，是域名 — 检查白名单（如果 host 不在白名单且 allow_private=False，warning 但不拒绝，因为用户可能用自定义服务）
        if not allow_private and not any(host == d or host.endswith("." + d) for d in _ALERT_DOMAIN_WHITELIST):
            logger.warning(
                f"[alert SSRF] URL 域名 '{host}' 不在已知白名单内，"
                f"如非攻击请检查配置或将其加入白名单"
            )
    return True


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
            env_var: 对应的环境变量名（保留兼容老调用）

        Returns:
            解析后的密钥；都没有时返回空字符串
        """
        # 优先走统一 settings 单例（PR 1）；旧 env_var 名称兼容查询
        try:
            from src.core.settings import settings
            key_attr = env_var.lower()
            val = getattr(settings, key_attr, "")
            if val:
                return val.strip() if isinstance(val, str) else str(val)
        except Exception:
            pass
        # 兜底：直接读 os.environ
        env_val = os.environ.get(env_var, "").strip()
        if env_val:
            return env_val
        return (cfg_value or "").strip()

    @classmethod
    def _http_post(cls, url: str, allow_private: bool = False, **kwargs) -> requests.Response:
        """封装 POST + 默认超时 + SSRF 校验"""
        if not _validate_url(url, allow_private=allow_private):
            raise ValueError(f"URL 未通过 SSRF 校验: {url}")
        kwargs.setdefault("timeout", 10)
        return requests.post(url, **kwargs)

    @classmethod
    def _http_get(cls, url: str, allow_private: bool = False, **kwargs) -> requests.Response:
        """封装 GET + 默认超时 + SSRF 校验"""
        if not _validate_url(url, allow_private=allow_private):
            raise ValueError(f"URL 未通过 SSRF 校验: {url}")
        kwargs.setdefault("timeout", 10)
        return requests.get(url, **kwargs)
