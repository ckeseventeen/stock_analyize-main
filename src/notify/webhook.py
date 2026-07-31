"""
src/automation/alert/webhook.py — Webhook 推送通道（开放 API）

把告警事件以 JSON POST 到用户自己的系统（自建服务 / n8n / 飞书机器人网关等）。

请求体（application/json）：
    {
      "title": "...", "body": "...", "event_key": "...",
      "stock_code": "600519", "stock_name": "贵州茅台",
      "event_type": "price_below", "timestamp": "2026-07-10T15:00:00",
      "extras": {...}
    }

签名（可选）：配置 secret 后，对原始 JSON body 做 HMAC-SHA256，
放在请求头 `X-Alert-Signature-256: sha256=<hexdigest>`，
接收方用同样算法校验，防伪造（与 GitHub Webhook 同规范）。
"""
from __future__ import annotations

import hashlib
import hmac
import json

from src.notify.base import AlertChannel, AlertEvent
from src.utils.logger import get_logger

logger = get_logger("alert")


class WebhookChannel(AlertChannel):
    """
    通用 Webhook 推送通道。

    配置示例 (config/alerts.yaml):
        webhook:
          enable: true
          url: "https://your-server.com/hooks/stock-alert"
          secret: ""             # 可选，HMAC 签名密钥；或环境变量 WEBHOOK_SECRET
          headers:               # 可选，附加请求头（如鉴权 token）
            Authorization: "Bearer xxx"
          allow_private: false   # 自建内网服务需置 true（放行私网 IP）
          timeout: 10
    """

    name = "webhook"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.url = (self.config.get("url") or "").strip()
        self.secret = self._resolve_secret(self.config.get("secret"), "WEBHOOK_SECRET")
        self.extra_headers = dict(self.config.get("headers") or {})
        self.allow_private = bool(self.config.get("allow_private", False))
        self.timeout = int(self.config.get("timeout", self.timeout) or self.timeout)

        # 未配置 URL 时强制禁用，避免每次发送都失败刷日志
        if self.enabled and not self.url:
            logger.warning("[webhook] 未配置 url，已自动禁用该通道")
            self.enabled = False

    def _send_impl(self, event: AlertEvent) -> bool:
        raw_body = json.dumps(self._build_payload(event), ensure_ascii=False).encode("utf-8")

        headers = {"Content-Type": "application/json; charset=utf-8"}
        headers.update(self.extra_headers)
        if event.event_type:
            headers["X-Alert-Event"] = event.event_type
        if self.secret:
            digest = hmac.new(self.secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
            headers["X-Alert-Signature-256"] = f"sha256={digest}"

        resp = self._http_post(
            self.url,
            allow_private=self.allow_private,
            data=raw_body,
            headers=headers,
            timeout=self.timeout,
        )
        if 200 <= resp.status_code < 300:
            return True
        logger.error(f"[webhook] 目标返回 HTTP {resp.status_code}: {resp.text[:200]}")
        # 5xx 视为可重试（抛异常走基类重试），4xx 配置问题直接失败
        if resp.status_code >= 500:
            resp.raise_for_status()
        return False

    @staticmethod
    def _build_payload(event: AlertEvent) -> dict:
        return {
            "title": event.title,
            "body": event.body,
            "event_key": event.event_key,
            "stock_code": event.stock_code,
            "stock_name": event.stock_name,
            "event_type": event.event_type,
            "timestamp": event.timestamp.isoformat(timespec="seconds"),
            "extras": event.extras or {},
        }
