"""
src/automation/alert/pushplus.py — PushPlus 推送通道

API 文档：https://www.pushplus.plus/doc/
端点：POST http://www.pushplus.plus/send
请求体 JSON：
  - token: 用户 token
  - title: 标题
  - content: 正文（支持 HTML）
  - template: html / txt / json / markdown
  - topic: 可选群组编码（群发）
响应：{"code":200,"msg":"...","data":"..."}
"""
from __future__ import annotations

from src.automation.alert.base import AlertChannel, AlertEvent
from src.utils.logger import get_logger

logger = get_logger("alert")


class PushPlusChannel(AlertChannel):
    """
    PushPlus 推送通道（支持微信公众号推送）。

    配置示例 (config/alerts.yaml):
        pushplus:
          enable: true
          token: "xxxxxx"           # 或设置环境变量 PUSHPLUS_TOKEN
          template: "html"           # html/txt/json/markdown
          topic: ""                  # 群发编码（可选）
    """

    name = "pushplus"
    _ENDPOINT = "http://www.pushplus.plus/send"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.token = self._resolve_secret(self.config.get("token"), "PUSHPLUS_TOKEN")
        self.template = self.config.get("template", "html")
        self.topic = self.config.get("topic", "")

        if self.enabled and not self.token:
            logger.warning("[pushplus] 未配置 token（YAML 或 PUSHPLUS_TOKEN），已自动禁用该通道")
            self.enabled = False

    def _send_impl(self, event: AlertEvent) -> bool:
        payload = {
            "token": self.token,
            "title": event.title[:64],
            "content": self._format_body(event),
            "template": self.template,
        }
        if self.topic:
            payload["topic"] = self.topic

        resp = self._http_post(self._ENDPOINT, json=payload, timeout=self.timeout)
        resp.raise_for_status()

        try:
            result = resp.json()
            if result.get("code") == 200:
                return True
            logger.error(f"[pushplus] API 返回错误: {result}")
            return False
        except ValueError:
            return resp.status_code == 200

    def _format_body(self, event: AlertEvent) -> str:
        """根据 template 类型包装正文"""
        if self.template == "html":
            # HTML 模板：保留换行 + 元信息
            meta = []
            if event.stock_name or event.stock_code:
                meta.append(f"<b>{event.stock_name}</b> ({event.stock_code})")
            if event.event_type:
                meta.append(f"类型: <code>{event.event_type}</code>")
            meta.append(f"时间: {event.timestamp:%Y-%m-%d %H:%M:%S}")
            body_html = event.body.replace("\n", "<br/>")
            return "<p>" + " | ".join(meta) + "</p><hr/><p>" + body_html + "</p>"
        return event.body
