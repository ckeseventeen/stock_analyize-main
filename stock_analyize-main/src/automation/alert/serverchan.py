"""
src/automation/alert/serverchan.py — Server酱 推送通道

API 文档：https://sct.ftqq.com/
端点：https://sctapi.ftqq.com/{SENDKEY}.send
请求体（form-data 或 application/x-www-form-urlencoded）：
  - title: 标题（必填，32 字内）
  - desp : 正文（Markdown 格式，支持 #、**、![]() 等）
响应示例：{"code":0,"message":"...","data":{"pushid":"..."}}
"""
from __future__ import annotations

from src.automation.alert.base import AlertChannel, AlertEvent
from src.utils.logger import get_logger

logger = get_logger("alert")


class ServerChanChannel(AlertChannel):
    """
    Server酱 Turbo 版推送通道。

    配置示例 (config/alerts.yaml):
        serverchan:
          enable: true
          sendkey: "SCT..."              # 或设置环境变量 SERVERCHAN_KEY
    """

    name = "serverchan"

    # Server酱端点模板
    _ENDPOINT = "https://sctapi.ftqq.com/{sendkey}.send"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        # 敏感字段：优先读环境变量
        self.sendkey = self._resolve_secret(self.config.get("sendkey"), "SERVERCHAN_KEY")

        # 未配置 sendkey 时强制禁用，避免每次发送都失败刷日志
        if self.enabled and not self.sendkey:
            logger.warning("[serverchan] 未配置 sendkey（YAML 或 SERVERCHAN_KEY），已自动禁用该通道")
            self.enabled = False

    def _send_impl(self, event: AlertEvent) -> bool:
        url = self._ENDPOINT.format(sendkey=self.sendkey)
        payload = {
            # Server酱 title 限长 32 字符，超长截断
            "title": event.title[:32],
            # desp 支持 Markdown，附加元信息提升可读性
            "desp": self._format_body(event),
        }
        resp = self._http_post(url, data=payload, timeout=self.timeout)
        resp.raise_for_status()

        # 解析返回码：0 表示成功
        try:
            result = resp.json()
            code = result.get("code", -1)
            if code == 0:
                return True
            logger.error(f"[serverchan] API 返回错误码 {code}: {result.get('message')}")
            return False
        except ValueError:
            # 非 JSON 返回，按 HTTP 200 视为成功
            return resp.status_code == 200

    @staticmethod
    def _format_body(event: AlertEvent) -> str:
        """组装 Markdown 正文：事件元信息 + 用户正文"""
        lines = []
        meta_parts = []
        if event.stock_name or event.stock_code:
            meta_parts.append(f"**{event.stock_name}** ({event.stock_code})")
        if event.event_type:
            meta_parts.append(f"类型: `{event.event_type}`")
        meta_parts.append(f"时间: {event.timestamp:%Y-%m-%d %H:%M:%S}")
        if meta_parts:
            lines.append(" | ".join(meta_parts))
            lines.append("")  # 空行分隔
        lines.append(event.body)
        return "\n".join(lines)
