"""
src/automation/alert/bark.py — Bark (iOS) 推送通道

API 文档：https://bark.day.app/
端点（两种写法任选）：
  POST https://api.day.app/push   (JSON 体)
  GET  https://api.day.app/{key}/{title}/{body}
本实现使用 POST JSON，便于传递特殊字符和扩展参数。
"""
from __future__ import annotations

from src.automation.alert.base import AlertChannel, AlertEvent
from src.utils.logger import get_logger

logger = get_logger("alert")


class BarkChannel(AlertChannel):
    """
    Bark iOS 推送通道。

    配置示例 (config/alerts.yaml):
        bark:
          enable: true
          key: "xxxxxx"            # 或设置环境变量 BARK_KEY
          server: "https://api.day.app"  # 私有服务器可覆盖
          group: "股票预警"          # 分组标签
          sound: "bell"              # 铃声
          level: "active"            # active/timeSensitive/passive
    """

    name = "bark"

    # Bark 官方公共服务器
    _DEFAULT_SERVER = "https://api.day.app"

    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.key = self._resolve_secret(self.config.get("key"), "BARK_KEY")
        self.server = (self.config.get("server") or self._DEFAULT_SERVER).rstrip("/")
        # 可选样式参数
        self.group = self.config.get("group", "股票预警")
        self.sound = self.config.get("sound", "")
        self.level = self.config.get("level", "")  # active/timeSensitive/passive

        if self.enabled and not self.key:
            logger.warning("[bark] 未配置 key（YAML 或 BARK_KEY），已自动禁用该通道")
            self.enabled = False

    def _send_impl(self, event: AlertEvent) -> bool:
        url = f"{self.server}/push"
        payload = {
            "device_key": self.key,
            "title": event.title[:64],
            "body": event.body[:1024],  # 控制正文长度
            "group": self.group,
        }
        if self.sound:
            payload["sound"] = self.sound
        if self.level:
            payload["level"] = self.level

        # 合并事件 extras 里的字段（支持用户自定义 badge / icon / url）
        for k, v in (event.extras or {}).items():
            if v is not None:
                payload[k] = v

        resp = self._http_post(url, json=payload, timeout=self.timeout)
        resp.raise_for_status()

        try:
            result = resp.json()
            # Bark 返回 {"code":200,"message":"success"} 表示成功
            if result.get("code") == 200:
                return True
            logger.error(f"[bark] API 返回错误: {result}")
            return False
        except ValueError:
            return resp.status_code == 200
