"""
src/notify/ — 告警通道包（跨层横切能力）

位置说明：本包原在 `src/automation/alert/`。告警是**横切关注点**——
services（页面触发的预警）、automation（调度任务）、ml（训练完成通知）
都要用它。放在 automation 下导致 services↔automation、ml↔automation
两对循环依赖，故下沉为与业务层平级的独立包。

依赖方向：notify 只依赖 core/utils，不反向依赖任何业务层。

对外 API：
  - AlertChannel / AlertEvent   : 基类与数据结构
  - CHANNEL_REGISTRY            : 通道名 → 类 的映射
  - build_channels(config)      : 从 YAML 配置构建已启用的通道列表
  - dispatch(event, channels, store) : 带去重的统一分发函数
  - AlertStateStore             : 去重状态存储
"""
from __future__ import annotations

from collections.abc import Iterable

from src.notify.bark import BarkChannel
from src.notify.base import AlertChannel, AlertEvent
from src.notify.console import ConsoleChannel
from src.notify.pushplus import PushPlusChannel
from src.notify.serverchan import ServerChanChannel
from src.notify.state import AlertStateStore
from src.notify.webhook import WebhookChannel
from src.utils.logger import get_logger

logger = get_logger("alert")


# 通道注册表：新增通道只需在此追加一行
CHANNEL_REGISTRY: dict[str, type[AlertChannel]] = {
    "console": ConsoleChannel,
    "serverchan": ServerChanChannel,
    "bark": BarkChannel,
    "pushplus": PushPlusChannel,
    "webhook": WebhookChannel,
}


def _check_secret_in_yaml(name: str, ch_cfg: dict) -> None:
    """
    SEC1 修复：启动时检测 alerts.yaml 是否包含明文密钥。

    任意密钥字段非空 + 不是环境变量占位符 → 警告（提示用户切换到环境变量）。
    """
    secret_keys = ("sendkey", "key", "token")
    suspicious = []
    for k in secret_keys:
        v = ch_cfg.get(k)
        if isinstance(v, str) and v.strip() and not v.strip().startswith("${"):
            # 非空且非 ${ENV_VAR} 占位符 → 怀疑泄露
            suspicious.append(k)
    if suspicious:
        logger.warning(
            f"⚠️ [SEC1] 通道 '{name}' 的 alerts.yaml 包含明文密钥字段: {suspicious}。"
            f"建议改用环境变量（SERVERCHAN_KEY / BARK_KEY / PUSHPLUS_TOKEN）"
            f"或将密钥写入 config/alerts.local.yaml（已在 .gitignore 排除）。"
        )


def build_channels(config: dict) -> list[AlertChannel]:
    """
    从 alerts.yaml 解析后的 dict 构建启用的通道列表。

    Args:
        config: 形如
            {
              "channels": {
                "serverchan": {"enable": True, "sendkey": "..."},
                "bark": {"enable": False, ...},
                ...
              }
            }

    Returns:
        已启用通道实例列表（enabled=True 且配置合法）
    """
    channels_cfg = (config or {}).get("channels", {}) or {}
    channels: list[AlertChannel] = []

    for name, cls in CHANNEL_REGISTRY.items():
        ch_cfg = channels_cfg.get(name) or {}
        if not ch_cfg.get("enable", False):
            continue
        # SEC1：检测密钥泄露风险
        _check_secret_in_yaml(name, ch_cfg)
        try:
            ch = cls(ch_cfg)
            if ch.enabled:  # 构造函数内可能因缺密钥而自动禁用
                channels.append(ch)
                logger.info(f"告警通道已启用: {name}")
        except Exception as e:
            logger.error(f"告警通道 [{name}] 初始化失败: {e}", exc_info=True)

    if not channels:
        logger.warning("未启用任何告警通道；默认加入 console 通道兜底")
        channels.append(ConsoleChannel({"enable": True}))

    return channels


def dispatch(
    event: AlertEvent,
    channels: Iterable[AlertChannel],
    store: AlertStateStore | None = None,
    cooldown_hours: int = 24,
) -> dict[str, bool]:
    """
    把单个事件派发到所有通道，带去重检查。

    Args:
        event: 告警事件
        channels: 通道列表
        store: 去重存储（None 时跳过去重）
        cooldown_hours: 冷却窗口

    Returns:
        各通道的发送结果 {通道名: True/False}；若被去重跳过，整体返回空 dict
    """
    if store is not None and store.was_fired(event.event_key, cooldown_hours):
        logger.debug(f"事件在冷却窗口内，跳过推送: {event.event_key}")
        return {}

    results: dict[str, bool] = {}
    any_success = False
    for ch in channels:
        ok = ch.send(event)
        results[ch.name] = ok
        any_success = any_success or ok

    # 至少有一个通道成功才记录冷却（避免全部失败时永久屏蔽）
    if any_success and store is not None:
        store.mark_fired(event.event_key)

    return results


__all__ = [
    "AlertChannel",
    "AlertEvent",
    "AlertStateStore",
    "CHANNEL_REGISTRY",
    "build_channels",
    "dispatch",
    "ConsoleChannel",
    "ServerChanChannel",
    "BarkChannel",
    "PushPlusChannel",
    "WebhookChannel",
]
