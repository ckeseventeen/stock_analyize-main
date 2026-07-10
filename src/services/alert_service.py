"""
src/services/alert_service.py — 价格预警的无头服务层

原 pages/5_价格预警.py 内嵌的编排逻辑下沉至此：
  - test_rule(): 单条规则试跑（不推送、不写冷却状态）
  - scan_all(): 扫描全部启用规则并经配置通道推送
"""
from __future__ import annotations

from src.utils.logger import get_logger
from src.core.config_io import PATH_ALERTS, load_yaml

logger = get_logger("alert_service")


def test_rule(rule: dict, direction: str):
    """
    单条规则试跑：不带通道、不写状态，只返回命中事件。

    Returns:
        AlertEvent | None（未命中）

    Raises:
        引擎异常原样抛出，由页面展示
    """
    from src.automation.monitor.buy_sell_alerts import BuySellAlertMonitor

    monitor = BuySellAlertMonitor(
        buy_alerts=[], sell_alerts=[],
        channels=[], state_store=None,
    )
    return monitor.test_single_rule(rule, direction)


def scan_all(
    buy_alerts: list[dict],
    sell_alerts: list[dict],
    *,
    cooldown_hours: int = 24,
    alerts_config: dict | None = None,
) -> list:
    """
    扫描全部启用规则，经 alerts.yaml 配置的通道推送（带冷却去重）。

    Args:
        alerts_config: 通道配置；None 时读 config/alerts.yaml

    Returns:
        命中的 AlertEvent 列表
    """
    from src.automation.alert import AlertStateStore, build_channels
    from src.automation.monitor.buy_sell_alerts import BuySellAlertMonitor

    cfg = alerts_config if alerts_config is not None else (load_yaml(PATH_ALERTS) or {})
    channels = build_channels(cfg)
    store = AlertStateStore()
    monitor = BuySellAlertMonitor(
        buy_alerts=buy_alerts, sell_alerts=sell_alerts,
        channels=channels, state_store=store,
        cooldown_hours=cooldown_hours,
    )
    return monitor.collect_events()
