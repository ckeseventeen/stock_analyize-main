"""
src/automation/monitor/base.py — 监控任务抽象基类

所有 Monitor（价格预警、财报监控等）均继承 BaseMonitor，
统一的 run() 流程：
  1. 拉取最新数据
  2. 与规则/配置比较
  3. 生成 AlertEvent 列表
  4. 通过 dispatch() 推送（带去重）
  5. 落盘明细 CSV（供前端展示）
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from src.automation.alert import AlertChannel, AlertEvent, AlertStateStore, dispatch
from src.utils.logger import get_logger

logger = get_logger("monitor")


class BaseMonitor(ABC):
    """
    监控任务抽象基类。

    子类需实现 `collect_events() -> list[AlertEvent]`；
    基类负责统一的分发、去重、CSV 落盘。
    """

    # 监控任务名（日志前缀 + CSV 文件名）
    name: str = "base"

    def __init__(
        self,
        channels: Iterable[AlertChannel],
        state_store: Optional[AlertStateStore] = None,
        cooldown_hours: int = 24,
        output_dir: str | Path = "./output",
    ):
        """
        Args:
            channels: 已构建的告警通道列表
            state_store: 去重存储（None 时跳过去重）
            cooldown_hours: 冷却窗口（小时）
            output_dir: 事件明细 CSV 输出目录
        """
        self.channels = list(channels)
        self.state_store = state_store
        self.cooldown_hours = cooldown_hours
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def collect_events(self) -> list[AlertEvent]:
        """
        子类实现：扫描数据并返回待推送事件列表。

        Returns:
            AlertEvent 列表（可为空）
        """
        raise NotImplementedError

    def run(self) -> dict:
        """
        执行一次监控流程。

        Returns:
            统计字典 {total, sent, skipped, failed, events: [...]}。
            供 CLI/Scheduler/前端读取。
        """
        logger.info(f"[{self.name}] 监控任务启动")

        try:
            events = self.collect_events()
        except Exception as e:
            logger.error(f"[{self.name}] 采集事件时异常: {e}", exc_info=True)
            events = []

        total = len(events)
        sent = 0
        skipped = 0
        failed = 0
        event_records: list[dict] = []

        for event in events:
            result = dispatch(event, self.channels, self.state_store, self.cooldown_hours)

            if not result:
                skipped += 1
                status = "skipped"
            elif any(result.values()):
                sent += 1
                status = "sent"
            else:
                failed += 1
                status = "failed"

            event_records.append({
                "timestamp": event.timestamp.isoformat(timespec="seconds"),
                "event_type": event.event_type,
                "stock_code": event.stock_code,
                "stock_name": event.stock_name,
                "title": event.title,
                "body": event.body,
                "event_key": event.event_key,
                "status": status,
                "channels": ",".join(f"{k}={'ok' if v else 'fail'}" for k, v in result.items()) if result else "cooldown",
            })

        # 落盘 CSV（追加模式，前端可读取累计历史）
        if event_records:
            self._append_csv(event_records)

        logger.info(
            f"[{self.name}] 监控完成 - 总事件 {total} / 已推送 {sent} / 去重跳过 {skipped} / 失败 {failed}"
        )

        return {
            "total": total,
            "sent": sent,
            "skipped": skipped,
            "failed": failed,
            "events": event_records,
        }

    def _append_csv(self, records: list[dict]) -> None:
        """把事件明细追加写入 output/{name}_events.csv"""
        path = self.output_dir / f"{self.name}_events.csv"
        df = pd.DataFrame(records)
        # 文件已存在：不写 header，追加
        write_header = not path.exists()
        try:
            df.to_csv(path, mode="a", header=write_header, index=False, encoding="utf-8-sig")
            logger.debug(f"[{self.name}] 事件明细已追加: {path}")
        except Exception as e:
            logger.error(f"[{self.name}] 事件 CSV 写入失败: {e}")
