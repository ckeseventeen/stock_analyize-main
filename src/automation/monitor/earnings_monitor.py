"""
src/automation/monitor/earnings_monitor.py — 财报披露监控器

对关注列表中的股票，
  1. 拉取未来 N 天的披露日历
  2. 为即将披露（remind_days_ahead 天内）的股票生成"披露提醒"事件
  3. 为"业绩预告"等关键事件单独生成告警
  4. 把完整日历落盘到 output/earnings_calendar.csv 供前端展示
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.automation.alert import AlertEvent, AlertStateStore
from src.automation.monitor.base import BaseMonitor
from src.data.providers.earnings_fetcher import EarningsFetcher
from src.utils.logger import get_logger

logger = get_logger("earnings")


class EarningsMonitor(BaseMonitor):
    """
    财报披露监控。

    配置结构 (config/earnings_monitor.yaml):
        watchlist:
          a: ["600519", ...]
          hk: ["00700", ...]
          us: ["AAPL", ...]
        days_ahead: 30
        remind_days_ahead: 3
        track_forecasts: true
    """

    name = "earnings_monitor"

    def __init__(
        self,
        config: dict,
        channels,
        state_store: AlertStateStore | None = None,
        cooldown_hours: int = 72,
        output_dir: str = "./output",
        fetcher: EarningsFetcher | None = None,
    ):
        super().__init__(channels, state_store, cooldown_hours, output_dir)
        self.config = config or {}
        self.watchlist = self.config.get("watchlist", {}) or {}
        self.days_ahead = int(self.config.get("days_ahead", 30))
        self.remind_days_ahead = int(self.config.get("remind_days_ahead", 3))
        self.track_forecasts = bool(self.config.get("track_forecasts", True))
        self.fetcher = fetcher or EarningsFetcher()

    def collect_events(self) -> list[AlertEvent]:
        frames: list[pd.DataFrame] = []

        # A 股：用全市场日历后按 watchlist 过滤
        a_codes = set(map(str, self.watchlist.get("a", [])))
        if a_codes:
            df_a = self.fetcher.get_a_share_upcoming(days_ahead=self.days_ahead)
            if df_a is not None and not df_a.empty:
                df_a = df_a[df_a["code"].isin(a_codes)]
                frames.append(df_a)

        # 港股 / 美股：按 watchlist 单独拉
        hk_codes = list(map(str, self.watchlist.get("hk", [])))
        if hk_codes:
            df_hk = self.fetcher.get_hk_upcoming(hk_codes, days_ahead=self.days_ahead)
            if df_hk is not None and not df_hk.empty:
                frames.append(df_hk)

        us_codes = list(map(str, self.watchlist.get("us", [])))
        if us_codes:
            df_us = self.fetcher.get_us_upcoming(us_codes, days_ahead=self.days_ahead)
            if df_us is not None and not df_us.empty:
                frames.append(df_us)

        if not frames:
            logger.info("财报披露日历为空，无事件生成")
            return []

        calendar = pd.concat(frames, ignore_index=True)
        # 落盘完整日历（覆盖写，前端取最新）
        try:
            out_path = Path(self.output_dir) / "earnings_calendar.csv"
            calendar.to_csv(out_path, index=False, encoding="utf-8-sig")
            logger.info(f"披露日历已保存: {out_path} ({len(calendar)} 条)")
        except Exception as e:
            logger.error(f"披露日历落盘失败: {e}")

        # 生成告警事件
        events: list[AlertEvent] = []
        today = datetime.now().date()
        remind_cutoff = pd.Timestamp(today) + pd.Timedelta(days=self.remind_days_ahead)

        for _, row in calendar.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            name = row.get("name") or code
            market = str(row.get("market", "")).lower()
            etype = str(row.get("event_type", ""))
            disclose_date = pd.to_datetime(row.get("disclose_date"), errors="coerce")
            if pd.isna(disclose_date):
                continue

            # 业绩预告：无论何时都提醒（信息价值高）
            is_forecast = etype == "业绩预告"

            # 其他类型：仅 remind_days_ahead 天内提醒
            within_remind = disclose_date <= remind_cutoff

            if not (self.track_forecasts and is_forecast) and not within_remind:
                continue

            event_key = f"{market}:{code}:{etype}:{disclose_date.strftime('%Y%m%d')}"

            title_prefix = "📊 业绩预告" if is_forecast else "📅 披露提醒"
            title = f"{title_prefix}: {name} ({code})"

            body_lines = [
                f"市场: {market.upper()}",
                f"事件: {etype}",
                f"披露日: {disclose_date.strftime('%Y-%m-%d')}",
            ]
            if row.get("report_period"):
                body_lines.append(f"报告期: {row['report_period']}")
            if row.get("extra"):
                body_lines.append(f"详情: {row['extra']}")

            events.append(AlertEvent(
                title=title,
                body="\n".join(body_lines),
                event_key=event_key,
                stock_code=code,
                stock_name=str(name),
                event_type=f"earnings_{'forecast' if is_forecast else 'reminder'}",
            ))

        return events
