"""
src/monitors/holding_monitor.py — 持仓监控 + 卖出提醒推送

每日扫描所有持仓，对触发 L1 风控或 L2 信号 ≥ 阈值的持仓发推送。
集成 Server酱 / Bark / PushPlus 等已有告警通道。

调度方式：注册到 scheduler_manager 的"daily_holding_scan"任务，每个交易日开盘前 +
收盘后 各跑一次。
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analysis.screening.data_provider import ScreenerDataProvider
from src.monitors.base import BaseMonitor
from src.notify import AlertEvent, AlertStateStore
from src.portfolio import PortfolioManager, SellEngine
from src.portfolio.market_regime import MarketRegimeAnalyzer
from src.utils.logger import get_logger

logger = get_logger("holding_monitor")


class HoldingMonitor(BaseMonitor):
    """
    持仓监控任务。

    用法（注册到 scheduler）::

        from src.notify import build_channels
        from src.notify.state import AlertStateStore
        from src.monitors.holding_monitor import HoldingMonitor

        channels = build_channels(alerts_config)
        store = AlertStateStore(".cache/alert_state.db")
        mon = HoldingMonitor(channels, state_store=store)
        mon.run()
    """

    name = "holding_monitor"

    def __init__(
        self,
        channels,
        state_store: AlertStateStore | None = None,
        cooldown_hours: int = 8,           # 同一只 + 同信号 8 小时内只推一次
        output_dir: str | Path = "./output",
        holdings_path: str | Path = "config/holdings.yaml",
        enable_regime: bool = True,
    ):
        super().__init__(channels, state_store, cooldown_hours, output_dir)
        self.holdings_path = Path(holdings_path)
        self.enable_regime = enable_regime

    def collect_events(self) -> list[AlertEvent]:
        mgr = PortfolioManager(self.holdings_path)
        pf = mgr.load()
        if not pf.holdings:
            logger.info(f"[{self.name}] 无持仓，跳过")
            return []

        # 拉所有持仓的最新价
        provider = ScreenerDataProvider()
        try:
            spot_df = provider.get_all_a_shares()
        except Exception as e:
            logger.warning(f"[{self.name}] 拉 spot 失败: {e}")
            spot_df = pd.DataFrame()

        code_to_price: dict[str, float] = {}
        if not spot_df.empty:
            code_col = "代码" if "代码" in spot_df.columns else "code"
            price_col = "最新价" if "最新价" in spot_df.columns else "close"
            for _, row in spot_df.iterrows():
                try:
                    c = str(row.get(code_col, "")).zfill(6)
                    code_to_price[c] = float(row.get(price_col, 0) or 0)
                except (ValueError, TypeError):
                    continue

        # 大盘环境
        regime_multiplier = 1.0
        regime_summary = ""
        if self.enable_regime:
            try:
                regime = MarketRegimeAnalyzer(provider).analyze()
                regime_multiplier = regime.weight_multiplier
                regime_summary = f"\n[大盘 {regime.regime}] " if regime.regime != "bull" else ""
            except Exception as e:
                logger.debug(f"[{self.name}] 大盘环境分析失败: {e}")

        engine = SellEngine(pf.default_alerts)
        events: list[AlertEvent] = []

        for h in pf.holdings:
            # P1-5 Bug 修：支持 hk/us 持仓。A 股从 spot 取价，hk/us 从 K 线尾根取
            cur_price = code_to_price.get(h.code, 0.0)
            if cur_price <= 0 and h.market in ("hk", "us"):
                try:
                    intl_df = provider.get_daily_ohlcv(h.code, days_back=10,
                                                         market=h.market)
                    if intl_df is not None and not intl_df.empty:
                        close_col = "收盘" if "收盘" in intl_df.columns else "close"
                        cur_price = float(pd.to_numeric(intl_df[close_col],
                                                         errors="coerce")
                                          .dropna().iloc[-1])
                except Exception:
                    pass
            if cur_price <= 0:
                logger.warning(f"[{self.name}] {h.code} 无最新价，跳过")
                continue

            try:
                daily_df = provider.get_daily_ohlcv(h.code, days_back=250,
                                                     market=h.market)
                weekly_df = provider.get_weekly_ohlcv(h.code, days_back=365 * 3,
                                                       market=h.market)
            except Exception as e:
                logger.warning(f"[{self.name}] {h.code} 拉 K 线失败: {e}")
                continue

            if daily_df is None or daily_df.empty:
                continue

            verdict = engine.evaluate(h, cur_price, daily_df, weekly_df,
                                        regime_multiplier=regime_multiplier)
            for signal in SellEngine.to_signals(h, verdict):
                event = self._signal_to_event(signal, regime_summary)
                events.append(event)

        logger.info(f"[{self.name}] 收集 {len(events)} 条卖出预警")
        return events

    @staticmethod
    def _signal_to_event(signal, regime_summary: str = "") -> AlertEvent:
        """SellSignal → AlertEvent"""
        # 标题前缀按 severity 加 emoji
        emoji = {"critical": "⛔", "warning": "🟠", "info": "ℹ️"}.get(signal.severity, "")
        title = f"{emoji} 持仓预警: {signal.name}({signal.code}) [{signal.level}]"
        body = f"{regime_summary}{signal.message}"
        # event_key 含日期，每天最多推一次同信号
        date_key = signal.triggered_at.strftime("%Y-%m-%d")
        event_key = f"holding:{signal.code}:{signal.level}:{signal.rule}:{date_key}"

        return AlertEvent(
            title=title,
            body=body,
            event_key=event_key,
            stock_code=signal.code,
            stock_name=signal.name,
            event_type=f"holding_{signal.level}_{signal.severity}",
        )
