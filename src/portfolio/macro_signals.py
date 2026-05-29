"""
src/portfolio/macro_signals.py — L5 宏观/系统性信号

机构卖出体系第 5 层：宏观层面的减仓触发信号。

实现的信号：
  - 北向资金 N 日累计净流出（外资撤离 → 系统性下跌）
  - 中证 1000 / 沪深 300 相对强弱（小盘 vs 大盘转弱 → 风险偏好下降）
  - 隔夜美股纳指跌幅（科技股次日联动）

每个信号独立判断，调用方可按权重聚合 → 加到 SellVerdict 的 advisor 里。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("macro_signals")


@dataclass
class MacroSignal:
    """一条宏观信号"""
    name: str
    triggered: bool
    severity: str        # "info" / "warning" / "critical"
    value: Optional[float] = None
    message: str = ""


def northbound_outflow_signal(lookback_days: int = 5,
                                threshold_yi: float = -50.0) -> MacroSignal:
    """
    北向资金 N 日累计净流出 ≤ 阈值（单位：亿元）

    阈值参考：
      -50 亿: 轻微（warning）
      -100 亿: 中度（warning）
      -200 亿: 严重（critical）

    数据源：akshare 的 stock_hsgt_hist_em（沪深港通历史数据）
    """
    try:
        import akshare as ak
        df = ak.stock_hsgt_hist_em(symbol="北向资金")
        if df is None or df.empty:
            return MacroSignal(name="北向资金净流出", triggered=False, severity="info",
                                message="数据拉取失败")
        # 列名通常含 "当日成交净买额"，单位为亿元
        net_col = None
        for c in df.columns:
            if "净买" in str(c) or "净买入" in str(c):
                net_col = c
                break
        if net_col is None:
            return MacroSignal(name="北向资金净流出", triggered=False, severity="info",
                                message="找不到净买入列")
        df_recent = df.tail(lookback_days)
        cum_net = pd.to_numeric(df_recent[net_col], errors="coerce").sum()
        triggered = cum_net <= threshold_yi
        if triggered:
            severity = "critical" if cum_net <= -200 else "warning"
            msg = f"北向 {lookback_days} 日累计净流出 {abs(cum_net):.0f} 亿元，外资撤离"
        else:
            severity = "info"
            msg = f"北向 {lookback_days} 日累计 {cum_net:+.0f} 亿元，正常"
        return MacroSignal(
            name="北向资金净流出",
            triggered=triggered,
            severity=severity,
            value=float(cum_net),
            message=msg,
        )
    except Exception as e:
        logger.debug(f"北向资金信号失败: {e}")
        return MacroSignal(name="北向资金净流出", triggered=False, severity="info",
                            message=f"数据源异常: {type(e).__name__}")


def nasdaq_overnight_drop_signal(threshold_pct: float = -3.0) -> MacroSignal:
    """
    隔夜纳指跌幅 ≤ 阈值（默认 -3%），次日 A 股科技股大概率联动跳水

    数据源：akshare 的 macro_us_ndx 或 stock_us_daily(symbol="QQQ")
    """
    try:
        import akshare as ak
        # 用 QQQ（纳斯达克 100 ETF）作为代理，覆盖率更高
        df = ak.stock_us_daily(symbol="QQQ", adjust="qfq")
        if df is None or df.empty or len(df) < 2:
            return MacroSignal(name="纳指夜盘跌幅", triggered=False, severity="info",
                                message="数据拉取失败")
        # 最近两日
        df = df.tail(2)
        last_close = float(df.iloc[-1]["close"])
        prev_close = float(df.iloc[-2]["close"])
        chg_pct = (last_close - prev_close) / prev_close * 100
        triggered = chg_pct <= threshold_pct
        if triggered:
            severity = "critical" if chg_pct <= -5 else "warning"
            msg = f"隔夜纳指 (QQQ) 跌 {abs(chg_pct):.1f}%，A 股科技股次日大概率跟跌"
        else:
            severity = "info"
            msg = f"隔夜纳指 (QQQ) {chg_pct:+.1f}%"
        return MacroSignal(
            name="纳指夜盘跌幅",
            triggered=triggered,
            severity=severity,
            value=float(chg_pct),
            message=msg,
        )
    except Exception as e:
        logger.debug(f"纳指夜盘信号失败: {e}")
        return MacroSignal(name="纳指夜盘跌幅", triggered=False, severity="info",
                            message=f"数据源异常: {type(e).__name__}")


def small_cap_underperformance_signal(lookback_days: int = 20,
                                       threshold_pct: float = -8.0) -> MacroSignal:
    """
    中证 1000 vs 沪深 300 相对收益（小盘 vs 大盘）
    20 日中证 1000 涨幅 - 20 日沪深 300 涨幅 ≤ 阈值 → 小盘转弱 → 风险偏好下降

    数据源：用 ScreenerDataProvider（pytdx）拉指数 K 线
    """
    try:
        from src.analysis.screening.data_provider import ScreenerDataProvider
        provider = ScreenerDataProvider()

        def _chg_pct(code: str) -> Optional[float]:
            df = provider.get_daily_ohlcv(code, days_back=lookback_days + 20)
            if df is None or df.empty or len(df) < lookback_days + 1:
                return None
            close_col = "收盘" if "收盘" in df.columns else "close"
            close = pd.to_numeric(df[close_col], errors="coerce")
            return float((close.iloc[-1] / close.iloc[-lookback_days - 1] - 1) * 100)

        small = _chg_pct("000852")   # 中证 1000
        big = _chg_pct("000300")     # 沪深 300

        if small is None or big is None:
            return MacroSignal(name="小盘相对弱势", triggered=False, severity="info",
                                message="指数数据缺失")

        diff = small - big
        triggered = diff <= threshold_pct
        if triggered:
            severity = "warning"
            msg = (f"中证1000 ({small:+.1f}%) 跑输沪深300 ({big:+.1f}%) "
                   f"{abs(diff):.1f}pp，风险偏好下降")
        else:
            severity = "info"
            msg = f"中证1000 vs 沪深300: {diff:+.1f}pp"
        return MacroSignal(
            name="小盘相对弱势",
            triggered=triggered,
            severity=severity,
            value=float(diff),
            message=msg,
        )
    except Exception as e:
        logger.debug(f"小盘相对强度失败: {e}")
        return MacroSignal(name="小盘相对弱势", triggered=False, severity="info",
                            message=f"异常: {type(e).__name__}")


@dataclass
class MacroPanel:
    """L5 宏观面板综合状态"""
    signals: list[MacroSignal] = field(default_factory=list)
    critical_count: int = 0
    warning_count: int = 0
    checked_at: datetime = field(default_factory=datetime.now)

    @property
    def is_high_risk(self) -> bool:
        """≥ 2 个 warning 或 ≥ 1 个 critical → 高风险"""
        return self.critical_count >= 1 or self.warning_count >= 2


def analyze_macro() -> MacroPanel:
    """跑所有 L5 信号，返回综合面板"""
    signals = [
        northbound_outflow_signal(),
        nasdaq_overnight_drop_signal(),
        small_cap_underperformance_signal(),
    ]
    critical = sum(1 for s in signals if s.triggered and s.severity == "critical")
    warning = sum(1 for s in signals if s.triggered and s.severity == "warning")
    return MacroPanel(
        signals=signals,
        critical_count=critical,
        warning_count=warning,
    )
