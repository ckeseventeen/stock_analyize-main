"""
src/portfolio/sell_engine.py — L1 + L2 卖出决策引擎

机构 5 层卖出体系的前两层（Phase 1 范围）：
  L1 风控（硬规则）：固定止损 / 移动止盈
  L2 信号（多源加权）：复用 conditions.py 的 12 个 sell 类条件

输入：单只 Holding + 实时价格 + 日线/周线 OHLCV
输出：SellVerdict（综合判定 + 触发的所有信号 + 建议）

Phase 2 后追加：L4 大盘环境守门员（市场趋势弱时所有信号权重 ×1.5）
Phase 3 后追加：L3 仓位管理（分批止盈建议）
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from src.analysis.screening.conditions import CONDITION_LABELS, CONDITION_REGISTRY
from src.portfolio.models import Holding, SellSignal
from src.utils.logger import get_logger

logger = get_logger("sell_engine")


# ========================
# 卖出信号清单（带权重）
# ========================
# 每条 = (cond_type, kwargs, priority, weight)
# priority: 1=高优, 2=中优, 3=辅助
# weight: 综合评分权重
_SELL_PROFILE: list[tuple[str, dict, int, float]] = [
    # ── 高优（趋势衰竭 + 极端偏离）──
    ("weekly_macd_top_divergence", {"lookback_bars": 60}, 1, 2.0),
    ("daily_macd_top_divergence",  {"lookback_bars": 120}, 1, 2.0),
    ("bias",                       {"ma_period": 20, "threshold": 8.0, "direction": "above"}, 1, 2.0),
    ("volume_price_divergence",    {"lookback_bars": 30, "direction": "top"}, 1, 2.0),

    # ── 中优（破位 + 超买）──
    ("rsi_overbought",             {"threshold": 75, "period": 14}, 2, 1.0),
    ("kdj_death_cross",            {"j_threshold": 70}, 2, 1.0),
    ("ma_death_cross",             {"fast_period": 5, "slow_period": 20}, 2, 1.0),
    ("break_below_ma",             {"ma_period": 60, "lookback": 5}, 2, 1.5),  # 回测显示精度高

    # ── 辅助（远端偏离 + 派发）──
    ("bias",                       {"ma_period": 60, "threshold": 15.0, "direction": "above"}, 3, 0.5),
    ("volume_blowoff",             {"lookback_bars": 60, "vol_multiple": 3.0, "min_price_change_pct": 5.0}, 3, 0.5),
    ("bollinger_breakout",         {"period": 20, "std_dev": 2.0, "direction": "upper"}, 3, 0.5),
    ("volume_shrink",              {"lookback_bars": 10, "shrink_ratio": 0.6}, 3, 0.5),
]


# ========================
# 默认风控阈值（被 holding.alerts / portfolio.default_alerts 覆盖）
# ========================
_DEFAULT_THRESHOLDS = {
    "stop_loss_pct": 8.0,
    "trailing_pct": 10.0,
    "signal_threshold_pct": 60,
    "enable_signal_alert": True,
}


# ========================
# 评估结果
# ========================

@dataclass
class SellVerdict:
    """
    单只持仓的卖出综合判定结果

    Attributes:
        code: 股票代码
        name: 股票名称
        current_price: 评估时使用的最新价
        action: "hold" | "reduce_half" | "reduce_all" | "stop_loss"
        risk_pct: 综合风险分（0-100）
        risk_level: "🟢 安全" / "🟡 低" / "🟠 中" / "🔴 高"
        advice: 中文操作建议
        signals: 触发的所有信号
        l1_triggered: L1 风控触发列表（如 ["stop_loss", "trailing"]）
    """
    code: str
    name: str
    current_price: float
    action: str
    risk_pct: float
    risk_level: str
    advice: str
    signals: list[dict] = field(default_factory=list)
    l1_triggered: list[str] = field(default_factory=list)
    pnl_pct: float = 0.0  # 当前持仓盈亏百分比


class SellEngine:
    """
    卖出决策引擎

    用法:
        engine = SellEngine(global_thresholds)
        verdict = engine.evaluate(holding, current_price, daily_df, weekly_df, recent_high)
        for sig in engine.to_signals(holding, verdict):
            push(sig)
    """

    def __init__(self, global_thresholds: Optional[dict] = None):
        """
        Args:
            global_thresholds: Portfolio.default_alerts 全局阈值
        """
        self._global = {**_DEFAULT_THRESHOLDS, **(global_thresholds or {})}

    def _resolve_thresholds(self, holding: Holding) -> dict:
        """合并全局默认 + 单只覆盖"""
        return {**self._global, **(holding.alerts or {})}

    # ──────────────── L1 风控规则 ────────────────

    def _check_l1_rules(self, holding: Holding, current_price: float,
                        recent_high: Optional[float], thresholds: dict
                        ) -> tuple[list[str], list[str]]:
        """
        L1 硬规则检查

        Returns:
            (triggered_rules, advice_messages)
        """
        triggered: list[str] = []
        msgs: list[str] = []

        # 固定止损：浮亏 ≥ stop_loss_pct
        if holding.avg_cost > 0:
            pnl_pct = (current_price - holding.avg_cost) / holding.avg_cost * 100
            stop_loss_pct = float(thresholds.get("stop_loss_pct", 8.0))
            if pnl_pct <= -stop_loss_pct:
                triggered.append("stop_loss")
                msgs.append(
                    f"⛔ 触发固定止损：当前浮亏 {pnl_pct:.1f}% ≤ -{stop_loss_pct}%"
                )

        # 移动止盈：从持有期高点回落 ≥ trailing_pct
        if recent_high is not None and recent_high > 0:
            drawdown = (current_price - recent_high) / recent_high * 100
            trailing_pct = float(thresholds.get("trailing_pct", 10.0))
            if drawdown <= -trailing_pct:
                triggered.append("trailing_stop")
                msgs.append(
                    f"⛔ 触发移动止盈：从高点 {recent_high:.2f} 回落 {abs(drawdown):.1f}% "
                    f"≥ {trailing_pct}%"
                )

        return triggered, msgs

    # ──────────────── L2 信号评估 ────────────────

    def _check_l2_signals(self, holding: Holding,
                          daily_df: pd.DataFrame,
                          weekly_df: pd.DataFrame
                          ) -> tuple[list[dict], float]:
        """
        L2 信号评估：跑所有 sell condition，加权综合分

        Returns:
            (triggered_signals, weighted_pct_0_100)
        """
        triggered: list[dict] = []
        total_weight = 0.0
        hit_weight = 0.0

        for cond_type, kwargs, priority, weight in _SELL_PROFILE:
            cls = CONDITION_REGISTRY.get(cond_type)
            if cls is None:
                continue
            try:
                cond = cls(**kwargs)
            except Exception as e:
                logger.debug(f"构造 {cond_type} 失败: {e}")
                continue
            period = getattr(cond, "ohlcv_period", "daily")
            df = weekly_df if period == "weekly" else daily_df
            if df is None or df.empty or len(df) < 25:
                continue
            try:
                spot = pd.Series({"代码": holding.code, "名称": holding.name})
                hit = bool(cond.evaluate_full(spot, df))
            except Exception as e:
                logger.debug(f"{holding.code} 评估 {cond_type} 异常: {e}")
                continue

            total_weight += weight
            if hit:
                hit_weight += weight
                triggered.append({
                    "type": cond_type,
                    "label": CONDITION_LABELS.get(cond_type, cond_type),
                    "params": {k: v for k, v in kwargs.items() if k != "direction"},
                    "priority": priority,
                    "weight": weight,
                })

        pct = (hit_weight / total_weight * 100) if total_weight > 0 else 0.0
        return triggered, pct

    # ──────────────── 综合判定 ────────────────

    def evaluate(self, holding: Holding, current_price: float,
                 daily_df: pd.DataFrame, weekly_df: pd.DataFrame,
                 recent_high: Optional[float] = None,
                 regime_multiplier: float = 1.0) -> SellVerdict:
        """
        给单只持仓做综合卖出判定

        Args:
            holding: 持仓
            current_price: 最新价
            daily_df: 日线 OHLCV（用于 L2 信号）
            weekly_df: 周线 OHLCV
            recent_high: 持有期间的最高价（用于移动止盈）
                        如果未传则用 daily_df 中"buy_date 之后"的最高价代算
            regime_multiplier: L4 大盘环境给 L2 风险分的权重乘数
                              （bull=1.0, sideways=1.2, bear=1.5）
        """
        thresholds = self._resolve_thresholds(holding)

        # 自动算 recent_high
        if recent_high is None and holding.buy_date and daily_df is not None and not daily_df.empty:
            recent_high = self._auto_recent_high(daily_df, holding.buy_date)

        l1_triggered, l1_msgs = self._check_l1_rules(
            holding, current_price, recent_high, thresholds
        )

        signal_alert_enabled = bool(thresholds.get("enable_signal_alert", True))
        signals: list[dict] = []
        risk_pct = 0.0
        if signal_alert_enabled:
            signals, risk_pct = self._check_l2_signals(holding, daily_df, weekly_df)
            # L4 大盘环境放大：空头市场所有 sell 信号 ×1.5
            if regime_multiplier != 1.0:
                risk_pct = min(100.0, risk_pct * regime_multiplier)

        # ── 综合判定 ──
        signal_threshold = float(thresholds.get("signal_threshold_pct", 60))

        if l1_triggered:
            action = "stop_loss"
            risk_level = "🔴 高"
            advice = " | ".join(l1_msgs) + "\n**立即清仓**"
        elif risk_pct >= 80:
            action = "reduce_all"
            risk_level = "🔴 高"
            advice = "多重高优信号共振，**建议清仓**"
        elif risk_pct >= signal_threshold:
            action = "reduce_half"
            risk_level = "🟠 中"
            advice = f"信号综合分 {risk_pct:.0f}% ≥ {signal_threshold:.0f}%，**建议减仓 50%**"
        elif risk_pct >= 35:
            action = "hold"
            risk_level = "🟡 低"
            advice = f"信号综合分 {risk_pct:.0f}%，警示偏暖，密切跟踪"
        else:
            action = "hold"
            risk_level = "🟢 安全"
            advice = f"信号综合分 {risk_pct:.0f}%，无显著看跌信号"

        pnl_pct = holding.unrealized_pnl_pct(current_price) if holding.avg_cost > 0 else 0.0

        return SellVerdict(
            code=holding.code,
            name=holding.name,
            current_price=current_price,
            action=action,
            risk_pct=risk_pct,
            risk_level=risk_level,
            advice=advice,
            signals=signals,
            l1_triggered=l1_triggered,
            pnl_pct=pnl_pct,
        )

    @staticmethod
    def _auto_recent_high(daily_df: pd.DataFrame, buy_date: str) -> Optional[float]:
        """从 buy_date 起的日线最高价"""
        try:
            date_col = "日期" if "日期" in daily_df.columns else "date"
            high_col = "最高" if "最高" in daily_df.columns else "high"
            if date_col not in daily_df.columns or high_col not in daily_df.columns:
                return None
            df = daily_df.copy()
            df[date_col] = pd.to_datetime(df[date_col])
            buy = pd.to_datetime(buy_date)
            mask = df[date_col] >= buy
            if not mask.any():
                return None
            return float(df.loc[mask, high_col].max())
        except Exception:
            return None

    # ──────────────── 转推送信号 ────────────────

    @staticmethod
    def to_signals(holding: Holding, verdict: SellVerdict) -> list[SellSignal]:
        """
        把 verdict 拆解成多条 SellSignal（每条对应一次推送）

        L1 触发 → critical (P0 推送)
        L2 ≥ 80% → critical
        L2 ≥ 60% → warning
        其他 → 不推送
        """
        out: list[SellSignal] = []

        # P1-7 Bug 修：L1 多规则合并成一条，避免同一只股票同时触发
        # stop_loss + trailing_stop 时手机收到 2 条相同告警
        if verdict.l1_triggered:
            out.append(SellSignal(
                code=holding.code,
                name=holding.name,
                level="L1",
                rule="+".join(verdict.l1_triggered),
                severity="critical",
                message=verdict.advice,
                meta={"current_price": verdict.current_price,
                      "pnl_pct": verdict.pnl_pct,
                      "triggered_rules": list(verdict.l1_triggered)},
            ))

        # L2 信号触发：仅在 ≥60% 时推一条聚合
        if verdict.risk_pct >= 60 and not verdict.l1_triggered:
            severity = "critical" if verdict.risk_pct >= 80 else "warning"
            signal_labels = ", ".join(s["label"] for s in verdict.signals)
            out.append(SellSignal(
                code=holding.code,
                name=holding.name,
                level="L2",
                rule=f"signal_score_{int(verdict.risk_pct)}",
                severity=severity,
                message=(
                    f"{holding.name}({holding.code}) {verdict.risk_level}\n"
                    f"综合分 {verdict.risk_pct:.0f}% | 浮盈 {verdict.pnl_pct:+.1f}%\n"
                    f"触发：{signal_labels}\n"
                    f"{verdict.advice}"
                ),
                meta={"signals": verdict.signals,
                      "pnl_pct": verdict.pnl_pct,
                      "current_price": verdict.current_price},
            ))

        return out
