"""
src/portfolio/diagnostics.py — 组合诊断（持仓体检）

复用已有引擎，输出一份结构化「体检报告」：
  - L2 卖出信号健康度（SellEngine 加权风险分，按市值加权）
  - 集中度（单票 / 行业，基于 tag 首段）
  - 盈亏结构（深套仓位市值占比）
  - 大盘环境（MarketRegimeAnalyzer）

四维打分 → 综合健康分（0-100）+ 等级（A/B/C/D）+ 问题清单。
纯计算模块：数据全部通过参数注入，便于测试与复用（AI 周报直接消费
diagnose() 的返回值）。联网取数走 run_diagnosis() 便捷入口。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

import pandas as pd

from src.portfolio.market_regime import MarketRegime
from src.portfolio.models import Holding, Portfolio
from src.portfolio.sell_engine import SellEngine, SellVerdict
from src.utils.logger import get_logger

logger = get_logger("portfolio_diag")


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class HoldingDiagnosis:
    """单只持仓的诊断条目"""
    code: str
    name: str
    market: str
    industry: str              # tag 首段；无 tag 为 "未分类"
    price: float               # 0 = 无行情（诊断跳过信号评估）
    weight_pct: float          # 占组合市值比例
    pnl_pct: float
    market_value: float
    verdict: Optional[SellVerdict] = None   # None = 未评估（无行情/非 A 股）

    @property
    def risk_pct(self) -> float:
        return self.verdict.risk_pct if self.verdict else 0.0


@dataclass
class DimensionScore:
    """单个维度的得分与说明"""
    key: str          # signal / concentration / pnl / regime
    label: str
    score: float      # 0-100
    weight: float     # 综合分权重
    detail: str = ""


@dataclass
class PortfolioDiagnosis:
    """组合体检报告（结构化，可直接序列化/喂给 LLM）"""
    generated_at: str
    total_market_value: float
    total_cost: float
    total_pnl: float
    total_pnl_pct: float
    cash: float
    position_count: int
    score: float                     # 综合健康分 0-100
    grade: str                       # A / B / C / D
    dimensions: list[DimensionScore] = field(default_factory=list)
    holdings: list[HoldingDiagnosis] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)       # 需要处理的问题
    highlights: list[str] = field(default_factory=list)   # 良好项
    regime: Optional[MarketRegime] = None

    def to_report_dict(self) -> dict:
        """序列化为纯 dict（AI 报告 / API 输出用）"""
        return {
            "generated_at": self.generated_at,
            "score": round(self.score, 1),
            "grade": self.grade,
            "total_market_value": round(self.total_market_value, 2),
            "total_pnl_pct": round(self.total_pnl_pct, 2),
            "cash": round(self.cash, 2),
            "position_count": self.position_count,
            "dimensions": [
                {"key": d.key, "label": d.label, "score": round(d.score, 1),
                 "weight": d.weight, "detail": d.detail}
                for d in self.dimensions
            ],
            "issues": list(self.issues),
            "highlights": list(self.highlights),
            "regime": self.regime.regime if self.regime else "unknown",
            "holdings": [
                {"code": h.code, "name": h.name, "industry": h.industry,
                 "weight_pct": round(h.weight_pct, 1),
                 "pnl_pct": round(h.pnl_pct, 1),
                 "risk_pct": round(h.risk_pct, 0),
                 "action": h.verdict.action if h.verdict else "n/a"}
                for h in self.holdings
            ],
        }


# ============================================================================
# 打分规则
# ============================================================================

_GRADE_STEPS = [(85, "A"), (70, "B"), (55, "C")]

# 各维度权重（和为 1）
_WEIGHTS = {"signal": 0.40, "concentration": 0.25, "pnl": 0.20, "regime": 0.15}


def _grade(score: float) -> str:
    for threshold, g in _GRADE_STEPS:
        if score >= threshold:
            return g
    return "D"


def _industry_of(holding: Holding) -> str:
    tag = (holding.tag or "").strip()
    return tag.split("-")[0] if tag else "未分类"


def _score_signals(entries: list[HoldingDiagnosis]) -> DimensionScore:
    """信号健康 = 100 - 市值加权平均风险分"""
    evaluated = [e for e in entries if e.verdict is not None]
    if not evaluated:
        return DimensionScore("signal", "卖出信号", 70.0, _WEIGHTS["signal"],
                              "无可评估持仓（缺行情或非 A 股），给中性分")
    total_w = sum(e.weight_pct for e in evaluated) or 1.0
    weighted_risk = sum(e.risk_pct * e.weight_pct for e in evaluated) / total_w
    score = max(0.0, 100.0 - weighted_risk)
    danger = [e for e in evaluated if e.verdict.action in ("stop_loss", "reduce_all")]
    detail = f"市值加权风险分 {weighted_risk:.0f}%"
    if danger:
        detail += "；触发清仓/止损级信号: " + ", ".join(f"{e.name}({e.code})" for e in danger)
    return DimensionScore("signal", "卖出信号", score, _WEIGHTS["signal"], detail)


def _score_concentration(entries: list[HoldingDiagnosis]) -> DimensionScore:
    """集中度：单票上限 + 行业上限 + 只数分散"""
    score = 100.0
    details: list[str] = []
    if not entries:
        return DimensionScore("concentration", "集中度", 70.0,
                              _WEIGHTS["concentration"], "无持仓")

    max_single = max(entries, key=lambda e: e.weight_pct)
    if max_single.weight_pct > 40:
        score -= 40
        details.append(f"单票 {max_single.name} 占比 {max_single.weight_pct:.0f}%（>40% 重度集中）")
    elif max_single.weight_pct > 30:
        score -= 25
        details.append(f"单票 {max_single.name} 占比 {max_single.weight_pct:.0f}%（>30%）")
    elif max_single.weight_pct > 20:
        score -= 10
        details.append(f"单票 {max_single.name} 占比 {max_single.weight_pct:.0f}%（>20%）")

    industry_w: dict[str, float] = {}
    for e in entries:
        industry_w[e.industry] = industry_w.get(e.industry, 0.0) + e.weight_pct
    top_ind, top_w = max(industry_w.items(), key=lambda kv: kv[1])
    if top_w > 60:
        score -= 30
        details.append(f"行业「{top_ind}」占比 {top_w:.0f}%（>60% 行业孤注）")
    elif top_w > 45:
        score -= 15
        details.append(f"行业「{top_ind}」占比 {top_w:.0f}%（>45%）")

    if len(entries) < 3:
        score -= 10
        details.append(f"仅 {len(entries)} 只持仓，分散不足")

    return DimensionScore("concentration", "集中度", max(0.0, score),
                          _WEIGHTS["concentration"],
                          "；".join(details) or "单票/行业集中度均在合理范围")


def _score_pnl(entries: list[HoldingDiagnosis]) -> DimensionScore:
    """盈亏结构：深套仓位（浮亏 >10% / >20%）市值占比扣分"""
    if not entries:
        return DimensionScore("pnl", "盈亏结构", 70.0, _WEIGHTS["pnl"], "无持仓")
    total_w = sum(e.weight_pct for e in entries) or 1.0
    deep_w = sum(e.weight_pct for e in entries if e.pnl_pct <= -20) / total_w * 100
    mid_w = sum(e.weight_pct for e in entries if -20 < e.pnl_pct <= -10) / total_w * 100
    score = max(0.0, 100.0 - deep_w * 1.5 - mid_w * 0.8)
    details = []
    if deep_w > 0:
        details.append(f"浮亏 >20% 的仓位占 {deep_w:.0f}%")
    if mid_w > 0:
        details.append(f"浮亏 10-20% 的仓位占 {mid_w:.0f}%")
    return DimensionScore("pnl", "盈亏结构", score, _WEIGHTS["pnl"],
                          "；".join(details) or "无深套仓位")


def _score_regime(regime: Optional[MarketRegime]) -> DimensionScore:
    mapping = {"bull": 90.0, "sideways": 70.0, "bear": 40.0}
    if regime is None:
        return DimensionScore("regime", "大盘环境", 70.0, _WEIGHTS["regime"],
                              "未获取大盘数据，给中性分")
    score = mapping.get(regime.regime, 70.0)
    label = {"bull": "多头", "sideways": "震荡", "bear": "空头"}.get(regime.regime, "未知")
    return DimensionScore("regime", "大盘环境", score, _WEIGHTS["regime"], f"当前 {label}市")


# ============================================================================
# 主入口（纯计算，数据注入）
# ============================================================================

def diagnose(
    pf: Portfolio,
    price_map: dict[str, float],
    *,
    kline_fetcher: Optional[Callable[[str, str], pd.DataFrame]] = None,
    regime: Optional[MarketRegime] = None,
) -> PortfolioDiagnosis:
    """
    组合体检（纯计算）。

    Args:
        pf: 持仓组合
        price_map: {code: 最新价}；缺失的持仓跳过信号评估
        kline_fetcher: (code, period) -> OHLCV DataFrame；period ∈ {"daily","weekly"}。
                       None 时跳过 L2 信号评估（信号维度给中性分）
        regime: 大盘环境（None 给中性分）
    """
    total_mv = pf.total_market_value(price_map)
    total_cost = pf.total_cost_basis()
    total_pnl = pf.total_unrealized_pnl(price_map)
    total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0.0

    engine = SellEngine(pf.default_alerts)
    multiplier = regime.weight_multiplier if regime else 1.0

    entries: list[HoldingDiagnosis] = []
    for h in pf.holdings:
        price = float(price_map.get(h.code, 0) or 0)
        mv = h.market_value(price) if price > 0 else 0.0
        weight = (mv / total_mv * 100) if total_mv > 0 else 0.0
        pnl_pct = h.unrealized_pnl_pct(price) if (price > 0 and h.avg_cost > 0) else 0.0

        verdict: Optional[SellVerdict] = None
        if price > 0 and kline_fetcher is not None and h.market == "a":
            try:
                daily_df = kline_fetcher(h.code, "daily")
                weekly_df = kline_fetcher(h.code, "weekly")
                verdict = engine.evaluate(
                    h, price, daily_df, weekly_df, regime_multiplier=multiplier,
                )
            except Exception as e:
                logger.warning(f"持仓 {h.code} 信号评估失败: {e}")

        entries.append(HoldingDiagnosis(
            code=h.code, name=h.name, market=h.market,
            industry=_industry_of(h),
            price=price, weight_pct=weight, pnl_pct=pnl_pct,
            market_value=mv, verdict=verdict,
        ))

    dims = [
        _score_signals(entries),
        _score_concentration(entries),
        _score_pnl(entries),
        _score_regime(regime),
    ]
    overall = sum(d.score * d.weight for d in dims)

    # 问题清单 / 良好项
    issues: list[str] = []
    highlights: list[str] = []
    for e in entries:
        if e.verdict and e.verdict.action == "stop_loss":
            issues.append(f"⛔ {e.name}({e.code}) 触发止损规则：{e.verdict.advice.splitlines()[0]}")
        elif e.verdict and e.verdict.action == "reduce_all":
            issues.append(f"🔴 {e.name}({e.code}) 信号共振（风险分 {e.risk_pct:.0f}%），建议清仓")
        elif e.verdict and e.verdict.action == "reduce_half":
            issues.append(f"🟠 {e.name}({e.code}) 风险分 {e.risk_pct:.0f}%，建议减仓")
        if e.price <= 0:
            issues.append(f"⚠️ {e.name}({e.code}) 未取到行情，未纳入信号评估")
    for d in dims:
        if d.score >= 85 and d.key != "regime":
            highlights.append(f"✅ {d.label}良好：{d.detail}")
        elif d.score < 55:
            issues.append(f"📉 {d.label}偏弱：{d.detail}")

    return PortfolioDiagnosis(
        generated_at=datetime.now().isoformat(timespec="seconds"),
        total_market_value=total_mv,
        total_cost=total_cost,
        total_pnl=total_pnl,
        total_pnl_pct=total_pnl_pct,
        cash=pf.cash,
        position_count=len(pf.holdings),
        score=overall,
        grade=_grade(overall),
        dimensions=dims,
        holdings=entries,
        issues=issues,
        highlights=highlights,
        regime=regime,
    )


# ============================================================================
# 便捷入口（联网取数）
# ============================================================================

def run_diagnosis(pf: Optional[Portfolio] = None) -> PortfolioDiagnosis:
    """
    加载持仓 + 拉行情/K线/大盘环境 → 完整体检。

    A 股持仓做完整信号评估；港/美股仅参与市值与集中度统计（无风险信号）。
    """
    from src.analysis.screening.data_provider import ScreenerDataProvider
    from src.portfolio.manager import PortfolioManager
    from src.portfolio.market_regime import MarketRegimeAnalyzer

    if pf is None:
        pf = PortfolioManager().load()

    provider = ScreenerDataProvider()

    # A 股价格：一次拉全市场 spot
    price_map: dict[str, float] = {}
    try:
        spot = provider.get_all_a_shares()
        if spot is not None and not spot.empty:
            price_col = "最新价" if "最新价" in spot.columns else "close"
            for _, row in spot.iterrows():
                c = str(row.get("代码", "")).strip()
                try:
                    price_map[c] = float(row.get(price_col, 0) or 0)
                except (TypeError, ValueError):
                    continue
    except Exception as e:
        logger.warning(f"全市场行情获取失败: {e}")

    def _kline(code: str, period: str) -> pd.DataFrame:
        if period == "weekly":
            return provider.get_weekly_ohlcv(code)
        return provider.get_daily_ohlcv(code)

    regime: Optional[MarketRegime] = None
    try:
        regime = MarketRegimeAnalyzer(provider).analyze()
    except Exception as e:
        logger.warning(f"大盘环境分析失败: {e}")

    return diagnose(pf, price_map, kline_fetcher=_kline, regime=regime)
