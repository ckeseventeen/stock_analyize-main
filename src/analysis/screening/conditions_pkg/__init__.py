"""
src/analysis/screening/conditions_pkg — 筛选条件包

原 conditions.py 单文件 1869 行 / 40 个类，按用途拆分为：

    base.py         BaseCondition 契约（含 required_bars 数据深度协议）+ 共享基类
    fundamental.py  基本面/Spot 快照条件（市值/PE/PB/换手率/ST 排除）
    entry.py        买入向条件（底背离/金叉/突破/超卖/资金流）
    exit.py         卖出向条件（止损/超买/死叉/顶背离/放量滞涨）
    meta.py         条件分类表 + 买卖方向推断（前端编辑器用）

**向后兼容**：`from src.analysis.screening.conditions import XxxCondition`
仍然可用（conditions.py 已改为本包的 re-export shim），下游代码零改动。
"""
from __future__ import annotations

from src.analysis.screening.conditions_pkg.base import (
    BaseCondition,
    CompositeCondition,
    RankingCondition,
    _MACrossCondition,
)
from src.analysis.screening.conditions_pkg.entry import (
    BollingerBreakoutCondition,
    BoxBreakoutCondition,
    BoxBreakoutWithVolumeCondition,
    DailyMACDBottomDivergenceCondition,
    DowntrendBreakoutCondition,
    KDJGoldCrossCondition,
    MACDHistPositiveCondition,
    MAGoldCrossCondition,
    MultiMABullCondition,
    PriceAboveMACondition,
    PriceChangeCondition,
    RSIOversoldCondition,
    SupportMACondition,
    VolumeBreakCondition,
    VolumeShrinkCondition,
    WeeklyMACDBottomDivergenceCondition,
    WeeklyMACDGoldCrossCondition,
)
from src.analysis.screening.conditions_pkg.exit import (
    ATRTrailingStopCondition,
    BIASCondition,
    BreakBelowMACondition,
    DailyMACDTopDivergenceCondition,
    KDJDeathCrossCondition,
    MADeathCrossCondition,
    RSIOverboughtCondition,
    StopLossCondition,
    TrailingStopCondition,
    VolumeBlowoffCondition,
    VolumePriceDivergenceCondition,
    WeeklyMACDTopDivergenceCondition,
)
from src.analysis.screening.conditions_pkg.factor import (
    DividendYieldCondition,
    LowVolatilityCondition,
    MLTopKCondition,
    MomentumRankCondition,
    NorthboundFlowCondition,
)
from src.analysis.screening.conditions_pkg.fundamental import (
    ExcludeRiskCondition,
    MarketCapCondition,
    PBRangeCondition,
    PERangeCondition,
    PriceRangeCondition,
    ROEFilterCondition,
    TurnoverRateCondition,
)
from src.analysis.screening.conditions_pkg.meta import (
    CONDITION_CATEGORIES,
    CONDITION_LABELS,
    SIGNAL_DIRECTION,
    get_signal_direction,
    signal_emoji,
)

# ========================
# 条件注册表（YAML 配置解析用）
# ========================

CONDITION_REGISTRY: dict[str, type] = {
    "market_cap": MarketCapCondition,
    "pe_range": PERangeCondition,
    "pb_range": PBRangeCondition,
    "price_range": PriceRangeCondition,
    "turnover_rate": TurnoverRateCondition,
    "weekly_macd_divergence": WeeklyMACDBottomDivergenceCondition,
    "daily_macd_divergence": DailyMACDBottomDivergenceCondition,
    "rsi_oversold": RSIOversoldCondition,
    "rsi_overbought": RSIOverboughtCondition,
    "price_above_ma": PriceAboveMACondition,
    "box_breakout": BoxBreakoutCondition,
    "downtrend_breakout": DowntrendBreakoutCondition,
    "exclude_risk": ExcludeRiskCondition,
    "exclude_st": ExcludeRiskCondition,
    "exclude_delisting_risk": ExcludeRiskCondition,
    "roe_filter": ROEFilterCondition,
    "multi_ma_bull": MultiMABullCondition,
    "volume_break": VolumeBreakCondition,
    "weekly_macd_gold_cross": WeeklyMACDGoldCrossCondition,
    "volume_shrink": VolumeShrinkCondition,
    "support_ma": SupportMACondition,
    "bollinger_breakout": BollingerBreakoutCondition,
    "kdj_gold_cross": KDJGoldCrossCondition,
    "ma_gold_cross": MAGoldCrossCondition,
    "ma_death_cross": MADeathCrossCondition,
    "price_change": PriceChangeCondition,
    "macd_hist_positive": MACDHistPositiveCondition,
    "box_breakout_volume": BoxBreakoutWithVolumeCondition,
    "stop_loss": StopLossCondition,
    "trailing_stop": TrailingStopCondition,
    "volume_price_divergence": VolumePriceDivergenceCondition,
    "northbound_flow": NorthboundFlowCondition,
    "ml_top_k": MLTopKCondition,
    # 卖出 / 回调预警（Phase 5）
    "weekly_macd_top_divergence": WeeklyMACDTopDivergenceCondition,
    "daily_macd_top_divergence": DailyMACDTopDivergenceCondition,
    "kdj_death_cross": KDJDeathCrossCondition,
    "bias": BIASCondition,
    "break_below_ma": BreakBelowMACondition,
    "volume_blowoff": VolumeBlowoffCondition,
    "momentum_rank": MomentumRankCondition,
    "low_volatility": LowVolatilityCondition,
    "dividend_yield": DividendYieldCondition,
    "atr_trailing_stop": ATRTrailingStopCondition,
}


__all__ = [
    "BaseCondition",
    "_MACrossCondition",
    "MarketCapCondition",
    "PERangeCondition",
    "PBRangeCondition",
    "PriceRangeCondition",
    "TurnoverRateCondition",
    "ExcludeRiskCondition",
    "ROEFilterCondition",
    "WeeklyMACDBottomDivergenceCondition",
    "DailyMACDBottomDivergenceCondition",
    "RSIOversoldCondition",
    "PriceAboveMACondition",
    "BoxBreakoutCondition",
    "DowntrendBreakoutCondition",
    "MultiMABullCondition",
    "VolumeBreakCondition",
    "WeeklyMACDGoldCrossCondition",
    "VolumeShrinkCondition",
    "SupportMACondition",
    "BollingerBreakoutCondition",
    "KDJGoldCrossCondition",
    "MAGoldCrossCondition",
    "PriceChangeCondition",
    "MACDHistPositiveCondition",
    "BoxBreakoutWithVolumeCondition",
    "MLTopKCondition",
    "NorthboundFlowCondition",
    "LowVolatilityCondition",
    "MomentumRankCondition",
    "RankingCondition",
    "CompositeCondition",
    "DividendYieldCondition",
    "MADeathCrossCondition",
    "RSIOverboughtCondition",
    "StopLossCondition",
    "TrailingStopCondition",
    "VolumePriceDivergenceCondition",
    "WeeklyMACDTopDivergenceCondition",
    "DailyMACDTopDivergenceCondition",
    "KDJDeathCrossCondition",
    "BIASCondition",
    "BreakBelowMACondition",
    "ATRTrailingStopCondition",
    "VolumeBlowoffCondition",
    "CONDITION_REGISTRY",
    "CONDITION_CATEGORIES",
    "CONDITION_LABELS",
    "SIGNAL_DIRECTION",
    "get_signal_direction",
    "signal_emoji",
]
