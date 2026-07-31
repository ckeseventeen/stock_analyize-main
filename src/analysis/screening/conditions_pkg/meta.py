"""
src/analysis/screening/conditions_pkg/meta.py — 条件元数据

条件分类表（前端编辑器用）与买卖方向推断。
"""
from __future__ import annotations

CONDITION_CATEGORIES = {
    "基本面/排除": [
        "exclude_risk", "exclude_st", "exclude_delisting_risk",
        "roe_filter", "northbound_flow",
    ],
    "估值/财务": [
        "market_cap", "pe_range", "pb_range", "price_range",
        "turnover_rate", "price_change",
    ],
    "均线": [
        "price_above_ma", "multi_ma_bull", "ma_gold_cross",
        "ma_death_cross", "support_ma", "break_below_ma",
    ],
    "MACD": [
        "weekly_macd_divergence", "daily_macd_divergence",
        "weekly_macd_gold_cross", "macd_hist_positive",
        "weekly_macd_top_divergence", "daily_macd_top_divergence",
    ],
    "RSI": [
        "rsi_oversold", "rsi_overbought",
    ],
    "量价/突破": [
        "volume_break", "volume_shrink", "box_breakout",
        "box_breakout_volume", "downtrend_breakout",
        "bollinger_breakout", "volume_price_divergence",
        "volume_blowoff",
    ],
    "KDJ": [
        "kdj_gold_cross", "kdj_death_cross",
    ],
    "回调/超买": [
        "bias",
    ],
    "风控/止损": [
        "stop_loss", "trailing_stop", "atr_trailing_stop",
    ],
    "ML/自学习": [
        "ml_top_k",
    ],
}

# 条件的中文标签
CONDITION_LABELS: dict[str, str] = {
    "market_cap": "市值范围",
    "pe_range": "市盈率(PE)范围",
    "pb_range": "市净率(PB)范围",
    "price_range": "股价范围",
    "turnover_rate": "换手率范围",
    "price_change": "涨跌幅范围",
    "weekly_macd_divergence": "周线MACD底背离",
    "daily_macd_divergence": "日线MACD底背离",
    "weekly_macd_gold_cross": "周线MACD金叉",
    "macd_hist_positive": "MACD柱状图翻红",
    "rsi_oversold": "RSI超卖",
    "rsi_overbought": "RSI超买",
    "price_above_ma": "价格站上均线",
    "multi_ma_bull": "均线多头排列",
    "ma_gold_cross": "均线金叉",
    "ma_death_cross": "均线死叉",
    "support_ma": "均线支撑",
    "volume_break": "放量突破",
    "volume_shrink": "缩量",
    "box_breakout": "箱体突破",
    "box_breakout_volume": "箱体放量突破",
    "downtrend_breakout": "下降趋势线突破",
    "bollinger_breakout": "布林带突破",
    "kdj_gold_cross": "KDJ金叉",
    "exclude_risk": "排除风险股(ST/退市)",
    "exclude_st": "排除ST股(兼容旧配置)",
    "exclude_delisting_risk": "排除退市风险股(兼容旧配置)",
    "roe_filter": "ROE筛选",
    "stop_loss": "固定止损",
    "trailing_stop": "移动止损",
    "volume_price_divergence": "量价背离",
    "northbound_flow": "北向资金净买入",
    "ml_top_k": "ML预测排名Top-K",
    # Phase 5 卖出 / 回调预警
    "weekly_macd_top_divergence": "周线MACD顶背离",
    "daily_macd_top_divergence": "日线MACD顶背离",
    "kdj_death_cross": "KDJ高位死叉",
    "bias": "乖离率(BIAS)",
    "break_below_ma": "跌破均线",
    "volume_blowoff": "天量天价(派发)",
    "atr_trailing_stop": "ATR 动态止损",
}


# ========================
# 买卖方向分类（价格预警 / 策略回测都用）
# ========================
# 每个 condition 标记是入场（buy） / 出场（sell） / 中性（neutral）
# 中性 = 可买可卖看方向参数（如 bollinger_breakout 上轨突破偏买，下轨偏卖）
SIGNAL_DIRECTION: dict[str, str] = {
    # ──────────── 买点（入场） ────────────
    "weekly_macd_divergence":  "buy",   # 底背离
    "daily_macd_divergence":   "buy",
    "weekly_macd_gold_cross":  "buy",
    "macd_hist_positive":       "buy",   # MACD 柱翻红
    "rsi_oversold":             "buy",   # 超卖反弹
    "ma_gold_cross":            "buy",   # 金叉
    "kdj_gold_cross":           "buy",
    "multi_ma_bull":            "buy",   # 均线多头排列
    "support_ma":               "buy",   # 均线支撑
    "price_above_ma":           "buy",   # 站上均线
    "box_breakout":             "buy",   # 箱体突破
    "box_breakout_volume":      "buy",   # 箱体放量突破
    "downtrend_breakout":       "buy",   # 下降趋势线突破
    "volume_break":             "buy",   # 放量突破
    "northbound_flow":          "buy",   # 北向买入
    # ──────────── 卖点（出场） ────────────
    "rsi_overbought":           "sell",  # 超买
    "ma_death_cross":           "sell",  # 死叉
    "volume_shrink":            "sell",  # 缩量（疲软）
    "stop_loss":                "sell",
    "trailing_stop":            "sell",
    # Phase 5 新增卖点
    "weekly_macd_top_divergence": "sell",
    "daily_macd_top_divergence":  "sell",
    "kdj_death_cross":            "sell",
    "break_below_ma":             "sell",
    "volume_blowoff":             "sell",  # 天量天价派发
    "atr_trailing_stop":          "sell",  # ATR 动态止损
    # bias 看 direction 参数：above=sell, below=buy
    # ──────────── 中性（看方向参数）────────────
    "bollinger_breakout":       "neutral",   # direction=upper 买 / lower 卖
    "volume_price_divergence":  "neutral",   # direction=top 卖 / bottom 买
    "bias":                     "neutral",   # direction=above 卖 / below 买
    # ──────────── Spot 类（既非买也非卖，是范围筛选） ────────────
    "market_cap":     "filter",
    "pe_range":       "filter",
    "pb_range":       "filter",
    "price_range":    "filter",
    "turnover_rate":  "filter",
    "price_change":   "filter",
    "roe_filter":     "filter",
    "exclude_risk":   "filter",
    "exclude_st":     "filter",
    "exclude_delisting_risk": "filter",
    "ml_top_k":       "filter",   # ML 排名筛选
}


def get_signal_direction(condition_type: str, params: dict | None = None) -> str:
    """
    返回 signal 的方向：buy / sell / neutral / filter / unknown。

    对 neutral 类型（bollinger_breakout / volume_price_divergence），可传 params
    进一步根据 direction 字段细化为 buy / sell。
    """
    base = SIGNAL_DIRECTION.get(condition_type, "unknown")
    if base != "neutral" or not params:
        return base
    direction = str(params.get("direction", "")).lower()
    if condition_type == "bollinger_breakout":
        # upper 突破上轨 = 偏多；lower 跌破下轨 = 偏空
        return "buy" if direction == "upper" else "sell" if direction == "lower" else "neutral"
    if condition_type == "volume_price_divergence":
        # top 顶背离 = 偏空；bottom 底背离 = 偏多
        return "sell" if direction == "top" else "buy" if direction == "bottom" else "neutral"
    if condition_type == "bias":
        # above 偏离向上=偏空；below 偏离向下=偏多
        return "sell" if direction == "above" else "buy" if direction == "below" else "neutral"
    return base


def signal_emoji(direction: str) -> str:
    """方向 → emoji"""
    return {
        "buy": "📈",
        "sell": "📉",
        "neutral": "⚖️",
        "filter": "🔍",
    }.get(direction, "❓")
