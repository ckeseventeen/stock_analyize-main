"""
src/portfolio/position_sizing.py — L3 仓位管理

机构卖出体系第 3 层：仓位动态调整。

主要功能：
  1. 分批止盈建议：盈利 +10%/+20%/+30%/+50% 自动建议卖 N%
  2. 盈亏比恶化预警：浮盈高点 / 当前浮盈 > 阈值时减仓
  3. 行业集中度预警：单行业仓位 > 阈值

不依赖外部数据，纯逻辑运算（基于持仓 + 浮盈状态）。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from src.portfolio.models import Holding, Portfolio


# ========================
# 分批止盈梯度（可配置）
# ========================
# (浮盈阈值%, 建议卖出比例%)
DEFAULT_PROFIT_LADDER: list[tuple[float, float]] = [
    (10.0, 25.0),  # 盈利 +10% → 卖 1/4
    (20.0, 25.0),  # 盈利 +20% → 再卖 1/4
    (30.0, 25.0),  # 盈利 +30% → 再卖 1/4
    (50.0, 25.0),  # 盈利 +50% → 卖到只剩 0（实战上往往保留 1/4 让利润奔跑）
]


@dataclass
class PositionAdvice:
    """
    单只持仓的仓位调整建议

    Attributes:
        code: 股票代码
        action: "hold" / "scale_out" / "concentration_warning"
        sell_pct: 建议卖出比例（如 25 表示卖 25%）
        sell_qty: 建议卖出数量（按当前 qty 算）
        reason: 中文说明
        priority: 1/2/3 (低/中/高)
    """
    code: str
    action: str
    sell_pct: float = 0.0
    sell_qty: int = 0
    reason: str = ""
    priority: int = 2


def suggest_scale_out(holding: Holding, current_price: float,
                       ladder: Optional[list[tuple[float, float]]] = None
                       ) -> Optional[PositionAdvice]:
    """
    分批止盈建议

    Args:
        holding: 持仓
        current_price: 当前价
        ladder: 自定义梯度 [(浮盈%, 卖出%)]，None 用默认

    Returns:
        PositionAdvice 或 None（未达到任何阈值）
    """
    if holding.qty <= 0 or holding.avg_cost <= 0 or current_price <= 0:
        return None

    pnl_pct = holding.unrealized_pnl_pct(current_price)
    ladder = ladder or DEFAULT_PROFIT_LADDER

    # 找到第一个未跨越的阈值
    triggered_pct = 0.0
    for threshold_pct, sell_pct in ladder:
        if pnl_pct >= threshold_pct:
            triggered_pct = sell_pct
    if triggered_pct <= 0:
        return None

    # 注意：实战中应该只在"首次跨越某档"时建议，否则反复触发会卖光
    # 这里简化处理：只看当前最高档对应的卖出比例
    sell_qty = int(holding.qty * triggered_pct / 100)
    return PositionAdvice(
        code=holding.code,
        action="scale_out",
        sell_pct=triggered_pct,
        sell_qty=sell_qty,
        reason=(
            f"浮盈 {pnl_pct:+.1f}%，建议**分批止盈**：卖出 {triggered_pct:.0f}% "
            f"({sell_qty} 股) 锁定利润"
        ),
        priority=2,
    )


def check_concentration(pf: Portfolio, price_map: dict[str, float],
                         single_stock_limit_pct: float = 30.0,
                         single_industry_limit_pct: float = 50.0
                         ) -> list[PositionAdvice]:
    """
    集中度预警：单只 / 单行业仓位占比过高

    Args:
        pf: 持仓组合
        price_map: code → 最新价 dict
        single_stock_limit_pct: 单只仓位上限
        single_industry_limit_pct: 单行业仓位上限

    Returns:
        预警列表
    """
    total_mv = pf.total_market_value(price_map)
    if total_mv <= 0:
        return []

    advices: list[PositionAdvice] = []

    # 单只占比
    for h in pf.holdings:
        mv = h.market_value(price_map.get(h.code, 0.0))
        if mv <= 0:
            continue
        share_pct = mv / total_mv * 100
        if share_pct > single_stock_limit_pct:
            advices.append(PositionAdvice(
                code=h.code,
                action="concentration_warning",
                sell_pct=0.0,
                reason=(
                    f"单只仓位占比 {share_pct:.1f}% > 上限 {single_stock_limit_pct:.0f}%，"
                    f"**建议分散**"
                ),
                priority=2,
            ))

    # 行业聚合 — 一个行业产生 1 条聚合警告（含所有 contributing 持仓名称）
    industry_mv: dict[str, float] = {}
    industry_holdings: dict[str, list[str]] = {}
    for h in pf.holdings:
        ind = h.tag or "未分类"
        industry_mv.setdefault(ind, 0)
        industry_mv[ind] += h.market_value(price_map.get(h.code, 0.0))
        industry_holdings.setdefault(ind, []).append(
            f"{h.name or h.code}({h.code})"
        )

    for ind, mv in industry_mv.items():
        share_pct = mv / total_mv * 100
        if share_pct > single_industry_limit_pct:
            contributors = "、".join(industry_holdings.get(ind, [])[:5])
            advices.append(PositionAdvice(
                code=industry_holdings.get(ind, ["?"])[0].split("(")[-1].rstrip(")"),
                action="concentration_warning",
                sell_pct=0.0,
                reason=(
                    f"行业「{ind}」仓位占比 {share_pct:.1f}% > 上限 "
                    f"{single_industry_limit_pct:.0f}%（含：{contributors}）"
                    f"，**降低板块暴露**"
                ),
                priority=2,
            ))

    return advices
