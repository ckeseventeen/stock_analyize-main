"""
src/core/models.py — 领域 dataclass 模型

替代项目里到处漂的 dict / pd.Series 业务对象，提供类型安全 + IDE 自动补全 + 序列化。

设计要点：
  - `Stock` frozen=True：可作为 dict key、可哈希、不可变
  - 其他可变对象（AlertRule、ScreeningResult）frozen=False 便于增量修改
  - `from_dict` / `to_dict` 与现存 YAML/CSV 序列化兼容，迁移期可直接喂老格式 dict
  - 字段命名贴近原 YAML 配置，避免重命名带来的迁移成本
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Stock:
    """单只股票的元信息（不含行情/财务）"""
    code: str
    name: str
    market: str = "a"             # "a" / "hk" / "us" / ...
    category: str = ""             # 板块/分类（如 "白酒"、"科技巨头"）
    valuation: str = "pe"           # "pe" / "ps"
    pe_range: tuple[float, float, float] | None = None
    ps_range: tuple[float, float, float] | None = None

    @classmethod
    def from_dict(cls, d: dict) -> Stock:
        """从老格式 dict（含 market_name/category_name 等）安全构造"""
        return cls(
            code=str(d.get("code", "")),
            name=str(d.get("name", "")),
            market=str(d.get("market", "a")),
            category=str(d.get("category", d.get("category_name", ""))),
            valuation=str(d.get("valuation", "pe")).lower(),
            pe_range=_as_tuple3(d.get("pe_range")),
            ps_range=_as_tuple3(d.get("ps_range")),
        )

    def to_dict(self) -> dict:
        """转换回 dict（用于序列化到 YAML / 老 API 兼容）"""
        d = asdict(self)
        # 把 tuple 转回 list 方便 yaml 序列化
        if self.pe_range is not None:
            d["pe_range"] = list(self.pe_range)
        if self.ps_range is not None:
            d["ps_range"] = list(self.ps_range)
        return d


@dataclass
class AlertRule:
    """单条价格预警规则"""
    code: str
    name: str
    market: str = "a"
    conditions: list[dict] = field(default_factory=list)
    cooldown_hours: int = 24

    @classmethod
    def from_dict(cls, d: dict) -> AlertRule:
        return cls(
            code=str(d.get("code", "")),
            name=str(d.get("name", d.get("code", ""))),
            market=str(d.get("market", "a")),
            conditions=list(d.get("conditions") or []),
            cooldown_hours=int(d.get("cooldown_hours", 24)),
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Signal:
    """策略 / 监控产生的信号事件"""
    stock_code: str
    stock_name: str
    market: str
    kind: str                # "buy" / "sell" / "alert" / "warning"
    timestamp: datetime
    strength: float = 0.0     # [-1, +1] 越正越强烈看多
    source: str = ""          # 策略 ID / 监控类型
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class ScreeningResult:
    """一次筛选执行的结果"""
    strategy_id: str
    executed_at: datetime
    stocks: list[Stock] = field(default_factory=list)
    scores: dict[str, float] = field(default_factory=dict)  # code → score
    reason_counter: dict[str, int] = field(default_factory=dict)  # 失败原因统计

    @property
    def count(self) -> int:
        return len(self.stocks)


@dataclass
class BacktestReport:
    """单次回测的核心指标"""
    strategy_name: str
    initial_cash: float
    final_value: float
    total_return_pct: float
    annual_return_pct: float
    sharpe: float
    max_drawdown_pct: float
    total_trades: int
    win_rate_pct: float
    warmup_bars: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_runner_dict(cls, d: dict) -> BacktestReport:
        """从 BacktestRunner.get_report() 返回的中文键 dict 构造"""
        return cls(
            strategy_name=str(d.get("策略", "")),
            initial_cash=float(d.get("初始资金", 0)),
            final_value=float(d.get("最终资产", 0)),
            total_return_pct=float(d.get("总收益率(%)", 0)),
            annual_return_pct=float(d.get("年化收益率(%)", 0)),
            sharpe=float(d.get("夏普比率", 0)),
            max_drawdown_pct=float(d.get("最大回撤(%)", 0)),
            total_trades=int(d.get("总交易次数", 0)),
            win_rate_pct=float(d.get("胜率(%)", 0)),
            warmup_bars=int(d.get("预热bar数", 0)),
        )


# =============================================================================
# 辅助工具
# =============================================================================

def _as_tuple3(value: Any) -> tuple[float, float, float] | None:
    """把 yaml 里的 [low, mid, high] list 转为 frozen 友好的 tuple3"""
    if value is None:
        return None
    try:
        seq = list(value)
        if len(seq) != 3:
            return None
        return (float(seq[0]), float(seq[1]), float(seq[2]))
    except (TypeError, ValueError):
        return None
