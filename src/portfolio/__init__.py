"""
src/portfolio — 持仓管理与卖出决策引擎

模块组成：
  - models.py     : Holding / Transaction / Portfolio 数据类
  - manager.py    : 持仓 CRUD（基于 YAML），盈亏计算
  - sell_engine.py: L1+L2 卖出引擎（风控 + 信号加权）
  - market_regime.py (Phase 2): 大盘环境守门员
  - position_sizing.py (Phase 3): 仓位管理 / 分批止盈
  - macro_signals.py (Phase 5): 宏观信号（北向 / 美股夜盘）
"""
from src.portfolio.models import Holding, Portfolio, SellSignal, Transaction
from src.portfolio.manager import PortfolioManager
from src.portfolio.sell_engine import SellEngine, SellVerdict
from src.portfolio.market_regime import (
    MarketRegime,
    MarketRegimeAnalyzer,
    IndexState,
)
from src.portfolio.position_sizing import (
    PositionAdvice,
    check_concentration,
    suggest_scale_out,
)
from src.portfolio.macro_signals import (
    MacroPanel,
    MacroSignal,
    analyze_macro,
)

__all__ = [
    "Holding",
    "Portfolio",
    "SellSignal",
    "Transaction",
    "PortfolioManager",
    "SellEngine",
    "SellVerdict",
    "MarketRegime",
    "MarketRegimeAnalyzer",
    "IndexState",
    "PositionAdvice",
    "check_concentration",
    "suggest_scale_out",
    "MacroPanel",
    "MacroSignal",
    "analyze_macro",
]
