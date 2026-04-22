from src.analysis.factor.base import BaseFactor
from src.analysis.factor.engine import FACTOR_REGISTRY, FactorEngine, build_engine_from_config, register_factor
from src.analysis.factor.momentum import Return5D, Return20D, Return60D, Return120D, ReturnFactor
from src.analysis.factor.quality import AmplitudeFactor, GrossMarginFactor, RevenueGrowthFactor, ROEFactor
from src.analysis.factor.technical import MACDDivergenceFactor, PriceAboveMAFactor, RSIFactor
from src.analysis.factor.valuation import MarketCapFactor, PBFactor, PEFactor, PSFactor, TurnoverRateFactor

# ========================
# 填充因子注册表（供 build_engine_from_config 使用）
# key 与 config/factors.yaml 的 type 字段一一对应
# ========================
register_factor("pe_ttm", PEFactor)
register_factor("pb", PBFactor)
register_factor("ps", PSFactor)
register_factor("market_cap", MarketCapFactor)
register_factor("turnover_rate", TurnoverRateFactor)

register_factor("roe", ROEFactor)
register_factor("gross_margin", GrossMarginFactor)
register_factor("revenue_growth", RevenueGrowthFactor)
register_factor("amplitude", AmplitudeFactor)

register_factor("return_nd", ReturnFactor)  # 参数化：params.period
register_factor("return_5d", Return5D)
register_factor("return_20d", Return20D)
register_factor("return_60d", Return60D)
register_factor("return_120d", Return120D)

register_factor("rsi", RSIFactor)
register_factor("macd_bottom_divergence", MACDDivergenceFactor)
register_factor("price_above_ma", PriceAboveMAFactor)


__all__ = [
    "BaseFactor", "FactorEngine", "FACTOR_REGISTRY",
    "build_engine_from_config", "register_factor",
    "PEFactor", "PBFactor", "PSFactor", "MarketCapFactor", "TurnoverRateFactor",
    "ReturnFactor", "Return5D", "Return20D", "Return60D", "Return120D",
    "RSIFactor", "MACDDivergenceFactor", "PriceAboveMAFactor",
    "ROEFactor", "GrossMarginFactor", "RevenueGrowthFactor", "AmplitudeFactor",
]
