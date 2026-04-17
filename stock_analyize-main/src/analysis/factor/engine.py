"""
src/analysis/factor/engine.py — 因子计算引擎

组合多个因子，对批量股票数据进行因子值计算。
"""
from pathlib import Path

import pandas as pd
import yaml

from src.analysis.factor.base import BaseFactor
from src.utils.logger import get_logger

logger = get_logger("factor_engine")


# ========================
# 因子注册表（YAML 配置 → 类映射）
# ========================
# 延迟导入避免循环 import；由 __init__.py 在导入时填充。

FACTOR_REGISTRY: dict[str, type[BaseFactor]] = {}


def register_factor(key: str, factor_cls: type[BaseFactor]) -> None:
    """注册因子类到全局注册表"""
    FACTOR_REGISTRY[key] = factor_cls


def _default_config_path() -> Path:
    """定位 factors.yaml：相对于项目根的 config/factors.yaml"""
    # engine.py → src/analysis/factor/engine.py → 项目根是 parents[3]
    return Path(__file__).resolve().parents[3] / "config" / "factors.yaml"


class FactorEngine:
    """
    可组合因子计算引擎

    使用示例:
        engine = FactorEngine()
        engine.add_factor(PEFactor())
        engine.add_factor(MarketCapFactor())
        engine.add_factor(MACDDivergenceFactor())

        # stocks_data: {code: {"spot": Series, "daily_df": DataFrame, ...}}
        result_df = engine.compute(stocks_data)
    """

    def __init__(self):
        self.factors: list[BaseFactor] = []

    def add_factor(self, factor: BaseFactor) -> "FactorEngine":
        """链式添加因子"""
        self.factors.append(factor)
        return self

    def compute(self, stocks_data: dict[str, dict]) -> pd.DataFrame:
        """
        批量计算所有因子

        Args:
            stocks_data: {股票代码: {"spot": Series, "daily_df": DataFrame, "weekly_df": DataFrame}}

        Returns:
            DataFrame，索引为股票代码，列为各因子值
        """
        results = {}
        for code, data in stocks_data.items():
            row = {}
            for factor in self.factors:
                row[factor.name] = factor.safe_calculate(data)
            results[code] = row

        df = pd.DataFrame.from_dict(results, orient="index")
        df.index.name = "代码"
        return df

    def compute_single(self, code: str, data: dict) -> dict[str, float]:
        """单只股票因子计算"""
        result = {}
        for factor in self.factors:
            result[factor.name] = factor.safe_calculate(data)
        return result

    def get_factor_names(self) -> list[str]:
        """返回所有已添加因子的名称列表"""
        return [f.name for f in self.factors]

    def get_ohlcv_requirements(self) -> set[str]:
        """返回因子所需的OHLCV周期集合（如 {"daily", "weekly"}）"""
        periods = set()
        for f in self.factors:
            if f.requires_ohlcv:
                periods.add(f.ohlcv_period)
        return periods


def build_engine_from_config(profile: str | None = None,
                             config_path: Path | str | None = None) -> FactorEngine:
    """
    从 factors.yaml 读取指定 profile，按 enabled 过滤并实例化因子，返回 FactorEngine。

    - profile=None 时使用 YAML 中的 active_profile；若文件缺失则返回空引擎（不崩溃）
    - 未知的 type 会跳过并记录 warning，不抛异常
    - 实例化失败（参数不匹配等）同样跳过 + warning
    """
    path = Path(config_path) if config_path else _default_config_path()
    engine = FactorEngine()

    if not path.exists():
        logger.warning(f"factors.yaml 不存在 ({path})，返回空引擎")
        return engine

    try:
        with open(path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f"读取 factors.yaml 失败: {e}")
        return engine

    profiles = cfg.get("profiles") or {}
    name = profile or cfg.get("active_profile") or "default"
    prof = profiles.get(name) or {}
    factor_list = prof.get("factors") or []

    for entry in factor_list:
        if not isinstance(entry, dict):
            continue
        if not entry.get("enabled", True):
            continue
        type_key = entry.get("type")
        if not type_key or type_key not in FACTOR_REGISTRY:
            logger.warning(f"未知因子 type={type_key}，跳过")
            continue
        params = entry.get("params") or {}
        try:
            factor_instance = FACTOR_REGISTRY[type_key](**params)
            engine.add_factor(factor_instance)
        except Exception as e:
            logger.warning(f"实例化因子 {type_key}(params={params}) 失败: {e}")

    return engine
