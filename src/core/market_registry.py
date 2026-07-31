"""
src/core/market_registry.py — 多市场元数据注册中心

替代散落在 10+ 处的 `if market == "a"` / `"hk"` / `"us"` 分支判断。
加一个新市场（如日股）只需一处 `register_market(MarketSpec(...))`。

设计：
  - MarketSpec 是 dataclass，包含一个市场的全部元数据（fetcher class、analyzer class、
    config 路径、默认股票、代码归一化函数等）
  - 模块级 _REGISTRY dict[market_key] = MarketSpec
  - `get_market("a")` 单点查询，找不到抛 KeyError + 友好提示

注意：
  - 为避免循环 import，fetcher_cls / analyzer_cls 用字符串路径（如 "src.data.fetchers:AStockDataFetcher"）
    + 惰性 import + lru_cache。这样 market_registry 自己只依赖 stdlib。
"""
from __future__ import annotations

import importlib
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class MarketSpec:
    """单个市场的完整元数据"""
    key: str                               # "a" / "hk" / "us"
    label: str                              # "A 股" / "港股" / "美股"
    config_path: Path                       # config/stocks/*.yaml
    fetcher_class_path: str                 # "module.path:ClassName"
    analyzer_class_path: str
    default_picker_code: str = ""           # FCF/估值页表单的默认占位
    default_picker_name: str = ""
    yfinance_suffix: str = ""               # ".HK" / ".SS" / ".SZ"，美股留空
    code_pad_width: int = 0                 # A股 6 / 港股 5 / 美股 0
    currency: str = "CNY"                    # "CNY" / "HKD" / "USD"

    def normalize_symbol(self, code: str) -> str:
        """按市场约定补零（A 股 6 位、港股 5 位）"""
        s = str(code).split(".")[0].strip()
        if self.code_pad_width > 0:
            return s.zfill(self.code_pad_width)
        return s

    @property
    def fetcher_cls(self) -> type:
        """惰性 import fetcher 类，避开循环依赖"""
        return _import_attr(self.fetcher_class_path)

    @property
    def analyzer_cls(self) -> type:
        """惰性 import analyzer 类"""
        return _import_attr(self.analyzer_class_path)


# ============================================================================
# 注册中心
# ============================================================================

_REGISTRY: dict[str, MarketSpec] = {}


def register_market(spec: MarketSpec, *, replace: bool = False) -> None:
    """注册一个市场。重复注册需显式 replace=True，否则抛错。"""
    key = spec.key.lower()
    if key in _REGISTRY and not replace:
        raise ValueError(f"市场 '{key}' 已存在；如需覆盖请传 replace=True")
    _REGISTRY[key] = spec


def get_market(key: str) -> MarketSpec:
    """按 key 查询市场 spec；不存在抛 KeyError + 友好提示"""
    k = (key or "").lower()
    if k not in _REGISTRY:
        available = ", ".join(_REGISTRY.keys()) or "(空)"
        raise KeyError(f"未知市场 '{key}'。已注册市场: {available}")
    return _REGISTRY[k]


def list_markets() -> list[MarketSpec]:
    """返回所有已注册市场（按注册顺序）"""
    return list(_REGISTRY.values())


def market_keys() -> list[str]:
    """返回所有市场 key（A 股优先，便于 UI 默认下拉）"""
    keys = list(_REGISTRY.keys())
    # A 股置首
    if "a" in keys:
        keys.remove("a")
        keys = ["a"] + keys
    return keys


def market_labels() -> dict[str, str]:
    """返回 {key: label}，给 UI 选择框直接消费"""
    return {spec.key: spec.label for spec in list_markets()}


# ============================================================================
# 默认注册：A / HK / US 三市场（替代原 main.py 中的 MARKET_MAPPING）
# ============================================================================

def _register_defaults() -> None:
    """模块 import 时自动登记 A/港/美三市场"""
    register_market(MarketSpec(
        key="a",
        label="A 股",
        config_path=Path("./config/stocks/a_stock.yaml"),
        fetcher_class_path="src.data.fetchers:AStockDataFetcher",
        analyzer_class_path="src.core.analyzer:AStockAnalyzer",
        default_picker_code="600519",
        default_picker_name="贵州茅台",
        yfinance_suffix="",   # A 股按代码首位推导（6→.SS, 0/3→.SZ），单独处理
        code_pad_width=6,
        currency="CNY",
    ))
    register_market(MarketSpec(
        key="hk",
        label="港股",
        config_path=Path("./config/stocks/hk_stock.yaml"),
        fetcher_class_path="src.data.fetchers:HKStockDataFetcher",
        analyzer_class_path="src.core.analyzer:HKStockAnalyzer",
        default_picker_code="00700",
        default_picker_name="腾讯控股",
        yfinance_suffix=".HK",
        code_pad_width=5,
        currency="HKD",
    ))
    register_market(MarketSpec(
        key="us",
        label="美股",
        config_path=Path("./config/stocks/us_stock.yaml"),
        fetcher_class_path="src.data.fetchers:USStockDataFetcher",
        analyzer_class_path="src.core.analyzer:USStockAnalyzer",
        default_picker_code="AAPL",
        default_picker_name="Apple",
        yfinance_suffix="",   # 美股直接用原 ticker
        code_pad_width=0,
        currency="USD",
    ))


@lru_cache(maxsize=32)
def _import_attr(spec_path: str):
    """
    解析 'module.path:ClassName' 形式的路径，惰性 import 并返回类对象。
    """
    if ":" not in spec_path:
        raise ValueError(f"class_path 必须是 'module:Class' 形式，得到 '{spec_path}'")
    module_path, attr_name = spec_path.split(":", 1)
    module = importlib.import_module(module_path)
    if not hasattr(module, attr_name):
        raise ImportError(f"模块 {module_path} 中未找到 {attr_name}")
    return getattr(module, attr_name)


# 在 import 时自动注册默认市场
_register_defaults()
