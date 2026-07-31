"""
src/services/screening_service.py — 筛选/策略配置的无头服务层

把原先散在 pages/2_策略配置.py 和 pages/3_股票筛选.py 里的编排逻辑下沉到这里：
  - 策略 CRUD（读写 screen_config.yaml，原子写入）
  - 条件 schema 查询（分类/标签/参数控件规格），供动态表单与包校验共用
  - 试运行 / 正式筛选执行

页面（或未来的 HTTP API）只负责收集参数和渲染结果。
"""
from __future__ import annotations

import copy
import inspect
import time
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from src.analysis.screening.conditions import (
    CONDITION_CATEGORIES,
    CONDITION_LABELS,
    CONDITION_REGISTRY,
)
from src.analysis.screening.config_schema import (
    _PARAM_MAP,
    SPOT_ONLY_TYPES,
    _build_conditions,
)
from src.analysis.screening.config_schema import (
    list_strategies as _list_strategies,
)
from src.core.config_io import PATH_SCREEN, atomic_save_yaml, load_yaml
from src.utils.logger import get_logger

logger = get_logger("screening_service")


# ============================================================================
# 条件 schema：动态表单 / 包校验共用
# ============================================================================

# bool 型参数（历史 YAML 里可能存成 0/1，控件推断需要显式列出）
_BOOL_PARAMS = {
    "zero_axis_filter", "multi_level_check", "require_all_above",
    "require_up_trend", "require_zero_near", "close_touch", "strict",
}

# float 型参数
_FLOAT_PARAMS = {
    "breakout_pct", "consolidation_pct", "vol_multiple", "shrink_ratio",
    "std_dev", "j_threshold", "tolerance", "callback_pct", "max_loss_pct",
    "min_score", "multiplier", "min_net_buy", "min_price_change_pct",
    "min_roe", "threshold",
}

# 枚举型参数 → 可选值（同名参数在不同条件下取值不同，按 (条件类型, 参数) 定位）
_CHOICE_PARAMS: dict[tuple[str, str], list[str]] = {
    ("bollinger_breakout", "direction"): ["upper", "lower"],
    ("volume_price_divergence", "direction"): ["top", "bottom"],
    ("bias", "direction"): ["above", "below", "both"],
}


@dataclass(frozen=True)
class ParamSpec:
    """单个条件参数的控件规格（供动态表单渲染与导入校验）"""
    yaml_key: str
    kind: str                      # "bool" / "int" / "float" / "list" / "choice" / "str"
    choices: list | None = None    # kind == "choice" 时的可选值
    default: object = None         # 条件类 __init__ 的默认值（可能为 None）


def condition_categories() -> dict[str, list[str]]:
    """条件分类 → 类型列表（UI 分栏用）"""
    return dict(CONDITION_CATEGORIES)


def condition_label(ctype: str) -> str:
    """条件类型 → 可读中文标签"""
    return CONDITION_LABELS.get(ctype, ctype)


def condition_types() -> list[str]:
    """全部已注册条件类型"""
    return sorted(CONDITION_REGISTRY.keys())


def sellable_condition_types() -> list[str]:
    """可用作卖出信号的条件类型（排除仅 Spot 的基本面/排除类）"""
    return [ct for ct in CONDITION_REGISTRY if ct not in SPOT_ONLY_TYPES]


def is_spot_only(ctype: str) -> bool:
    """该条件是否仅用于实时筛选（不可回测）"""
    return ctype in SPOT_ONLY_TYPES


def _init_defaults(ctype: str) -> dict[str, object]:
    """从条件类 __init__ 签名提取 {yaml_key: 默认值}"""
    cls = CONDITION_REGISTRY.get(ctype)
    if not cls:
        return {}
    param_map = _PARAM_MAP.get(ctype, {})
    defaults: dict[str, object] = {}
    sig = inspect.signature(cls.__init__)
    for pname, param in sig.parameters.items():
        if pname == "self" or param.default is inspect.Parameter.empty:
            continue
        yaml_key = next((yk for yk, ik in param_map.items() if ik == pname), pname)
        defaults[yaml_key] = param.default
    return defaults


def condition_param_specs(ctype: str) -> list[ParamSpec]:
    """
    条件类型 → 参数控件规格列表。

    kind 判定优先级：显式枚举 > 默认值类型 > 名单兜底（默认值为 None 时）> str。
    """
    param_map = _PARAM_MAP.get(ctype, {})
    defaults = _init_defaults(ctype)
    specs: list[ParamSpec] = []
    for yaml_key in param_map:
        default = defaults.get(yaml_key)
        choices = None
        if (ctype, yaml_key) in _CHOICE_PARAMS:
            kind = "choice"
            choices = list(_CHOICE_PARAMS[(ctype, yaml_key)])
        elif isinstance(default, bool):
            kind = "bool"
        elif isinstance(default, int):
            kind = "int"
        elif isinstance(default, float):
            kind = "float"
        elif isinstance(default, list):
            kind = "list"
        elif yaml_key in _BOOL_PARAMS:
            kind = "bool"
        elif yaml_key in _FLOAT_PARAMS:
            kind = "float"
        else:
            kind = "str"
        specs.append(ParamSpec(yaml_key=yaml_key, kind=kind, choices=choices, default=default))
    return specs


def new_condition_dict(ctype: str) -> dict:
    """构造一个带默认参数的条件 dict（页面「添加条件」用）"""
    cond = {"type": ctype}
    cond.update(_init_defaults(ctype))
    return cond


def validate_conditions(conditions: list[dict]) -> list[str]:
    """
    校验条件配置列表（策略导入/保存前用）。

    Returns:
        错误信息列表；空列表 = 通过
    """
    errors: list[str] = []
    if not isinstance(conditions, list):
        return ["conditions 必须是列表"]
    for i, cond in enumerate(conditions):
        if not isinstance(cond, dict):
            errors.append(f"条件 #{i + 1} 不是字典")
            continue
        ctype = cond.get("type", "")
        if ctype not in CONDITION_REGISTRY:
            errors.append(f"条件 #{i + 1} 未知类型: '{ctype}'")
            continue
        known_keys = set(_PARAM_MAP.get(ctype, {})) | {"type"}
        unknown = set(cond.keys()) - known_keys
        if unknown:
            errors.append(f"条件 #{i + 1} ({ctype}) 含未知参数: {sorted(unknown)}")
    return errors


# ============================================================================
# 策略 CRUD（screen_config.yaml）
# ============================================================================

def default_strategy_body(name: str = "") -> dict:
    """新建策略的默认骨架"""
    return {
        "name": name,
        "conditions": [
            {"type": "exclude_st"},
            {"type": "exclude_delisting_risk"},
        ],
        "output": {"sort_by": "总市值(亿)", "ascending": False, "limit": 50},
    }


def load_all_strategies(config_path: Path | str | None = None) -> dict[str, dict]:
    """读取全部策略 {sid: 策略体}"""
    cfg = load_yaml(config_path or PATH_SCREEN) or {}
    return cfg.get("strategies", {}) or {}


def list_strategy_names(config_path: Path | str | None = None) -> dict[str, str]:
    """{sid: 显示名}（含旧格式兼容）"""
    return _list_strategies(str(config_path or PATH_SCREEN))


def get_strategy(sid: str, config_path: Path | str | None = None) -> dict:
    """单个策略体；不存在返回 {}"""
    return dict(load_all_strategies(config_path).get(sid, {}))


def save_all_strategies(strategies: dict[str, dict],
                        config_path: Path | str | None = None) -> bool:
    """整体写回 strategies 段（保留 YAML 其他顶层键）"""
    path = config_path or PATH_SCREEN
    cfg = load_yaml(path) or {}
    cfg["strategies"] = strategies
    return atomic_save_yaml(path, cfg)


def upsert_strategy(sid: str, body: dict,
                    config_path: Path | str | None = None) -> tuple[bool, str]:
    """
    新增或更新单个策略（带条件校验）。

    Returns:
        (成功?, 消息)
    """
    sid = (sid or "").strip()
    if not sid:
        return False, "策略 ID 不能为空"
    errors = validate_conditions(body.get("conditions", []))
    if errors:
        return False, "条件校验失败: " + "; ".join(errors)
    strategies = load_all_strategies(config_path)
    strategies[sid] = body
    ok = save_all_strategies(strategies, config_path)
    return ok, ("已保存" if ok else "写入失败")


def delete_strategy(sid: str, config_path: Path | str | None = None) -> tuple[bool, str]:
    """删除策略"""
    strategies = load_all_strategies(config_path)
    if sid not in strategies:
        return False, f"策略不存在: {sid}"
    del strategies[sid]
    ok = save_all_strategies(strategies, config_path)
    return ok, ("已删除" if ok else "写入失败")


def rename_strategy(old_sid: str, new_sid: str,
                    config_path: Path | str | None = None) -> tuple[bool, str]:
    """
    重命名策略 ID（新 ID 写入 + 旧 ID 删除，一次落盘）。
    显示名不变；改显示名直接改 body["name"] 再 upsert。
    """
    old_sid, new_sid = (old_sid or "").strip(), (new_sid or "").strip()
    if not new_sid:
        return False, "新 ID 不能为空"
    strategies = load_all_strategies(config_path)
    if old_sid not in strategies:
        return False, f"策略不存在: {old_sid}"
    if new_sid in strategies:
        return False, f"目标 ID 已存在: {new_sid}"
    strategies[new_sid] = strategies.pop(old_sid)
    ok = save_all_strategies(strategies, config_path)
    return ok, ("已重命名" if ok else "写入失败")


def duplicate_strategy_in_memory(strategies: dict[str, dict], sid: str) -> str:
    """
    在内存 dict 中复制策略，返回新 sid（页面暂存态用，不落盘）。
    """
    new_key = f"{sid}_copy"
    i = 1
    while new_key in strategies:
        new_key = f"{sid}_copy{i}"
        i += 1
    strategies[new_key] = copy.deepcopy(strategies[sid])
    strategies[new_key]["name"] = strategies[sid].get("name", "") + " (副本)"
    return new_key


# ============================================================================
# 筛选执行
# ============================================================================

@dataclass
class ScreeningRunResult:
    """一次筛选执行的完整结果"""
    df: pd.DataFrame
    elapsed_seconds: float
    strategy_ids: list[str] = field(default_factory=list)
    scope_labels: list[str] = field(default_factory=list)
    scope_size: int | None = None      # None = 全市场
    cleared_cache_files: int = 0
    warnings: list[str] = field(default_factory=list)


def scope_options() -> dict[str, str]:
    """板块/指数范围 {显示名: key}"""
    from src.analysis.screening.data_provider import ScreenerDataProvider
    return dict(ScreenerDataProvider.SCOPE_DEFINITIONS)


def run_screening(
    strategy_ids: list[str],
    *,
    scope_keys: list[str] | None = None,
    scope_labels: list[str] | None = None,
    max_workers: int = 8,
    request_delay: float = 0.0,
    clear_cache: bool = False,
    config_path: Path | str | None = None,
    use_processes: bool = False,
) -> ScreeningRunResult:
    """
    执行筛选（原 pages/3 内嵌的编排逻辑）。

    Args:
        strategy_ids: 要运行的策略 ID（多个取条件并集 = AND 组合）
        scope_keys: 板块/指数 key 列表；None 或含 "all" = 全市场
        scope_labels: 对应显示名（仅用于结果记录）
        clear_cache: 强制清 K 线缓存
        use_processes: Pass2 用进程池并行（CPU 密集条件多时提速；小候选集自动退回线程池）
    """
    from src.analysis.screening import ScreenerDataProvider, StockScreener

    warnings: list[str] = []
    provider = ScreenerDataProvider()

    cleared = 0
    if clear_cache:
        cleared = provider._cache.clear_all()

    stock_scope: set[str] | None = None
    if scope_keys and "all" not in scope_keys:
        stock_scope = provider.get_scope_codes(scope_keys)
        if not stock_scope:
            warnings.append("板块/指数成分股获取失败，已退化为全部 A 股")
            stock_scope = None

    screener = StockScreener(
        data_provider=provider,
        request_delay=float(request_delay),
        max_workers=int(max_workers),
        use_processes=bool(use_processes),
    )
    t0 = time.perf_counter()
    df = screener.run_from_config(
        str(config_path or PATH_SCREEN),
        strategy_ids=list(strategy_ids),
        stock_scope=stock_scope,
    )
    elapsed = time.perf_counter() - t0

    # 行情数据源退化时把原因带给用户（否则 0 命中会被误读成策略太严）
    if getattr(screener, "data_warning", ""):
        warnings.append(screener.data_warning)

    return ScreeningRunResult(
        df=df if df is not None else pd.DataFrame(),
        elapsed_seconds=elapsed,
        strategy_ids=list(strategy_ids),
        scope_labels=list(scope_labels or []),
        scope_size=len(stock_scope) if stock_scope is not None else None,
        cleared_cache_files=cleared,
        warnings=warnings,
    )


def dry_run_strategy(
    body: dict,
    *,
    sid: str = "(未保存)",
    max_workers: int = 6,
) -> pd.DataFrame:
    """
    试运行一个未保存的策略体（原 pages/2「⚡ 试运行」内嵌逻辑）。

    直接从条件 dict 构建条件对象，绕过 YAML 落盘。
    """
    from src.analysis.screening import ScreenerDataProvider, StockScreener

    screener = StockScreener(
        data_provider=ScreenerDataProvider(),
        max_workers=max_workers,
    )
    for cond in _build_conditions(body.get("conditions", []), sid=sid):
        screener.add_condition(cond)
    output = body.get("output") or {}
    return screener.run(
        sort_by=output.get("sort_by", "总市值(亿)"),
        limit=int(output.get("limit", 30)),
    )
