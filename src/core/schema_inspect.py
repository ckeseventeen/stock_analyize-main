"""
src/core/schema_inspect.py — 从 Python 类自动推导参数 schema

目的：消除 `STRATEGY_PARAM_SCHEMAS` / `_PARAM_MAP` 等手写 schema 与类定义的双写。

支持的源（按优先级）：
  1. 类属性 `_param_schema = [{...}, ...]`（显式手写，最高优先级）
  2. 类属性 `_param_aliases = {"yaml_key": "init_key"}`（YAML 别名映射）
  3. dataclass `__dataclass_fields__` 的 metadata
  4. backtrader 风格的 `params = (("name", default), ...)`
  5. `inspect.signature(__init__)`

输出统一为字段字典列表：
    [
      {"key": "fast_period",
       "type": "int",        # int / float / str / bool / list / dict / yaml
       "default": 10,
       "label": "短期均线 (Fast MA)",
       "min": 3, "max": 60, "step": 1,
       "help": "金叉快线周期"},
      ...
    ]
"""
from __future__ import annotations

import dataclasses
import inspect
from typing import Any, get_type_hints


def derive_schema(cls: type) -> list[dict]:
    """从类自动推导 schema 字段列表"""
    # 优先级 1：显式手写 _param_schema
    explicit = getattr(cls, "_param_schema", None)
    if explicit:
        return [dict(f) for f in explicit]

    # 优先级 2：dataclass field metadata
    if dataclasses.is_dataclass(cls):
        return _from_dataclass(cls)

    # 优先级 3：backtrader-style params tuple
    bt_params = getattr(cls, "params", None)
    if bt_params is not None:
        out = _from_backtrader_params(bt_params, cls)
        if out:
            return out

    # 优先级 4：__init__ 签名
    return _from_init_signature(cls)


# =============================================================================
# 各种来源的推导实现
# =============================================================================

def _from_dataclass(cls: type) -> list[dict]:
    """从 @dataclass 的 field.metadata 抽 schema"""
    schemas = []
    for f in dataclasses.fields(cls):
        meta = dict(f.metadata) if f.metadata else {}
        if meta.get("hidden"):
            continue
        entry = {
            "key": f.name,
            "type": _infer_type_from_annotation(f.type),
            "default": f.default if f.default is not dataclasses.MISSING else None,
            "label": meta.get("label", f.name),
        }
        for opt in ("min", "max", "step", "help", "options", "format"):
            if opt in meta:
                entry[opt] = meta[opt]
        schemas.append(entry)
    return schemas


def _from_backtrader_params(params: Any, cls: type) -> list[dict]:
    """
    从 Backtrader 风格的 `params = (("fast_period", 10), ...)` 抽 schema。

    Backtrader 可能用 tuple-of-tuples 或 AutoInfoClass，统一处理。
    """
    schemas = []
    items: list[tuple] = []

    # 形式 1：tuple of tuples
    if isinstance(params, tuple):
        items = list(params)
    elif hasattr(params, "_getpairs"):
        # AutoInfoClass
        items = list(params._getpairs())
    elif hasattr(params, "__iter__"):
        try:
            items = list(params)
        except TypeError:
            return []

    for item in items:
        if not isinstance(item, tuple) or len(item) < 2:
            continue
        name, default = item[0], item[1]
        schemas.append({
            "key": str(name),
            "type": _infer_type_from_value(default),
            "default": default,
            "label": str(name),
        })
    return schemas


def _from_init_signature(cls: type) -> list[dict]:
    """从 __init__ 签名兜底（剔除 self）"""
    try:
        sig = inspect.signature(cls.__init__)
        hints = get_type_hints(cls.__init__)
    except (ValueError, TypeError):
        return []

    schemas = []
    for name, param in sig.parameters.items():
        if name in ("self", "args", "kwargs"):
            continue
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            continue
        default = param.default if param.default is not param.empty else None
        type_hint = hints.get(name, type(default) if default is not None else str)
        schemas.append({
            "key": name,
            "type": _infer_type_from_annotation(type_hint) or _infer_type_from_value(default),
            "default": default,
            "label": name,
        })
    return schemas


# =============================================================================
# 类型推导
# =============================================================================

_PYTYPE_TO_SCHEMA = {
    int: "int", float: "float", str: "str", bool: "bool",
    list: "list", dict: "dict", tuple: "list",
}


def _infer_type_from_annotation(ann) -> str:
    """从类型注解推断 schema type"""
    if ann is None:
        return "str"
    if isinstance(ann, str):
        # PEP 563 延迟注解（未 evaluated 字符串），按名称匹配
        return _PYTYPE_TO_SCHEMA.get(_str_to_type(ann), "str")
    if ann in _PYTYPE_TO_SCHEMA:
        return _PYTYPE_TO_SCHEMA[ann]
    # Optional[X] / Union[X, None] / list[X] 之类
    origin = getattr(ann, "__origin__", None)
    if origin is not None and origin in _PYTYPE_TO_SCHEMA:
        return _PYTYPE_TO_SCHEMA[origin]
    return "str"


def _infer_type_from_value(value) -> str:
    """从默认值推断 type"""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, (list, tuple)):
        return "list"
    if isinstance(value, dict):
        return "dict"
    return "str"


def _str_to_type(ann_str: str) -> type:
    """把字符串形式的类型注解粗略转 type"""
    base = ann_str.split("[")[0].strip().lower()
    return {
        "int": int, "float": float, "str": str, "bool": bool,
        "list": list, "dict": dict, "tuple": tuple,
    }.get(base, str)


# =============================================================================
# 反向：dict 实例化（YAML 配置 → 类实例）
# =============================================================================

def instantiate_from_dict(cls: type, params: dict) -> Any:
    """
    把 yaml dict 喂给类。处理参数名别名（_param_aliases）。

    比如 yaml 用 "min"，类 init 用 "min_pe"：
        class PERangeCondition:
            _param_aliases = {"min": "min_pe", "max": "max_pe"}
            def __init__(self, min_pe=0, max_pe=...): ...

    instantiate_from_dict(PERangeCondition, {"min": 10, "max": 30})
    → PERangeCondition(min_pe=10, max_pe=30)
    """
    aliases = getattr(cls, "_param_aliases", {}) or {}
    kwargs = {}
    for k, v in params.items():
        actual_key = aliases.get(k, k)
        kwargs[actual_key] = v
    # 过滤未知参数（容错）
    if hasattr(cls, "__init__"):
        try:
            sig = inspect.signature(cls.__init__)
            valid = {p for p in sig.parameters if p != "self"}
            # 如果存在 **kwargs 就全部允许
            has_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD
                             for p in sig.parameters.values())
            if not has_kwargs:
                kwargs = {k: v for k, v in kwargs.items() if k in valid}
        except (ValueError, TypeError):
            pass
    return cls(**kwargs)
