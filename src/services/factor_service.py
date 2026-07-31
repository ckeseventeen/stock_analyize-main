"""
src/services/factor_service.py — 因子相关的服务层

存在意义：API 曾直接 import `src.analysis.factor.expression`。因子清单、
表达式校验、因子体检这些是可复用的业务动作，应由服务层统一暴露给
API / CLI / 调度器，而不是散在路由函数里。
"""
from __future__ import annotations

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("factor_service")


def list_alpha158() -> list[dict]:
    """Alpha 158 表达式因子清单（名称/表达式/说明/方向）"""
    from src.analysis.factor.expression import load_alpha158_config

    return load_alpha158_config()


def validate_expression(expr: str) -> tuple[bool, str]:
    """
    校验因子表达式是否合法（AST 白名单）。

    Returns:
        (是否合法, 错误信息)
    """
    from src.analysis.factor.expression import ExpressionError
    from src.analysis.factor.expression import (
        validate_expression as _validate,
    )

    try:
        _validate(expr)
        return True, ""
    except ExpressionError as e:
        return False, str(e)


def evaluate_on(df: pd.DataFrame, expr: str) -> pd.Series:
    """在给定 OHLCV 上求值一个表达式因子（前端"表达式预览"用）"""
    from src.analysis.factor.expression import evaluate_expression

    return evaluate_expression(df, expr)


def diagnose(ohlcv_by_code: dict[str, pd.DataFrame], expr: str,
             *, horizon: int = 20, n_layers: int = 5) -> dict:
    """
    因子体检：IC/ICIR/胜率 + 分层收益 + 衰减曲线。

    Returns:
        {"ic": {...}, "layers": {...}, "decay": {...}}（已剔除不可 JSON 化的
        ic_series，需要原始序列请直接调 analysis 层）
    """
    from src.analysis.factor.ic_analysis import factor_diagnostics

    report = factor_diagnostics(ohlcv_by_code, expr, horizon=horizon,
                                n_layers=n_layers)
    layers = report.get("layers")
    return {
        "ic": report.get("ic", {}),
        "layers": (layers.to_dict() if hasattr(layers, "to_dict") else {}),
        "decay": report.get("decay", {}),
    }
