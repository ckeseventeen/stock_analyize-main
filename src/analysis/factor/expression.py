"""
src/analysis/factor/expression.py — 表达式因子引擎（Alpha 158 风格）

对齐 QLib/VeighNa 的表达式因子写法：
    KMID:  (close - open) / (open + 1e-12)
    ROC20: delay(close, 20) / close
    RSV10: (close - ts_min(low, 10)) / (ts_max(high, 10) - ts_min(low, 10) + 1e-12)

安全性：表达式先经 AST 白名单校验（只允许算术/比较运算、白名单函数与字段名、
数值常量），再在无 builtins 的受限命名空间中求值。

字段说明（求值前统一从中文列归一化）：
    open/high/low/close/volume         标准 OHLCV
    amount                             成交额（缺失时为 NaN 序列）
    vwap                               典型价 (high+low+close)/3 —— 成交量单位
                                       跨市场不一致（A股为手），故用典型价代理
"""
from __future__ import annotations

import ast
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from src.analysis.factor.base import BaseFactor
from src.core.columns import normalize_ohlcv_columns
from src.utils.logger import get_logger

logger = get_logger("factor_expression")


# ============================================================================
# 时序算子（全部作用于 pd.Series，返回 pd.Series）
# ============================================================================

def _delay(s: pd.Series, n: int) -> pd.Series:
    """n 期前的值（QLib Ref）"""
    return s.shift(int(n))


def _mean(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n)).mean()


def _std(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n)).std()


def _sum(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n)).sum()


def _ts_max(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n)).max()


def _ts_min(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n)).min()


def _ts_rank(s: pd.Series, n: int) -> pd.Series:
    """窗口内最新值的分位排名（0~1）"""
    n = int(n)

    def _rank(w: np.ndarray) -> float:
        if len(w) <= 1:
            return np.nan
        return float((w[-1] > w[:-1]).sum()) / (len(w) - 1)

    return s.rolling(n).apply(_rank, raw=True)


def _slope(s: pd.Series, n: int) -> pd.Series:
    """窗口内对时间的线性回归斜率（QLib Slope）"""
    n = int(n)
    x = np.arange(n, dtype=float)
    x_mean = x.mean()
    denom = float(((x - x_mean) ** 2).sum())

    def _fit(w: np.ndarray) -> float:
        return float(((x - x_mean) * (w - w.mean())).sum() / denom)

    return s.rolling(n).apply(_fit, raw=True)


def _corr(a: pd.Series, b: pd.Series, n: int) -> pd.Series:
    return a.rolling(int(n)).corr(b)


def _quantile(s: pd.Series, n: int, q: float) -> pd.Series:
    return s.rolling(int(n)).quantile(float(q))


def _abs(s):
    return np.abs(s)


def _log(s):
    return np.log(s)


def _sign(s):
    return np.sign(s)


def _maximum(a, b):
    return np.maximum(a, b)


def _minimum(a, b):
    return np.minimum(a, b)


_ALLOWED_FUNCS: dict = {
    "delay": _delay,
    "mean": _mean,
    "std": _std,
    "sum": _sum,
    "ts_max": _ts_max,
    "ts_min": _ts_min,
    "ts_rank": _ts_rank,
    "slope": _slope,
    "corr": _corr,
    "quantile": _quantile,
    "abs": _abs,
    "log": _log,
    "sign": _sign,
    "maximum": _maximum,
    "minimum": _minimum,
}

_FIELDS = ("open", "high", "low", "close", "volume", "amount", "vwap")

# AST 白名单：算术 + 比较 + 函数调用 + 字段/常量
_ALLOWED_NODES = (
    ast.Expression, ast.BinOp, ast.UnaryOp, ast.Compare, ast.Call,
    ast.Name, ast.Constant, ast.Load,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow, ast.Mod,
    ast.USub, ast.UAdd,
    ast.Gt, ast.GtE, ast.Lt, ast.LtE, ast.Eq, ast.NotEq,
)


class ExpressionError(ValueError):
    """表达式非法（语法错误 / 使用了白名单以外的名字或节点）"""


def validate_expression(expr: str) -> ast.Expression:
    """
    校验表达式安全性，返回可复用的 AST。

    Raises:
        ExpressionError: 语法错误 / 非白名单节点 / 未知名字
    """
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise ExpressionError(f"表达式语法错误: {e}") from e

    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise ExpressionError(
                f"表达式含不允许的语法节点 {type(node).__name__}: {expr!r}"
            )
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in _ALLOWED_FUNCS:
                fname = getattr(getattr(node, "func", None), "id", "?")
                raise ExpressionError(f"未知函数 {fname!r}: {expr!r}")
            if node.keywords:
                raise ExpressionError(f"函数调用不支持关键字参数: {expr!r}")
        if isinstance(node, ast.Name):
            if node.id not in _ALLOWED_FUNCS and node.id not in _FIELDS:
                raise ExpressionError(f"未知名字 {node.id!r}: {expr!r}")
        if isinstance(node, ast.Constant) and not isinstance(node.value, (int, float)):
            raise ExpressionError(f"仅允许数值常量: {expr!r}")
    return tree


def _build_fields(df: pd.DataFrame) -> dict[str, pd.Series]:
    """OHLCV DataFrame（中/英文列均可）→ 表达式字段命名空间"""
    df = normalize_ohlcv_columns(df)
    n = len(df)
    idx = df.index

    def col(name: str) -> pd.Series:
        if name in df.columns:
            return pd.to_numeric(df[name], errors="coerce").reset_index(drop=True)
        return pd.Series(np.nan, index=range(n))

    fields = {name: col(name) for name in ("open", "high", "low", "close",
                                           "volume", "amount")}
    # vwap 用典型价代理（跨市场成交量单位不一致，amount/volume 不可靠）
    fields["vwap"] = (fields["high"] + fields["low"] + fields["close"]) / 3.0
    fields["_index"] = idx
    return fields


@lru_cache(maxsize=512)
def _compiled_expression(expr: str):
    """校验 + 编译表达式并缓存（批量因子计算时同一表达式会被求值数千次）"""
    tree = validate_expression(expr)
    return compile(tree, "<factor-expr>", "eval")


def evaluate_expression(df: pd.DataFrame, expr: str) -> pd.Series:
    """
    对单只股票的 OHLCV 计算表达式因子的完整时序。

    Args:
        df: OHLCV DataFrame（中文或英文列名）
        expr: 表达式（见模块 docstring）

    Returns:
        与 df 等长的 pd.Series（索引对齐 df.index）；比较运算结果转 float
    """
    code = _compiled_expression(expr)
    fields = _build_fields(df)
    idx = fields.pop("_index")
    namespace = {**_ALLOWED_FUNCS, **fields, "__builtins__": {}}
    result = eval(code, namespace)  # noqa: S307

    if isinstance(result, (int, float)):
        result = pd.Series(float(result), index=range(len(idx)))
    if result.dtype == bool:
        result = result.astype(float)
    result.index = idx
    return result


# ============================================================================
# ExpressionFactor：接入现有 FactorEngine（type: expression）
# ============================================================================

class ExpressionFactor(BaseFactor):
    """
    表达式因子：daily_df 上求值，取最新一期值作为因子值。

    factors.yaml 用法::
        - type: expression
          enabled: true
          params:
            name: KMID
            expr: "(close - open) / (open + 1e-12)"
    """

    requires_ohlcv = True
    ohlcv_period = "daily"

    def __init__(self, expr: str, name: str = "", description: str = "",
                 higher_is_better: bool = True, min_bars: int = 5):
        validate_expression(expr)  # 尽早失败：配置写错时在构造期报出
        self.expr = expr
        self.name = name or expr
        self.description = description or f"表达式因子: {expr}"
        self.higher_is_better = bool(higher_is_better)
        self.min_bars = int(min_bars)

    def validate(self, data: dict) -> bool:
        df = data.get("daily_df")
        return df is not None and not df.empty and len(df) >= self.min_bars

    def calculate(self, data: dict) -> float:
        series = evaluate_expression(data["daily_df"], self.expr).dropna()
        if series.empty:
            return float("nan")
        return float(series.iloc[-1])


# ============================================================================
# Alpha 158 因子集加载（config/factors_alpha158.yaml）
# ============================================================================

def _default_alpha158_path() -> Path:
    return Path(__file__).resolve().parents[3] / "config" / "factors_alpha158.yaml"


def load_alpha158_config(config_path: Path | str | None = None) -> list[dict]:
    """读取 Alpha 158 因子配置，返回 [{name, expr, description, ...}]（enabled 过滤后）"""
    path = Path(config_path) if config_path else _default_alpha158_path()
    if not path.exists():
        logger.warning(f"factors_alpha158.yaml 不存在 ({path})")
        return []
    try:
        with open(path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f"读取 factors_alpha158.yaml 失败: {e}")
        return []
    entries = cfg.get("alpha_factors") or []
    return [e for e in entries
            if isinstance(e, dict) and e.get("name") and e.get("expr")
            and e.get("enabled", True)]


def build_alpha158_factors(config_path: Path | str | None = None) -> list[ExpressionFactor]:
    """把 Alpha 158 配置实例化为 ExpressionFactor 列表（单条失败跳过 + warning）"""
    factors: list[ExpressionFactor] = []
    for entry in load_alpha158_config(config_path):
        try:
            factors.append(ExpressionFactor(
                expr=entry["expr"],
                name=entry["name"],
                description=entry.get("description", ""),
                higher_is_better=entry.get("higher_is_better", True),
                min_bars=entry.get("min_bars", 5),
            ))
        except ExpressionError as e:
            logger.warning(f"Alpha158 因子 {entry.get('name')} 表达式非法，跳过: {e}")
    return factors
