"""
src/analysis/factor/ic_analysis.py — 因子检验框架（IC / 分层回测 / 衰减）

面板约定：DataFrame，index = 交易日（datetime），columns = 股票代码。

典型用法::
    from src.analysis.factor.ic_analysis import factor_diagnostics

    # ohlcv_by_code: {code: 日线 OHLCV DataFrame}
    report = factor_diagnostics(ohlcv_by_code, expr="mean(close,20)/(close+1e-12)")
    report["ic"]          # {ic_mean, ic_std, icir, ic_win_rate, n_periods}
    report["layers"]      # 分层平均前瞻收益（L1 最低分位 → L5 最高分位）
    report["decay"]       # 各持有期的 IC 均值（因子衰减曲线）
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.analysis.factor.expression import evaluate_expression
from src.core.columns import normalize_ohlcv_columns
from src.utils.logger import get_logger

logger = get_logger("factor_ic")

# 单个截面上参与计算的最少有效样本数（低于此数的日期跳过）
MIN_CROSS_SECTION = 3


def _date_indexed_close(df: pd.DataFrame) -> pd.Series:
    """OHLCV → 以交易日为索引的收盘价序列"""
    df = normalize_ohlcv_columns(df)
    if "close" not in df.columns:
        return pd.Series(dtype=float)
    if "date" in df.columns:
        idx = pd.to_datetime(df["date"], errors="coerce")
    else:
        idx = pd.to_datetime(df.index, errors="coerce")
    s = pd.to_numeric(df["close"], errors="coerce")
    s.index = idx
    return s[~s.index.isna()]


def build_factor_panel(ohlcv_by_code: dict[str, pd.DataFrame],
                       expr: str) -> pd.DataFrame:
    """
    逐只股票求表达式因子时序，拼成 日期 × 代码 的因子面板。

    单只失败（数据缺列/太短）跳过并 warning，不影响整体。
    """
    columns: dict[str, pd.Series] = {}
    for code, df in ohlcv_by_code.items():
        if df is None or df.empty:
            continue
        try:
            values = evaluate_expression(df, expr)
            ndf = normalize_ohlcv_columns(df)
            if "date" in ndf.columns:
                values.index = pd.to_datetime(ndf["date"], errors="coerce")
            else:
                values.index = pd.to_datetime(ndf.index, errors="coerce")
            values = values[~values.index.isna()]
            columns[str(code)] = values[~values.index.duplicated(keep="last")]
        except Exception as e:
            logger.warning(f"{code} 因子求值失败，跳过: {e}")
    if not columns:
        return pd.DataFrame()
    return pd.DataFrame(columns).sort_index()


def build_close_panel(ohlcv_by_code: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """日期 × 代码 的收盘价面板"""
    columns = {}
    for code, df in ohlcv_by_code.items():
        if df is None or df.empty:
            continue
        s = _date_indexed_close(df)
        if not s.empty:
            columns[str(code)] = s[~s.index.duplicated(keep="last")]
    if not columns:
        return pd.DataFrame()
    return pd.DataFrame(columns).sort_index()


def forward_returns(close_panel: pd.DataFrame, horizon: int = 20) -> pd.DataFrame:
    """t 日买入、持有 horizon 个交易日的前瞻收益率面板"""
    return close_panel.shift(-int(horizon)) / close_panel - 1.0


def ic_series(factor_panel: pd.DataFrame, fwd_ret_panel: pd.DataFrame,
              method: str = "spearman") -> pd.Series:
    """
    逐日截面 IC：当日因子值与前瞻收益的截面相关系数（默认 RankIC）。

    有效样本 < MIN_CROSS_SECTION 的日期跳过。
    """
    factor_panel, fwd_ret_panel = factor_panel.align(fwd_ret_panel, join="inner")
    out = {}
    for dt in factor_panel.index:
        f = factor_panel.loc[dt]
        r = fwd_ret_panel.loc[dt]
        mask = f.notna() & r.notna()
        if int(mask.sum()) < MIN_CROSS_SECTION:
            continue
        ic = f[mask].corr(r[mask], method=method)
        if pd.notna(ic):
            out[dt] = float(ic)
    return pd.Series(out, dtype=float).sort_index()


def ic_summary(ic: pd.Series) -> dict:
    """IC 序列 → 汇总指标（IC均值/标准差/ICIR/胜率/t值/期数）"""
    if ic is None or ic.empty:
        return {"ic_mean": np.nan, "ic_std": np.nan, "icir": np.nan,
                "ic_win_rate": np.nan, "t_stat": np.nan, "n_periods": 0}
    mean = float(ic.mean())
    std = float(ic.std())
    n = int(len(ic))
    icir = mean / std if std > 0 else np.nan
    return {
        "ic_mean": round(mean, 4),
        "ic_std": round(std, 4),
        "icir": round(icir, 4) if pd.notna(icir) else np.nan,
        "ic_win_rate": round(float((ic > 0).mean()), 4),
        "t_stat": round(icir * np.sqrt(n), 4) if pd.notna(icir) else np.nan,
        "n_periods": n,
    }


def layered_returns(factor_panel: pd.DataFrame, fwd_ret_panel: pd.DataFrame,
                    n_layers: int = 5) -> pd.Series:
    """
    分层回测：逐日按因子值分成 n_layers 个分位组，取组内前瞻收益均值，
    再对全部日期求均值。

    Returns:
        pd.Series，索引 L1（因子最低）→ L{n}（因子最高），值为平均前瞻收益；
        另附 "L{n}-L1" 多空价差
    """
    factor_panel, fwd_ret_panel = factor_panel.align(fwd_ret_panel, join="inner")
    labels = [f"L{i + 1}" for i in range(n_layers)]
    rows = []
    for dt in factor_panel.index:
        f = factor_panel.loc[dt]
        r = fwd_ret_panel.loc[dt]
        mask = f.notna() & r.notna()
        if int(mask.sum()) < n_layers:
            continue
        try:
            buckets = pd.qcut(f[mask].rank(method="first"), n_layers, labels=labels)
        except ValueError:
            continue
        rows.append(r[mask].groupby(buckets, observed=False).mean())
    if not rows:
        return pd.Series(dtype=float)
    result = pd.DataFrame(rows).mean()
    result[f"L{n_layers}-L1"] = result[labels[-1]] - result[labels[0]]
    return result.round(6)


def ic_decay(factor_panel: pd.DataFrame, close_panel: pd.DataFrame,
             horizons: tuple[int, ...] = (1, 5, 10, 20, 60),
             method: str = "spearman") -> dict[int, float]:
    """因子衰减：各持有期的 IC 均值曲线"""
    out = {}
    for h in horizons:
        ic = ic_series(factor_panel, forward_returns(close_panel, h), method=method)
        out[int(h)] = round(float(ic.mean()), 4) if not ic.empty else np.nan
    return out


def factor_diagnostics(ohlcv_by_code: dict[str, pd.DataFrame], expr: str,
                       horizon: int = 20, n_layers: int = 5,
                       decay_horizons: tuple[int, ...] = (1, 5, 10, 20, 60)) -> dict:
    """
    一站式因子体检：IC 汇总 + 分层收益 + 衰减曲线。

    Args:
        ohlcv_by_code: {code: 日线 OHLCV DataFrame}
        expr: 因子表达式
        horizon: IC/分层用的前瞻持有期（交易日）

    Returns:
        {"ic": dict, "ic_series": pd.Series, "layers": pd.Series, "decay": dict}
    """
    factor_panel = build_factor_panel(ohlcv_by_code, expr)
    close_panel = build_close_panel(ohlcv_by_code)
    if factor_panel.empty or close_panel.empty:
        return {"ic": ic_summary(pd.Series(dtype=float)),
                "ic_series": pd.Series(dtype=float),
                "layers": pd.Series(dtype=float), "decay": {}}

    fwd = forward_returns(close_panel, horizon)
    ic = ic_series(factor_panel, fwd)
    return {
        "ic": ic_summary(ic),
        "ic_series": ic,
        "layers": layered_returns(factor_panel, fwd, n_layers=n_layers),
        "decay": ic_decay(factor_panel, close_panel, horizons=decay_horizons),
    }
