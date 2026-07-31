"""
src/strategy/backtest/vector_engine.py — 向量化回测引擎

绕过 Backtrader 的逐 bar 事件循环，用 NumPy/pandas 列运算一次算完整条净值曲线。
适用场景：信号可以预先批量算出的策略（均线交叉/因子阈值/ML 分数等），
参数寻优、批量粗筛时比 Backtrader 快 1-2 个数量级。

交易模型：
  - 信号在 bar t 收盘后产生，bar t+1 成交（避免未来函数）
  - execution="next_close"（默认）：t+1 **收盘价**成交
    execution="next_open"：t+1 **开盘价**成交——与 BacktestRunner(Backtrader)
    口径一致；参数寻优结果要迁移到 Backtrader 精细回测复验时建议用这一档
  - 全仓进出（position ∈ {0, 1}），成本按成交额比例计
  - 买入成本 = 佣金 + 滑点；卖出成本 = 佣金 + 印花税 + 滑点（A 股口径）

局限（精细模拟请用 BacktestRunner）：
  - 不支持分批建仓/加减仓、涨跌停无法成交、资金不足撮合
  - 成本按权益比例近似（无最低佣金 5 元制度）
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from src.core.columns import normalize_ohlcv_columns
from src.utils.logger import get_logger

logger = get_logger("vector_engine")

TRADING_DAYS_PER_YEAR = 252


# ============================================================================
# 信号生成器（示例：均线交叉；自定义策略可直接传 signals 数组）
# ============================================================================

def ma_crossover_signals(close: pd.Series, fast: int = 5, slow: int = 20) -> np.ndarray:
    """
    均线金叉/死叉信号：金叉 → 1（买入），死叉 → -1（卖出），其余 0。
    """
    fast_ma = close.rolling(fast).mean()
    slow_ma = close.rolling(slow).mean()
    above = (fast_ma > slow_ma).astype(int)
    cross = above.diff().fillna(0)
    signals = np.zeros(len(close), dtype=np.int8)
    signals[cross > 0] = 1
    signals[cross < 0] = -1
    return signals


# ============================================================================
# 核心引擎
# ============================================================================

@dataclass
class VectorBacktestResult:
    """向量化回测结果"""
    equity: pd.Series                 # 每 bar 组合净值（元）
    positions: pd.Series              # 每 bar 实际持仓（0/1，已含 T+1 生效延迟）
    trades: pd.DataFrame              # 每笔平仓交易明细
    report: dict = field(default_factory=dict)   # 指标（键名与 BacktestRunner 对齐）


def run_vector_backtest(
    df: pd.DataFrame,
    signals: np.ndarray | pd.Series | list,
    *,
    initial_cash: float = 100_000.0,
    commission: float = 0.00025,
    stamp_tax: float = 0.001,
    slippage: float = 0.0,
    execution: str = "next_close",
) -> VectorBacktestResult:
    """
    向量化回测。

    Args:
        df: OHLCV DataFrame（中/英文列名均可，需含收盘价；日期列可选）
        signals: 与 df 等长的信号数组，1=买入 / -1=卖出 / 0=持有不变
        commission: 双边佣金率
        stamp_tax: 卖出印花税率
        slippage: 单边滑点率
        execution: "next_close"（次 bar 收盘成交，默认）|
                   "next_open"（次 bar 开盘成交，与 Backtrader 口径一致，需开盘价列）

    Returns:
        VectorBacktestResult；数据不足（<2 bar）时 equity 为空、report 为 {}
    """
    if execution not in ("next_close", "next_open"):
        raise ValueError(f"未知成交模式: {execution}，支持 next_close / next_open")
    ndf = normalize_ohlcv_columns(df)
    if "close" not in ndf.columns or len(ndf) < 2:
        return VectorBacktestResult(
            equity=pd.Series(dtype=float), positions=pd.Series(dtype=float),
            trades=pd.DataFrame(), report={},
        )
    close = pd.to_numeric(ndf["close"], errors="coerce").reset_index(drop=True)
    n = len(close)
    sig = np.asarray(signals, dtype=float)
    if len(sig) != n:
        raise ValueError(f"signals 长度 {len(sig)} 与数据长度 {n} 不一致")
    if execution == "next_open":
        if "open" not in ndf.columns:
            raise ValueError("execution='next_open' 需要开盘价列（open/开盘）")
        open_ = pd.to_numeric(ndf["open"], errors="coerce").reset_index(drop=True)
    else:
        open_ = None

    if "date" in ndf.columns:
        dates = pd.to_datetime(ndf["date"], errors="coerce").reset_index(drop=True)
    else:
        dates = pd.Series(pd.to_datetime(ndf.index, errors="coerce"))

    # ── 期望仓位：信号 1 → 1，-1 → 0，0 → 保持前值 ──
    desired = pd.Series(np.where(sig > 0, 1.0, np.where(sig < 0, 0.0, np.nan)))
    desired = desired.ffill().fillna(0.0)
    # 实际仓位：次 bar 成交生效（T+1）
    exec_pos = desired.shift(1).fillna(0.0)

    # ── 收益与成本（全部列运算）──
    ret = close.pct_change().fillna(0.0)
    pos_prev = exec_pos.shift(1).fillna(0.0)
    if execution == "next_open":
        # 开盘成交：建仓 bar 赚 开→收，持仓 bar 赚 收→收，平仓 bar 赚 前收→开
        entry_bar = (exec_pos > 0) & (pos_prev == 0)
        exit_bar = (exec_pos == 0) & (pos_prev > 0)
        holding = (exec_pos > 0) & (pos_prev > 0)
        earn = pd.Series(0.0, index=close.index)
        earn[holding] = ret[holding]
        earn[entry_bar] = close[entry_bar] / open_[entry_bar] - 1.0
        prev_close = close.shift(1)
        earn[exit_bar] = open_[exit_bar] / prev_close[exit_bar] - 1.0
        earn = earn.fillna(0.0)
    else:
        # 收盘成交：当 bar 收益按上一 bar 收盘时已持有的仓位计
        earn = pos_prev * ret

    turnover = exec_pos.diff().fillna(exec_pos.iloc[0])
    entry = turnover.clip(lower=0.0)     # 0→1 建仓
    exit_ = (-turnover).clip(lower=0.0)  # 1→0 平仓
    buy_cost = commission + slippage
    sell_cost = commission + stamp_tax + slippage
    net = earn - entry * buy_cost - exit_ * sell_cost

    equity = initial_cash * (1.0 + net).cumprod()
    equity.index = dates
    positions = exec_pos.copy()
    positions.index = dates

    exec_price = open_ if execution == "next_open" else close
    trades = _extract_trades(exec_pos, exec_price, close, dates,
                             buy_cost, sell_cost)
    report = _build_report(equity, net, trades, initial_cash, n)
    return VectorBacktestResult(equity=equity, positions=positions,
                                trades=trades, report=report)


def _extract_trades(exec_pos: pd.Series, exec_price: pd.Series,
                    close: pd.Series, dates: pd.Series,
                    buy_cost: float, sell_cost: float) -> pd.DataFrame:
    """从仓位序列还原逐笔交易（成交价 = 成交 bar 的成交价序列取值）"""
    turnover = exec_pos.diff().fillna(exec_pos.iloc[0])
    entry_idx = np.flatnonzero(turnover.values > 0)
    exit_idx = np.flatnonzero(turnover.values < 0)

    rows = []
    for ei in entry_idx:
        matching_exits = exit_idx[exit_idx > ei]
        xi = int(matching_exits[0]) if len(matching_exits) else None
        entry_price = float(exec_price.iloc[ei])
        if xi is not None:
            exit_price = float(exec_price.iloc[xi])
            closed = True
        else:
            exit_price = float(close.iloc[-1])   # 期末未平仓按最后收盘估值
            xi = len(close) - 1
            closed = False
        pnl_pct = exit_price * (1 - sell_cost) / (entry_price * (1 + buy_cost)) - 1
        rows.append({
            "entry_date": dates.iloc[ei], "exit_date": dates.iloc[xi],
            "entry_price": round(entry_price, 4), "exit_price": round(exit_price, 4),
            "return_pct": round(pnl_pct * 100, 4),
            "bars_held": int(xi - ei),
            "closed": closed,
        })
    return pd.DataFrame(rows)


def _build_report(equity: pd.Series, net: pd.Series, trades: pd.DataFrame,
                  initial_cash: float, n_bars: int) -> dict:
    """指标汇总（键名与 BacktestRunner 报告对齐，便于复用对比表/前端）"""
    final = float(equity.iloc[-1])
    total_return = final / initial_cash - 1.0

    years = n_bars / TRADING_DAYS_PER_YEAR
    annual = (1.0 + total_return) ** (1.0 / years) - 1.0 if years > 0 else 0.0

    peak = equity.cummax()
    max_dd = float((1.0 - equity / peak).max())

    std = float(net.std())
    sharpe = float(net.mean()) / std * np.sqrt(TRADING_DAYS_PER_YEAR) if std > 0 else 0.0

    closed = trades[trades["closed"]] if not trades.empty else trades
    n_closed = int(len(closed))
    win_rate = float((closed["return_pct"] > 0).mean()) * 100 if n_closed else 0.0

    return {
        "初始资金": round(initial_cash, 2),
        "最终资金": round(final, 2),
        "总收益率(%)": round(total_return * 100, 2),
        "年化收益率(%)": round(annual * 100, 2),
        "最大回撤(%)": round(max_dd * 100, 2),
        "夏普比率": round(sharpe, 3),
        "交易次数": n_closed,
        "胜率(%)": round(win_rate, 2),
        "引擎": "vector",
    }


# ============================================================================
# 批量回测：参数寻优 / 多标的粗筛的入口
# ============================================================================

def batch_vector_backtest(
    ohlcv_by_code: dict[str, pd.DataFrame],
    signal_func,
    *,
    initial_cash: float = 100_000.0,
    commission: float = 0.00025,
    **signal_kwargs,
) -> pd.DataFrame:
    """
    对一批标的跑同一信号函数的向量化回测，返回按总收益率排序的汇总表。

    Args:
        ohlcv_by_code: {code: OHLCV DataFrame}
        signal_func: callable(close: pd.Series, **signal_kwargs) -> signals 数组

    Returns:
        DataFrame（index=code，列=report 指标），空输入返回空表
    """
    rows = {}
    for code, df in ohlcv_by_code.items():
        try:
            ndf = normalize_ohlcv_columns(df) if df is not None else None
            if ndf is None or "close" not in ndf.columns or len(ndf) < 2:
                continue
            close = pd.to_numeric(ndf["close"], errors="coerce")
            result = run_vector_backtest(
                df, signal_func(close.reset_index(drop=True), **signal_kwargs),
                initial_cash=initial_cash, commission=commission,
            )
            if result.report:
                rows[str(code)] = result.report
        except Exception as e:
            logger.warning(f"{code} 向量化回测失败，跳过: {e}")
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame.from_dict(rows, orient="index")
    return out.sort_values("总收益率(%)", ascending=False)
