"""
src/services/backtest_service.py — 回测的无头服务层

原 pages/4_策略回测.py 内嵌的编排逻辑下沉至此：
  - run_compare(): 多策略对比（取数 → 列名归一化 → run_all_strategies）
  - run_single(): 单策略回测（按周期取数 → BacktestRunner）
  - build_chart_frames(): 回测可视化的指标计算（MACD/金叉死叉/背离）
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("backtest_service")


# ============================================================================
# 数据获取
# ============================================================================

# 模块级 provider 单例：磁盘缓存共享，无需每次请求重建（构造含 mkdir 等 I/O）
_PROVIDER = None


def _get_provider():
    global _PROVIDER
    if _PROVIDER is None:
        from src.analysis.screening.data_provider import ScreenerDataProvider
        _PROVIDER = ScreenerDataProvider()
    return _PROVIDER


def fetch_ohlcv(code: str, market: str, days_back: int,
                frequency: str = "d") -> pd.DataFrame:
    """
    按周期拉 OHLCV。frequency ∈ {d, w, m, y} 或分钟周期 {1m, 5m, 15m, 30m, 60m}。

    分钟周期时 days_back 仍指日历天数（东财 1m 只保留最近 ~5 个交易日）。

    Returns:
        原始 DataFrame（可能为空，调用方自行判断）
    """
    from src.analysis.screening.data_provider import ScreenerDataProvider

    provider = _get_provider()
    if frequency in ScreenerDataProvider.MINUTE_FREQ_MAP:
        df = provider.get_minute_ohlcv(
            code.strip(), freq=frequency, days_back=int(days_back), market=market,
        )
        return df if df is not None else pd.DataFrame()

    fetchers = {
        "d": provider.get_daily_ohlcv,
        "w": provider.get_weekly_ohlcv,
        "m": provider.get_monthly_ohlcv,
        "y": provider.get_yearly_ohlcv,
    }
    fetch = fetchers.get(frequency, provider.get_daily_ohlcv)
    df = fetch(code.strip(), days_back=int(days_back), market=market)
    return df if df is not None else pd.DataFrame()


def _normalize_for_charts(df: pd.DataFrame) -> pd.DataFrame:
    """列名归一化 + date 设为索引（K 线图/基准曲线/Backtrader 共用一份 df）"""
    from src.core.columns import normalize_ohlcv_columns

    df = normalize_ohlcv_columns(df)
    if "date" in df.columns and df.index.name != "date":
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).set_index("date")
    return df


# ============================================================================
# 多策略对比
# ============================================================================

def run_compare(
    code: str,
    market: str,
    days_back: int,
    *,
    initial_cash: float = 100_000.0,
    commission: float = 0.00025,
    strategy_keys: list[str] | None = None,
    custom_params: dict[str, dict] | None = None,
    progress_cb: Callable | None = None,
):
    """
    多策略对比：取数 → 归一化 → run_all_strategies。

    Returns:
        (CompareResult, 归一化后的 df)；数据为空时 df.empty 为 True，CompareResult 为 None

    Raises:
        取数/运行中的异常原样抛出，由页面统一展示
    """
    from src.strategy.backtest import run_all_strategies

    raw = fetch_ohlcv(code, market, days_back, frequency="d")
    if raw.empty:
        return None, raw

    df = _normalize_for_charts(raw)
    result = run_all_strategies(
        df, stock_code=code.strip(), market=market,
        initial_cash=float(initial_cash), commission=float(commission),
        strategy_keys=strategy_keys,
        custom_params=custom_params or None,
        progress_cb=progress_cb,
    )
    return result, df


def best_of(compare_result) -> object | None:
    """从 CompareResult 里取总收益率最高的成功策略；无成功策略返回 None"""
    successful = [r for r in compare_result.results if r.success]
    return max(
        successful,
        key=lambda r: (r.report or {}).get("总收益率(%)", -1e9),
        default=None,
    )


# ============================================================================
# 单策略回测
# ============================================================================

@dataclass
class SingleRun:
    """单策略回测结果"""
    report: dict
    data_df: pd.DataFrame          # runner 内部使用的标准化数据（画 K 线用）
    raw_df: pd.DataFrame           # 原始拉取数据（数据表展示用）


def run_single(
    strategy_key: str,
    code: str,
    market: str,
    days_back: int,
    *,
    frequency: str = "d",
    initial_cash: float = 100_000.0,
    commission: float = 0.0002,
    params: dict | None = None,
) -> SingleRun | None:
    """
    单策略回测。数据为空返回 None。

    Raises:
        回测引擎异常原样抛出
    """
    from src.strategy.backtest import STRATEGY_REGISTRY, BacktestRunner

    strategy_cls = STRATEGY_REGISTRY[strategy_key]
    df = fetch_ohlcv(code, market, days_back, frequency=frequency)
    if df.empty:
        return None

    runner = BacktestRunner(
        strategy_class=strategy_cls,
        data_df=df,
        **(params or {}),
    )
    report = runner.run(initial_cash=initial_cash, commission=commission)
    return SingleRun(
        report=report,
        data_df=runner._data_df.copy() if report else pd.DataFrame(),
        raw_df=df,
    )


# ============================================================================
# 按筛选策略回测（策略统一管理：策略体 → screener_rule 引擎）
# ============================================================================

def run_strategy_backtest(
    sid: str,
    code: str = "",
    market: str = "a",
    days_back: int = 0,
    *,
    initial_cash: float = 100_000.0,
    commission: float = 0.00025,
    config_path=None,
) -> tuple[dict | None, str]:
    """
    直接用 screen_config.yaml 里的策略做单标的回测：
    技术条件 → 买入信号；策略 backtest 段 → 卖出条件/仓位。

    Args:
        code/days_back: 缺省用策略 backtest 段的 default_stock / days_back

    Returns:
        (report | None, 错误消息)；report 含 总收益率/年化/最大回撤/夏普/胜率 等
    """
    from src.services import screening_service as svc

    body = svc.get_strategy(sid, config_path)
    if not body:
        return None, f"策略不存在: {sid}"

    tech_conds = [c for c in body.get("conditions", [])
                  if not svc.is_spot_only(c.get("type", ""))]
    if not tech_conds:
        return None, "该策略没有技术条件（纯基本面策略无法逐日回测）"

    bt = body.get("backtest") or {}
    sell_conds = bt.get("sell_conditions") or []
    if not sell_conds:
        return None, "该策略未配置卖出条件（backtest.sell_conditions）"

    code = (code or str(bt.get("default_stock", ""))).strip()
    if not code:
        return None, "缺少回测标的代码"
    days_back = int(days_back or bt.get("days_back", 1000))

    single = run_single(
        "screener_rule", code, market, days_back,
        initial_cash=initial_cash, commission=commission,
        params={
            "buy_conditions": tech_conds,
            "sell_conditions": sell_conds,
            "buy_logic": bt.get("buy_logic", "all"),
            "sell_logic": bt.get("sell_logic", "any"),
            "position_size": float(bt.get("position_size", 0.9)),
        },
    )
    if single is None:
        return None, f"未能获取 {code} 的行情数据"
    return single.report, ""


# ============================================================================
# 参数寻优（向量化引擎 + optuna）
# ============================================================================

OPTIMIZE_METHODS = ("grid", "random", "bayesian")
_OPTIMIZE_MAX_TRIALS = 500
_WALK_FORWARD_MAX_TRIALS = 100


def list_optimizable_strategies() -> list[dict]:
    """可寻优策略清单：[{key, label, space}]"""
    from src.strategy.backtest.optimizer import OPTIMIZABLE_STRATEGIES

    return [{"key": k, "label": v["label"],
             "space": {p: list(s) for p, s in v["space"].items()}}
            for k, v in OPTIMIZABLE_STRATEGIES.items()]


def is_optimizable_strategy(key: str) -> bool:
    from src.strategy.backtest.optimizer import OPTIMIZABLE_STRATEGIES

    return key in OPTIMIZABLE_STRATEGIES


def run_optimization(
    strategy: str,
    code: str,
    market: str = "a",
    days: int = 750,
    *,
    method: str = "bayesian",
    n_trials: int = 100,
    metric: str = "总收益率(%)",
    walk_forward: bool = False,
) -> dict:
    """
    策略参数寻优（跑在向量化引擎上，秒级数百 trial）+ 可选 Walk-Forward 验证。

    Returns:
        {strategy, method, metric, n_trials, elapsed, best_params, best_score,
         top_trials: [...], walk_forward: {...} | None}

    Raises:
        KeyError: 未知策略
        ValueError: 未知寻优方法 / 参数非法 / 数据不足
        LookupError: 未能获取行情数据
    """
    import time as _time

    from src.strategy.backtest.optimizer import OPTIMIZABLE_STRATEGIES, ParamOptimizer

    spec = OPTIMIZABLE_STRATEGIES.get(strategy)
    if spec is None:
        raise KeyError(f"未知可寻优策略: {strategy}，可选 {sorted(OPTIMIZABLE_STRATEGIES)}")
    if method not in OPTIMIZE_METHODS:
        raise ValueError(f"未知寻优方法: {method}，支持 {'/'.join(OPTIMIZE_METHODS)}")

    df = fetch_ohlcv(code, market, days)
    if df is None or df.empty:
        raise LookupError("未能获取行情数据")

    opt = ParamOptimizer(spec["func"], spec["space"],
                         metric=metric, constraint=spec.get("constraint"))
    t0 = _time.perf_counter()
    result = opt.optimize(
        df, method=method,
        n_trials=max(10, min(int(n_trials), _OPTIMIZE_MAX_TRIALS)))

    wf = None
    if walk_forward:
        wf_res = opt.walk_forward(
            df, method=method,
            n_trials=max(10, min(int(n_trials), _WALK_FORWARD_MAX_TRIALS)))
        wf = {"summary": wf_res.summary, "folds": wf_res.folds}

    return {
        "strategy": strategy,
        "method": result.method,
        "metric": result.metric,
        "n_trials": result.n_trials,
        "elapsed": round(_time.perf_counter() - t0, 2),
        "best_params": result.best_params,
        "best_score": result.best_score,
        "top_trials": result.trials.head(10),
        "walk_forward": wf,
    }


# ============================================================================
# 可视化数据准备（原页面内嵌的指标计算）
# ============================================================================

@dataclass
class ChartFrames:
    """回测可视化所需的派生数据"""
    macd_df: pd.DataFrame                      # 含 macd / macd_signal / macd_hist
    golden_cross_dates: pd.Index = field(default_factory=pd.Index)
    death_cross_dates: pd.Index = field(default_factory=pd.Index)
    bottom_divergences: list = field(default_factory=list)   # 底背离日期
    top_divergences: list = field(default_factory=list)      # 顶背离日期


def build_chart_frames(k_df: pd.DataFrame) -> ChartFrames:
    """从 K 线 df 计算 MACD、金叉/死叉、顶底背离（scipy 缺失时背离为空）"""
    from src.analysis.technical.indicators import TechnicalAnalyzer

    ta = TechnicalAnalyzer(k_df)
    ta.add_macd()
    m_df = ta.get_dataframe()

    m_df["prev_macd"] = m_df["macd"].shift(1)
    m_df["prev_signal"] = m_df["macd_signal"].shift(1)
    golden = (m_df["macd"] > m_df["macd_signal"]) & (m_df["prev_macd"] <= m_df["prev_signal"])
    death = (m_df["macd"] < m_df["macd_signal"]) & (m_df["prev_macd"] >= m_df["prev_signal"])

    bottom_divs: list = []
    top_divs: list = []
    try:
        from scipy.signal import argrelmax, argrelmin

        price = m_df["close"].values
        hist = m_df["macd_hist"].values
        troughs = argrelmin(price, order=5)[0]
        for i in range(1, len(troughs)):
            curr, prev = troughs[i], troughs[i - 1]
            if (price[curr] < price[prev] and hist[curr] > hist[prev]
                    and hist[curr] < 0 and hist[prev] < 0):
                bottom_divs.append(m_df.index[curr])
        peaks = argrelmax(price, order=5)[0]
        for i in range(1, len(peaks)):
            curr, prev = peaks[i], peaks[i - 1]
            if (price[curr] > price[prev] and hist[curr] < hist[prev]
                    and hist[curr] > 0 and hist[prev] > 0):
                top_divs.append(m_df.index[curr])
    except ImportError:
        pass

    return ChartFrames(
        macd_df=m_df,
        golden_cross_dates=m_df.index[golden],
        death_cross_dates=m_df.index[death],
        bottom_divergences=bottom_divs,
        top_divergences=top_divs,
    )
