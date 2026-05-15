"""
src/strategy/backtest/compare.py — 多策略对比器

输入：一个标的的 OHLCV DataFrame + 一组「策略 key → 参数」字典
输出：每个策略的回测 report + 汇总 DataFrame + 累计收益对比 DataFrame

设计：
  - 串行跑（保留 Backtrader 实例对应关系，便于诊断；并行收益有限）
  - 单个策略失败不阻塞其他策略（捕获 + 记录 error 字段）
  - ML 策略在模型未训练时优雅跳过
"""
from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from src.strategy.backtest.defaults import get_all_default_strategies, get_default_params
from src.strategy.backtest.runner import BacktestRunner
from src.utils.logger import get_logger

logger = get_logger("backtest_compare")


@dataclass
class StrategyResult:
    """单策略回测结果（成功或失败的统一容器）"""
    key: str
    label: str
    success: bool
    report: dict | None = None
    error: str | None = None
    skipped: bool = False
    skip_reason: str | None = None
    params_used: dict = field(default_factory=dict)


@dataclass
class CompareResult:
    """多策略对比的汇总结果"""
    stock_code: str
    market: str
    data_df: pd.DataFrame
    initial_cash: float
    results: list[StrategyResult]

    @property
    def successful(self) -> list[StrategyResult]:
        return [r for r in self.results if r.success]

    def summary_df(self) -> pd.DataFrame:
        """每个策略一行的 KPI 汇总表"""
        rows = []
        for r in self.results:
            if not r.success:
                rows.append({
                    "策略": r.label,
                    "key": r.key,
                    "状态": "⚠️ 跳过" if r.skipped else "❌ 失败",
                    "说明": r.skip_reason or r.error or "",
                    "总收益率(%)": None, "年化收益率(%)": None,
                    "夏普": None, "最大回撤(%)": None,
                    "交易数": None, "胜率(%)": None,
                })
                continue
            rep = r.report or {}
            rows.append({
                "策略": r.label,
                "key": r.key,
                "状态": "✅",
                "说明": "",
                "总收益率(%)": rep.get("总收益率(%)"),
                "年化收益率(%)": rep.get("年化收益率(%)"),
                "夏普": rep.get("夏普比率"),
                "最大回撤(%)": rep.get("最大回撤(%)"),
                "交易数": rep.get("总交易次数"),
                "胜率(%)": rep.get("胜率(%)"),
            })
        return pd.DataFrame(rows)

    def equity_curves_df(self) -> pd.DataFrame:
        """
        合并所有成功策略的权益曲线为宽表：列名 = 策略 label，行 index = datetime。
        额外加一列 "Buy & Hold"（基准）。
        """
        frames: list[pd.DataFrame] = []
        for r in self.results:
            if not r.success or r.report is None:
                continue
            eq: pd.DataFrame = r.report.get("equity_curve")
            if eq is None or eq.empty:
                continue
            df = eq.copy()
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.set_index("datetime")[["value"]]
            df.columns = [r.label]
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        merged = pd.concat(frames, axis=1).sort_index()

        # Buy & Hold 基准：用 data_df 的 close 推算
        bh = self._buy_and_hold_curve()
        if bh is not None:
            merged = merged.join(bh, how="outer").sort_index()
        return merged.ffill()

    def _buy_and_hold_curve(self) -> pd.DataFrame | None:
        """构造 Buy & Hold 基准曲线"""
        if self.data_df is None or self.data_df.empty:
            return None
        close = self.data_df.get("close")
        if close is None:
            return None
        close = pd.to_numeric(close, errors="coerce").dropna()
        if close.empty:
            return None
        # 第一根 bar 全仓买入
        shares = self.initial_cash / close.iloc[0]
        bh_values = close * shares
        bh_df = pd.DataFrame({"Buy & Hold": bh_values})
        bh_df.index = pd.to_datetime(bh_df.index)
        bh_df.index.name = "datetime"
        return bh_df


def run_all_strategies(
    data_df: pd.DataFrame,
    *,
    stock_code: str,
    market: str = "a",
    initial_cash: float = 100000,
    commission: float = 0.00025,
    slippage_perc: float = 0.001,
    strategy_keys: list[str] | None = None,
    custom_params: dict[str, dict] | None = None,
    progress_cb=None,
) -> CompareResult:
    """
    在同一标的上跑多个策略，返回 CompareResult。

    Args:
        data_df:         OHLCV DataFrame（与 BacktestRunner._prepare_data 同格式）
        stock_code/market: 仅用于在结果里标记
        initial_cash:    所有策略统一的初始资金
        commission:      佣金率
        strategy_keys:   要跑的策略 key 列表；None=跑全部已注册
        custom_params:   {strategy_key: params_override}，覆盖默认参数
        progress_cb:     可选 callback(idx, total, label, status) 给前端进度条用

    Returns:
        CompareResult
    """
    # 延迟 import 避免循环依赖（__init__.py 里 import 本模块）
    from src.strategy.backtest import STRATEGY_LABELS, STRATEGY_REGISTRY

    keys = list(strategy_keys) if strategy_keys else list(STRATEGY_REGISTRY.keys())
    overrides = custom_params or {}
    results: list[StrategyResult] = []

    for idx, key in enumerate(keys):
        label = STRATEGY_LABELS.get(key, key) if hasattr(STRATEGY_LABELS, "get") else key
        if progress_cb:
            try:
                progress_cb(idx, len(keys), label, "running")
            except Exception:
                pass

        # 合并参数：默认 + 用户 override
        params = get_default_params(key)
        params.update(overrides.get(key, {}))

        try:
            cls = STRATEGY_REGISTRY[key]
        except Exception as e:
            results.append(StrategyResult(
                key=key, label=label, success=False,
                error=f"找不到策略类: {e}",
            ))
            continue

        try:
            runner = BacktestRunner(strategy_class=cls, data_df=data_df, **params)
            report = runner.run(
                initial_cash=initial_cash,
                commission=commission,
                slippage_perc=slippage_perc,
                market=market,
            )
            results.append(StrategyResult(
                key=key, label=label, success=True,
                report=report, params_used=params,
            ))
            if progress_cb:
                try:
                    progress_cb(idx, len(keys), label, "done")
                except Exception:
                    pass
        except RuntimeError as e:
            # ML 模型未训练等可恢复错误
            msg = str(e)
            is_skip = "训练" in msg or "lgbm_latest" in msg
            results.append(StrategyResult(
                key=key, label=label, success=False,
                skipped=is_skip,
                skip_reason=msg if is_skip else None,
                error=None if is_skip else msg,
                params_used=params,
            ))
            logger.warning(f"[compare] {key} 跳过/失败: {msg}")
            if progress_cb:
                try:
                    progress_cb(idx, len(keys), label, "skipped" if is_skip else "error")
                except Exception:
                    pass
        except Exception as e:
            tb = traceback.format_exc(limit=3)
            logger.error(f"[compare] {key} 异常: {e}\n{tb}")
            results.append(StrategyResult(
                key=key, label=label, success=False,
                error=f"{type(e).__name__}: {e}",
                params_used=params,
            ))
            if progress_cb:
                try:
                    progress_cb(idx, len(keys), label, "error")
                except Exception:
                    pass

    return CompareResult(
        stock_code=stock_code,
        market=market,
        data_df=data_df,
        initial_cash=initial_cash,
        results=results,
    )
