"""
src/strategy/backtest/optimizer.py — 策略参数寻优框架（路线图 §2.7）

三种寻优方法 + Walk-Forward 前向验证，全部跑在向量化引擎上（秒级数百 trial）：

  grid       网格搜索：参数空间笛卡尔积（tuple 区间自动取样 ≤10 个点）
  random     随机搜索：均匀采样 n_trials 次（可复现，seed 固定）
  bayesian   贝叶斯优化：optuna TPE（未安装 optuna 时抛 RuntimeError 并提示）

参数空间写法::
    param_space = {
        "fast": (3, 15),          # int 区间（两端 int → 整数参数）
        "slow": (10.0, 60.0),     # float 区间
        "mode": ["a", "b"],       # 离散候选
    }

用法::
    from src.strategy.backtest.optimizer import ParamOptimizer
    from src.strategy.backtest.vector_engine import ma_crossover_signals

    opt = ParamOptimizer(ma_crossover_signals, {"fast": (3, 15), "slow": (10, 60)},
                         constraint=lambda p: p["fast"] < p["slow"])
    result = opt.optimize(df, method="bayesian", n_trials=100)
    result.best_params, result.best_score, result.trials

过拟合检测：walk_forward() 滚动切分训练/验证窗口，报告 IS vs OOS 差异。
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from src.core.columns import normalize_ohlcv_columns
from src.strategy.backtest.vector_engine import run_vector_backtest
from src.utils.logger import get_logger

logger = get_logger("param_optimizer")

# 网格搜索里连续区间的默认取样点数
GRID_POINTS_PER_DIM = 10


@dataclass
class OptimizationResult:
    """单次寻优结果"""
    best_params: dict
    best_score: float
    trials: pd.DataFrame          # 每 trial 一行：参数列 + score 列，按 score 降序
    metric: str
    method: str
    n_trials: int


@dataclass
class WalkForwardResult:
    """Walk-Forward 前向验证结果"""
    folds: pd.DataFrame           # 每折一行：fold/train_bars/test_bars/params/is_score/oos_score
    is_mean: float                # 样本内均值
    oos_mean: float               # 样本外均值
    overfit_gap: float            # is_mean - oos_mean（越大越过拟合）
    metric: str
    summary: dict = field(default_factory=dict)


def _is_int_pair(spec: tuple) -> bool:
    return all(isinstance(x, (int, np.integer)) for x in spec)


class ParamOptimizer:
    """
    基于向量化回测引擎的参数寻优器。

    Args:
        signal_func: callable(close: pd.Series, **params) -> 信号数组（1/-1/0）
        param_space: 参数空间（见模块 docstring）
        metric: 优化目标（向量化引擎 report 的键，如 "总收益率(%)" / "夏普比率"）
        constraint: callable(params) -> bool；False 的组合直接记 -inf 不回测
    """

    def __init__(self, signal_func, param_space: dict, *,
                 metric: str = "总收益率(%)", constraint=None,
                 initial_cash: float = 100_000.0,
                 commission: float = 0.00025, stamp_tax: float = 0.001):
        if not param_space:
            raise ValueError("param_space 不能为空")
        self.signal_func = signal_func
        self.param_space = dict(param_space)
        self.metric = metric
        self.constraint = constraint
        self.initial_cash = float(initial_cash)
        self.commission = float(commission)
        self.stamp_tax = float(stamp_tax)

    # ------------------------------------------------------------------
    # 打分
    # ------------------------------------------------------------------

    def _score(self, close: pd.Series, df: pd.DataFrame, params: dict) -> float:
        """跑一次向量化回测取 metric；约束不满足/回测失败 → -inf"""
        if self.constraint is not None and not self.constraint(params):
            return float("-inf")
        try:
            signals = self.signal_func(close, **params)
            result = run_vector_backtest(
                df, signals,
                initial_cash=self.initial_cash,
                commission=self.commission, stamp_tax=self.stamp_tax,
            )
            score = result.report.get(self.metric)
            return float(score) if score is not None else float("-inf")
        except Exception as e:
            logger.debug(f"trial {params} 失败: {e}")
            return float("-inf")

    @staticmethod
    def _prepare_close(df: pd.DataFrame) -> pd.Series:
        ndf = normalize_ohlcv_columns(df)
        if "close" not in ndf.columns:
            raise ValueError("数据缺少收盘价列（close/收盘）")
        return pd.to_numeric(ndf["close"], errors="coerce").reset_index(drop=True)

    # ------------------------------------------------------------------
    # 候选生成
    # ------------------------------------------------------------------

    def _grid_candidates(self, n_cap: int) -> list[dict]:
        """网格：list 用全部候选；tuple 区间取样 ≤GRID_POINTS_PER_DIM 个点；总数超 n_cap 均匀抽稀"""
        axes: dict[str, list] = {}
        for name, spec in self.param_space.items():
            if isinstance(spec, (list, tuple)) and not (
                    isinstance(spec, tuple) and len(spec) == 2
                    and all(isinstance(x, (int, float, np.integer, np.floating))
                            for x in spec)):
                axes[name] = list(spec)
            else:
                low, high = spec
                if _is_int_pair(spec):
                    span = int(high) - int(low) + 1
                    n = min(GRID_POINTS_PER_DIM, span)
                    axes[name] = sorted({int(round(v)) for v in
                                         np.linspace(int(low), int(high), n)})
                else:
                    axes[name] = [float(v) for v in
                                  np.linspace(float(low), float(high),
                                              GRID_POINTS_PER_DIM)]
        names = list(axes)
        combos = [dict(zip(names, values))
                  for values in itertools.product(*axes.values())]
        if len(combos) > n_cap:
            idx = np.linspace(0, len(combos) - 1, n_cap).astype(int)
            combos = [combos[i] for i in idx]
        return combos

    def _random_candidates(self, n_trials: int, seed: int) -> list[dict]:
        rng = np.random.default_rng(seed)
        combos = []
        for _ in range(int(n_trials)):
            params = {}
            for name, spec in self.param_space.items():
                if isinstance(spec, list):
                    params[name] = spec[int(rng.integers(0, len(spec)))]
                else:
                    low, high = spec
                    if _is_int_pair(spec):
                        params[name] = int(rng.integers(int(low), int(high) + 1))
                    else:
                        params[name] = float(rng.uniform(float(low), float(high)))
            combos.append(params)
        return combos

    # ------------------------------------------------------------------
    # 寻优入口
    # ------------------------------------------------------------------

    def optimize(self, df: pd.DataFrame, method: str = "grid",
                 n_trials: int = 100, seed: int = 42) -> OptimizationResult:
        """
        执行寻优。

        Args:
            df: OHLCV DataFrame（中/英文列名均可）
            method: "grid" | "random" | "bayesian"
            n_trials: random/bayesian 的 trial 数；grid 的组合数上限
        """
        close = self._prepare_close(df)

        if method == "bayesian":
            return self._optimize_bayesian(close, df, n_trials, seed)

        if method == "grid":
            candidates = self._grid_candidates(n_cap=int(n_trials))
        elif method == "random":
            candidates = self._random_candidates(n_trials, seed)
        else:
            raise ValueError(f"未知寻优方法: {method}，支持 grid/random/bayesian")

        # 注意：不要从 trials.iloc[0] 反提取参数——pandas 跨列取行会把 int
        # 上转成 float64，导致 rolling(3.0) 之类的下游调用崩溃
        rows = []
        best_params: dict | None = None
        best_score = float("-inf")
        for params in candidates:
            score = self._score(close, df, params)
            rows.append({**params, "score": score})
            if score > best_score:
                best_score, best_params = score, dict(params)
        if best_params is None:
            best_params = dict(candidates[0]) if candidates else {}
        trials = pd.DataFrame(rows).sort_values(
            "score", ascending=False, ignore_index=True)
        return OptimizationResult(
            best_params=best_params, best_score=best_score,
            trials=trials, metric=self.metric, method=method,
            n_trials=len(trials),
        )

    def _optimize_bayesian(self, close: pd.Series, df: pd.DataFrame,
                           n_trials: int, seed: int) -> OptimizationResult:
        try:
            import optuna
        except ImportError as e:
            raise RuntimeError(
                "贝叶斯寻优需要 optuna，请先安装：pip install optuna"
            ) from e
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        def objective(trial: optuna.Trial) -> float:
            params = {}
            for name, spec in self.param_space.items():
                if isinstance(spec, list):
                    params[name] = trial.suggest_categorical(name, spec)
                elif _is_int_pair(spec):
                    params[name] = trial.suggest_int(name, int(spec[0]), int(spec[1]))
                else:
                    params[name] = trial.suggest_float(
                        name, float(spec[0]), float(spec[1]))
            score = self._score(close, df, params)
            # optuna 不接受 -inf，用极小值代替
            return score if np.isfinite(score) else -1e18

        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(seed=seed),
        )
        study.optimize(objective, n_trials=int(n_trials), show_progress_bar=False)

        rows = [{**t.params, "score": t.value} for t in study.trials
                if t.value is not None]
        trials = pd.DataFrame(rows).sort_values(
            "score", ascending=False, ignore_index=True)
        return OptimizationResult(
            best_params=dict(study.best_params), best_score=float(study.best_value),
            trials=trials, metric=self.metric, method="bayesian",
            n_trials=len(study.trials),
        )

    # ------------------------------------------------------------------
    # Walk-Forward 前向验证（过拟合检测）
    # ------------------------------------------------------------------

    def walk_forward(self, df: pd.DataFrame, *, n_splits: int = 4,
                     train_ratio: float = 0.7, method: str = "grid",
                     n_trials: int = 50, seed: int = 42) -> WalkForwardResult:
        """
        滚动窗口前向验证：把时间序列切成 n_splits 个连续窗口，
        每个窗口内前 train_ratio 训练（寻优），剩余部分样本外验证。

        Returns:
            WalkForwardResult；overfit_gap = IS 均值 - OOS 均值，
            显著为正说明参数过拟合训练段
        """
        ndf = normalize_ohlcv_columns(df).reset_index(drop=True)
        n = len(ndf)
        if n < n_splits * 20:
            raise ValueError(f"数据太短（{n} bar），无法做 {n_splits} 折 Walk-Forward")

        fold_len = n // n_splits
        rows = []
        for i in range(n_splits):
            start = i * fold_len
            end = n if i == n_splits - 1 else (i + 1) * fold_len
            window = ndf.iloc[start:end].reset_index(drop=True)
            split = int(len(window) * train_ratio)
            train, test = window.iloc[:split], window.iloc[split:].reset_index(drop=True)
            if len(train) < 10 or len(test) < 5:
                continue

            result = self.optimize(train, method=method, n_trials=n_trials, seed=seed)
            oos_score = self._score(self._prepare_close(test), test,
                                    result.best_params)
            rows.append({
                "fold": i + 1,
                "train_bars": len(train), "test_bars": len(test),
                **{f"param_{k}": v for k, v in result.best_params.items()},
                "is_score": result.best_score,
                "oos_score": oos_score,
            })

        folds = pd.DataFrame(rows)
        if folds.empty:
            raise ValueError(
                f"Walk-Forward 无有效折：{n_splits} 折 × train_ratio={train_ratio} "
                f"切出的训练段(<10 bar)或验证段(<5 bar)都太短，请减少折数或调整比例"
            )
        finite_is = folds["is_score"].replace([np.inf, -np.inf], np.nan)
        finite_oos = folds["oos_score"].replace([np.inf, -np.inf], np.nan)
        is_mean = float(finite_is.mean()) if not folds.empty else float("nan")
        oos_mean = float(finite_oos.mean()) if not folds.empty else float("nan")
        return WalkForwardResult(
            folds=folds, is_mean=is_mean, oos_mean=oos_mean,
            overfit_gap=is_mean - oos_mean, metric=self.metric,
            summary={
                "n_folds": len(folds),
                "is_mean": round(is_mean, 4),
                "oos_mean": round(oos_mean, 4),
                "overfit_gap": round(is_mean - oos_mean, 4),
            },
        )


# ============================================================================
# 可寻优策略注册表（API/前端用）
# ============================================================================

def _ma_constraint(p: dict) -> bool:
    return p["fast"] < p["slow"]


def _build_ma_crossover():
    from src.strategy.backtest.vector_engine import ma_crossover_signals
    return {
        "label": "均线交叉（MA金叉/死叉）",
        "func": ma_crossover_signals,
        "space": {"fast": (3, 15), "slow": (10, 60)},
        "constraint": _ma_constraint,
    }


OPTIMIZABLE_STRATEGIES: dict[str, dict] = {
    "ma_crossover": _build_ma_crossover(),
}
