"""
src/strategy/backtest/ml_strategy.py — ML 预测驱动的轮动策略（B 路径）

策略逻辑：
  - 每 rebalance_days 个交易日，用 ML 模型对当前日的特征做预测
  - 预测分 > buy_threshold 时建仓
  - 预测分 < sell_threshold 时清仓
  - 默认单标的策略（与现有回测框架对齐：一个 BacktestRunner 一次跑一只）

修复要点（2026-05-15）：
  1. __init__ 立即检查模型存在，缺失时直接 raise（避免回测跑完 0 交易才发现是没训练）
  2. 默认阈值降低：buy=0.0 / sell=-0.5（IC 通常 0.05 量级，原 1.0/-1.0 过严难触发）
  3. 每次预测写 debug log，stop() 输出总结（预测分布 + 触发次数）
"""
from __future__ import annotations

import math

import backtrader as bt
import pandas as pd

from src.strategy.backtest.base_strategy import BaseStrategy
from src.utils.logger import get_logger

logger = get_logger("backtest.ml")


class MLRebalanceStrategy(BaseStrategy):
    """
    ML 单标的轮动策略。

    Params:
        rebalance_days: 调仓周期（默认 20 交易日，约 1 月）
        buy_threshold: 预测超额收益 ≥ 此值时建仓（百分点；默认 0.0）
        sell_threshold: 预测超额收益 ≤ 此值时清仓（默认 -0.5）
        position_size: 仓位比例
        warmup_bars: 预热期，至少 250（特征需要 MA250 长指标）
        skip_if_no_model: True=模型不存在时 __init__ 直接 raise；False=warning 后空跑
    """

    params = (
        ("rebalance_days", 20),
        ("buy_threshold", 0.0),         # 修复：原 1.0 过严，多数 IC=0.05 量级模型很难超
        ("sell_threshold", -0.5),       # 修复：原 -1.0 过严
        ("position_size", 0.95),
        ("warmup_bars", 250),
        ("skip_if_no_model", True),     # True 时 init 阶段就报错
    )

    def __init__(self):
        self._bar_count = 0
        self._predictor = None
        # 统计用：诊断 0 交易问题时输出预测分布
        self._predict_count = 0
        self._predict_scores: list[float] = []
        self._buy_signals = 0
        self._sell_signals = 0
        self._failures = 0

        # 立即尝试加载模型 — 模型不存在时尽早暴露，不要等到 next() 里静默失败
        from src.ml.predictor import get_predictor
        self._predictor = get_predictor()
        if self._predictor is None and self.p.skip_if_no_model:
            raise RuntimeError(
                "MLRebalanceStrategy 需要训练好的 ML 模型，但 cache/ml_models/lgbm_latest.joblib 不存在。\n"
                "请先训练：python -m src.ml.cli train\n"
                "调试期可加 skip_if_no_model=False 让策略跳过预测（不会有交易）。"
            )
        if self._predictor is not None:
            cv_ic = self._predictor.metadata.get("cv_ic_mean", "N/A")
            logger.info(f"[MLRebalance] 加载模型: {self._predictor.model_path.name}, CV IC={cv_ic}")

    def _build_features_df(self) -> pd.DataFrame:
        """
        从 Backtrader data feed 构造 DataFrame，供 predictor 计算特征。

        修复：用 len(self) 而非 len(self.data)，只取已观察到的历史（防 lookahead）。
        """
        n = len(self)   # 当前 bar 序号（已观察过的 bar 数）
        if n <= 0:
            return pd.DataFrame()

        dates, opens, highs, lows, closes, vols = [], [], [], [], [], []
        # i 从 n-1 到 0：分别是最早 bar 到最新 bar
        # self.data.close[-i]：i bar 之前；i=0 是当前 bar
        for i in range(n - 1, -1, -1):
            try:
                dt = bt.num2date(self.data.datetime[-i])
                dates.append(dt)
                opens.append(float(self.data.open[-i]))
                highs.append(float(self.data.high[-i]))
                lows.append(float(self.data.low[-i]))
                closes.append(float(self.data.close[-i]))
                vols.append(float(self.data.volume[-i]))
            except (IndexError, ValueError):
                continue
        df = pd.DataFrame({
            "日期": dates, "开盘": opens, "最高": highs,
            "最低": lows, "收盘": closes, "成交量": vols,
        })
        return df

    def next(self):
        if not self.is_warmup_done():
            return
        self._bar_count += 1
        if self._bar_count % self.params.rebalance_days != 0:
            return

        # 模型不存在时不再 try 重新加载（init 已经决定了）
        if self._predictor is None:
            return

        # 算预测分
        try:
            df = self._build_features_df()
            score = self._predictor.predict_from_daily_df(df)
        except Exception as e:
            self._failures += 1
            self.log(f"[MLRebalance] 预测异常: {type(e).__name__}: {e}")
            return

        if not math.isfinite(score):
            self._failures += 1
            return

        self._predict_count += 1
        self._predict_scores.append(float(score))

        if not self.position:
            if score >= self.params.buy_threshold:
                order = self.buy_target_percent(target=self.params.position_size)
                if order is not None:
                    self._buy_signals += 1
                    self.log(
                        f"[MLRebalance] 买入信号: 预测超额={score:+.3f}% ≥ {self.params.buy_threshold:+.2f}%"
                    )
        else:
            if score <= self.params.sell_threshold:
                self.close_all()
                self._sell_signals += 1
                self.log(
                    f"[MLRebalance] 卖出信号: 预测超额={score:+.3f}% ≤ {self.params.sell_threshold:+.2f}%"
                )

    def stop(self):
        """回测结束时输出诊断总结，方便 debug 0 交易问题"""
        if self._predict_count == 0:
            if self._failures > 0:
                logger.warning(
                    f"[MLRebalance] 回测结束: 模型预测全部失败（{self._failures} 次）。"
                    f"请检查 cache/ml_models/lgbm_latest.joblib 与训练特征是否对齐。"
                )
            else:
                logger.warning(
                    f"[MLRebalance] 回测结束: 0 次预测发生。常见原因：\n"
                    f"  - warmup_bars={self.p.warmup_bars} 太大，回测数据不够预热\n"
                    f"  - rebalance_days={self.p.rebalance_days} 太长，没到调仓节点回测就结束\n"
                    f"建议：把回测时长设到至少 warmup_bars + rebalance_days × 3 = {self.p.warmup_bars + self.p.rebalance_days * 3} 个交易日。"
                )
            return

        scores = pd.Series(self._predict_scores)
        logger.info(
            f"[MLRebalance] 回测结束统计:\n"
            f"  预测次数: {self._predict_count}（失败 {self._failures}）\n"
            f"  预测分布: min={scores.min():+.3f}, "
            f"p25={scores.quantile(0.25):+.3f}, "
            f"median={scores.median():+.3f}, "
            f"p75={scores.quantile(0.75):+.3f}, "
            f"max={scores.max():+.3f}\n"
            f"  买入信号 {self._buy_signals} 次（阈值 ≥ {self.p.buy_threshold:+.2f}%）\n"
            f"  卖出信号 {self._sell_signals} 次（阈值 ≤ {self.p.sell_threshold:+.2f}%）"
        )

        # 关键提示：如果预测分布完全没碰到阈值，明确告诉用户怎么调
        if self._buy_signals == 0 and self._predict_count > 0:
            p90 = scores.quantile(0.90)
            logger.warning(
                f"[MLRebalance] 0 买入信号！预测分 90% 分位 = {p90:+.3f}%，"
                f"低于 buy_threshold={self.p.buy_threshold:+.2f}%。\n"
                f"建议把 buy_threshold 调到 {p90:+.2f} 或更低试试。"
            )
