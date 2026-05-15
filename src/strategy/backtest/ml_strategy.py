"""
src/strategy/backtest/ml_strategy.py — ML 预测驱动的轮动策略（B 路径）

策略逻辑：
  - 每 rebalance_days 个交易日，用 ML 模型对当前日的特征做预测
  - 预测分 > buy_threshold 时建仓
  - 预测分 < sell_threshold 时清仓
  - 默认单标的策略（与现有回测框架对齐：一个 BacktestRunner 一次跑一只）

注意：本策略是单标的版本，"top-N 横截面轮动"需要多 datafeed 支持，
      待 BacktestRunner 升级后再扩展。
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
        buy_threshold: 预测超额收益 > 此值时建仓（百分点；默认 1.0）
        sell_threshold: 预测超额收益 < 此值时清仓（默认 -1.0）
        position_size: 仓位比例
        warmup_bars: 预热期，至少 250（特征需要 MA250 长指标）
    """

    params = (
        ("rebalance_days", 20),
        ("buy_threshold", 1.0),
        ("sell_threshold", -1.0),
        ("position_size", 0.95),
        ("warmup_bars", 250),
    )

    def __init__(self):
        self._bar_count = 0
        # 延迟到第一次 next 时加载，避免回测构造阶段就拉模型
        self._predictor = None

    def _get_predictor(self):
        if self._predictor is None:
            from src.ml.predictor import get_predictor
            self._predictor = get_predictor()
            if self._predictor is None:
                raise RuntimeError(
                    "MLRebalanceStrategy 需要训练好的 ML 模型。请先运行 "
                    "python -m src.ml.cli train"
                )
        return self._predictor

    def _build_features_df(self) -> pd.DataFrame:
        """
        从 Backtrader data feed 构造 DataFrame，供 predictor 计算特征。

        日期、OHLCV 都来自当前可见的历史（不含未来），与 dataset_builder 的特征工程一致。
        """
        n = len(self.data)
        # 收集已观察到的全部历史
        # Backtrader 的 data.close[-i] 表示 i 个 bar 之前的值
        dates, opens, highs, lows, closes, vols = [], [], [], [], [], []
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

        # 算预测分
        try:
            predictor = self._get_predictor()
            df = self._build_features_df()
            score = predictor.predict_from_daily_df(df)
            if not math.isfinite(score):
                return
        except Exception as e:
            self.log(f"ML 预测失败: {e}")
            return

        if not self.position:
            if score >= self.params.buy_threshold:
                order = self.buy_target_percent(target=self.params.position_size)
                if order is not None:
                    self.log(f"ML 信号买入: 预测超额 {score:.2f}% > {self.params.buy_threshold:.2f}%")
        else:
            if score <= self.params.sell_threshold:
                self.close_all()
                self.log(f"ML 信号卖出: 预测超额 {score:.2f}% < {self.params.sell_threshold:.2f}%")
