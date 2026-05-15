"""
src/ml/ — 机器学习因子加权模块（B 路径）

通过 LightGBM 学习"多因子 → 未来 20 日相对沪深 300 超额收益"的映射。
模型预测分以三种方式接入既有系统：
  1. 作为 FactorEngine 中的 ml_score 因子（src/analysis/factor/ml_factor.py）
  2. 作为筛选条件 ml_top_k（src/analysis/screening/conditions.py）
  3. 作为回测策略 MLRebalanceStrategy（src/strategy/backtest/ml_strategy.py）

调度器在 config/scheduler.yaml 中配置月度自动重训（每月 1 号收盘后）。

子模块：
  - dataset_builder: 拉数据 → 算特征 → 算超额收益标签 → parquet
  - trainer:         Purged Walk-Forward CV + LightGBM 训练 → joblib
  - predictor:       加载最新模型 + 推理（带缓存）
"""

from src.ml.dataset_builder import DatasetBuilder
from src.ml.predictor import MLPredictor, get_predictor
from src.ml.trainer import MLTrainer

__all__ = [
    "DatasetBuilder",
    "MLTrainer",
    "MLPredictor",
    "get_predictor",
]
