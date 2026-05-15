"""
src/analysis/factor/ml_factor.py — ML 预测因子

把 src/ml/predictor.py 的预测分包装成一个标准的 BaseFactor，
通过 FactorEngine 与现有 PE/PB/动量等因子并列使用。

注册：在 src/analysis/factor/__init__.py 中已 register_factor("ml_score", MLScoreFactor)。
启用：config/factors.yaml 加 `- type: ml_score, enabled: true` 即可。

降级行为：
  - 模型未训练 / 加载失败 → safe_calculate 返回 NaN，不影响其他因子
"""
from __future__ import annotations

from src.analysis.factor.base import BaseFactor
from src.utils.logger import get_logger

logger = get_logger("factor.ml")


class MLScoreFactor(BaseFactor):
    """
    ML 预测因子：基于 daily OHLCV 预测未来 20 日相对沪深 300 的超额收益（%）。

    higher_is_better = True（分越高，预期超额收益越高）。
    """

    requires_ohlcv = True
    ohlcv_period = "daily"
    higher_is_better = True

    def __init__(self):
        self.name = "ml_score"
        self.description = "ML 预测的未来 20 日超额收益（%）"

    def validate(self, data: dict) -> bool:
        df = data.get("daily_df")
        return df is not None and not df.empty and len(df) >= 250

    def calculate(self, data: dict) -> float:
        from src.ml.predictor import get_predictor

        predictor = get_predictor()
        if predictor is None:
            return float("nan")
        df = data["daily_df"]
        try:
            return float(predictor.predict_from_daily_df(df))
        except Exception as e:
            logger.debug(f"ml_score 计算失败: {e}")
            return float("nan")
