"""
src/ml/predictor.py — ML 模型推理 + 进程级单例 + 推理缓存

设计：
  - **进程级单例**：避免每次预测都重新 joblib.load 模型（200ms+）
  - **特征字典 → 标准化向量**：调用方传 dict，predictor 按训练时的 feature_cols 顺序填充
  - **缺失值容忍**：LightGBM 原生支持 NaN，所以传 None 或缺字段都 OK
  - **批量预测**：predict_batch(list[dict]) 比逐次调用快
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("ml.predictor")

_DEFAULT_MODEL_DIR = Path("./cache/ml_models")


class MLPredictor:
    """LightGBM 模型推理器"""

    def __init__(self, model_path: Path | str | None = None):
        """
        Args:
            model_path: 模型文件路径，None=自动加载 cache/ml_models/lgbm_latest.joblib
        """
        if model_path is None:
            model_path = _DEFAULT_MODEL_DIR / "lgbm_latest.joblib"
        self.model_path = Path(model_path)
        if not self.model_path.exists():
            raise FileNotFoundError(
                f"ML 模型不存在: {self.model_path}。先跑一次训练："
                f"python -m src.ml.trainer"
            )
        bundle = joblib.load(self.model_path)
        self.model = bundle["model"]
        self.feature_cols: list[str] = bundle["feature_cols"]
        self.label_col: str = bundle.get("label_col", "y")
        self.params: dict = bundle.get("params", {})

        # 读取 metadata
        meta_path = self.model_path.with_suffix(".meta.json")
        if not meta_path.exists():
            # latest 模型对应的 meta 路径
            meta_path = self.model_path.parent / f"{self.model_path.stem}.meta.json"
        self.metadata: dict = {}
        if meta_path.exists():
            try:
                self.metadata = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                pass

        logger.info(
            f"加载 ML 模型: {self.model_path.name} "
            f"({len(self.feature_cols)} 特征, "
            f"CV IC={self.metadata.get('cv_ic_mean', 'NA')})"
        )

    def predict_one(self, feat: dict) -> float:
        """
        单条预测。

        Args:
            feat: 特征字典 {feature_name: value}；缺失字段会用 NaN 替代

        Returns:
            预测的超额收益百分比（如 +2.5 表示未来 20 日跑赢大盘 2.5%）
        """
        return float(self.predict_batch([feat])[0])

    def predict_batch(self, feats: list[dict]) -> np.ndarray:
        """批量预测，比单次循环快很多"""
        if not feats:
            return np.array([])
        df = pd.DataFrame(feats)
        # 按训练时的 feature_cols 顺序对齐；缺列填 NaN（LightGBM 原生支持）
        X = df.reindex(columns=self.feature_cols)
        # 任何非数值类型强制转 float
        for col in X.columns:
            X[col] = pd.to_numeric(X[col], errors="coerce")
        return self.model.predict(X.values)

    # 推理时缺失特征占比超过此阈值就告警：说明特征工程与训练时已经漂移，
    # 预测值不可信（LightGBM 对 NaN 走默认分支，会塌向一个常数）
    _MISSING_FEATURE_WARN_RATIO = 0.20

    def predict_from_daily_df(self, daily_df: pd.DataFrame,
                              df_val: pd.DataFrame | None = None) -> float:
        """
        便捷方法：从日线 OHLCV 直接算特征 + 预测。

        走 `DatasetBuilder.build_feature_frame`——与训练侧**同一个装配入口**。
        曾因两侧各自拼装导致推理只给 15/55 维特征，其余 40 维全 NaN，
        预测恒为负、ML 策略 0 笔交易。
        """
        from src.ml.dataset_builder import DatasetBuilder

        feats = DatasetBuilder.build_feature_frame(daily_df, df_val=df_val)
        if feats.empty:
            return float("nan")
        last_row = feats.iloc[-1].drop("date", errors="ignore").to_dict()
        self._warn_if_features_drifted(last_row)
        return self.predict_one(last_row)

    def _warn_if_features_drifted(self, feat: dict) -> None:
        """特征覆盖率体检：缺太多说明训练/推理已漂移，必须让它可见而非静默劣化"""
        missing = [c for c in self.feature_cols if c not in feat]
        if not missing:
            return
        ratio = len(missing) / max(len(self.feature_cols), 1)
        if ratio >= self._MISSING_FEATURE_WARN_RATIO:
            logger.warning(
                f"特征漂移：推理只提供 {len(self.feature_cols) - len(missing)}/"
                f"{len(self.feature_cols)} 维，缺 {len(missing)} 维（{ratio:.0%}）——"
                f"预测值不可信。缺失示例: {missing[:6]}"
            )

    def feature_coverage(self, daily_df: pd.DataFrame,
                         df_val: pd.DataFrame | None = None) -> dict:
        """
        推理侧能给出多少训练时用到的特征（供测试与线上自检）。

        Returns:
            {"expected": 55, "provided": 46, "missing": [...], "ratio": 0.84}
        """
        from src.ml.dataset_builder import DatasetBuilder

        feats = DatasetBuilder.build_feature_frame(daily_df, df_val=df_val)
        got = set(feats.columns) if not feats.empty else set()
        missing = [c for c in self.feature_cols if c not in got]
        return {
            "expected": len(self.feature_cols),
            "provided": len(self.feature_cols) - len(missing),
            "missing": missing,
            "ratio": (len(self.feature_cols) - len(missing)) / max(len(self.feature_cols), 1),
        }


# =============================================================================
# 进程级单例（避免重复加载模型）
# =============================================================================

_predictor_singleton: MLPredictor | None = None
_predictor_lock = threading.Lock()


def get_predictor(model_path: Path | str | None = None,
                  force_reload: bool = False) -> MLPredictor | None:
    """
    获取共享 predictor 实例。

    Returns:
        MLPredictor 实例；模型未训练时返回 None（调用方应优雅降级）
    """
    global _predictor_singleton

    if not force_reload and _predictor_singleton is not None:
        return _predictor_singleton

    with _predictor_lock:
        if not force_reload and _predictor_singleton is not None:
            return _predictor_singleton
        try:
            _predictor_singleton = MLPredictor(model_path=model_path)
            return _predictor_singleton
        except FileNotFoundError:
            logger.warning("ML 模型尚未训练，predictor 不可用")
            return None
        except Exception as e:
            logger.error(f"加载 ML predictor 失败: {e}", exc_info=True)
            return None


def reset_predictor() -> None:
    """主动重置单例（重训后调用）"""
    global _predictor_singleton
    with _predictor_lock:
        _predictor_singleton = None
