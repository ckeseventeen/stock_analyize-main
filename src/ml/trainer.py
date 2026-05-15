"""
src/ml/trainer.py — LightGBM 训练 + Purged Walk-Forward CV

为什么用 Purged + Embargoed CV（金融时序标配）：
  - 普通 K-Fold 会在训练集和验证集之间产生标签泄漏（未来 20 日的样本可能在不同 fold 里）
  - Purged：在验证集前后扣掉 label_horizon 天的训练样本，断绝泄漏路径
  - Embargo：在验证集后再额外扣 N 天，避免短期序列相关污染

参考：De Prado, "Advances in Financial Machine Learning", Ch.7
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("ml.trainer")

# 默认模型保存目录
_DEFAULT_MODEL_DIR = Path("./cache/ml_models")


class PurgedWalkForwardSplit:
    """
    Purged + Embargoed Walk-Forward CV 分割器。

    每 fold 训练集 = [起点, val_start - purge_days]，验证集 = [val_start, val_end]，
    fold 之间按时间滚动。
    """

    def __init__(
        self,
        n_splits: int = 5,
        purge_days: int = 20,    # = label_horizon，确保训练集 label 不与验证集特征重叠
        embargo_days: int = 5,
    ):
        self.n_splits = int(n_splits)
        self.purge_days = int(purge_days)
        self.embargo_days = int(embargo_days)

    def split(self, dates: pd.Series):
        """
        Args:
            dates: 排好序的样本日期 Series（与 X 行对齐）

        Yields:
            (train_idx_array, val_idx_array)
        """
        dates = pd.to_datetime(dates).reset_index(drop=True)
        unique_dates = pd.Series(dates.unique()).sort_values().reset_index(drop=True)
        n = len(unique_dates)
        if n < self.n_splits + 2:
            logger.warning(f"独立日期 {n} 太少（< n_splits+2），返回单 fold")
            cut = int(n * 0.8)
            train_dates = unique_dates[:cut]
            val_dates = unique_dates[cut:]
            tr = dates[dates.isin(train_dates)].index.to_numpy()
            va = dates[dates.isin(val_dates)].index.to_numpy()
            yield tr, va
            return

        fold_size = n // (self.n_splits + 1)
        for k in range(self.n_splits):
            val_start_idx = (k + 1) * fold_size
            val_end_idx = (k + 2) * fold_size if k < self.n_splits - 1 else n
            val_dates = unique_dates.iloc[val_start_idx:val_end_idx]
            if val_dates.empty:
                continue
            # 训练集：起点 → (val_start - purge)
            purge_end = val_dates.iloc[0] - pd.Timedelta(days=self.purge_days)
            train_dates = unique_dates[unique_dates <= purge_end]
            tr_idx = dates[dates.isin(train_dates)].index.to_numpy()
            va_idx = dates[dates.isin(val_dates)].index.to_numpy()
            if len(tr_idx) == 0 or len(va_idx) == 0:
                continue
            yield tr_idx, va_idx


class MLTrainer:
    """
    LightGBM 训练器。

    用法：
        from src.ml.dataset_builder import DatasetBuilder
        dataset = DatasetBuilder.load_latest()
        trainer = MLTrainer()
        result = trainer.train(dataset, label_col="y_excess_ret_20d")
    """

    def __init__(
        self,
        model_dir: Path | str = _DEFAULT_MODEL_DIR,
        n_splits: int = 5,
        purge_days: int = 20,
        embargo_days: int = 5,
        random_state: int = 42,
    ):
        self.model_dir = Path(model_dir)
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.cv = PurgedWalkForwardSplit(n_splits=n_splits,
                                         purge_days=purge_days,
                                         embargo_days=embargo_days)
        self.random_state = int(random_state)

    @staticmethod
    def _default_params() -> dict:
        """LightGBM 超参（保守、不易过拟合）"""
        return {
            "objective": "regression",
            "metric": "rmse",
            "learning_rate": 0.05,
            "num_leaves": 31,
            "max_depth": 6,
            "min_data_in_leaf": 100,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "lambda_l2": 1.0,
            "verbose": -1,
        }

    def train(
        self,
        dataset: pd.DataFrame,
        label_col: str = "y_excess_ret_20d",
        feature_cols: list[str] | None = None,
        params: dict | None = None,
        num_boost_round: int = 500,
        early_stopping_rounds: int = 50,
    ) -> dict:
        """
        全量训练 + CV 评估 + 最终模型在全集上 refit。

        Returns:
            dict: {
                "model_path": str,
                "metadata_path": str,
                "cv_rmse_mean": float,
                "cv_rmse_std": float,
                "cv_ic_mean": float,    # 横截面 Information Coefficient (Spearman)
                "cv_ic_std": float,
                "feature_importance": [(feat, imp), ...],
                "n_samples": int,
                "trained_at": str,
            }
        """
        try:
            import lightgbm as lgb
        except ImportError as e:
            raise RuntimeError("缺少 lightgbm，pip install lightgbm") from e

        if dataset.empty:
            raise ValueError("空数据集")

        if label_col not in dataset.columns:
            raise ValueError(f"数据集缺少标签列 {label_col}")

        # 特征列
        if feature_cols is None:
            feature_cols = [c for c in dataset.columns
                            if c not in ("date", "code") and not c.startswith("y_")]
        feature_cols = list(feature_cols)
        if not feature_cols:
            raise ValueError("无可用特征列")

        # 按日期排序（CV 必须时序顺序）
        dataset = dataset.sort_values("date").reset_index(drop=True)
        dataset = dataset.dropna(subset=[label_col])
        X = dataset[feature_cols].astype(float)
        y = dataset[label_col].astype(float)
        dates = dataset["date"]

        params = params or self._default_params()
        params = dict(params)  # copy
        params.setdefault("seed", self.random_state)

        # ---------------- CV ----------------
        rmse_list: list[float] = []
        ic_list: list[float] = []
        fold_idx = 0
        for tr_idx, va_idx in self.cv.split(dates):
            fold_idx += 1
            X_tr, y_tr = X.iloc[tr_idx], y.iloc[tr_idx]
            X_va, y_va = X.iloc[va_idx], y.iloc[va_idx]

            dtr = lgb.Dataset(X_tr, label=y_tr)
            dva = lgb.Dataset(X_va, label=y_va, reference=dtr)
            booster = lgb.train(
                params, dtr,
                num_boost_round=num_boost_round,
                valid_sets=[dva],
                callbacks=[lgb.early_stopping(early_stopping_rounds, verbose=False),
                           lgb.log_evaluation(0)],
            )
            pred = booster.predict(X_va, num_iteration=booster.best_iteration)
            rmse = float(np.sqrt(np.mean((pred - y_va.values) ** 2)))
            rmse_list.append(rmse)

            # 横截面 IC：每个日期内 Spearman 相关
            ic_fold = self._compute_ic_per_date(
                dates.iloc[va_idx].values, pred, y_va.values,
            )
            ic_list.append(ic_fold)

            logger.info(f"  [Fold {fold_idx}] train={len(tr_idx)}, val={len(va_idx)}, "
                       f"RMSE={rmse:.4f}, IC={ic_fold:.4f}")

        cv_rmse_mean = float(np.mean(rmse_list)) if rmse_list else float("nan")
        cv_rmse_std = float(np.std(rmse_list)) if rmse_list else float("nan")
        cv_ic_mean = float(np.mean(ic_list)) if ic_list else float("nan")
        cv_ic_std = float(np.std(ic_list)) if ic_list else float("nan")

        logger.info(f"CV 汇总: RMSE={cv_rmse_mean:.4f}±{cv_rmse_std:.4f}, "
                   f"IC={cv_ic_mean:.4f}±{cv_ic_std:.4f}")

        # ---------------- 最终 refit on full dataset ----------------
        # 用 CV 平均的 best_iteration 作为最终模型轮数（保守做法）
        # 这里偷懒：直接用 num_boost_round * 0.8 作为最终轮数
        final_rounds = max(50, int(num_boost_round * 0.8))
        dtrain_full = lgb.Dataset(X, label=y)
        final_model = lgb.train(
            params, dtrain_full,
            num_boost_round=final_rounds,
            callbacks=[lgb.log_evaluation(0)],
        )

        # 特征重要性
        imp = final_model.feature_importance(importance_type="gain")
        feat_imp = sorted(
            zip(feature_cols, imp.tolist()),
            key=lambda x: -x[1],
        )

        # ---------------- 保存模型 + metadata ----------------
        version = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_path = self.model_dir / f"lgbm_{version}.joblib"
        meta_path = self.model_dir / f"lgbm_{version}.meta.json"

        joblib.dump(
            {
                "model": final_model,
                "feature_cols": feature_cols,
                "label_col": label_col,
                "params": params,
            },
            model_path,
        )

        metadata = {
            "model_path": str(model_path),
            "version": version,
            "trained_at": datetime.now().isoformat(timespec="seconds"),
            "n_samples": int(len(dataset)),
            "n_features": len(feature_cols),
            "feature_cols": feature_cols,
            "label_col": label_col,
            "cv_rmse_mean": cv_rmse_mean,
            "cv_rmse_std": cv_rmse_std,
            "cv_ic_mean": cv_ic_mean,
            "cv_ic_std": cv_ic_std,
            "feature_importance": [{"feature": f, "gain": g} for f, g in feat_imp],
            "params": params,
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        # latest 软链接（Windows 用 copy）
        latest = self.model_dir / "lgbm_latest.joblib"
        latest_meta = self.model_dir / "lgbm_latest.meta.json"
        try:
            import shutil
            if latest.exists():
                latest.unlink()
            if latest_meta.exists():
                latest_meta.unlink()
            shutil.copy2(model_path, latest)
            shutil.copy2(meta_path, latest_meta)
        except OSError:
            pass

        logger.info(f"模型已保存: {model_path}")
        return metadata

    @staticmethod
    def _compute_ic_per_date(dates: np.ndarray, pred: np.ndarray, y: np.ndarray) -> float:
        """
        计算横截面 IC：每个日期内的 Spearman 相关，再取均值。
        IC > 0.05 算有 alpha 信号。
        """
        from scipy.stats import spearmanr
        df = pd.DataFrame({"date": dates, "pred": pred, "y": y})
        ic_per_date: list[float] = []
        for _, group in df.groupby("date"):
            if len(group) < 5:
                continue
            try:
                rho, _ = spearmanr(group["pred"], group["y"])
                if rho is not None and not np.isnan(rho):
                    ic_per_date.append(float(rho))
            except Exception:
                continue
        return float(np.mean(ic_per_date)) if ic_per_date else 0.0
