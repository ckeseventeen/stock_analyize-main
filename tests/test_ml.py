"""
tests/test_ml.py — B 路径自学习模块测试

覆盖：
  - 特征工程（dataset_builder._compute_features）
  - Purged Walk-Forward CV 切分正确性
  - MLTrainer 在合成数据上能正常收敛
  - MLPredictor 加载 + 推理
  - MLScoreFactor 在模型不存在时返回 NaN（降级）
  - MLTopKCondition 接口
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ml.dataset_builder import DatasetBuilder
from src.ml.trainer import MLTrainer, PurgedWalkForwardSplit


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def synth_daily_300d() -> pd.DataFrame:
    """合成 300 个交易日的 OHLCV"""
    np.random.seed(7)
    n = 300
    dates = pd.bdate_range("2022-01-03", periods=n)
    base = 50 + np.cumsum(np.random.randn(n) * 0.3 + 0.01)
    close = base + np.random.randn(n) * 0.2
    open_ = close + np.random.randn(n) * 0.1
    high = np.maximum(open_, close) + np.abs(np.random.randn(n) * 0.3)
    low = np.minimum(open_, close) - np.abs(np.random.randn(n) * 0.3)
    vol = np.random.randint(1e5, 5e6, size=n).astype(float)
    turnover = np.random.uniform(0.5, 5.0, size=n)
    return pd.DataFrame({
        "日期": dates, "开盘": open_, "最高": high, "最低": low,
        "收盘": close, "成交量": vol, "换手率": turnover,
    })


@pytest.fixture
def synth_dataset() -> pd.DataFrame:
    """构造 5000 条样本的 ML 数据集（特征 + 标签）"""
    np.random.seed(42)
    n_codes = 30
    n_dates = 200
    dates = pd.bdate_range("2022-01-03", periods=n_dates, freq="W-FRI")
    rows = []
    for code in [f"{i:06d}" for i in range(n_codes)]:
        # 给每只股票一组随机特征
        f1 = np.random.randn(n_dates)
        f2 = np.random.randn(n_dates) * 2
        f3 = np.random.randn(n_dates) + f1 * 0.3  # 让模型可学到一定相关性
        # y = 真实信号 + 噪声
        y = 0.5 * f1 + 0.2 * f2 + np.random.randn(n_dates) * 0.5
        for i, dt in enumerate(dates):
            rows.append({
                "date": dt, "code": code,
                "ret_5d": f1[i], "ret_20d": f2[i], "ret_60d": f3[i],
                "rsi_14": np.random.uniform(20, 80),
                "macd_hist": np.random.randn(),
                "y_excess_ret_20d": float(y[i]),
            })
    return pd.DataFrame(rows)


# =============================================================================
# 特征工程
# =============================================================================

@pytest.mark.unit
class TestFeatureEngineering:
    def test_compute_features_shape(self, synth_daily_300d):
        feats = DatasetBuilder._compute_features(synth_daily_300d)
        assert not feats.empty
        # 应有日期列 + 多个特征列
        assert "date" in feats.columns
        for col in ("ret_5d", "ret_20d", "rsi_14", "macd", "macd_hist"):
            assert col in feats.columns
        # 行数应与输入一致
        assert len(feats) == len(synth_daily_300d)

    def test_compute_features_short_df_returns_empty(self):
        short = pd.DataFrame({
            "日期": pd.bdate_range("2024-01-01", periods=50),
            "开盘": np.random.randn(50) + 100,
            "最高": np.random.randn(50) + 101,
            "最低": np.random.randn(50) + 99,
            "收盘": np.random.randn(50) + 100,
            "成交量": np.random.randint(1e5, 1e6, 50).astype(float),
        })
        feats = DatasetBuilder._compute_features(short)
        assert feats.empty, "样本不足应返回空"

    def test_compute_features_handles_chinese_columns(self, synth_daily_300d):
        # 输入是中文列，函数内部应自动 rename
        feats = DatasetBuilder._compute_features(synth_daily_300d)
        # 关键特征应有非 NaN 值（除前若干行）
        assert feats["ret_20d"].notna().any()
        assert feats["rsi_14"].notna().any()


# =============================================================================
# Purged CV
# =============================================================================

@pytest.mark.unit
class TestPurgedWalkForwardSplit:
    def test_no_overlap_train_val(self):
        """验证集和训练集日期不重叠，且训练集 < val_start - purge_days"""
        dates = pd.Series(pd.bdate_range("2022-01-03", periods=200, freq="W-FRI"))
        cv = PurgedWalkForwardSplit(n_splits=3, purge_days=20)
        for tr, va in cv.split(dates):
            tr_dates = dates.iloc[tr]
            va_dates = dates.iloc[va]
            # 验证集起点 - 20 天 >= 训练集终点
            assert tr_dates.max() <= va_dates.min() - pd.Timedelta(days=20), \
                "Purge 失败：训练集与验证集距离不足"

    def test_n_splits_count(self):
        dates = pd.Series(pd.bdate_range("2022-01-03", periods=200, freq="W-FRI"))
        cv = PurgedWalkForwardSplit(n_splits=5, purge_days=20)
        folds = list(cv.split(dates))
        assert len(folds) >= 1
        assert len(folds) <= 5

    def test_chronological_order(self):
        """每个 fold 验证集应在训练集之后"""
        dates = pd.Series(pd.bdate_range("2022-01-03", periods=200, freq="W-FRI"))
        cv = PurgedWalkForwardSplit(n_splits=3)
        for tr, va in cv.split(dates):
            assert dates.iloc[tr].max() < dates.iloc[va].min()


# =============================================================================
# 训练
# =============================================================================

@pytest.mark.unit
class TestMLTrainer:
    def test_train_runs_and_saves_model(self, synth_dataset, tmp_path):
        trainer = MLTrainer(model_dir=str(tmp_path), n_splits=3, purge_days=20)
        meta = trainer.train(
            synth_dataset,
            label_col="y_excess_ret_20d",
            num_boost_round=50,
            early_stopping_rounds=10,
        )
        assert "model_path" in meta
        assert os.path.exists(meta["model_path"])
        # latest 副本也应存在
        assert (tmp_path / "lgbm_latest.joblib").exists()
        assert (tmp_path / "lgbm_latest.meta.json").exists()

    def test_cv_metrics_present(self, synth_dataset, tmp_path):
        trainer = MLTrainer(model_dir=str(tmp_path), n_splits=3)
        meta = trainer.train(
            synth_dataset, label_col="y_excess_ret_20d",
            num_boost_round=30, early_stopping_rounds=5,
        )
        for key in ("cv_rmse_mean", "cv_ic_mean", "feature_importance"):
            assert key in meta

    def test_empty_dataset_raises(self, tmp_path):
        trainer = MLTrainer(model_dir=str(tmp_path))
        with pytest.raises(ValueError):
            trainer.train(pd.DataFrame(), label_col="y_excess_ret_20d")


# =============================================================================
# 推理 + 因子集成
# =============================================================================

@pytest.mark.unit
class TestMLPredictor:
    def test_predictor_loads_after_training(self, synth_dataset, tmp_path, monkeypatch):
        # 训练后用同一目录加载
        trainer = MLTrainer(model_dir=str(tmp_path), n_splits=3)
        trainer.train(synth_dataset, label_col="y_excess_ret_20d", num_boost_round=30)

        from src.ml.predictor import MLPredictor

        model_path = tmp_path / "lgbm_latest.joblib"
        predictor = MLPredictor(model_path=model_path)
        # 单条预测
        pred = predictor.predict_one({
            "ret_5d": 0.5, "ret_20d": 1.0, "ret_60d": 0.3,
            "rsi_14": 50, "macd_hist": 0.1,
        })
        assert isinstance(pred, float)

    def test_predictor_missing_features_uses_nan(self, synth_dataset, tmp_path):
        trainer = MLTrainer(model_dir=str(tmp_path), n_splits=3)
        trainer.train(synth_dataset, label_col="y_excess_ret_20d", num_boost_round=30)

        from src.ml.predictor import MLPredictor

        predictor = MLPredictor(model_path=tmp_path / "lgbm_latest.joblib")
        # 只给 1 个特征，其他用 NaN 自动填充
        pred = predictor.predict_one({"ret_5d": 0.5})
        assert isinstance(pred, float)

    def test_get_predictor_returns_none_when_no_model(self, tmp_path, monkeypatch):
        from src.ml import predictor as predictor_module

        # 重置单例 + 指向不存在的目录
        predictor_module.reset_predictor()
        # 临时把默认路径指向空目录
        monkeypatch.setattr(predictor_module, "_DEFAULT_MODEL_DIR", tmp_path / "empty")
        p = predictor_module.get_predictor()
        assert p is None, "模型不存在时应优雅返回 None"


# =============================================================================
# 因子 + 筛选条件接入
# =============================================================================

@pytest.mark.unit
class TestMLFactorIntegration:
    def test_ml_score_factor_returns_nan_when_no_model(self, synth_daily_300d, monkeypatch):
        """模型不存在时，MLScoreFactor.safe_calculate 应返回 NaN（不抛异常）"""
        from src.analysis.factor.ml_factor import MLScoreFactor
        from src.ml import predictor as predictor_module

        predictor_module.reset_predictor()
        # 指向不存在的目录
        monkeypatch.setattr(
            predictor_module, "_DEFAULT_MODEL_DIR",
            pytest.importorskip("pathlib").Path("/nonexistent/dir/xxx"),
        )

        factor = MLScoreFactor()
        data = {"daily_df": synth_daily_300d, "weekly_df": pd.DataFrame()}
        result = factor.safe_calculate(data)
        assert np.isnan(result), "无模型时应返回 NaN"

    def test_ml_top_k_condition_passes_when_no_model(self, synth_daily_300d, monkeypatch):
        """ml_top_k 条件在模型不存在时应放行（视为通过）"""
        from src.analysis.screening.conditions import MLTopKCondition
        from src.ml import predictor as predictor_module

        predictor_module.reset_predictor()
        monkeypatch.setattr(
            predictor_module, "_DEFAULT_MODEL_DIR",
            pytest.importorskip("pathlib").Path("/nonexistent/dir/xxx"),
        )

        cond = MLTopKCondition(top_k=50)
        spot_row = pd.Series({"代码": "600519", "名称": "测试"})
        assert cond.evaluate_full(spot_row, synth_daily_300d) is True

    def test_ml_top_k_in_condition_registry(self):
        from src.analysis.screening.conditions import CONDITION_REGISTRY
        assert "ml_top_k" in CONDITION_REGISTRY

    def test_ml_score_in_factor_registry(self):
        from src.analysis.factor import FACTOR_REGISTRY
        assert "ml_score" in FACTOR_REGISTRY


# =============================================================================
# Backtest 策略接入
# =============================================================================

@pytest.mark.unit
class TestMLStrategyRegistration:
    def test_ml_rebalance_in_strategy_registry(self):
        from src.strategy.backtest import STRATEGY_REGISTRY
        assert "ml_rebalance" in STRATEGY_REGISTRY

    def test_ml_rebalance_has_param_schema(self):
        from src.strategy.backtest import STRATEGY_PARAM_SCHEMAS
        assert "ml_rebalance" in STRATEGY_PARAM_SCHEMAS
        keys = {p["key"] for p in STRATEGY_PARAM_SCHEMAS["ml_rebalance"]}
        assert {"rebalance_days", "buy_threshold", "sell_threshold"} <= keys


# =============================================================================
# Scheduler job 注册
# =============================================================================

@pytest.mark.unit
class TestSchedulerIntegration:
    def test_ml_retrain_builder_registered(self):
        from src.automation.scheduler import JOB_BUILDERS
        assert "ml_retrain" in JOB_BUILDERS

    def test_ml_retrain_callable_constructable(self):
        from src.automation.scheduler import JOB_BUILDERS
        builder = JOB_BUILDERS["ml_retrain"]
        callable_fn = builder({
            "label_horizon_days": 20,
            "start_date": "2020-01-01",
            "num_boost_round": 100,
            "n_splits": 3,
        })
        assert callable(callable_fn)
