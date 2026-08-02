"""
tests/test_ml_feature_parity.py — 训练/推理特征一致性（train-serve skew 事故回归）

## 事故

用户报「ML 模型已训练，但回测里 ML 策略不参与」。查下来模型是好的，
问题在**推理侧只算了 15/55 维特征**：

  - 训练侧把 15 基础 + 31 Alpha158 + 9 估值 拼成 55 维
  - 推理侧 `predict_from_daily_df` 只调 `_compute_features`（15 维）

其余 40 维全是 NaN。LightGBM 对 NaN 走默认分支，预测整体塌向一个偏负的常数
（同一模型在训练特征上 67.8% 的预测为正，在推理特征上 0% 为正），
买入阈值 ≥0 于是永不触发——**每只股票 0 笔交易**。

还有两个同源的隐蔽点：
  - 换手率没并进日线 → turnover_ma20 量纲差 100 倍（0.612 vs 0.006）
  - 日线自带「换手率」列时 merge 撞成 _x/_y → 该特征静默变 NaN

## 契约

装配只允许有**一个入口** `DatasetBuilder.build_feature_frame`，
训练与推理都走它；推理产出的特征必须覆盖模型 `feature_cols` 的全部维度。
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ml.dataset_builder import DatasetBuilder


def _fake_daily(n: int = 400, with_turnover: bool = False) -> pd.DataFrame:
    """构造足够长的日线（Alpha158 与 MA250 都需要长窗口）"""
    rng = np.random.default_rng(42)
    dates = pd.bdate_range("2023-01-02", periods=n)
    close = pd.Series(100 * np.cumprod(1 + rng.normal(0.0004, 0.015, n)))
    df = pd.DataFrame({
        "日期": dates,
        "开盘": close * 0.995,
        "最高": close * 1.02,
        "最低": close * 0.98,
        "收盘": close,
        "成交量": rng.integers(1e6, 5e6, n).astype(float),
        "成交额": rng.integers(1e8, 5e8, n).astype(float),
    })
    if with_turnover:
        # 部分日线通道自带换手率，且口径与 Baostock 不同
        df["换手率"] = rng.uniform(5, 15, n)
    return df


def _fake_val(daily: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    n = len(daily)
    return pd.DataFrame({
        "date": pd.to_datetime(daily["日期"]),
        "pe_ttm": rng.uniform(8, 30, n),
        "pb_mrq": rng.uniform(0.5, 3, n),
        "ps_ttm": rng.uniform(1, 6, n),
        "turn": rng.uniform(0.3, 1.2, n),
    })


@pytest.mark.unit
class TestSingleAssemblyEntry:
    def test_base_features_present(self):
        f = DatasetBuilder.build_feature_frame(_fake_daily(), use_alpha158=False)
        assert not f.empty
        for col in ("ret_20d", "ma20_dev", "rsi_14", "macd"):
            assert col in f.columns

    def test_alpha158_included_by_default(self):
        """这条是事故核心：默认必须带上 Alpha158，否则推理缺 31 维"""
        f = DatasetBuilder.build_feature_frame(_fake_daily())
        a158 = [c for c in f.columns if c.startswith("a158_")]
        assert len(a158) >= 20, f"Alpha158 特征缺失，只有 {len(a158)} 个"

    def test_valuation_features_included_when_val_given(self):
        daily = _fake_daily()
        f = DatasetBuilder.build_feature_frame(daily, df_val=_fake_val(daily))
        for col in ("pe_ttm", "pe_ttm_z252", "pb_mrq", "ps_ttm_chg_60d"):
            assert col in f.columns

    def test_missing_val_degrades_gracefully(self):
        """拿不到估值序列时只该缺那 9 维，不能整体失败"""
        f = DatasetBuilder.build_feature_frame(_fake_daily(), df_val=None)
        assert not f.empty
        assert "pe_ttm" not in f.columns
        assert any(c.startswith("a158_") for c in f.columns)


@pytest.mark.unit
class TestTurnoverAlignment:
    def test_turnover_comes_from_valuation_source(self):
        """换手率必须取自 Baostock turn——训练用的就是它，量纲才对得上"""
        daily = _fake_daily()
        val = _fake_val(daily)
        f = DatasetBuilder.build_feature_frame(daily, df_val=val)
        assert "turnover_ma20" in f.columns
        tail = f["turnover_ma20"].dropna()
        assert not tail.empty, "turnover_ma20 全 NaN"
        # turn 取值 0.3~1.2，其 20 日均值必须落在这个量级
        assert 0.1 < float(tail.iloc[-1]) < 2.0

    def test_local_turnover_column_does_not_shadow(self):
        """
        日线自带「换手率」时不能撞成 _x/_y——那会让 turnover_ma20 静默变 NaN。
        自带列取值 5~15，Baostock 的 turn 取值 0.3~1.2，据此可判断用了哪个源。
        """
        daily = _fake_daily(with_turnover=True)
        val = _fake_val(daily)
        f = DatasetBuilder.build_feature_frame(daily, df_val=val)
        assert not any(c.startswith("换手率_") for c in f.columns)
        tail = f["turnover_ma20"].dropna()
        assert not tail.empty, "撞列导致 turnover_ma20 全 NaN（事故现象）"
        assert float(tail.iloc[-1]) < 2.0, "用成了本地换手率列，量纲与训练不一致"

    def test_string_dates_do_not_break_merge(self):
        """不同取数通道给的日期 dtype 不同，装配函数不该依赖调用方"""
        daily = _fake_daily()
        daily["日期"] = daily["日期"].dt.strftime("%Y-%m-%d")
        f = DatasetBuilder.build_feature_frame(daily, df_val=_fake_val(_fake_daily()))
        assert not f.empty


@pytest.mark.unit
class TestPredictorParity:
    """有模型时才跑：推理必须覆盖模型训练时用到的**全部**特征"""

    def test_inference_covers_all_trained_features(self):
        from src.ml.predictor import get_predictor

        p = get_predictor()
        if p is None:
            pytest.skip("本机未训练模型")
        daily = _fake_daily()
        cov = p.feature_coverage(daily, df_val=_fake_val(daily))
        assert cov["missing"] == [], (
            f"推理缺 {len(cov['missing'])}/{cov['expected']} 维特征："
            f"{cov['missing'][:8]}——LightGBM 会把它们当 NaN，预测将系统性偏移")

    @staticmethod
    def _capture(fn):
        """
        直接挂到 predictor 模块自己的 logger 上。

        项目 logger 设了 propagate=False（见 src/utils/logger.py），
        pytest 的 caplog 挂在 root 上因此抓不到——这里不绕弯子。
        """
        import logging

        import src.ml.predictor as mod
        records: list[logging.LogRecord] = []

        class _Sink(logging.Handler):
            def emit(self, record):
                records.append(record)

        h = _Sink()
        mod.logger.addHandler(h)
        try:
            fn()
        finally:
            mod.logger.removeHandler(h)
        return " ".join(r.getMessage() for r in records)

    def test_drift_is_reported_not_silent(self):
        """特征漂移必须留下告警，不能静默劣化成"策略不交易" """
        from src.ml.predictor import get_predictor

        p = get_predictor()
        if p is None:
            pytest.skip("本机未训练模型")
        text = self._capture(lambda: p._warn_if_features_drifted({"ret_20d": 1.0}))
        assert "特征漂移" in text

    def test_no_warning_when_features_complete(self):
        """特征齐全时不该刷告警，否则真告警会被噪音淹没"""
        from src.ml.predictor import get_predictor

        p = get_predictor()
        if p is None:
            pytest.skip("本机未训练模型")
        text = self._capture(
            lambda: p._warn_if_features_drifted(dict.fromkeys(p.feature_cols, 0.0)))
        assert "特征漂移" not in text
