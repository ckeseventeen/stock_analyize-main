"""
src/services/ml_service.py — ML 模型训练/状态的服务层

存在意义：训练编排（建数据集 → 训练 → 重置预测器 → 后台线程与状态跟踪）
本属于业务逻辑，之前内联在 API 路由函数里——CLI 与调度器要复用只能复制一遍，
且训练状态是路由函数的局部变量，多次调用互相看不见（真 bug：并发点击
"训练"会启动多个训练任务，因为 status 每次都是新的局部 dict）。
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from src.utils.logger import get_logger

logger = get_logger("ml_service")

# 训练默认参数（与原 API 内联实现保持一致）
DEFAULT_LABEL_HORIZON_DAYS = 20
DEFAULT_START_DATE = "2022-01-01"
DEFAULT_MAX_CODES = 50
DEFAULT_N_SPLITS = 3
DEFAULT_BOOST_ROUNDS = 300


@dataclass
class TrainingState:
    """进程级训练状态（模块级单例，避免并发重复启动）"""

    running: bool = False
    done: bool = False
    error: str | None = None
    started_at: str = ""
    finished_at: str = ""
    lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> dict:
        return {
            "running": self.running,
            "done": self.done,
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


_STATE = TrainingState()


def training_status() -> dict:
    """当前训练状态"""
    return _STATE.snapshot()


MODEL_DIR = Path("cache/ml_models")
MODEL_PATH = MODEL_DIR / "lgbm_latest.joblib"
MODEL_META_PATH = MODEL_DIR / "lgbm_latest.meta.json"


def model_status() -> dict:
    """
    模型是否已训练 + 元信息（读模型文件旁的 meta.json，不加载模型本体）。
    """
    if not MODEL_PATH.exists():
        return {"trained": False, "message": "ML 模型未训练"}

    meta_data: dict = {}
    if MODEL_META_PATH.exists():
        try:
            meta_data = json.loads(MODEL_META_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"读取模型元信息失败: {type(e).__name__}: {e}")

    # 键名对齐 trainer 实际写入的字段（n_samples/feature_cols/label_col）。
    # 历史上读的是 train_samples/features/label_horizon_days——三个都不存在，
    # 于是"已训练"的模型在前端显示样本数与特征数为空
    features = meta_data.get("feature_cols") or meta_data.get("features") or []
    label_col = meta_data.get("label_col", "")
    horizon = DEFAULT_LABEL_HORIZON_DAYS
    if label_col.endswith("d") and "_" in label_col:      # y_excess_ret_20d → 20
        tail = label_col.rsplit("_", 1)[-1].rstrip("d")
        horizon = int(tail) if tail.isdigit() else horizon

    return {
        "trained": True,
        "model_path": str(MODEL_PATH),
        "cv_ic_mean": meta_data.get("cv_ic_mean"),
        "cv_ic_std": meta_data.get("cv_ic_std"),
        "cv_rmse_mean": meta_data.get("cv_rmse_mean"),
        "train_samples": meta_data.get("n_samples") or meta_data.get("train_samples"),
        "n_features": meta_data.get("n_features") or len(features),
        "features": features,
        "label_col": label_col,
        "label_horizon": horizon,
        "trained_at": meta_data.get("trained_at"),
    }


def _run_training(*, label_horizon_days: int, start_date: str,
                  max_codes: int, n_splits: int, boost_rounds: int) -> None:
    """实际训练流程（在后台线程里跑）"""
    from src.ml.dataset_builder import DatasetBuilder
    from src.ml.predictor import reset_predictor
    from src.ml.trainer import MLTrainer

    try:
        logger.info("ML 训练开始：建数据集…")
        builder = DatasetBuilder(label_horizon_days=label_horizon_days)
        dataset = builder.build(start_date=start_date, end_date=None,
                                max_codes=max_codes)
        builder.save(dataset)

        logger.info("ML 训练：拟合模型…")
        trainer = MLTrainer(n_splits=n_splits)
        trainer.train(dataset, label_col=f"y_excess_ret_{label_horizon_days}d",
                      num_boost_round=boost_rounds)
        reset_predictor()

        with _STATE.lock:
            _STATE.done = True
            _STATE.error = None
        logger.info("ML 训练完成")
    except Exception as e:
        logger.error(f"ML 训练失败: {type(e).__name__}: {e}", exc_info=True)
        with _STATE.lock:
            _STATE.error = f"{type(e).__name__}: {e}"
            _STATE.done = False
    finally:
        with _STATE.lock:
            _STATE.running = False
            _STATE.finished_at = datetime.now().isoformat(timespec="seconds")


def start_training(
    *,
    label_horizon_days: int = DEFAULT_LABEL_HORIZON_DAYS,
    start_date: str = DEFAULT_START_DATE,
    max_codes: int = DEFAULT_MAX_CODES,
    n_splits: int = DEFAULT_N_SPLITS,
    boost_rounds: int = DEFAULT_BOOST_ROUNDS,
) -> tuple[bool, str]:
    """
    启动后台训练。

    修复了原 API 内联版本的并发缺陷：训练状态改为**进程级**单例并加锁，
    重复点击不会再启动多个训练任务。

    Returns:
        (是否已启动, 提示消息)
    """
    with _STATE.lock:
        if _STATE.running:
            return False, "训练已在进行中，请等待完成"
        _STATE.running = True
        _STATE.done = False
        _STATE.error = None
        _STATE.started_at = datetime.now().isoformat(timespec="seconds")
        _STATE.finished_at = ""

    threading.Thread(
        target=_run_training,
        kwargs={"label_horizon_days": label_horizon_days,
                "start_date": start_date, "max_codes": max_codes,
                "n_splits": n_splits, "boost_rounds": boost_rounds},
        daemon=True, name="ml-train",
    ).start()
    return True, "ML 模型训练已启动（后台执行，预计 5-15 分钟）"
