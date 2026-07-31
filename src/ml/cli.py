"""
src/ml/cli.py — ML 模块命令行入口

用法：
    # 完整流程：拉数据 → 训练 → 保存模型
    python -m src.ml.cli train

    # 只重建数据集（不训练）
    python -m src.ml.cli build-dataset

    # 只训练（用最新数据集）
    python -m src.ml.cli train-only

    # 查看最新模型 metadata
    python -m src.ml.cli info

参数：
    --start-date 2020-01-01    数据起点
    --end-date   2025-12-31    数据终点（None=今天）
    --max-codes  300           上限（调试时小一点）
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.utils.logger import get_logger

logger = get_logger("ml.cli")


def cmd_build_dataset(args: argparse.Namespace) -> int:
    from src.ml.dataset_builder import DatasetBuilder

    builder = DatasetBuilder(label_horizon_days=args.horizon)
    dataset = builder.build(
        start_date=args.start_date,
        end_date=args.end_date,
        max_codes=args.max_codes,
    )
    if dataset.empty:
        logger.error("数据集为空，无法保存")
        return 1
    builder.save(dataset)
    logger.info(f"数据集已保存，共 {len(dataset)} 条样本")
    return 0


def cmd_train_only(args: argparse.Namespace) -> int:
    from src.ml.dataset_builder import DatasetBuilder
    from src.ml.predictor import reset_predictor
    from src.ml.trainer import MLTrainer

    try:
        dataset = DatasetBuilder.load_latest()
    except FileNotFoundError as e:
        logger.error(f"{e}，请先 build-dataset")
        return 1

    trainer = MLTrainer(n_splits=args.n_splits)
    metadata = trainer.train(
        dataset,
        label_col=f"y_excess_ret_{args.horizon}d",
        num_boost_round=args.num_rounds,
    )
    logger.info(
        f"训练完成: IC={metadata['cv_ic_mean']:.4f}±{metadata['cv_ic_std']:.4f}, "
        f"RMSE={metadata['cv_rmse_mean']:.4f}"
    )
    # 重置 predictor 单例，让下次推理使用新模型
    reset_predictor()
    # 推送训练完成通知（如果配置了 alerts）
    _notify_training_done(metadata)
    return 0


def _notify_training_done(metadata: dict) -> None:
    """训练完成发推送（alerts.yaml 未配置或无通道时静默跳过）"""
    try:
        from pathlib import Path

        import yaml as _yaml

        from src.notify import AlertEvent, build_channels

        alerts_path = Path("./config/alerts.yaml")
        if not alerts_path.exists():
            return
        with open(alerts_path, encoding="utf-8") as f:
            alerts_cfg = _yaml.safe_load(f) or {}
        channels = build_channels(alerts_cfg)
        if not channels:
            return

        ic = metadata.get("cv_ic_mean", 0)
        ic_emoji = "🟢" if ic >= 0.05 else ("🟡" if ic >= 0.03 else "🔴")
        title = f"{ic_emoji} ML 模型训练完成"
        body = (
            f"CV IC: {ic:+.4f} ± {metadata.get('cv_ic_std', 0):.4f}\n"
            f"RMSE: {metadata.get('cv_rmse_mean', 0):.4f}\n"
            f"样本: {metadata.get('n_samples', 0):,}\n"
            f"特征: {metadata.get('n_features', 0)}\n"
            f"版本: {metadata.get('version', '')}"
        )
        event = AlertEvent(
            title=title,
            body=body,
            event_key=f"ml_training_done:{metadata.get('version', '')}",
            event_type="ml_training_done",
        )
        for ch in channels:
            ch.send(event)
        logger.info(f"训练完成推送已发出（{len(channels)} 个通道）")
    except Exception as e:
        logger.debug(f"训练完成推送失败（不影响训练本身）: {e}")


def cmd_train(args: argparse.Namespace) -> int:
    """完整流程：build-dataset + train-only"""
    rc = cmd_build_dataset(args)
    if rc != 0:
        return rc
    return cmd_train_only(args)


def cmd_info(args: argparse.Namespace) -> int:
    meta_path = Path("./cache/ml_models/lgbm_latest.meta.json")
    if not meta_path.exists():
        logger.error(f"未找到训练记录: {meta_path}")
        return 1
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    print("=" * 60)
    print(f"模型版本: {meta.get('version')}")
    print(f"训练时间: {meta.get('trained_at')}")
    print(f"样本数:   {meta.get('n_samples')}")
    print(f"特征数:   {meta.get('n_features')}")
    print(f"CV IC:    {meta.get('cv_ic_mean'):.4f} ± {meta.get('cv_ic_std'):.4f}")
    print(f"CV RMSE:  {meta.get('cv_rmse_mean'):.4f} ± {meta.get('cv_rmse_std'):.4f}")
    print()
    print("Top 10 特征重要性 (gain):")
    for item in (meta.get("feature_importance") or [])[:10]:
        print(f"  {item['feature']:20s}  {item['gain']:12.1f}")
    print("=" * 60)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="ML 自学习模块 CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_build = sub.add_parser("build-dataset", help="构建数据集")
    p_build.add_argument("--start-date", default="2020-01-01")
    p_build.add_argument("--end-date", default=None)
    p_build.add_argument("--max-codes", type=int, default=None)
    p_build.add_argument("--horizon", type=int, default=20, help="标签时间窗（天）")

    p_train_only = sub.add_parser("train-only", help="用最新数据集训练")
    p_train_only.add_argument("--horizon", type=int, default=20)
    p_train_only.add_argument("--n-splits", type=int, default=5)
    p_train_only.add_argument("--num-rounds", type=int, default=500)

    p_train = sub.add_parser("train", help="完整流程：build + train")
    p_train.add_argument("--start-date", default="2020-01-01")
    p_train.add_argument("--end-date", default=None)
    p_train.add_argument("--max-codes", type=int, default=None)
    p_train.add_argument("--horizon", type=int, default=20)
    p_train.add_argument("--n-splits", type=int, default=5)
    p_train.add_argument("--num-rounds", type=int, default=500)

    sub.add_parser("info", help="查看最新模型元数据")

    args = parser.parse_args()

    if args.cmd == "build-dataset":
        return cmd_build_dataset(args)
    elif args.cmd == "train-only":
        return cmd_train_only(args)
    elif args.cmd == "train":
        return cmd_train(args)
    elif args.cmd == "info":
        return cmd_info(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
