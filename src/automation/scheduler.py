"""
src/automation/scheduler.py — APScheduler 任务调度器

长驻进程，加载 config/scheduler.yaml 中定义的 Job，按 cron/interval 周期执行：
  - price_monitor    : 价格预警（交易时段内每 5 分钟）
  - earnings_monitor : 财报披露监控（每天 08:30）
  - scraper          : 资讯抓取（多种子类型，按 interval 或 cron 触发）

启动命令：
    python -m src.automation.scheduler
    python -m src.automation.scheduler --config ./config/scheduler.yaml

优雅停止：
    收到 SIGTERM / SIGINT 时 scheduler.shutdown(wait=True)

测试入口：
    build_scheduler(config) 返回未启动的 Scheduler 实例，便于单测注入时钟。
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Callable

import yaml

from src.utils.file_lock import acquire_pid_lock, release_pid_lock
from src.utils.logger import get_logger

logger = get_logger("scheduler")


# 默认配置路径
DEFAULT_CONFIG = Path("./config/scheduler.yaml")


# ========================
# Job 构造工厂
# ========================

def _load_yaml(path: Path | str) -> dict:
    """读取 YAML 配置。缺失或异常时返回空 dict（避免启动直接崩溃）"""
    p = Path(path)
    if not p.exists():
        logger.warning(f"配置文件不存在: {p}，使用空配置")
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"配置文件解析失败 [{p}]: {e}", exc_info=True)
        return {}


def _build_buy_sell_alerts_callable(job_cfg: dict) -> Callable[[], Any]:
    """
    买/卖预警 Job —— 替代旧 price_monitor + batch_signal。

    读取 config/price_alerts.yaml 的 buy_alerts / sell_alerts，逐条评估。
    每条规则可以是单股盯盘（填 code）或批量扫描（填 scopes）。
    """

    def _run():
        from src.automation.alert import AlertStateStore, build_channels
        from src.automation.monitor.buy_sell_alerts import BuySellAlertMonitor

        alerts_cfg = _load_yaml(job_cfg.get("alerts_config", "./config/alerts.yaml"))
        rules_cfg = _load_yaml(job_cfg.get("rules_config", "./config/price_alerts.yaml"))
        buy_alerts = rules_cfg.get("buy_alerts", []) or []
        sell_alerts = rules_cfg.get("sell_alerts", []) or []
        default_cd = int(rules_cfg.get("default_cooldown_hours", 24))

        if not buy_alerts and not sell_alerts:
            logger.info("[buy_sell_alerts] 无任何买卖预警规则，跳过")
            return

        channels = build_channels(alerts_cfg)
        store = AlertStateStore()
        monitor = BuySellAlertMonitor(
            buy_alerts=buy_alerts,
            sell_alerts=sell_alerts,
            channels=channels,
            state_store=store,
            cooldown_hours=default_cd,
        )
        monitor.run()

    return _run


def _build_earnings_monitor_callable(job_cfg: dict) -> Callable[[], Any]:
    """构造 earnings_monitor 可调用对象"""

    def _run():
        from src.automation.alert import AlertStateStore, build_channels
        from src.automation.monitor.earnings_monitor import EarningsMonitor

        alerts_cfg = _load_yaml(job_cfg.get("alerts_config", "./config/alerts.yaml"))
        earnings_cfg = _load_yaml(
            job_cfg.get("earnings_config", "./config/earnings_monitor.yaml")
        )

        channels = build_channels(alerts_cfg)
        store = AlertStateStore()
        monitor = EarningsMonitor(
            config=earnings_cfg,
            channels=channels,
            state_store=store,
            cooldown_hours=int(earnings_cfg.get("cooldown_hours", 72)),
        )
        monitor.run()

    return _run


def _build_scraper_callable(job_cfg: dict) -> Callable[[], Any]:
    """
    构造 scraper 可调用对象。
    job_cfg["scraper_type"] 可为 news / announcements / holdings / research / all
    """
    scraper_type = str(job_cfg.get("scraper_type", "all")).lower()

    def _run():
        from src.data.scrapers import (
            AnnouncementScraper,
            HoldingsScraper,
            NewsScraper,
            ResearchScraper,
            run_all,
        )

        cfg = _load_yaml(job_cfg.get("scraper_config", "./config/scraper.yaml"))
        output_dir = job_cfg.get("output_dir", "./output")

        if scraper_type == "all":
            run_all(cfg, output_dir=output_dir)
            return

        # 单独一种抓取器
        SINGLE_CLS = {
            "news": NewsScraper,
            "announcements": AnnouncementScraper,
            "holdings": HoldingsScraper,
            "research": ResearchScraper,
        }
        cls = SINGLE_CLS.get(scraper_type)
        if not cls:
            logger.warning(f"[scraper] 未知 scraper_type={scraper_type}，跳过")
            return

        section = (cfg.get(scraper_type) or {}).copy()
        section.pop("enable", None)
        try:
            scraper = cls(**section)
            df = scraper.fetch()
            scraper.save_csv(df, output_dir)
            logger.info(f"[scraper:{scraper_type}] 执行完成，{len(df)} 条")
        except Exception as e:
            logger.error(f"[scraper:{scraper_type}] 执行失败: {e}", exc_info=True)

    return _run


def _build_screener_callable(job_cfg: dict) -> Callable[[], Any]:
    """构造一个 screener 可调用对象：收盘后自动跑筛选并推送结果摘要"""

    strategy_ids: list[str] | None = None
    raw_strategies = job_cfg.get("strategies")
    if isinstance(raw_strategies, list) and raw_strategies:
        strategy_ids = [str(s) for s in raw_strategies]
    screen_config = job_cfg.get("screen_config", "./config/screen_config.yaml")

    def _run():
        from src.analysis.screening.screener import StockScreener
        from src.automation.alert import AlertEvent, AlertStateStore, build_channels

        alerts_cfg = _load_yaml(job_cfg.get("alerts_config", "./config/alerts.yaml"))
        channels = build_channels(alerts_cfg)
        # TRIG-4 修复：使用 AlertStateStore 防止 scheduler 重启导致重复推送
        store = AlertStateStore()
        screener_cooldown = int(job_cfg.get("cooldown_hours", 12))

        def _push_event(event: AlertEvent):
            """推送前检查冷却"""
            if store.was_fired(event.event_key, cooldown_hours=screener_cooldown):
                logger.info(f"[screener] 事件 '{event.event_key}' 仍在冷却期内，跳过推送")
                return
            for ch in channels:
                ch.send(event)
            store.mark_fired(event.event_key)

        try:
            logger.info(f"[screener] 开始执行自动筛选，策略: {strategy_ids or '全部'}")
            screener = StockScreener(max_workers=8)
            result = screener.run_from_config(
                screen_config, strategy_ids=strategy_ids,
            )
            if result is None or result.empty:
                msg = f"[screener] 筛选完成，无符合条件股票（策略: {strategy_ids or '全部'}）"
                logger.info(msg)
                event = AlertEvent(
                    title="📊 股票筛选结果",
                    body=msg,
                    event_key="screener:empty",
                    event_type="screener_result",
                )
                _push_event(event)
                return

            total = len(result)
            top5 = result.head(5).to_string(index=False)
            summary = (
                f"📊 每日筛选报告\n"
                f"策略: {strategy_ids or '全部'}\n"
                f"符合条件: {total} 只\n\n"
                f"前5名:\n{top5}"
            )
            logger.info(f"[screener] 筛选完成，{total} 只股票符合条件")
            event = AlertEvent(
                title="📊 股票筛选结果",
                body=summary,
                event_key="screener:result",
                event_type="screener_result",
            )
            _push_event(event)

            # 保存 CSV
            output_dir = job_cfg.get("output_dir", "./output")
            csv_path = Path(output_dir) / "screen_result.csv"
            result.to_csv(csv_path, index=False, encoding="utf-8-sig")
            logger.info(f"[screener] 结果已保存: {csv_path}")

        except Exception as e:
            logger.error(f"[screener] 执行失败: {e}", exc_info=True)
            err_event = AlertEvent(
                title="📊 股票筛选异常",
                body=f"🚨 筛选执行失败: {e}",
                event_key="screener:error",
                event_type="screener_error",
            )
            _push_event(err_event)

    return _run


def _build_ml_retrain_callable(job_cfg: dict) -> Callable[[], Any]:
    """构造 ML 月度重训 Job：拉数据 + 训练 + 重置 predictor 单例（B 路径）"""

    horizon = int(job_cfg.get("label_horizon_days", 20))
    start_date = str(job_cfg.get("start_date", "2020-01-01"))
    max_codes = job_cfg.get("max_codes")
    num_rounds = int(job_cfg.get("num_boost_round", 500))
    n_splits = int(job_cfg.get("n_splits", 5))

    def _run():
        from src.ml.dataset_builder import DatasetBuilder
        from src.ml.predictor import reset_predictor
        from src.ml.trainer import MLTrainer

        try:
            logger.info(f"[ml_retrain] 开始重训：horizon={horizon}d, start={start_date}")
            builder = DatasetBuilder(label_horizon_days=horizon)
            dataset = builder.build(
                start_date=start_date,
                end_date=None,
                max_codes=int(max_codes) if max_codes else None,
            )
            if dataset.empty:
                logger.error("[ml_retrain] 数据集为空，跳过训练")
                return
            builder.save(dataset)

            trainer = MLTrainer(n_splits=n_splits)
            meta = trainer.train(
                dataset,
                label_col=f"y_excess_ret_{horizon}d",
                num_boost_round=num_rounds,
            )
            reset_predictor()
            logger.info(
                f"[ml_retrain] 完成: IC={meta['cv_ic_mean']:.4f}, "
                f"RMSE={meta['cv_rmse_mean']:.4f}, n={meta['n_samples']}"
            )
        except Exception as e:
            logger.error(f"[ml_retrain] 异常: {e}", exc_info=True)

    return _run


# Job 类型 → (可调用工厂, 默认触发方式)
JOB_BUILDERS: dict[str, Callable[[dict], Callable[[], Any]]] = {
    "buy_sell_alerts": _build_buy_sell_alerts_callable,
    "earnings_monitor": _build_earnings_monitor_callable,
    "scraper": _build_scraper_callable,
    "screener": _build_screener_callable,
    "ml_retrain": _build_ml_retrain_callable,
}


# ========================
# 触发器解析
# ========================

def _parse_trigger(job_cfg: dict) -> tuple[str, dict]:
    """
    根据 job 配置解析 APScheduler 触发器类型与参数。

    支持：
      - cron: "*/5 9-11,13-15 * * MON-FRI"  → CronTrigger
      - interval_minutes: 30                 → IntervalTrigger

    Returns:
        (trigger_type, kwargs)  如 ("cron", {"minute": "*/5", ...})
    """
    cron_expr = job_cfg.get("cron")
    interval_minutes = job_cfg.get("interval_minutes")
    timezone = job_cfg.get("timezone", "Asia/Shanghai")

    if cron_expr:
        # 解析 "分 时 日 月 星期"
        parts = str(cron_expr).split()
        if len(parts) != 5:
            raise ValueError(f"cron 表达式格式错误（期望 5 段）: {cron_expr}")
        minute, hour, day, month, day_of_week = parts
        return "cron", {
            "minute": minute,
            "hour": hour,
            "day": day,
            "month": month,
            "day_of_week": day_of_week,
            "timezone": timezone,
        }

    if interval_minutes:
        return "interval", {
            "minutes": int(interval_minutes),
            "timezone": timezone,
        }

    raise ValueError("Job 必须配置 cron 或 interval_minutes 之一")


# ========================
# Scheduler 构建
# ========================

def build_scheduler(config: dict, scheduler_cls=None):
    """
    基于 config 构建 APScheduler 实例并注册所有启用的 Job。

    Args:
        config: scheduler.yaml 解析结果
        scheduler_cls: 可注入自定义 Scheduler 类（测试用 BackgroundScheduler）

    Returns:
        APScheduler 实例（未调用 start()）
    """
    # 延迟导入，允许 APScheduler 未安装时模块本身仍可 import
    if scheduler_cls is None:
        from apscheduler.schedulers.blocking import BlockingScheduler
        scheduler_cls = BlockingScheduler

    scheduler = scheduler_cls()

    jobs = (config or {}).get("jobs", []) or []
    if not jobs:
        logger.warning("scheduler.yaml 无 jobs 配置")
        return scheduler

    for job_cfg in jobs:
        job_id = job_cfg.get("id", "")
        job_type = job_cfg.get("type", "")

        if not job_cfg.get("enable", True):
            logger.info(f"[scheduler] Job [{job_id}] 已禁用，跳过")
            continue

        builder = JOB_BUILDERS.get(job_type)
        if not builder:
            logger.error(f"[scheduler] 未知 Job 类型 '{job_type}'，跳过 [{job_id}]")
            continue

        try:
            raw_func = builder(job_cfg)
            # B23：包装一层 timing wrapper，运行时间超过阈值或触发器间隔的 80% 就告警
            interval_seconds = _estimate_interval_seconds(job_cfg)
            warn_threshold = max(60, interval_seconds * 0.8) if interval_seconds else None
            # TRIG-3：跳过非交易日（默认 buy_sell_alerts/screener 开启）
            skip_non_trading = job_cfg.get(
                "skip_non_trading_day",
                job_type in ("buy_sell_alerts", "screener"),
            )
            func = _wrap_with_timing(
                raw_func, job_id or job_type, warn_threshold,
                skip_non_trading=skip_non_trading,
            )

            trigger_type, trigger_kwargs = _parse_trigger(job_cfg)
            # B23：misfire_grace_time 改为可配，默认放大到 300s 适配抓取/筛选这种慢任务
            misfire_grace = int(job_cfg.get("misfire_grace_time", 300))
            scheduler.add_job(
                func,
                trigger=trigger_type,
                id=job_id or f"{job_type}_{len(jobs)}",
                max_instances=1,  # 同名 Job 不并发
                misfire_grace_time=misfire_grace,
                coalesce=True,  # 多次错过合并为一次
                **trigger_kwargs,
            )
            logger.info(
                f"[scheduler] 已注册 Job: {job_id} ({job_type}) → {trigger_type}={trigger_kwargs}, "
                f"misfire_grace={misfire_grace}s, skip_non_trading={skip_non_trading}"
            )
        except Exception as e:
            logger.error(f"[scheduler] Job [{job_id}] 注册失败: {e}", exc_info=True)

    return scheduler


def _estimate_interval_seconds(job_cfg: dict) -> float | None:
    """估算 Job 触发间隔（秒），用于运行时长告警"""
    iv = job_cfg.get("interval_minutes")
    if iv:
        try:
            return float(iv) * 60.0
        except (ValueError, TypeError):
            return None
    # cron 难以精确估算，返回 None 即可（不做时长告警）
    return None


def _is_trading_day() -> bool:
    """
    简单判断今天是否为 A 股交易日（TRIG-3）。
    排除：周末 + 主要法定假日（春节、国庆、元旦、清明、劳动节、端午、中秋）。
    注意：这里使用硬编码的假日列表，每年需更新一次。
    如需精确判断，可接入 exchange_calendars 库。
    """
    from datetime import date

    today = date.today()
    # 周末
    if today.weekday() >= 5:
        return False
    # 中国法定假日（每年更新，此处为近几年常见假日月日模式）
    md = (today.month, today.day)
    # 元旦 1/1, 劳动节 5/1-5/5, 国庆 10/1-10/7
    fixed_holidays = {
        (1, 1),
        (5, 1), (5, 2), (5, 3), (5, 4), (5, 5),
        (10, 1), (10, 2), (10, 3), (10, 4), (10, 5), (10, 6), (10, 7),
    }
    if md in fixed_holidays:
        return False
    return True


def _wrap_with_timing(func: Callable[[], Any], job_name: str,
                     warn_threshold: float | None,
                     skip_non_trading: bool = False) -> Callable[[], Any]:
    """
    Job 执行计时 + 超时告警包装。

    B23：单次 Job 超过 warn_threshold（默认触发器间隔 80%）时记 warning，
    便于运维及早发现"任务跑不完"的退化。

    TRIG-3：skip_non_trading=True 时，非交易日自动跳过。
    """

    def _wrapped():
        if skip_non_trading and not _is_trading_day():
            logger.info(
                f"[scheduler] Job '{job_name}' 跳过（非交易日）"
            )
            return
        start = time.monotonic()
        try:
            return func()
        finally:
            elapsed = time.monotonic() - start
            if warn_threshold and elapsed > warn_threshold:
                logger.warning(
                    f"[scheduler] Job '{job_name}' 耗时 {elapsed:.1f}s，"
                    f"超过预警阈值 {warn_threshold:.0f}s（可能影响下一次触发）"
                )
            else:
                logger.debug(f"[scheduler] Job '{job_name}' 完成，耗时 {elapsed:.1f}s")

    return _wrapped


# ========================
# 信号处理
# ========================

_current_scheduler = None  # 模块级引用，便于 signal handler 访问
_scheduler_lock = __import__("threading").Lock()


def _install_signal_handlers(scheduler, pid_lock_path: Path | None = None) -> None:
    """安装 SIGTERM / SIGINT 处理器，实现优雅停止（等待 Job 完成）"""
    global _current_scheduler
    with _scheduler_lock:
        _current_scheduler = scheduler

    def _graceful_shutdown(signum, frame):
        logger.info(f"收到信号 {signum}，开始优雅停止...")
        with _scheduler_lock:
            sched = _current_scheduler
        if sched is None:
            if pid_lock_path:
                release_pid_lock(pid_lock_path)
            sys.exit(0)
        try:
            sched.shutdown(wait=True)
            logger.info("调度器已停止")
        except Exception as e:
            logger.error(f"停止调度器异常: {e}")
        finally:
            if pid_lock_path:
                release_pid_lock(pid_lock_path)
        sys.exit(0)

    # Windows 上只能处理 SIGINT/SIGTERM（不支持 SIGHUP 等）
    signal.signal(signal.SIGINT, _graceful_shutdown)
    try:
        signal.signal(signal.SIGTERM, _graceful_shutdown)
    except (AttributeError, ValueError):
        # Windows 可能 SIGTERM 不可用，忽略
        pass


# ========================
# 主入口
# ========================

def main(config_path: str | None = None) -> None:
    """
    命令行入口：加载配置 → 构建调度器 → 安装信号处理 → 启动（阻塞）

    SEC6 修复：用 PID 锁防止双启动（Web 进程 + scheduler 进程重复初始化）。
    锁文件 cache/scheduler.pid，进程退出时自动释放。
    """
    parser = argparse.ArgumentParser(description="Stock Analyze 任务调度器")
    parser.add_argument(
        "--config",
        type=str,
        default=str(DEFAULT_CONFIG),
        help="调度器配置文件路径（默认 ./config/scheduler.yaml）",
    )
    parser.add_argument(
        "--pid-file",
        type=str,
        default="./cache/scheduler.pid",
        help="PID 锁文件路径（用于防止双启动）",
    )
    parser.add_argument(
        "--no-lock",
        action="store_true",
        help="跳过 PID 锁检查（仅供测试/调试）",
    )
    args, _ = parser.parse_known_args()
    cfg_path = config_path or args.config

    pid_lock = Path(args.pid_file)
    if not args.no_lock:
        if not acquire_pid_lock(pid_lock):
            existing = pid_lock.read_text().strip() if pid_lock.exists() else "?"
            logger.error(
                f"调度器已在运行（PID={existing}，锁文件 {pid_lock}）。"
                f"如确认已停止，删除该文件后重启；或加 --no-lock 跳过。"
            )
            sys.exit(2)

    logger.info(f"========== 调度器启动，配置: {cfg_path}，PID={os.getpid()} ==========")
    config = _load_yaml(cfg_path)
    scheduler = build_scheduler(config)

    _install_signal_handlers(scheduler, pid_lock if not args.no_lock else None)

    if not scheduler.get_jobs():
        logger.error("无任何有效 Job，调度器退出")
        if not args.no_lock:
            release_pid_lock(pid_lock)
        return

    logger.info(f"已注册 {len(scheduler.get_jobs())} 个 Job，开始阻塞执行...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("用户中断，调度器退出")
    finally:
        if not args.no_lock:
            release_pid_lock(pid_lock)


if __name__ == "__main__":
    main()
