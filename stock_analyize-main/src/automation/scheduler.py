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
import signal
import sys
from pathlib import Path
from typing import Any, Callable

import yaml

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


def _build_price_monitor_callable(job_cfg: dict) -> Callable[[], Any]:
    """构造一个 price_monitor 可调用对象（闭包捕获 config）"""

    def _run():
        """调度器调用入口：重新读取配置 + 执行一次"""
        from src.automation.alert import AlertStateStore, build_channels
        from src.automation.monitor.price_monitor import PriceMonitor

        alerts_cfg = _load_yaml(job_cfg.get("alerts_config", "./config/alerts.yaml"))
        rules_cfg = _load_yaml(job_cfg.get("rules_config", "./config/price_alerts.yaml"))
        rules = rules_cfg.get("rules", []) or []
        default_cd = int(rules_cfg.get("default_cooldown_hours", 24))

        if not rules:
            logger.info("[price_monitor] 规则为空，跳过本次执行")
            return

        channels = build_channels(alerts_cfg)
        store = AlertStateStore()
        monitor = PriceMonitor(
            rules=rules,
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
            watchlist=earnings_cfg.get("watchlist", {}) or {},
            days_ahead=int(earnings_cfg.get("days_ahead", 30)),
            remind_days_ahead=int(earnings_cfg.get("remind_days_ahead", 3)),
            track_forecasts=bool(earnings_cfg.get("track_forecasts", True)),
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
        from src.data.scraper import (
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


# Job 类型 → (可调用工厂, 默认触发方式)
JOB_BUILDERS: dict[str, Callable[[dict], Callable[[], Any]]] = {
    "price_monitor": _build_price_monitor_callable,
    "earnings_monitor": _build_earnings_monitor_callable,
    "scraper": _build_scraper_callable,
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
            func = builder(job_cfg)
            trigger_type, trigger_kwargs = _parse_trigger(job_cfg)
            scheduler.add_job(
                func,
                trigger=trigger_type,
                id=job_id or f"{job_type}_{len(jobs)}",
                max_instances=1,  # 同名 Job 不并发
                misfire_grace_time=120,  # 错过触发窗口 120s 内仍补发
                coalesce=True,  # 多次错过合并为一次
                **trigger_kwargs,
            )
            logger.info(f"[scheduler] 已注册 Job: {job_id} ({job_type}) → {trigger_type}={trigger_kwargs}")
        except Exception as e:
            logger.error(f"[scheduler] Job [{job_id}] 注册失败: {e}", exc_info=True)

    return scheduler


# ========================
# 信号处理
# ========================

_current_scheduler = None  # 模块级引用，便于 signal handler 访问


def _install_signal_handlers(scheduler) -> None:
    """安装 SIGTERM / SIGINT 处理器，实现优雅停止（等待 Job 完成）"""
    global _current_scheduler
    _current_scheduler = scheduler

    def _graceful_shutdown(signum, frame):
        logger.info(f"收到信号 {signum}，开始优雅停止...")
        try:
            scheduler.shutdown(wait=True)
            logger.info("调度器已停止")
        except Exception as e:
            logger.error(f"停止调度器异常: {e}")
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
    """
    parser = argparse.ArgumentParser(description="Stock Analyze 任务调度器")
    parser.add_argument(
        "--config",
        type=str,
        default=str(DEFAULT_CONFIG),
        help="调度器配置文件路径（默认 ./config/scheduler.yaml）",
    )
    args, _ = parser.parse_known_args()
    cfg_path = config_path or args.config

    logger.info(f"========== 调度器启动，配置: {cfg_path} ==========")
    config = _load_yaml(cfg_path)
    scheduler = build_scheduler(config)

    _install_signal_handlers(scheduler)

    if not scheduler.get_jobs():
        logger.error("无任何有效 Job，调度器退出")
        return

    logger.info(f"已注册 {len(scheduler.get_jobs())} 个 Job，开始阻塞执行...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("用户中断，调度器退出")


if __name__ == "__main__":
    main()
