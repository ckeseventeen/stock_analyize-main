"""
src/automation/scheduler_manager.py — 进程级单例调度器管理

将 APScheduler BackgroundScheduler 嵌入 Streamlit 进程，
启动后在后台守护线程中按 scheduler.yaml 配置周期执行 Job。

核心 API:
    start()          — 幂等启动（首次调用启动，后续跳过）
    stop()           — 优雅停止
    get_status()     — 返回调度器状态 + 所有 Job 信息
    get_job_history() — 最近 N 次执行记录
    trigger_job(id)  — 立即手动触发一个 Job

使用示例（在 streamlit_app.py 中）:
    from src.automation.scheduler_manager import start as start_scheduler
    start_scheduler()  # 幂等，Streamlit rerun 不会重复启动
"""
from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any

from src.utils.logger import get_logger

logger = get_logger("scheduler_mgr")

# ========================
# 模块级全局状态（进程级单例）
# ========================
_scheduler = None          # BackgroundScheduler 实例
_started = False           # 是否已启动
_lock = threading.Lock()   # 保护 _scheduler 和 _started 的并发修改

# 执行历史（有界队列，最近 200 条）
_history: deque[dict] = deque(maxlen=200)

# 默认配置路径
_DEFAULT_CONFIG = Path("./config/scheduler.yaml")


# ========================
# 执行历史监听器
# ========================

def _on_job_executed(event):
    """APScheduler EVENT_JOB_EXECUTED 回调"""
    _history.appendleft({
        "job_id": event.job_id,
        "run_time": event.scheduled_run_time.isoformat(timespec="seconds")
            if event.scheduled_run_time else datetime.now().isoformat(timespec="seconds"),
        "status": "success",
        "duration": f"{event.retval:.1f}s" if isinstance(event.retval, (int, float)) else "-",
        "error": None,
    })


def _on_job_error(event):
    """APScheduler EVENT_JOB_ERROR 回调"""
    _history.appendleft({
        "job_id": event.job_id,
        "run_time": event.scheduled_run_time.isoformat(timespec="seconds")
            if event.scheduled_run_time else datetime.now().isoformat(timespec="seconds"),
        "status": "error",
        "duration": "-",
        "error": str(event.exception)[:200] if event.exception else "unknown",
    })


# ========================
# 核心 API
# ========================

def start(config_path: str | Path | None = None) -> bool:
    """
    幂等启动调度器。

    首次调用：读取配置 → 构建 BackgroundScheduler → 注册 Job → 启动后台线程。
    后续调用：直接跳过，返回 True。

    Args:
        config_path: scheduler.yaml 路径，None 时用默认路径

    Returns:
        True = 调度器正在运行
    """
    global _scheduler, _started

    if _started and _scheduler is not None:
        return True

    with _lock:
        # 双重检查（另一个线程可能刚启动完）
        if _started and _scheduler is not None:
            return True

        try:
            from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
            from apscheduler.schedulers.background import BackgroundScheduler

            from src.automation.scheduler import _load_yaml, build_scheduler

            cfg_path = Path(config_path) if config_path else _DEFAULT_CONFIG
            logger.info(f"[scheduler_mgr] 加载配置: {cfg_path}")
            config = _load_yaml(cfg_path)

            scheduler = build_scheduler(config, scheduler_cls=BackgroundScheduler)

            if not scheduler.get_jobs():
                logger.warning("[scheduler_mgr] 无有效 Job，调度器未启动")
                return False

            # 注册执行历史监听器
            scheduler.add_listener(_on_job_executed, EVENT_JOB_EXECUTED)
            scheduler.add_listener(_on_job_error, EVENT_JOB_ERROR)

            scheduler.start()
            _scheduler = scheduler
            _started = True

            job_ids = [j.id for j in scheduler.get_jobs()]
            logger.info(f"[scheduler_mgr] ✅ 后台调度器已启动，{len(job_ids)} 个 Job: {job_ids}")
            return True

        except Exception as e:
            logger.error(f"[scheduler_mgr] 启动失败: {e}", exc_info=True)
            return False


def stop() -> bool:
    """优雅停止调度器，等待当前 Job 完成。"""
    global _scheduler, _started

    with _lock:
        if not _started or _scheduler is None:
            return True
        try:
            _scheduler.shutdown(wait=True)
            logger.info("[scheduler_mgr] 调度器已停止")
        except Exception as e:
            logger.error(f"[scheduler_mgr] 停止异常: {e}")
        _started = False
        _scheduler = None
        return True


def is_running() -> bool:
    """调度器是否正在运行。"""
    return _started and _scheduler is not None and _scheduler.running


def get_status() -> dict[str, Any]:
    """
    返回调度器运行状态和所有 Job 的信息。

    Returns:
        {
            "running": bool,
            "job_count": int,
            "total_executions": int,
            "jobs": [
                {
                    "id": str,
                    "name": str,
                    "trigger": str,
                    "next_run": str | None,
                    "pending": bool,
                },
                ...
            ]
        }
    """
    running = is_running()
    jobs_info = []

    if running and _scheduler is not None:
        for job in _scheduler.get_jobs():
            next_run = job.next_run_time
            jobs_info.append({
                "id": job.id,
                "name": job.name or job.id,
                "trigger": str(job.trigger),
                "next_run": next_run.strftime("%Y-%m-%d %H:%M:%S") if next_run else "暂停中",
                "pending": job.pending,
            })

    return {
        "running": running,
        "job_count": len(jobs_info),
        "total_executions": len(_history),
        "jobs": jobs_info,
    }


def get_job_history(limit: int = 50) -> list[dict]:
    """返回最近 N 条执行记录（最新在前）。"""
    return list(_history)[:limit]


def trigger_job(job_id: str) -> bool:
    """
    立即手动触发一个 Job（不影响原有调度）。

    Returns:
        True = 触发成功
    """
    if not is_running() or _scheduler is None:
        logger.warning(f"[scheduler_mgr] 调度器未运行，无法触发 {job_id}")
        return False

    job = _scheduler.get_job(job_id)
    if job is None:
        logger.warning(f"[scheduler_mgr] Job 不存在: {job_id}")
        return False

    try:
        job.modify(next_run_time=datetime.now())
        logger.info(f"[scheduler_mgr] 已手动触发 Job: {job_id}")
        return True
    except Exception as e:
        logger.error(f"[scheduler_mgr] 触发 {job_id} 失败: {e}")
        return False


def pause_job(job_id: str) -> bool:
    """暂停一个 Job。"""
    if not is_running() or _scheduler is None:
        return False
    try:
        _scheduler.pause_job(job_id)
        logger.info(f"[scheduler_mgr] Job 已暂停: {job_id}")
        return True
    except Exception as e:
        logger.error(f"[scheduler_mgr] 暂停 {job_id} 失败: {e}")
        return False


def resume_job(job_id: str) -> bool:
    """恢复一个已暂停的 Job。"""
    if not is_running() or _scheduler is None:
        return False
    try:
        _scheduler.resume_job(job_id)
        logger.info(f"[scheduler_mgr] Job 已恢复: {job_id}")
        return True
    except Exception as e:
        logger.error(f"[scheduler_mgr] 恢复 {job_id} 失败: {e}")
        return False
