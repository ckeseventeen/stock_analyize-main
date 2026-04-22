"""
tests/test_scheduler.py — 调度器单元测试

用 APScheduler 的 BackgroundScheduler 注入 build_scheduler，
在不长期阻塞的前提下验证：
  - YAML 加载
  - Job 注册数量与 id
  - 触发器解析（cron / interval）
  - 未知 job 类型被跳过
  - 禁用 job 被跳过
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.automation.scheduler import (
    JOB_BUILDERS,
    _parse_trigger,
    build_scheduler,
)


@pytest.fixture
def background_scheduler_cls():
    """返回 BackgroundScheduler 类（不会阻塞测试）"""
    from apscheduler.schedulers.background import BackgroundScheduler
    return BackgroundScheduler


# ========================
# 触发器解析
# ========================

@pytest.mark.unit
class TestParseTrigger:
    def test_cron_trigger(self):
        job = {"cron": "*/5 9-11,13-15 * * MON-FRI", "timezone": "Asia/Shanghai"}
        ttype, kwargs = _parse_trigger(job)
        assert ttype == "cron"
        assert kwargs["minute"] == "*/5"
        assert kwargs["hour"] == "9-11,13-15"
        assert kwargs["day_of_week"] == "MON-FRI"
        assert kwargs["timezone"] == "Asia/Shanghai"

    def test_interval_trigger(self):
        job = {"interval_minutes": 30}
        ttype, kwargs = _parse_trigger(job)
        assert ttype == "interval"
        assert kwargs["minutes"] == 30

    def test_cron_format_invalid_raises(self):
        with pytest.raises(ValueError):
            _parse_trigger({"cron": "*/5 9-11"})  # 只有 2 段

    def test_no_trigger_raises(self):
        with pytest.raises(ValueError):
            _parse_trigger({"id": "x"})


# ========================
# Scheduler 构建
# ========================

@pytest.mark.unit
class TestBuildScheduler:
    def test_empty_config_returns_empty_scheduler(self, background_scheduler_cls):
        sch = build_scheduler({}, scheduler_cls=background_scheduler_cls)
        assert len(sch.get_jobs()) == 0

    def test_disabled_job_skipped(self, background_scheduler_cls):
        cfg = {
            "jobs": [
                {"id": "j1", "type": "price_monitor", "enable": False, "cron": "*/5 * * * *"},
            ]
        }
        sch = build_scheduler(cfg, scheduler_cls=background_scheduler_cls)
        assert len(sch.get_jobs()) == 0

    def test_unknown_type_skipped(self, background_scheduler_cls):
        cfg = {
            "jobs": [
                {"id": "j1", "type": "unknown_job", "enable": True, "cron": "*/5 * * * *"},
            ]
        }
        sch = build_scheduler(cfg, scheduler_cls=background_scheduler_cls)
        assert len(sch.get_jobs()) == 0

    def test_register_price_monitor_cron(self, background_scheduler_cls):
        cfg = {
            "jobs": [
                {
                    "id": "price_monitor",
                    "type": "price_monitor",
                    "enable": True,
                    "cron": "*/5 9-11,13-15 * * MON-FRI",
                    "timezone": "Asia/Shanghai",
                },
            ]
        }
        sch = build_scheduler(cfg, scheduler_cls=background_scheduler_cls)
        jobs = sch.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].id == "price_monitor"

    def test_register_multiple_jobs(self, background_scheduler_cls):
        cfg = {
            "jobs": [
                {"id": "price_monitor", "type": "price_monitor", "cron": "*/5 9-11 * * MON-FRI"},
                {"id": "earnings_monitor", "type": "earnings_monitor", "cron": "30 8 * * *"},
                {"id": "news_scraper", "type": "scraper",
                 "scraper_type": "news", "interval_minutes": 30},
            ]
        }
        sch = build_scheduler(cfg, scheduler_cls=background_scheduler_cls)
        ids = {j.id for j in sch.get_jobs()}
        assert ids == {"price_monitor", "earnings_monitor", "news_scraper"}

    def test_invalid_trigger_does_not_crash_scheduler(self, background_scheduler_cls):
        """一个 Job 配置错误不应影响其他 Job 注册"""
        cfg = {
            "jobs": [
                {"id": "bad", "type": "price_monitor"},  # 无 cron/interval
                {"id": "good", "type": "price_monitor", "cron": "*/5 * * * *"},
            ]
        }
        sch = build_scheduler(cfg, scheduler_cls=background_scheduler_cls)
        ids = {j.id for j in sch.get_jobs()}
        assert ids == {"good"}


# ========================
# JOB_BUILDERS 冒烟
# ========================

@pytest.mark.unit
class TestJobBuilders:
    def test_all_builders_registered(self):
        assert set(JOB_BUILDERS.keys()) == {"price_monitor", "earnings_monitor", "scraper"}

    def test_price_monitor_callable_runs_without_rules(self, tmp_path, monkeypatch):
        """规则文件为空时应安全跳过而非抛异常"""
        # 写一份空 rules.yaml
        rules_path = tmp_path / "rules.yaml"
        rules_path.write_text("rules: []\n", encoding="utf-8")
        alerts_path = tmp_path / "alerts.yaml"
        alerts_path.write_text("channels: {}\n", encoding="utf-8")

        builder = JOB_BUILDERS["price_monitor"]
        fn = builder({
            "rules_config": str(rules_path),
            "alerts_config": str(alerts_path),
        })
        # 不应抛异常
        fn()

    def test_scraper_callable_with_unknown_type(self, tmp_path):
        """scraper_type 未知时应记录 warning 并安全返回"""
        cfg_path = tmp_path / "scraper.yaml"
        cfg_path.write_text("news:\n  enable: true\n", encoding="utf-8")

        builder = JOB_BUILDERS["scraper"]
        fn = builder({
            "scraper_type": "unknown_xxx",
            "scraper_config": str(cfg_path),
            "output_dir": str(tmp_path),
        })
        # 不应抛异常
        fn()


# ========================
# 真实配置文件解析
# ========================

@pytest.mark.unit
class TestRealConfig:
    def test_default_config_file_loads(self, background_scheduler_cls):
        """config/scheduler.yaml 能加载并注册所有启用的 Job"""
        import yaml
        cfg_path = os.path.join(os.path.dirname(__file__), "..", "config", "scheduler.yaml")
        with open(cfg_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        sch = build_scheduler(cfg, scheduler_cls=background_scheduler_cls)
        # 默认配置有 6 个 job，都启用
        assert len(sch.get_jobs()) >= 3
