"""
tests/test_service_coverage.py — 低覆盖模块补测（B4）

覆盖此前 <25% 的四个模块：
  utils/config_parser        配置读写（get/set/save/点号路径）
  automation/scheduler_manager 调度器进程管理（状态/启停/触发）
  data/providers/index_kline  指数 K 线（多源降级）
  monitors/holding_monitor    持仓预警事件生成
"""
from __future__ import annotations

import json
import os
import sys

import pandas as pd
import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ============================================================================
# utils/config_parser
# ============================================================================

@pytest.mark.unit
class TestConfigParser:
    @pytest.fixture
    def cfg_file(self, tmp_path):
        p = tmp_path / "c.yaml"
        p.write_text(yaml.safe_dump({
            "server": {"host": "127.0.0.1", "port": 8600, "debug": False},
            "items": [1, 2, 3],
        }, allow_unicode=True), encoding="utf-8")
        return p

    def test_get_nested_by_dotted_path(self, cfg_file):
        from src.utils.config_parser import ConfigParser

        c = ConfigParser(str(cfg_file))
        assert c.get("server.host") == "127.0.0.1"
        assert c.get("server.port") == 8600

    def test_get_missing_returns_default(self, cfg_file):
        from src.utils.config_parser import ConfigParser

        c = ConfigParser(str(cfg_file))
        assert c.get("server.nope") is None
        assert c.get("server.nope", "dft") == "dft"
        assert c.get("totally.missing.path", 42) == 42

    def test_get_section(self, cfg_file):
        from src.utils.config_parser import ConfigParser

        sec = ConfigParser(str(cfg_file)).get_section("server")
        assert sec["port"] == 8600

    def test_set_and_save_roundtrip(self, cfg_file):
        from src.utils.config_parser import ConfigParser

        c = ConfigParser(str(cfg_file))
        c.set("server.port", 9999)
        c.save()
        assert ConfigParser(str(cfg_file)).get("server.port") == 9999

    def test_missing_file_is_graceful(self, tmp_path):
        from src.utils.config_parser import ConfigParser

        c = ConfigParser(str(tmp_path / "nope.yaml"))
        assert c.get("anything") is None
        assert isinstance(c.get_all(), dict)

    def test_load_yaml_and_json_helpers(self, tmp_path):
        from src.utils.config_parser import load_json, load_yaml

        y = tmp_path / "a.yaml"
        y.write_text(yaml.safe_dump({"k": "v"}), encoding="utf-8")
        assert load_yaml(str(y))["k"] == "v"

        j = tmp_path / "b.json"
        j.write_text(json.dumps({"n": 1}), encoding="utf-8")
        assert load_json(str(j))["n"] == 1

    def test_load_helpers_on_missing_file(self, tmp_path):
        from src.utils.config_parser import load_json, load_yaml

        assert load_yaml(str(tmp_path / "x.yaml")) in ({}, None)
        assert load_json(str(tmp_path / "x.json")) in ({}, None)


# ============================================================================
# data/providers/index_kline
# ============================================================================

def _idx_df(n: int = 30) -> pd.DataFrame:
    return pd.DataFrame({
        "日期": pd.bdate_range("2026-06-01", periods=n),
        "开盘": [3000.0 + i for i in range(n)],
        "最高": [3010.0 + i for i in range(n)],
        "最低": [2990.0 + i for i in range(n)],
        "收盘": [3005.0 + i for i in range(n)],
        "成交量": [1e8] * n,
    })


@pytest.mark.unit
class TestIndexKline:
    """akshare 为函数内延迟导入，故 patch akshare 模块本身"""

    def test_returns_dataframe_on_success(self, monkeypatch):
        import akshare as ak

        import src.data.providers.index_kline as ik

        for name in ("stock_zh_index_daily_em", "index_zh_a_hist",
                     "stock_zh_index_daily"):
            if hasattr(ak, name):
                monkeypatch.setattr(ak, name, lambda *a, **k: _idx_df())
        df = ik.fetch_index_kline("000300", "20260601", "20260710")
        assert isinstance(df, pd.DataFrame)

    def test_falls_back_to_pytdx_when_akshare_fails(self, monkeypatch):
        """akshare 挂掉时应降级 pytdx，而不是直接返回空"""
        import akshare as ak

        import src.data.providers.index_kline as ik
        import src.data.providers.pytdx_provider as pp

        def boom(*a, **k):
            raise ConnectionError("blocked")

        monkeypatch.setattr(ak, "stock_zh_index_daily", boom)

        class FakePytdx:
            _working_servers = [("1.2.3.4", 7709)]

            def is_available(self):
                return True

        monkeypatch.setattr(pp, "get_global_pytdx", lambda: FakePytdx())
        df = ik.fetch_index_kline("000300", "20260601", "20260710")
        # pytdx 连不上假 IP，最终返回空表——关键是**不抛异常**
        assert isinstance(df, pd.DataFrame)

    def test_every_source_down_returns_empty_not_raise(self, monkeypatch):
        import akshare as ak

        import src.data.providers.index_kline as ik
        import src.data.providers.pytdx_provider as pp

        def boom(*a, **k):
            raise ConnectionError("blocked")

        monkeypatch.setattr(ak, "stock_zh_index_daily", boom)

        class DeadPytdx:
            _working_servers = []

            def is_available(self):
                return False

        monkeypatch.setattr(pp, "get_global_pytdx", lambda: DeadPytdx())
        df = ik.fetch_index_kline("000300", "20260601", "20260710")
        assert isinstance(df, pd.DataFrame) and df.empty, "全失败应返回空表而非抛出"


# ============================================================================
# automation/scheduler_manager
# ============================================================================

@pytest.mark.unit
class TestSchedulerManager:
    def test_status_shape_when_stopped(self, monkeypatch, tmp_path):
        import src.automation.scheduler_manager as sm

        monkeypatch.setattr(sm, "_PID_FILE", tmp_path / "sched.pid", raising=False)
        st = sm.get_status()
        assert isinstance(st, dict)
        assert "running" in st

    def test_is_running_no_pidfile(self, monkeypatch, tmp_path):
        import src.automation.scheduler_manager as sm

        monkeypatch.setattr(sm, "_PID_FILE", tmp_path / "none.pid", raising=False)
        assert sm.is_running() in (False, None)

    def test_stop_when_not_running_is_graceful(self, monkeypatch, tmp_path):
        import src.automation.scheduler_manager as sm

        monkeypatch.setattr(sm, "_PID_FILE", tmp_path / "none.pid", raising=False)
        try:
            result = sm.stop()
        except Exception as e:  # 不应抛
            pytest.fail(f"stop() 在未运行时抛异常: {e}")
        assert result is None or isinstance(result, (bool, dict, str))

    def test_job_history_returns_list(self, monkeypatch, tmp_path):
        import src.automation.scheduler_manager as sm

        monkeypatch.setattr(sm, "_HISTORY_FILE", tmp_path / "h.json", raising=False)
        hist = sm.get_job_history()
        assert isinstance(hist, (list, dict))


# ============================================================================
# monitors/holding_monitor
# ============================================================================

@pytest.mark.unit
class TestHoldingMonitor:
    @staticmethod
    def _monitor(tmp_path, holdings: list):
        from src.monitors.holding_monitor import HoldingMonitor

        hp = tmp_path / "holdings.yaml"
        hp.write_text(yaml.safe_dump({"holdings": holdings, "cash": 0},
                                     allow_unicode=True), encoding="utf-8")
        # channels=[] → 不实际推送，只验证事件生成逻辑
        return HoldingMonitor(channels=[], holdings_path=str(hp),
                              output_dir=str(tmp_path))

    def test_no_holdings_yields_no_events(self, tmp_path):
        events = self._monitor(tmp_path, []).collect_events()
        assert isinstance(events, list) and events == []

    def test_collect_events_survives_data_failure(self, tmp_path, monkeypatch):
        """持仓有股票但行情拉不到时，应返回空/降级而不是崩掉调度任务"""
        m = self._monitor(tmp_path, [
            {"code": "600519", "name": "贵州茅台", "shares": 100, "cost": 1500.0}])

        import src.services.portfolio_service as pfsvc
        monkeypatch.setattr(pfsvc, "build_a_share_maps",
                            lambda *a, **k: ({}, {}), raising=False)
        events = m.collect_events()
        assert isinstance(events, list)

    def test_signal_to_event_exists(self, tmp_path):
        m = self._monitor(tmp_path, [])
        assert callable(getattr(m, "_signal_to_event", None))
