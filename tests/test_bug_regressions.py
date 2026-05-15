"""
tests/test_bug_regressions.py — 审计报告 (~/.claude/plans/bug-bright-dawn.md) 的回归测试

每个 test 对应一个或多个 bug ID，确保它们将来不会再现。
"""
from __future__ import annotations

import json
import os
import sys
import threading
from datetime import datetime

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.analysis.factor.fcf_analyzer import FCFAnalyzer
from src.core.analyzer import BaseAnalyzer


class _StubAnalyzer(BaseAnalyzer):
    """跳过子类清洗，直接使用注入的 fin_ts"""

    def _clean_financial_data(self) -> pd.DataFrame:
        return self.raw_fin


# =========================================================================
# B6: 闰年 Feb 29 → 非闰年时 latest_date.replace 抛 ValueError 未捕获
# =========================================================================

@pytest.mark.unit
class TestB6LeapYear:
    def test_feb29_does_not_crash(self):
        """latest_date = 2024-02-29 时不应崩溃"""
        dates = pd.to_datetime([
            "2024-02-29",  # 闰年同期
            "2023-12-31",
            "2023-02-28",  # 去年同期（非闰年）
            "2022-12-31",
        ])
        df = pd.DataFrame({
            "归母净利润": [200, 800, 180, 700],
            "营业总收入": [400, 1600, 360, 1400],
            "营业成本": [200, 800, 180, 700],
        }, index=dates).sort_index(ascending=False)
        analyzer = _StubAnalyzer(df, pd.DataFrame(),
                                 {"price": 10, "market_cap": 1e9},
                                 {"name": "test", "code": "x", "valuation": "pe",
                                  "pe_range": [10, 20, 30]})
        result = analyzer.process()
        # 不抛异常即视为修复
        assert isinstance(result, dict)


# =========================================================================
# B7: _detect_fiscal_year_month 应正确识别非 12 月财年（如苹果 9 月）
# =========================================================================

@pytest.mark.unit
class TestB7FiscalYearDetection:
    def test_september_fiscal_year_detected_correctly(self):
        """9 月财年应识别为 9 而非 12"""
        dates = pd.to_datetime([
            "2024-06-30",  # Q3 季报
            "2023-09-30",  # 年报（金额最大）
            "2023-06-30",
            "2022-09-30",
            "2021-09-30",
        ])
        df = pd.DataFrame({
            "归母净利润": [70e9, 97e9, 65e9, 99e9, 94e9],
            "营业总收入": [140e9, 383e9, 130e9, 394e9, 365e9],
        }, index=dates).sort_index(ascending=False)
        analyzer = _StubAnalyzer(df, pd.DataFrame(),
                                 {"price": 100, "market_cap": 1e12},
                                 {"name": "AAPL", "code": "AAPL", "valuation": "pe",
                                  "pe_range": [15, 25, 35]})
        assert analyzer._detect_fiscal_year_month(df) == 9

    def test_june_fiscal_year_detected_correctly(self):
        """6 月财年应识别为 6（微软场景）"""
        dates = pd.to_datetime([
            "2024-03-31",  # Q3 季报
            "2023-06-30",  # 年报
            "2023-03-31",
            "2022-06-30",
            "2021-06-30",
        ])
        df = pd.DataFrame({
            "归母净利润": [25e9, 72e9, 20e9, 73e9, 61e9],
        }, index=dates).sort_index(ascending=False)
        analyzer = _StubAnalyzer(df, pd.DataFrame(),
                                 {"price": 100, "market_cap": 1e12},
                                 {"name": "MSFT", "code": "MSFT", "valuation": "pe",
                                  "pe_range": [20, 30, 40]})
        assert analyzer._detect_fiscal_year_month(df) == 6


# =========================================================================
# B8: 中文金额单位 "万亿" 未处理
# =========================================================================

@pytest.mark.unit
class TestB8ChineseUnits:
    def test_wan_yi_converts_to_1e12(self):
        from src.core.analyzer import AStockAnalyzer
        raw = pd.DataFrame({"指标": ["营业总收入"], "20231231": ["1.5万亿"]})
        analyzer = AStockAnalyzer(
            raw, pd.DataFrame(),
            {"price": 10, "market_cap": 1e9},
            {"name": "test", "code": "x", "valuation": "pe", "pe_range": [10, 20, 30]},
        )
        fin_ts = analyzer._clean_financial_data()
        # "1.5万亿" → 1.5e12
        assert abs(fin_ts.iloc[0, 0] - 1.5e12) < 1e6, fin_ts

    def test_yi_still_works(self):
        from src.core.analyzer import AStockAnalyzer
        raw = pd.DataFrame({"指标": ["营业总收入"], "20231231": ["3.2亿"]})
        analyzer = AStockAnalyzer(
            raw, pd.DataFrame(),
            {"price": 10, "market_cap": 1e9},
            {"name": "test", "code": "x", "valuation": "pe", "pe_range": [10, 20, 30]},
        )
        fin_ts = analyzer._clean_financial_data()
        assert abs(fin_ts.iloc[0, 0] - 3.2e8) < 1e3


# =========================================================================
# B4: FCFAnalyzer 季度数据应自动 × 4 年化
# =========================================================================

@pytest.mark.unit
class TestB4FCFAnnualization:
    def test_quarterly_data_auto_annualized(self):
        """季度数据 FCF Yield 应按 × 4 年化"""
        quarterly_idx = pd.date_range("2023-03-31", periods=4, freq="3ME")
        df = pd.DataFrame({
            "operating_cash_flow": [25e8] * 4,
            "capex": [5e8] * 4,
            "revenue": [100e8] * 4,
            "net_profit": [20e8] * 4,
        }, index=quarterly_idx)

        analyzer = FCFAnalyzer(df, market_cap=1000e8)
        # 自动检测：相邻间隔 ~90 天 → annualized=False
        assert not analyzer.annualized, "应识别为季度数据"

        analyzer.calculate_metrics()
        sc = analyzer.generate_scorecard()
        # 季度 FCF = 25-5 = 20亿；年化后 = 80亿；Yield = 80/1000 = 8%
        # 8% > 5% → yield 满分 20
        assert sc["scores"]["yield"] == 20

    def test_annual_data_not_double_annualized(self):
        annual_idx = pd.date_range("2020-12-31", periods=4, freq="YE")
        df = pd.DataFrame({
            "operating_cash_flow": [100e8] * 4,
            "capex": [20e8] * 4,
            "revenue": [400e8] * 4,
            "net_profit": [80e8] * 4,
        }, index=annual_idx)

        analyzer = FCFAnalyzer(df, market_cap=1000e8)
        assert analyzer.annualized is True
        analyzer.calculate_metrics()
        sc = analyzer.generate_scorecard()
        # 年度 FCF = 100-20 = 80亿；Yield = 80/1000 = 8% → 满分
        assert sc["scores"]["yield"] == 20


# =========================================================================
# B2/B9: AlertStateStore 跨进程 UTC 时间 + 文件锁
# =========================================================================

@pytest.mark.unit
class TestB2B9AlertState:
    def test_utc_timestamp_format(self, tmp_path):
        from src.automation.alert.state import AlertStateStore

        path = tmp_path / "state.json"
        store = AlertStateStore(path=path)
        store.mark_fired("test_key")

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        ts = data["test_key"]["fired_at"]
        # 带时区的 ISO 应能 parse 出 tzinfo
        parsed = datetime.fromisoformat(ts)
        assert parsed.tzinfo is not None, f"时间戳应带时区: {ts}"

    def test_was_fired_within_cooldown(self, tmp_path):
        from src.automation.alert.state import AlertStateStore

        store = AlertStateStore(path=tmp_path / "state.json")
        store.mark_fired("ev")
        assert store.was_fired("ev", cooldown_hours=1) is True

    def test_thread_safe(self, tmp_path):
        """同进程多线程并发写入不丢条目"""
        from src.automation.alert.state import AlertStateStore

        store = AlertStateStore(path=tmp_path / "state.json")

        def writer(prefix: str):
            for i in range(20):
                store.mark_fired(f"{prefix}_{i}")

        threads = [threading.Thread(target=writer, args=(f"t{i}",)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        records = store.all_records()
        assert len(records) == 4 * 20, f"期望 80 条，实际 {len(records)}"


# =========================================================================
# B17: 缓存文件魔术头校验
# =========================================================================

@pytest.mark.unit
class TestB17CacheHeader:
    def test_corrupted_cache_returns_none(self, tmp_path):
        from src.data.providers.cache_manager import CacheManager

        cm = CacheManager(cache_dir=str(tmp_path), ttl_hours=24)
        # 写入一个"看似正确但没有魔术头"的旧格式 pkl
        bad_path = cm._cache_path("evil_key")
        with open(bad_path, "wb") as f:
            f.write(b"\x80\x04Nq\x00.")  # 裸 pickle，no header
        result = cm.get("evil_key")
        assert result is None
        # 损坏文件应被自动删除
        assert not bad_path.exists()

    def test_valid_cache_roundtrip(self, tmp_path):
        from src.data.providers.cache_manager import CacheManager

        cm = CacheManager(cache_dir=str(tmp_path), ttl_hours=24)
        df = pd.DataFrame({"a": [1, 2, 3]})
        cm.set("good", df)
        loaded = cm.get("good")
        pd.testing.assert_frame_equal(loaded, df)


# =========================================================================
# B5/SEC2: 路径穿越校验
# =========================================================================

@pytest.mark.unit
class TestB5PathTraversal:
    def test_reject_parent_traversal(self):
        from src.web.config_ops import validate_user_path

        with pytest.raises(ValueError):
            validate_user_path("../../../etc/passwd")

    def test_reject_outside_allowed_roots(self, tmp_path):
        from src.web.config_ops import validate_user_path

        with pytest.raises(ValueError):
            validate_user_path(str(tmp_path / "evil.yaml"))

    def test_accept_legit_config(self):
        from src.web.config_ops import CONFIG_DIR, validate_user_path

        # 仅当 config 目录里存在 screen_config.yaml 时才校验
        target = CONFIG_DIR / "screen_config.yaml"
        if target.exists():
            result = validate_user_path(str(target))
            assert result.exists()


# =========================================================================
# SEC3: SSRF 防护
# =========================================================================

@pytest.mark.unit
class TestSEC3SSRF:
    def test_block_localhost(self):
        from src.automation.alert.base import _validate_url

        assert _validate_url("http://127.0.0.1:8080/api") is False
        assert _validate_url("http://localhost/api") is False

    def test_block_aws_metadata(self):
        from src.automation.alert.base import _validate_url

        assert _validate_url("http://169.254.169.254/latest/meta-data/") is False

    def test_block_private_ip(self):
        from src.automation.alert.base import _validate_url

        # 10.0.0.0/8 私网
        assert _validate_url("http://10.0.0.5/webhook") is False

    def test_allow_official_serverchan(self):
        from src.automation.alert.base import _validate_url

        assert _validate_url("https://sctapi.ftqq.com/SCT123.send") is True

    def test_allow_private_when_explicit(self):
        from src.automation.alert.base import _validate_url

        # 用户显式 allow_private=True（自建 Bark）
        assert _validate_url("http://10.0.0.5/webhook", allow_private=True) is True


# =========================================================================
# B10: ScreenerRuleStrategy 应过滤 Spot-only 条件
# =========================================================================

@pytest.mark.unit
class TestB10SpotFilter:
    def test_spot_conditions_filtered_out(self):
        """market_cap、pe_range 等 spot 条件应被过滤"""
        # 通过 backtrader Strategy 的 params 模拟入口
        from src.strategy.backtest.screener_rule import ScreenerRuleStrategy

        configs = [
            {"type": "market_cap", "min": 100},   # 应过滤
            {"type": "pe_range", "min": 10},      # 应过滤
            {"type": "rsi_oversold", "threshold": 30},  # 保留
        ]

        # 直接调用未实例化的 _init_conditions（通过 mock self.params）
        class FakeStrat:
            params = type("P", (), {"buy_conditions": configs, "sell_conditions": []})()

        obj = ScreenerRuleStrategy._init_conditions(FakeStrat(), configs)
        # 只保留 rsi_oversold
        assert len(obj) == 1
        assert obj[0].name == "rsi_oversold"

    def test_unknown_condition_raises(self):
        from src.strategy.backtest.screener_rule import ScreenerRuleStrategy

        configs = [{"type": "totally_made_up_condition"}]

        class FakeStrat:
            params = type("P", (), {"buy_conditions": configs, "sell_conditions": []})()

        with pytest.raises(ValueError, match="未知的回测条件类型"):
            ScreenerRuleStrategy._init_conditions(FakeStrat(), configs)


# =========================================================================
# File lock cross-process simulation（B1 / B2 都依赖）
# =========================================================================

@pytest.mark.unit
class TestFileLock:
    def test_file_lock_serializes(self, tmp_path):
        from src.utils.file_lock import file_lock

        log_path = tmp_path / "log.txt"
        log_path.write_text("")

        def writer(idx: int):
            for _ in range(50):
                with file_lock(tmp_path / "lock"):
                    cur = log_path.read_text()
                    cur += f"{idx}\n"
                    log_path.write_text(cur)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        lines = [ln for ln in log_path.read_text().split("\n") if ln]
        assert len(lines) == 200, f"应有 200 行，实际 {len(lines)}"

    def test_pid_lock_idempotent(self, tmp_path):
        from src.utils.file_lock import acquire_pid_lock, release_pid_lock

        pid_path = tmp_path / "scheduler.pid"
        assert acquire_pid_lock(pid_path) is True
        # 同一进程再次获取应识别为自己（释放后可重新获取）
        release_pid_lock(pid_path)
        assert acquire_pid_lock(pid_path) is True
        release_pid_lock(pid_path)
