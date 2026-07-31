"""
tests/test_market_monitor_service.py — 市场监控服务层（B4 覆盖率补强）

该模块 480 语句此前几乎无测试，却是"市场监控"页与两个调度任务的唯一数据源。
全部 akshare 调用均 mock，无网络依赖。
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import akshare as ak  # 被测模块用函数内延迟导入，patch 模块对象本身即可生效

import src.services.market_monitor_service as mms

# ============================================================================
# 交易时段判定（调度器据此决定是否刷新，判错会全天空转或整天不刷新）
# ============================================================================

@pytest.mark.unit
class TestTradingTime:
    @pytest.mark.parametrize("ts,expected", [
        ("2026-07-20 09:29", False),   # 开盘前 1 分钟
        ("2026-07-20 09:30", True),    # 开盘（左闭）
        ("2026-07-20 11:29", True),
        ("2026-07-20 11:30", False),   # 上午收盘（右开）
        ("2026-07-20 12:00", False),   # 午休
        ("2026-07-20 13:00", True),    # 下午开盘
        ("2026-07-20 14:59", True),
        ("2026-07-20 15:00", False),   # 收盘
    ])
    def test_trading_time_boundaries(self, ts, expected):
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M")
        assert dt.weekday() < 5, "该用例应落在工作日"
        assert mms.is_a_share_trading_time(dt) is expected

    def test_weekend_never_trading(self):
        sat = datetime(2026, 7, 25, 10, 0)
        sun = datetime(2026, 7, 26, 14, 0)
        assert sat.weekday() == 5 and sun.weekday() == 6
        assert mms.is_a_share_trading_time(sat) is False
        assert mms.is_a_share_trading_time(sun) is False

    def test_pre_market_returns_bool(self):
        assert isinstance(mms.is_pre_market_time(datetime(2026, 7, 20, 8, 40)), bool)

    def test_defaults_to_now(self):
        assert isinstance(mms.is_a_share_trading_time(), bool)
        assert isinstance(mms.is_pre_market_time(), bool)


# ============================================================================
# 投机情绪（涨停/连板/炸板）
# ============================================================================

def _zt_pool(n: int = 8) -> pd.DataFrame:
    return pd.DataFrame({
        "代码": [f"{600000 + i:06d}" for i in range(n)],
        "名称": [f"股{i}" for i in range(n)],
        "最新价": [10.0 + i for i in range(n)],
        "成交额": [1e8 * (i + 1) for i in range(n)],
        "连板数": ([1, 1, 2, 3, 1, 5, 2, 1] * 2)[:n],
        "换手率": [5.0] * n,
        "封板资金": [1e7] * n,
    })


@pytest.mark.unit
class TestSpeculationSentiment:
    def test_counts_and_shape(self, monkeypatch):
        monkeypatch.setattr(ak, "stock_zt_pool_em", lambda date=None: _zt_pool())
        monkeypatch.setattr(ak, "stock_zt_pool_dtgc_em",
                            lambda date=None: pd.DataFrame({"代码": ["000001"]}))
        monkeypatch.setattr(ak, "stock_zt_pool_zbgc_em",
                            lambda date=None: pd.DataFrame({"代码": ["000002", "000003"]}))
        r = mms.get_speculation_sentiment()
        assert r["limit_up_count"] == 8
        assert r["limit_down_count"] == 1
        assert 0 <= r["failed_limit_rate"] <= 100
        assert r["max_board_height"] == 5        # 连板数最大值
        assert isinstance(r["high_board_stocks"], list)

    def test_all_sources_fail_is_graceful(self, monkeypatch):
        def boom(*a, **k):
            raise ConnectionError("blocked")

        for fn in ("stock_zt_pool_em", "stock_zt_pool_dtgc_em", "stock_zt_pool_zbgc_em"):
            monkeypatch.setattr(ak, fn, boom)
        r = mms.get_speculation_sentiment()
        assert isinstance(r, dict)
        assert r.get("limit_up_count", 0) == 0

    def test_empty_pool(self, monkeypatch):
        for fn in ("stock_zt_pool_em", "stock_zt_pool_dtgc_em", "stock_zt_pool_zbgc_em"):
            monkeypatch.setattr(ak, fn, lambda date=None: pd.DataFrame())
        assert mms.get_speculation_sentiment()["limit_up_count"] == 0


# ============================================================================
# 市场情绪（指数 + 涨跌比）
# ============================================================================

def _index_spot() -> pd.DataFrame:
    return pd.DataFrame({
        "代码": ["000001", "399001", "399006"],
        "名称": ["上证指数", "深证成指", "创业板指"],
        "最新价": [3200.0, 10500.0, 2100.0],
        "涨跌幅": [1.2, -0.5, 2.3],
        "成交额": [3e11, 4e11, 1e11],
    })


def _all_spot(n: int = 100, up_ratio: float = 0.6) -> pd.DataFrame:
    ups = int(n * up_ratio)
    return pd.DataFrame({
        "代码": [f"{600000 + i:06d}" for i in range(n)],
        "名称": [f"股{i}" for i in range(n)],
        "涨跌幅": [1.5] * ups + [-1.2] * (n - ups),
        "成交额": [1e8] * n,
    })


@pytest.mark.unit
class TestMarketSentiment:
    def test_returns_dict(self, monkeypatch):
        monkeypatch.setattr(ak, "stock_zh_index_spot_em", lambda *a, **k: _index_spot())
        monkeypatch.setattr(ak, "stock_zh_a_spot_em", lambda: _all_spot())
        assert isinstance(mms.get_market_sentiment(), dict)

    def test_falls_back_when_primary_fails(self, monkeypatch):
        def boom(*a, **k):
            raise ConnectionError("blocked")

        monkeypatch.setattr(ak, "stock_zh_index_spot_em", boom)
        monkeypatch.setattr(ak, "stock_zh_index_spot_sina", lambda *a, **k: _index_spot())
        monkeypatch.setattr(ak, "stock_zh_a_spot_em", lambda: _all_spot())
        assert isinstance(mms.get_market_sentiment(), dict), "主源失败应降级而非抛出"

    def test_total_failure_graceful(self, monkeypatch):
        def boom(*a, **k):
            raise ConnectionError("x")

        for fn in ("stock_zh_index_spot_em", "stock_zh_index_spot_sina",
                   "stock_zh_a_spot_em", "stock_zh_a_spot"):
            monkeypatch.setattr(ak, fn, boom)
        assert isinstance(mms.get_market_sentiment(), dict)


# ============================================================================
# 板块情绪
# ============================================================================

@pytest.mark.unit
class TestSectorSentiment:
    def test_concept_and_industry(self, monkeypatch):
        board = pd.DataFrame({
            "板块名称": [f"概念{i}" for i in range(12)],
            "涨跌幅": [5.0 - i * 0.3 for i in range(12)],
            "总市值": [1e11] * 12,
            "领涨股票": [f"股{i}" for i in range(12)],
        })
        monkeypatch.setattr(ak, "stock_board_concept_name_em", lambda: board)
        monkeypatch.setattr(ak, "stock_board_industry_name_em", lambda: board)
        assert isinstance(mms.get_sector_sentiment(), dict)

    def test_graceful_on_failure(self, monkeypatch):
        def boom(*a, **k):
            raise ConnectionError("x")

        for fn in ("stock_board_concept_name_em", "stock_board_industry_name_em",
                   "stock_sector_spot", "stock_board_industry_summary_ths"):
            monkeypatch.setattr(ak, fn, boom)
        assert isinstance(mms.get_sector_sentiment(), dict)


# ============================================================================
# 美股总结 + 全面板
# ============================================================================

# get_full_panel 把各子结果**扁平展开**到顶层并直接索引键，
# 因此 stub 必须提供完整键集（这也说明该函数对键缺失比较脆弱）
_SPEC_STUB = {
    "limit_up_count": 5, "limit_down_count": 1, "failed_limit_rate": 20.0,
    "consecutive_boards": 3, "max_board_height": 4, "limit_up_premium": 2.5,
    "high_board_stocks": [], "limit_up_details": [],
}
_MARKET_STUB = {
    "index_strength": {}, "up_count": 2000, "down_count": 1000,
    "flat_count": 100, "up_down_ratio": 2.0,
}
_SECTOR_STUB = {"top_concepts": [], "top_industries": [], "sector_heat": 55}


@pytest.mark.unit
class TestUsSummaryAndFullPanel:
    def test_us_summary(self, monkeypatch):
        df = pd.DataFrame({
            "date": pd.date_range("2026-07-14", periods=5),
            "close": [100.0, 102.0, 101.0, 105.0, 107.0],
            "open": [99.0, 101.0, 102.0, 103.0, 106.0],
        })
        monkeypatch.setattr(ak, "index_us_stock_sina", lambda symbol: df)
        r = mms.us_market_summary()
        assert "indices" in r and isinstance(r["indices"], list)

    def test_us_summary_failure_graceful(self, monkeypatch):
        def boom(symbol):
            raise ConnectionError("blocked")

        monkeypatch.setattr(ak, "index_us_stock_sina", boom)
        r = mms.us_market_summary()
        assert isinstance(r, dict) and isinstance(r.get("indices", []), list)

    def test_full_panel_required_keys(self, monkeypatch):
        """调度器与前端都依赖这些键存在，缺任一个会 KeyError"""
        monkeypatch.setattr(mms, "get_speculation_sentiment",
                            lambda *a, **k: dict(_SPEC_STUB))
        monkeypatch.setattr(mms, "get_market_sentiment", lambda *a, **k: dict(_MARKET_STUB))
        monkeypatch.setattr(mms, "get_sector_sentiment", lambda *a, **k: dict(_SECTOR_STUB))
        d = mms.get_full_panel()
        for k in ("market_sentiment_score", "market_sentiment_label",
                  "speculation_sentiment_score", "speculation_sentiment_label",
                  "updated_at"):
            assert k in d, f"面板缺少键 {k}（调度器/前端会 KeyError）"

    def test_full_panel_survives_partial_failure(self, monkeypatch):
        def boom(*a, **k):
            raise ConnectionError("x")

        monkeypatch.setattr(mms, "get_speculation_sentiment", boom)
        monkeypatch.setattr(mms, "get_market_sentiment", lambda *a, **k: dict(_MARKET_STUB))
        monkeypatch.setattr(mms, "get_sector_sentiment", lambda *a, **k: dict(_SECTOR_STUB))
        d = mms.get_full_panel()
        assert isinstance(d, dict) and "updated_at" in d
        assert d.get("errors"), "部分失败应记录在 errors 里而非静默"
