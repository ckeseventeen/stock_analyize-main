"""
tests/test_core_refactor.py — PR 1 重构基础设施测试

覆盖：
  - src/core/columns.py  : Col 常量、OHLCV 中英映射、智能列名探测
  - src/core/models.py   : Stock/AlertRule/Signal dataclass + round-trip
  - src/core/settings.py : pydantic-settings 单例
  - src/core/market_registry.py : 注册 / 查询 / 列举 / 重复注册告警
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core.columns import (
    OHLCV_CN_TO_EN,
    OHLCV_EN_TO_CN,
    Col,
    get_close_col,
    get_volume_col,
    normalize_ohlcv_columns,
)
from src.core.market_registry import (
    MarketSpec,
    get_market,
    market_keys,
    market_labels,
    register_market,
)
# =============================================================================
# columns.py
# =============================================================================

@pytest.mark.unit
class TestColumnConstants:
    def test_spot_columns_unicode_intact(self):
        """中文列名常量应保持原 akshare 字面值，不破坏数据格式"""
        assert Col.CODE == "代码"
        assert Col.NAME == "名称"
        assert Col.PRICE == "最新价"
        assert Col.PE_DYN == "市盈率-动态"
        assert Col.MARKET_CAP == "总市值"
        assert Col.TURNOVER == "换手率"

    def test_ohlcv_bidirectional_mapping(self):
        # 中→英映射存在
        assert OHLCV_CN_TO_EN[Col.CLOSE] == Col.EN_CLOSE
        assert OHLCV_CN_TO_EN[Col.VOLUME] == Col.EN_VOLUME
        # 反向映射对称
        assert OHLCV_EN_TO_CN[Col.EN_CLOSE] == Col.CLOSE
        # 全部双向自洽
        for cn, en in OHLCV_CN_TO_EN.items():
            assert OHLCV_EN_TO_CN[en] == cn

    def test_get_close_col_handles_both_languages(self):
        df_cn = pd.DataFrame(columns=["日期", "开盘", "收盘"])
        df_en = pd.DataFrame(columns=["date", "open", "close"])
        df_neither = pd.DataFrame(columns=["foo"])
        assert get_close_col(df_cn.columns) == "收盘"
        assert get_close_col(df_en.columns) == "close"
        assert get_close_col(df_neither.columns) is None

    def test_get_volume_col(self):
        df = pd.DataFrame(columns=["volume", "close"])
        assert get_volume_col(df.columns) == "volume"

    def test_normalize_ohlcv_renames_chinese_to_english(self):
        df = pd.DataFrame({
            "日期": ["2024-01-01"], "开盘": [10], "最高": [11],
            "最低": [9], "收盘": [10.5], "成交量": [1000],
        })
        out = normalize_ohlcv_columns(df)
        assert list(out.columns) == ["date", "open", "high", "low", "close", "volume"]

    def test_normalize_ohlcv_no_op_for_english(self):
        df = pd.DataFrame(columns=["date", "open", "close"])
        out = normalize_ohlcv_columns(df)
        # 没有中文列时应原样返回，不报错
        assert list(out.columns) == ["date", "open", "close"]


# =============================================================================
# settings.py
# =============================================================================

@pytest.mark.unit
class TestSettings:
    def test_defaults(self):
        from src.core.settings import settings
        # 不是 None；如果 .env 没设这些就是默认 False
        assert isinstance(settings.scheduler_disabled, bool)
        assert isinstance(settings.serverchan_key, str)

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("SCHEDULER_DISABLED", "1")
        from src.core.settings import reload_settings
        s = reload_settings()
        assert s.scheduler_disabled is True


# =============================================================================
# market_registry.py
# =============================================================================

@pytest.mark.unit
class TestMarketRegistry:
    def test_default_markets_registered(self):
        # 模块 import 时已自动注册 A/HK/US
        keys = market_keys()
        assert "a" in keys
        assert "hk" in keys
        assert "us" in keys

    def test_a_market_metadata(self):
        spec = get_market("a")
        assert spec.label == "A 股"
        assert spec.code_pad_width == 6
        assert spec.currency == "CNY"
        assert spec.default_picker_code == "600519"

    def test_hk_yfinance_suffix(self):
        spec = get_market("hk")
        assert spec.yfinance_suffix == ".HK"

    def test_us_no_suffix(self):
        spec = get_market("us")
        assert spec.yfinance_suffix == ""
        assert spec.code_pad_width == 0

    def test_get_market_unknown_raises(self):
        with pytest.raises(KeyError, match="未知市场"):
            get_market("xx")

    def test_normalize_symbol_pads_a_share(self):
        assert get_market("a").normalize_symbol("519") == "000519"
        assert get_market("a").normalize_symbol("600519") == "600519"

    def test_normalize_symbol_pads_hk(self):
        assert get_market("hk").normalize_symbol("700") == "00700"

    def test_normalize_symbol_us_passthrough(self):
        assert get_market("us").normalize_symbol("AAPL") == "AAPL"

    def test_market_labels_mapping(self):
        labels = market_labels()
        assert labels["a"] == "A 股"
        assert labels["hk"] == "港股"

    def test_register_duplicate_raises_without_replace(self):
        # 重复注册应抛错
        spec = MarketSpec(
            key="a", label="X",
            config_path=Path("./x.yaml"),
            fetcher_class_path="src.core.data_fetcher:AStockDataFetcher",
            analyzer_class_path="src.core.analyzer:AStockAnalyzer",
        )
        with pytest.raises(ValueError, match="已存在"):
            register_market(spec)

    def test_register_with_replace_succeeds(self):
        """允许 replace=True 覆盖（测试结束后还原）"""
        old = get_market("a")
        try:
            new_spec = MarketSpec(
                key="a", label="A 测试",
                config_path=old.config_path,
                fetcher_class_path=old.fetcher_class_path,
                analyzer_class_path=old.analyzer_class_path,
                default_picker_code="000001",
                code_pad_width=6,
            )
            register_market(new_spec, replace=True)
            assert get_market("a").default_picker_code == "000001"
        finally:
            register_market(old, replace=True)

    def test_fetcher_cls_lazy_imports(self):
        spec = get_market("a")
        cls = spec.fetcher_cls
        assert cls.__name__ == "AStockDataFetcher"

    def test_analyzer_cls_lazy_imports(self):
        spec = get_market("us")
        cls = spec.analyzer_cls
        assert cls.__name__ in ("USStockAnalyzer", "InternationalStockAnalyzer")


# =============================================================================
# 集成：MARKET_LABELS / MARKET_CONFIG_PATHS 应从 registry 派生
# =============================================================================

@pytest.mark.unit
class TestConfigOpsIntegration:
    def test_market_labels_re_export_matches_registry(self):
        from src.core.config_io import MARKET_LABELS
        labels = market_labels()
        assert MARKET_LABELS == labels

    def test_market_config_paths_re_export(self):
        from src.core.config_io import MARKET_CONFIG_PATHS
        for key in ("a", "hk", "us"):
            assert key in MARKET_CONFIG_PATHS
            assert get_market(key).config_path == MARKET_CONFIG_PATHS[key]

    def test_main_market_mapping_uses_registry(self):
        from main import MARKET_MAPPING
        assert MARKET_MAPPING["a"]["name"] == "A 股"
        # fetcher 类应能被实例化（这里不实例化，避免触发 baostock 连接）
        cls = MARKET_MAPPING["us"]["fetcher_class"]
        assert callable(cls)
