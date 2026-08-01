"""
tests/test_screener.py — 筛选器条件测试
"""

import pandas as pd
import pytest

from src.analysis.screening.conditions import (
    MarketCapCondition,
    PBRangeCondition,
    PERangeCondition,
    PriceAboveMACondition,
    PriceRangeCondition,
    RSIOversoldCondition,
    TurnoverRateCondition,
    WeeklyMACDBottomDivergenceCondition,
)
from src.analysis.screening.config_schema import parse_screen_config


@pytest.mark.unit
class TestSpotConditions:
    """Spot 条件（快速过滤）测试"""

    def test_market_cap_in_range(self, spot_data_row):
        """市值在范围内应通过"""
        cond = MarketCapCondition(min_cap=10000, max_cap=30000)
        assert cond.evaluate_spot(spot_data_row) is True

    def test_market_cap_below_range(self, spot_data_row):
        """市值低于范围应不通过"""
        cond = MarketCapCondition(min_cap=30000, max_cap=50000)
        assert cond.evaluate_spot(spot_data_row) is False

    def test_market_cap_above_range(self, spot_data_row):
        """市值高于范围应不通过"""
        cond = MarketCapCondition(min_cap=0, max_cap=100)
        assert cond.evaluate_spot(spot_data_row) is False

    def test_pe_range_normal(self, spot_data_row):
        """正常PE范围"""
        cond = PERangeCondition(min_pe=20, max_pe=35)
        assert cond.evaluate_spot(spot_data_row) is True

    def test_pe_range_excludes_loss(self, spot_data_row_loss):
        """亏损股（PE为负）应被排除"""
        cond = PERangeCondition(min_pe=0, max_pe=100)
        assert cond.evaluate_spot(spot_data_row_loss) is False

    def test_pb_range(self, spot_data_row):
        """PB范围"""
        cond = PBRangeCondition(min_pb=5, max_pb=10)
        assert cond.evaluate_spot(spot_data_row) is True

    def test_price_range(self, spot_data_row):
        """股价范围"""
        cond = PriceRangeCondition(min_price=1000, max_price=2000)
        assert cond.evaluate_spot(spot_data_row) is True

    def test_price_range_out(self, spot_data_row):
        """股价超出范围"""
        cond = PriceRangeCondition(min_price=0, max_price=100)
        assert cond.evaluate_spot(spot_data_row) is False

    def test_turnover_rate(self, spot_data_row):
        """换手率范围"""
        cond = TurnoverRateCondition(min_rate=0, max_rate=1.0)
        assert cond.evaluate_spot(spot_data_row) is True


@pytest.mark.unit
class TestOHLCVConditions:
    """OHLCV 条件测试"""

    def test_weekly_divergence_spot_always_true(self, spot_data_row):
        """OHLCV条件在spot阶段应始终返回True"""
        cond = WeeklyMACDBottomDivergenceCondition()
        assert cond.evaluate_spot(spot_data_row) is True
        assert cond.requires_ohlcv is True

    def test_weekly_divergence_empty_df(self, spot_data_row):
        """空K线数据应返回False"""
        cond = WeeklyMACDBottomDivergenceCondition()
        assert cond.evaluate_full(spot_data_row, pd.DataFrame()) is False

    def test_weekly_divergence_with_data(self, spot_data_row, divergence_bottom_df):
        """有数据时应正常运行不报错"""
        cond = WeeklyMACDBottomDivergenceCondition(lookback_bars=80)
        result = cond.evaluate_full(spot_data_row, divergence_bottom_df)
        assert isinstance(result, bool)

    def test_rsi_oversold_empty(self, spot_data_row):
        """RSI条件空数据应返回False"""
        cond = RSIOversoldCondition(threshold=30)
        assert cond.evaluate_full(spot_data_row, pd.DataFrame()) is False

    def test_rsi_oversold_with_data(self, spot_data_row, synthetic_ohlcv_en):
        """RSI条件有数据时应正常运行"""
        cond = RSIOversoldCondition(threshold=80)  # 高阈值以确保通过
        result = cond.evaluate_full(spot_data_row, synthetic_ohlcv_en)
        assert isinstance(result, bool)

    def test_price_above_ma(self, spot_data_row, synthetic_ohlcv_en):
        """均线条件应正常运行"""
        cond = PriceAboveMACondition(ma_period=20)
        result = cond.evaluate_full(spot_data_row, synthetic_ohlcv_en)
        assert isinstance(result, bool)


@pytest.mark.unit
class TestConfigParsing:
    """筛选配置解析测试"""

    def test_parse_valid_config(self, tmp_path):
        """解析有效配置"""
        config_content = """
screen:
  conditions:
    - type: market_cap
      min: 50
      max: 500
    - type: pe_range
      min: 0
      max: 30
  output:
    sort_by: "总市值(亿)"
    limit: 20
"""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content, encoding="utf-8")

        conditions, output_config = parse_screen_config(str(config_file))
        assert len(conditions) == 2
        assert output_config["limit"] == 20

    def test_parse_with_ohlcv_conditions(self, tmp_path):
        """解析含OHLCV条件的配置"""
        config_content = """
screen:
  conditions:
    - type: weekly_macd_divergence
      lookback_bars: 40
    - type: rsi_oversold
      threshold: 25
      period: 14
"""
        config_file = tmp_path / "test_config2.yaml"
        config_file.write_text(config_content, encoding="utf-8")

        conditions, _ = parse_screen_config(str(config_file))
        assert len(conditions) == 2
        assert conditions[0].requires_ohlcv is True
        assert conditions[1].requires_ohlcv is True

    def test_parse_unknown_condition_skipped(self, tmp_path):
        """未知条件类型应被跳过"""
        config_content = """
screen:
  conditions:
    - type: market_cap
      min: 50
    - type: unknown_type
      foo: bar
"""
        config_file = tmp_path / "test_config3.yaml"
        config_file.write_text(config_content, encoding="utf-8")

        conditions, _ = parse_screen_config(str(config_file))
        assert len(conditions) == 1  # 只有 market_cap

    def test_parse_missing_file(self):
        """不存在的配置文件应返回空"""
        conditions, output = parse_screen_config("/nonexistent/path.yaml")
        assert conditions == []
        assert output == {}


# =============================================================================
# 筛选恒 0 命中回归（2026-07 修复）
# 根因：get_daily_ohlcv 默认 days_back=120 日历天 ≈ 81 根交易 bar，
# 而 price_above_ma(120)/multi_ma_bull(ma250) 等长窗口条件需要更多 bar，
# len(df) < 窗口 → 条件恒 False → 筛选恒 0
# =============================================================================

@pytest.mark.unit
class TestRequiredDailyBars:
    def test_price_above_ma_120(self):
        from src.analysis.screening.conditions import PriceAboveMACondition
        from src.analysis.screening.screener import required_daily_bars

        assert required_daily_bars([PriceAboveMACondition(ma_period=120)]) == 140

    def test_multi_ma_bull_list_attr(self):
        from src.analysis.screening.conditions import MultiMABullCondition
        from src.analysis.screening.screener import required_daily_bars

        cond = MultiMABullCondition(ma_list=[5, 10, 20, 60, 250])
        assert required_daily_bars([cond]) == 270

    def test_weekly_condition_ignored(self):
        from src.analysis.screening.conditions import (
            WeeklyMACDBottomDivergenceCondition,
        )
        from src.analysis.screening.screener import required_daily_bars

        # 周线条件不影响日线深度 → 回落到基线 60
        cond = WeeklyMACDBottomDivergenceCondition(lookback_bars=250)
        assert required_daily_bars([cond]) == 60

    def test_baseline_and_days_conversion(self):
        from src.analysis.screening.screener import (
            daily_days_back_for,
            required_daily_bars,
        )

        assert required_daily_bars([]) == 60
        # 140 bar → int(140*1.6)+10 = 234 日历天
        from src.analysis.screening.conditions import PriceAboveMACondition
        assert daily_days_back_for([PriceAboveMACondition(ma_period=120)]) == 234

    def test_evaluate_row_uses_computed_days_back(self):
        """_evaluate_row 必须把 daily_days_back 透传给 get_daily_ohlcv"""
        from types import SimpleNamespace

        import numpy as np

        from src.analysis.screening.conditions import PriceAboveMACondition
        from src.analysis.screening.screener import _evaluate_row

        seen = {}

        def fake_daily(code, days_back=120):
            seen["days_back"] = days_back
            n = 200
            close = np.linspace(10, 30, n)
            return pd.DataFrame({
                "日期": pd.bdate_range("2025-01-02", periods=n),
                "开盘": close, "最高": close * 1.01,
                "最低": close * 0.99, "收盘": close,
                "成交量": np.full(n, 1e6),
            })

        provider = SimpleNamespace(
            get_weekly_ohlcv=lambda code: pd.DataFrame(),
            get_daily_ohlcv=fake_daily,
        )
        ok, _, _, reason, _ = _evaluate_row(
            provider, pd.Series({"代码": "600519", "名称": "测试"}),
            [PriceAboveMACondition(ma_period=120)],
            need_weekly=False, need_daily=True, daily_days_back=234)
        assert seen["days_back"] == 234
        assert ok and reason == "ok", "200 根上行 bar 应通过 MA120 条件"


@pytest.mark.unit
class TestWencaiV8Guard:
    """
    pywencai 的 py_mini_racer 在部分环境 FATAL 崩进程 → 必须隔离子进程探测。

    探测逻辑统一收在 src/core/v8_guard；这里只验证问财**跟随**探测结论，
    不再自己重复探测一遍（探测本身的用例见 tests/test_v8_guard.py）。
    """

    @pytest.fixture(autouse=True)
    def _reset_probe_cache(self):
        from src.core import v8_guard
        from src.data.providers.wencai_provider import WencaiProvider

        WencaiProvider._V8_PROBE_RESULT = None
        v8_guard._reset_probe_for_tests()
        yield
        WencaiProvider._V8_PROBE_RESULT = None
        v8_guard._reset_probe_for_tests()

    def test_unsafe_v8_disables_provider(self, monkeypatch):
        import src.data.providers.wencai_provider as wp_mod
        from src.data.providers.wencai_provider import WencaiProvider

        monkeypatch.setattr("src.core.v8_guard.v8_usable", lambda: False)
        wp = WencaiProvider()
        wp._pywencai = object()   # 假装 pywencai 已安装
        assert wp.is_available() is False
        # 结论缓存到类上，第二次不再重新判定
        assert WencaiProvider._V8_PROBE_RESULT is False
        assert wp_mod is not None

    def test_safe_v8_enables_provider(self, monkeypatch):
        """V8 探测通过时问财应可用——一刀切禁用会白白丢掉一个兜底源"""
        from src.data.providers.wencai_provider import WencaiProvider

        monkeypatch.setattr("src.core.v8_guard.v8_usable", lambda: True)
        wp = WencaiProvider()
        wp._pywencai = object()
        assert wp.is_available() is True

    def test_missing_pywencai_is_unavailable_regardless(self, monkeypatch):
        """没装 pywencai 时，V8 再安全也不可用"""
        from src.data.providers.wencai_provider import WencaiProvider

        monkeypatch.setattr("src.core.v8_guard.v8_usable", lambda: True)
        wp = WencaiProvider()
        wp._pywencai = None
        assert wp.is_available() is False


@pytest.mark.unit
class TestRequiredBarsProtocol:
    """F4：required_bars() 下沉为 BaseCondition 协议（子类可覆写）"""

    def test_base_condition_heuristic(self):
        from src.analysis.screening.conditions import (
            MultiMABullCondition,
            PriceAboveMACondition,
        )

        assert PriceAboveMACondition(ma_period=120).required_bars() == 140
        assert MultiMABullCondition(ma_list=[5, 10, 250]).required_bars() == 270

    def test_custom_condition_can_override(self):
        from src.analysis.screening.conditions import BaseCondition
        from src.analysis.screening.screener import required_daily_bars

        class WeirdCondition(BaseCondition):
            name = "weird"
            requires_ohlcv = True
            ohlcv_period = "daily"

            def evaluate_spot(self, spot_row):
                return True

            def required_bars(self):
                return 300   # 非常规参数名的条件自行声明需求

        assert required_daily_bars([WeirdCondition()]) == 300

    def test_weekly_days_back_scaling(self):
        from src.analysis.screening.conditions import (
            WeeklyMACDBottomDivergenceCondition,
        )
        from src.analysis.screening.screener import weekly_days_back_for

        # 默认 60 周 bar → 80 需求 → 仍受 3 年下限保护
        small = WeeklyMACDBottomDivergenceCondition(lookback_bars=60)
        assert weekly_days_back_for([small]) == 365 * 3
        # 250 周 bar → 270 需求 → 270*7*1.1+30 = 2109 天 > 3 年下限
        big = WeeklyMACDBottomDivergenceCondition(lookback_bars=250)
        assert weekly_days_back_for([big]) == int(270 * 7 * 1.1) + 30


@pytest.mark.unit
class TestMinuteBreakerIsolation:
    """F1：港/美股分钟线失败不应污染 A 股共享熔断器"""

    @pytest.fixture(autouse=True)
    def _reset_breaker(self):
        import src.analysis.screening.data_provider as dp

        dp._akshare_cb_state.update(fail_count=0, cool_until=0.0)
        yield
        dp._akshare_cb_state.update(fail_count=0, cool_until=0.0)

    def test_hk_minute_failure_does_not_trip_breaker(self, tmp_path, monkeypatch):
        import src.analysis.screening.data_provider as dp
        from src.analysis.screening.data_provider import ScreenerDataProvider

        def boom(**kwargs):
            raise ConnectionError("hk minute blocked")

        monkeypatch.setattr(dp.ak, "stock_hk_hist_min_em", boom)
        provider = ScreenerDataProvider(cache_dir=str(tmp_path / "c"))
        for _ in range(5):
            provider._fetch_minute_akshare("00700", "5m", 5, market="hk")
        assert dp._akshare_cb_state["fail_count"] == 0
        assert not dp._akshare_circuit_open()

    def test_a_share_minute_failure_still_counts(self, tmp_path, monkeypatch):
        import src.analysis.screening.data_provider as dp
        from src.analysis.screening.data_provider import ScreenerDataProvider

        def boom(**kwargs):
            raise ConnectionError("a minute blocked")

        monkeypatch.setattr(dp.ak, "stock_zh_a_hist_min_em", boom)
        provider = ScreenerDataProvider(cache_dir=str(tmp_path / "c"))
        provider._fetch_minute_akshare("600519", "5m", 5, market="a")
        provider._fetch_minute_akshare("600519", "5m", 5, market="a")
        assert dp._akshare_circuit_open()

    def test_empty_result_does_not_count_as_failure(self, tmp_path, monkeypatch):
        import pandas as _pd

        import src.analysis.screening.data_provider as dp
        from src.analysis.screening.data_provider import ScreenerDataProvider

        monkeypatch.setattr(dp.ak, "stock_zh_a_hist_min_em",
                            lambda **kw: _pd.DataFrame())
        provider = ScreenerDataProvider(cache_dir=str(tmp_path / "c"))
        for _ in range(5):
            provider._fetch_minute_akshare("600519", "1m", 5, market="a")
        assert dp._akshare_cb_state["fail_count"] == 0


# =============================================================================
# 全A行情数据质量校验（2026-07-21 线上事故回归）
# 事故：问财分页只返回 2649/5300 只且 97.9% 总市值为 NaN、缺全部大盘蓝筹，
# 旧代码只校验"非空+列名齐全"→ 残缺数据被缓存 12h → 指数成分股筛选恒 0 命中
# =============================================================================

@pytest.mark.unit
class TestSpotQualityValidation:
    @staticmethod
    def _mk(n=5000, mktcap_ratio=1.0, sentinels=True):
        import numpy as np

        codes = [f"{600000 + i:06d}" for i in range(n)]
        if sentinels and n >= 3:
            codes[:3] = ["600519", "000001", "600036"]
        mc = np.full(n, 5e10)
        n_bad = int(n * (1 - mktcap_ratio))
        if n_bad:
            mc[-n_bad:] = np.nan
        return pd.DataFrame({"代码": codes, "名称": [f"股{i}" for i in range(n)],
                             "总市值": mc})

    def test_healthy_data_passes(self):
        from src.analysis.screening.data_provider import validate_spot_quality

        ok, reason = validate_spot_quality(self._mk())
        assert ok and reason == ""

    def test_empty_rejected(self):
        from src.analysis.screening.data_provider import validate_spot_quality

        assert validate_spot_quality(pd.DataFrame())[0] is False

    def test_truncated_rows_rejected(self):
        """线上事故值：2649 只（不足全A的一半）"""
        from src.analysis.screening.data_provider import validate_spot_quality

        ok, reason = validate_spot_quality(self._mk(2649))
        assert not ok and "覆盖不全" in reason

    def test_mostly_nan_marketcap_rejected(self):
        """线上事故值：总市值有效率仅 2.1%"""
        from src.analysis.screening.data_provider import validate_spot_quality

        ok, reason = validate_spot_quality(self._mk(5000, mktcap_ratio=0.021))
        assert not ok and "总市值有效率" in reason

    def test_missing_bluechips_rejected(self):
        """分页按市值排序未取完 → 缺茅台/平安银行"""
        from src.analysis.screening.data_provider import validate_spot_quality

        ok, reason = validate_spot_quality(self._mk(5000, sentinels=False))
        assert not ok and "基准蓝筹" in reason

    def test_missing_columns_rejected(self):
        from src.analysis.screening.data_provider import validate_spot_quality

        ok, reason = validate_spot_quality(pd.DataFrame({"代码": ["600519"]}))
        assert not ok and "缺关键字段" in reason


@pytest.mark.unit
class TestSpotFallbackChain:
    """降级链必须跳过不合格数据源，全不合格时返回最完整的一份并打退化标记"""

    def _provider(self, tmp_path):
        from src.analysis.screening.data_provider import ScreenerDataProvider

        return ScreenerDataProvider(cache_dir=str(tmp_path / "spot"))

    def test_skips_degraded_source_and_uses_healthy_one(self, tmp_path, monkeypatch):
        import src.analysis.screening.data_provider as dp

        good = TestSpotQualityValidation._mk(5000)
        bad = TestSpotQualityValidation._mk(2649)
        monkeypatch.setattr(dp.ak, "stock_zh_a_spot_em", lambda: bad)
        monkeypatch.setattr(dp.ak, "stock_zh_a_spot", lambda: good.rename(
            columns={"代码": "code", "名称": "name", "总市值": "mktcap"}))
        # 备用API 会把 mktcap 万元→元，这里直接让它拿到合格数据
        monkeypatch.setattr(dp.ScreenerDataProvider, "_fetch_k_akshare",
                            lambda *a, **k: pd.DataFrame())
        p = self._provider(tmp_path)
        df = p.get_all_a_shares()
        assert len(df) >= 4000, "应跳过 2649 行的残缺数据，采用完整源"

    def test_all_degraded_marks_attrs(self, tmp_path, monkeypatch):
        import src.analysis.screening.data_provider as dp

        bad_small = TestSpotQualityValidation._mk(1000)
        bad_bigger = TestSpotQualityValidation._mk(2649)
        monkeypatch.setattr(dp.ak, "stock_zh_a_spot_em", lambda: bad_small)
        monkeypatch.setattr(dp.ak, "stock_zh_a_spot", lambda: bad_bigger.rename(
            columns={"代码": "code", "名称": "name", "总市值": "mktcap"}))
        monkeypatch.setattr(dp.WencaiProvider, "is_available", lambda self: False)
        monkeypatch.setattr(dp.BaostockProvider, "get_all_stocks",
                            lambda self: pd.DataFrame())
        p = self._provider(tmp_path)
        df = p.get_all_a_shares()
        assert not df.empty, "全不合格时应返回最完整的一份而非空表"
        assert df.attrs.get("degraded") is True
        assert df.attrs.get("degrade_reason")

    def test_screener_surfaces_degrade_warning(self, tmp_path, monkeypatch):
        """退化状态必须冒泡到 screener.data_warning（前端要显示原因）"""
        from src.analysis.screening.conditions import MarketCapCondition
        from src.analysis.screening.screener import StockScreener

        degraded = TestSpotQualityValidation._mk(2649)
        degraded.attrs["degraded"] = True
        degraded.attrs["degrade_reason"] = "覆盖不全：仅 2649 只"

        class FakeProvider:
            cache_dir = str(tmp_path)

            def get_all_a_shares(self):
                return degraded

        s = StockScreener(data_provider=FakeProvider())
        s.add_condition(MarketCapCondition(min_cap=1, max_cap=999999))
        s.run(limit=5)
        assert "数据源退化" in s.data_warning
        assert "2649" in s.data_warning
