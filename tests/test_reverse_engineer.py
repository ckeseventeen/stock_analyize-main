# -*- coding: utf-8 -*-
"""
tests/test_reverse_engineer.py — 荐股逆向工程模块单元测试

全 mock 测试，不依赖真实数据源。
"""
import os
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.analysis.reverse_engineer import (
    PicksReverseEngineer,
    extract_features,
    limit_pct,
    market_percentiles,
    shift_date,
    to_dash_date,
    to_compact_date,
)


# ======================================================================
# 工具函数测试
# ======================================================================
class TestUtilFunctions:
    """工具函数的基础测试"""

    def test_limit_pct_main_board(self):
        """主板(60/00开头)涨停幅度应为10%"""
        assert limit_pct("600519") == 0.10
        assert limit_pct("000001") == 0.10

    def test_limit_pct_chinext(self):
        """创业板(30开头)涨停幅度应为20%"""
        assert limit_pct("300001") == 0.20
        assert limit_pct("301338") == 0.20

    def test_limit_pct_star(self):
        """科创板(68开头)涨停幅度应为20%"""
        assert limit_pct("688411") == 0.20

    def test_shift_date_forward(self):
        """日期向前偏移"""
        assert shift_date("20260528", 3) == "20260531"

    def test_shift_date_backward(self):
        """日期向后偏移"""
        assert shift_date("20260601", -1) == "20260531"

    def test_to_dash_date(self):
        """YYYYMMDD → YYYY-MM-DD 转换"""
        assert to_dash_date("20260528") == "2026-05-28"

    def test_to_compact_date(self):
        """YYYY-MM-DD → YYYYMMDD 转换"""
        assert to_compact_date("2026-05-28") == "20260528"


# ======================================================================
# 合成测试数据
# ======================================================================
def make_daily_df(n=80, start_date="2026-03-01", base_price=50.0, seed=42):
    """
    构造一份合成日线 DataFrame，模拟 baostock get_k_data 返回格式。
    列: date, open, high, low, close, volume, amount, turn, pctChg, peTTM, pbMRQ
    """
    np.random.seed(seed)
    dates = pd.bdate_range(start_date, periods=n)

    close = base_price + np.cumsum(np.random.randn(n) * 0.5 + 0.05)
    open_ = close + np.random.randn(n) * 0.2
    high = np.maximum(open_, close) + np.abs(np.random.randn(n) * 0.3)
    low = np.minimum(open_, close) - np.abs(np.random.randn(n) * 0.3)
    volume = np.random.randint(500000, 5000000, size=n).astype(float)
    amount = volume * close * 0.01  # 近似成交额
    turn = np.random.uniform(0.5, 5.0, size=n)

    # pctChg: 日涨跌幅
    pct_chg = np.zeros(n)
    pct_chg[1:] = (close[1:] / close[:-1] - 1) * 100

    pe_ttm = np.random.uniform(10, 60, size=n)
    pb_mrq = np.random.uniform(1, 8, size=n)

    return pd.DataFrame({
        "date": [d.strftime("%Y-%m-%d") for d in dates],
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "amount": amount,
        "turn": turn,
        "pctChg": pct_chg,
        "peTTM": pe_ttm,
        "pbMRQ": pb_mrq,
    })


# ======================================================================
# extract_features 测试
# ======================================================================
class TestExtractFeatures:
    """特征提取函数的测试"""

    def test_basic_features(self):
        """正常数据应能提取出基本特征"""
        df = make_daily_df(n=80, start_date="2026-03-01", base_price=40.0)
        # 选一个中间日期作为 trade_date
        trade_date_dash = df.iloc[60]["date"]
        trade_date = trade_date_dash.replace("-", "")

        bp = MagicMock()
        bp.get_k_data.return_value = df

        feat = extract_features(bp, trade_date, "600378", "昊华科技", "化学制品", 40.16)

        assert feat["ts_code"] == "600378"
        assert feat["name"] == "昊华科技"
        assert feat["trade_date"] == trade_date
        assert "error" not in feat
        assert "pct_chg_T" in feat
        assert "amplitude_T" in feat
        assert "close_vs_trigger_pct" in feat
        assert "vol_ratio_5d" in feat
        assert "turnover_rate" in feat
        assert "pe_ttm" in feat

    def test_no_data_returns_error(self):
        """无数据时应返回 error"""
        bp = MagicMock()
        bp.get_k_data.return_value = pd.DataFrame()

        feat = extract_features(bp, "20260528", "999999", "空票", "未知", 10.0)
        assert "error" in feat
        assert feat["error"] == "no daily data"

    def test_suspended_stock(self):
        """触发日不在日线数据中(停牌)应返回 error"""
        df = make_daily_df(n=30, start_date="2026-05-01")
        # trade_date 选一个不在 df 中的日期
        bp = MagicMock()
        bp.get_k_data.return_value = df

        feat = extract_features(bp, "20260101", "600378", "昊华科技", "化学制品", 40.16)
        assert "error" in feat
        assert "trigger date" in feat["error"]

    def test_momentum_features(self):
        """应计算近期动量特征"""
        df = make_daily_df(n=80, start_date="2026-03-01")
        trade_date_dash = df.iloc[60]["date"]
        trade_date = trade_date_dash.replace("-", "")

        bp = MagicMock()
        bp.get_k_data.return_value = df

        feat = extract_features(bp, trade_date, "301338", "凯格精机", "专业设备", 214.34)

        assert "ret_3d_pct" in feat
        assert "ret_5d_pct" in feat
        assert "ret_10d_pct" in feat
        assert "ret_20d_pct" in feat
        assert "consec_up_days" in feat
        assert "limit_ups_prev10" in feat

    def test_ma_features(self):
        """应计算均线偏离度"""
        df = make_daily_df(n=80, start_date="2026-03-01")
        trade_date_dash = df.iloc[65]["date"]
        trade_date = trade_date_dash.replace("-", "")

        bp = MagicMock()
        bp.get_k_data.return_value = df

        feat = extract_features(bp, trade_date, "600171", "上海贝岭", "半导体", 31.03)

        assert "close_vs_ma5_pct" in feat
        assert "close_vs_ma10_pct" in feat
        assert "close_vs_ma20_pct" in feat
        assert "close_vs_ma60_pct" in feat

    def test_breakout_features(self):
        """应计算突破特征"""
        df = make_daily_df(n=80, start_date="2026-03-01")
        trade_date_dash = df.iloc[65]["date"]
        trade_date = trade_date_dash.replace("-", "")

        bp = MagicMock()
        bp.get_k_data.return_value = df

        feat = extract_features(bp, trade_date, "000519", "中兵红箭", "专业设备", 19.40)

        assert "breakout_60d_high" in feat
        assert "breakout_20d_high" in feat
        assert "dist_to_60d_high_pct" in feat

    def test_forward_returns(self):
        """应计算前瞻收益"""
        df = make_daily_df(n=80, start_date="2026-03-01")
        # 选一个中间位置，确保有前瞻数据
        trade_date_dash = df.iloc[60]["date"]
        trade_date = trade_date_dash.replace("-", "")

        bp = MagicMock()
        bp.get_k_data.return_value = df

        feat = extract_features(bp, trade_date, "300031", "宝通科技", "游戏", 25.34)

        assert "next_open_ret_pct" in feat
        assert "hold_T1_close_ret_pct" in feat
        assert "max_high_5d_vs_trigger_pct" in feat
        assert "max_high_5d_vs_buy_pct" in feat
        assert "max_dd_5d_vs_buy_pct" in feat

    def test_limit_up_detection(self):
        """涨停检测: 主板 9.8% 以上算涨停"""
        df = make_daily_df(n=40, start_date="2026-04-01", base_price=20.0)
        # 手动设定一个涨停日
        trade_date_dash = df.iloc[20]["date"]
        trade_date = trade_date_dash.replace("-", "")
        df.loc[20, "pctChg"] = 9.95  # 接近涨停

        bp = MagicMock()
        bp.get_k_data.return_value = df

        feat = extract_features(bp, trade_date, "000519", "中兵红箭", "专业设备", 19.40)
        assert feat["is_limit_up_T"] == 1
        assert feat["near_limit_T"] == 1


# ======================================================================
# market_percentiles 测试
# ======================================================================
class TestMarketPercentiles:
    """全市场横截面分位测试"""

    def _make_spot_df(self, n=100):
        """构造模拟全A行情数据"""
        np.random.seed(123)
        codes = [f"{i:06d}" for i in range(n)]
        return pd.DataFrame({
            "代码": codes,
            "名称": [f"股票{i}" for i in range(n)],
            "最新价": np.random.uniform(5, 200, n),
            "涨跌幅": np.random.uniform(-10, 10, n),
            "总市值": np.random.uniform(1e9, 1e12, n),
            "换手率": np.random.uniform(0.1, 20, n),
            "成交额": np.random.uniform(1e6, 1e10, n),
        })

    def test_basic_percentile(self):
        """应返回分位数和市场环境"""
        df = self._make_spot_df(100)
        # 设一个已知的标的
        df.loc[50, "代码"] = "600378"
        df.loc[50, "涨跌幅"] = 8.5  # 高于大部分

        pcts, ctx = market_percentiles(df, ["600378"])

        assert "600378" in pcts
        assert "mkt_pctile_pct_chg" in pcts["600378"]
        assert pcts["600378"]["mkt_pctile_pct_chg"] > 50  # 应在中位数以上

        assert "n_stocks" in ctx
        assert ctx["n_stocks"] == 100
        assert "pct_up" in ctx

    def test_empty_spot(self):
        """空 spot 数据应返回空字典"""
        pcts, ctx = market_percentiles(pd.DataFrame(), ["600378"])
        assert pcts == {}
        assert ctx == {}

    def test_unknown_code(self):
        """不在 spot 中的代码应被跳过"""
        df = self._make_spot_df(10)
        pcts, ctx = market_percentiles(df, ["999999"])
        assert "999999" not in pcts
        assert "n_stocks" in ctx  # 环境仍应返回

    def test_multiple_picks(self):
        """多个标的应分别计算分位"""
        df = self._make_spot_df(100)
        df.loc[10, "代码"] = "600171"
        df.loc[20, "代码"] = "600378"

        pcts, ctx = market_percentiles(df, ["600171", "600378"])
        assert "600171" in pcts
        assert "600378" in pcts


# ======================================================================
# PicksReverseEngineer 集成测试
# ======================================================================
class TestPicksReverseEngineer:
    """完整流程的集成测试 (全 mock)"""

    @patch("src.analysis.reverse_engineer.ScreenerDataProvider")
    @patch("src.analysis.reverse_engineer.BaostockProvider")
    def test_full_run(self, mock_bp_cls, mock_sdp_cls, tmp_path):
        """完整流程应生成两张 CSV"""
        # Mock BaostockProvider
        df = make_daily_df(n=80, start_date="2026-03-01")
        mock_bp = MagicMock()
        mock_bp.get_k_data.return_value = df
        mock_bp.__enter__ = MagicMock(return_value=mock_bp)
        mock_bp.__exit__ = MagicMock(return_value=False)
        mock_bp_cls.return_value = mock_bp

        # Mock ScreenerDataProvider
        np.random.seed(42)
        spot_df = pd.DataFrame({
            "代码": [f"{i:06d}" for i in range(50)],
            "名称": [f"股票{i}" for i in range(50)],
            "最新价": np.random.uniform(5, 200, 50),
            "涨跌幅": np.random.uniform(-10, 10, 50),
            "总市值": np.random.uniform(1e9, 1e12, 50),
            "换手率": np.random.uniform(0.1, 20, 50),
            "成交额": np.random.uniform(1e6, 1e10, 50),
        })
        mock_sdp = MagicMock()
        mock_sdp.get_all_a_shares.return_value = spot_df
        mock_sdp_cls.return_value = mock_sdp

        # 用一条简化的推荐记录
        trade_date_dash = df.iloc[60]["date"]
        trade_date = trade_date_dash.replace("-", "")
        picks = [(trade_date, "600378", "昊华科技", "化学制品", 40.16)]

        output_dir = str(tmp_path / "reverse_test")
        engine = PicksReverseEngineer(
            picks=picks,
            empty_days=[],
            output_dir=output_dir,
        )
        feats_df, ctx_df = engine.run()

        # 验证输出
        assert not feats_df.empty
        assert len(feats_df) == 1
        assert feats_df.iloc[0]["ts_code"] == "600378"

        # 验证 CSV 文件已生成
        assert os.path.exists(os.path.join(output_dir, "picks_features.csv"))
        assert os.path.exists(os.path.join(output_dir, "market_context.csv"))

    @patch("src.analysis.reverse_engineer.ScreenerDataProvider")
    @patch("src.analysis.reverse_engineer.BaostockProvider")
    def test_empty_data_graceful(self, mock_bp_cls, mock_sdp_cls, tmp_path):
        """数据为空时应优雅处理"""
        mock_bp = MagicMock()
        mock_bp.get_k_data.return_value = pd.DataFrame()
        mock_bp.__enter__ = MagicMock(return_value=mock_bp)
        mock_bp.__exit__ = MagicMock(return_value=False)
        mock_bp_cls.return_value = mock_bp

        mock_sdp = MagicMock()
        mock_sdp.get_all_a_shares.return_value = pd.DataFrame()
        mock_sdp_cls.return_value = mock_sdp

        output_dir = str(tmp_path / "reverse_empty")
        engine = PicksReverseEngineer(
            picks=[("20260528", "999999", "空票", "未知", 10.0)],
            empty_days=[],
            output_dir=output_dir,
        )
        feats_df, ctx_df = engine.run()

        # 应有一行带 error 字段
        assert len(feats_df) == 1
        assert "error" in feats_df.columns

    @patch("src.analysis.reverse_engineer.ScreenerDataProvider")
    @patch("src.analysis.reverse_engineer.BaostockProvider")
    def test_empty_days_context(self, mock_bp_cls, mock_sdp_cls, tmp_path):
        """空仓日应在 market_context 中出现"""
        mock_bp = MagicMock()
        mock_bp.get_k_data.return_value = pd.DataFrame()
        mock_bp.__enter__ = MagicMock(return_value=mock_bp)
        mock_bp.__exit__ = MagicMock(return_value=False)
        mock_bp_cls.return_value = mock_bp

        spot_df = pd.DataFrame({
            "代码": ["000001"],
            "名称": ["测试"],
            "最新价": [10.0],
            "涨跌幅": [1.0],
            "总市值": [1e10],
            "换手率": [2.0],
            "成交额": [1e8],
        })
        mock_sdp = MagicMock()
        mock_sdp.get_all_a_shares.return_value = spot_df
        mock_sdp_cls.return_value = mock_sdp

        output_dir = str(tmp_path / "reverse_empty_days")
        engine = PicksReverseEngineer(
            picks=[],
            empty_days=["20260529", "20260601"],
            output_dir=output_dir,
        )
        feats_df, ctx_df = engine.run()

        # context 应包含两个空仓日
        assert len(ctx_df) == 2
        assert set(ctx_df["trade_date"]) == {"20260529", "20260601"}
        assert all(ctx_df["has_picks"] == 0)
