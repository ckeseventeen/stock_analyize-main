"""
tests/conftest.py — 共享测试 Fixtures

提供合成数据用于技术指标、背离检测、因子计算、筛选器测试。
"""
import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def synthetic_ohlcv_df():
    """
    合成 250 行日线 OHLCV 数据（模拟震荡上行走势）

    列名为 akshare 中文格式：日期, 开盘, 最高, 最低, 收盘, 成交量
    """
    np.random.seed(42)
    n = 250
    dates = pd.bdate_range("2024-01-02", periods=n)

    # 基准价格：带噪声的缓慢上行
    base = 50 + np.cumsum(np.random.randn(n) * 0.5 + 0.02)
    close = base + np.random.randn(n) * 0.3
    open_ = close + np.random.randn(n) * 0.2
    high = np.maximum(open_, close) + np.abs(np.random.randn(n) * 0.5)
    low = np.minimum(open_, close) - np.abs(np.random.randn(n) * 0.5)
    volume = np.random.randint(100000, 5000000, size=n).astype(float)

    return pd.DataFrame({
        "日期": dates,
        "开盘": open_,
        "最高": high,
        "最低": low,
        "收盘": close,
        "成交量": volume,
    })


@pytest.fixture
def synthetic_ohlcv_en():
    """合成 OHLCV 数据（英文列名版本）"""
    np.random.seed(42)
    n = 250
    dates = pd.bdate_range("2024-01-02", periods=n)
    base = 50 + np.cumsum(np.random.randn(n) * 0.5 + 0.02)
    close = base + np.random.randn(n) * 0.3
    open_ = close + np.random.randn(n) * 0.2
    high = np.maximum(open_, close) + np.abs(np.random.randn(n) * 0.5)
    low = np.minimum(open_, close) - np.abs(np.random.randn(n) * 0.5)
    volume = np.random.randint(100000, 5000000, size=n).astype(float)

    return pd.DataFrame({
        "date": dates,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    })


@pytest.fixture
def divergence_bottom_df():
    """
    专门构造的底背离数据：
    - 价格创新低（第二个低点比第一个更低）
    - MACD柱状图未创新低（第二个低点的MACD比第一个更高）

    构造方法：两段下跌，第二段价格更低但下跌速度减缓
    """
    np.random.seed(100)
    n = 80

    # 第一段下跌 (0-30)：从100跌到70
    seg1 = np.linspace(100, 70, 30)
    # 反弹 (30-50)：70回到85
    seg2 = np.linspace(70, 85, 20)
    # 第二段下跌 (50-70)：85跌到65（价格更低），但斜率缓和
    seg3 = np.linspace(85, 65, 20)
    # 尾部回升 (70-80)
    seg4 = np.linspace(65, 72, 10)

    close = np.concatenate([seg1, seg2, seg3, seg4])
    close += np.random.randn(n) * 0.5  # 加微小噪声

    dates = pd.bdate_range("2024-01-02", periods=n)
    high = close + np.abs(np.random.randn(n) * 0.3)
    low = close - np.abs(np.random.randn(n) * 0.3)
    open_ = close + np.random.randn(n) * 0.1
    volume = np.random.randint(100000, 3000000, size=n).astype(float)

    return pd.DataFrame({
        "date": dates,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    })


@pytest.fixture
def monotone_uptrend_df():
    """单调上涨的 OHLCV 数据，不应检测到底背离"""
    n = 100
    dates = pd.bdate_range("2024-01-02", periods=n)
    close = np.linspace(50, 150, n) + np.random.randn(n) * 0.1
    high = close + 0.5
    low = close - 0.5
    open_ = close - 0.2
    volume = np.full(n, 1000000.0)

    return pd.DataFrame({
        "date": dates,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    })


@pytest.fixture
def spot_data_row():
    """模拟 ak.stock_zh_a_spot_em() 的单行数据"""
    return pd.Series({
        "代码": "600519",
        "名称": "贵州茅台",
        "最新价": 1800.0,
        "涨跌幅": 1.25,
        "总市值": 2.26e12,  # 2.26万亿
        "流通市值": 2.26e12,
        "市盈率-动态": 28.5,
        "市净率": 8.2,
        "换手率": 0.35,
        "振幅": 2.1,
    })


@pytest.fixture
def spot_data_row_loss():
    """模拟亏损股的 spot 数据（PE为负）"""
    return pd.Series({
        "代码": "000001",
        "名称": "测试亏损",
        "最新价": 5.0,
        "涨跌幅": -2.0,
        "总市值": 5e9,
        "流通市值": 4e9,
        "市盈率-动态": -15.0,
        "市净率": 0.8,
        "换手率": 3.5,
        "振幅": 4.0,
    })


@pytest.fixture
def sample_financial_df():
    """
    模拟 A 股 analyzer 清洗后的财务 DataFrame

    索引为 datetime，列包含：归母净利润, 营业总收入, 营业成本
    模拟 5 个年报 + 1 个最新季报
    """
    dates = pd.to_datetime([
        "2024-09-30",  # Q3 季报（最新）
        "2023-12-31",  # 年报
        "2023-09-30",  # Q3
        "2022-12-31",  # 年报
        "2021-12-31",  # 年报
        "2020-12-31",  # 年报
        "2019-12-31",  # 年报
    ])
    data = {
        "归母净利润": [450e8, 600e8, 420e8, 550e8, 500e8, 460e8, 400e8],
        "营业总收入": [900e8, 1200e8, 850e8, 1100e8, 1000e8, 920e8, 800e8],
        "营业成本": [400e8, 500e8, 380e8, 480e8, 450e8, 420e8, 380e8],
    }
    df = pd.DataFrame(data, index=dates)
    df.sort_index(ascending=False, inplace=True)
    return df


@pytest.fixture
def sample_financial_df_sept_fiscal():
    """
    模拟美股（如苹果）9月财年的财务数据
    """
    dates = pd.to_datetime([
        "2024-06-30",  # Q3（最新）
        "2023-09-30",  # 年报
        "2023-06-30",  # Q3
        "2022-09-30",  # 年报
        "2021-09-30",  # 年报
    ])
    data = {
        "归母净利润": [70e9, 97e9, 65e9, 99e9, 94e9],
        "营业总收入": [140e9, 383e9, 130e9, 394e9, 365e9],
        "营业成本": [60e9, 160e9, 55e9, 170e9, 155e9],
    }
    df = pd.DataFrame(data, index=dates)
    df.sort_index(ascending=False, inplace=True)
    return df
