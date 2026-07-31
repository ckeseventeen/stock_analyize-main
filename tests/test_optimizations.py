import pytest
import pandas as pd
import numpy as np
from src.core.visualizer import Visualizer
from src.data.providers.cache_manager import CacheManager
from src.data.fetchers import retry_on_exception

def test_visualizer_plotly():
    # Mock data
    dates = pd.date_range("2023-01-01", periods=5, freq="YE")
    annual_df = pd.DataFrame({
        "营业总收入": [100e8, 110e8, 120e8, 130e8, 140e8],
        "归母净利润": [10e8, 11e8, 12e8, 13e8, 14e8],
        "营业成本": [80e8, 85e8, 90e8, 95e8, 100e8],
        "毛利率": [0.2, 0.22, 0.25, 0.27, 0.28]
    }, index=dates)

    hist_dates = pd.date_range("2023-01-01", periods=100, freq="D")
    hist_val = pd.DataFrame({
        "trade_date": hist_dates.strftime("%Y-%m-%d"),
        "pe_ttm": np.random.uniform(10, 20, 100)
    })

    result = {
        "annual_df": annual_df,
        "hist_val": hist_val,
        "hist_percentile": 25.0,
        "scenarios": [100.0, 120.0, 140.0],
        "price": 110.0,
        "current_pe": 15.0,
        "ttm_revenue": 140e8,
        "ttm_net_profit": 14e8,
        "market_name": "A股"
    }

    stock_config = {
        "name": "测试股票",
        "code": "000001",
        "valuation": "pe",
        "pe_range": [10, 15, 20],
        "category_name": "测试"
    }

    viz = Visualizer(result, stock_config)
    
    fig_rev = viz.plot_revenue_profit_plotly()
    assert fig_rev is not None

    fig_hist = viz.plot_hist_valuation_plotly()
    assert fig_hist is not None

    fig_scen = viz.plot_scenario_plotly()
    assert fig_scen is not None


def test_cache_manager_lru_trigger(tmp_path):
    cm = CacheManager(cache_dir=str(tmp_path), max_mb=1) # 1MB limit
    
    # Check that enforce capacity is only run when write_count % 50 == 0
    # Let's count how many times _enforce_capacity was run by mocking it
    runs = 0
    original_enforce = cm._enforce_capacity
    def mock_enforce():
        nonlocal runs
        runs += 1
        original_enforce()
    
    cm._enforce_capacity = mock_enforce

    for i in range(120):
        cm.set(f"key_{i}", pd.DataFrame({"a": [i]}))

    # 120 writes should trigger enforce capacity twice (at 50 and 100)
    assert runs == 2


def test_retry_on_exception_decorator():
    calls = 0

    @retry_on_exception(max_retries=3, initial_delay=0.01)
    def flaker():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("Transient error")
        return "success"

    res = flaker()
    assert res == "success"
    assert calls == 3
