import time
import pandas as pd
from src.strategy.backtest.screener_rule import ScreenerRuleStrategy

def test_perf():
    # Mock data
    import numpy as np
    dates = pd.date_range("2020-01-01", periods=1000, freq="D")
    df = pd.DataFrame({
        "open": np.random.randn(1000).cumsum() + 100,
        "high": np.random.randn(1000).cumsum() + 102,
        "low": np.random.randn(1000).cumsum() + 98,
        "close": np.random.randn(1000).cumsum() + 100,
        "volume": np.random.randint(1000, 10000, 1000)
    }, index=dates)
    
    # Just to make it a mock BT data feed format roughly
    class MockData:
        def __init__(self, df):
            self.open = type('obj', (object,), {'array': df['open'].values})()
            self.high = type('obj', (object,), {'array': df['high'].values})()
            self.low = type('obj', (object,), {'array': df['low'].values})()
            self.close = type('obj', (object,), {'array': df['close'].values})()
            self.volume = type('obj', (object,), {'array': df['volume'].values})()
            self._len = len(df)
        def __len__(self): return self._len

    strat = ScreenerRuleStrategy()
    strat.data = MockData(df)
    
    # Init conditions
    conds = strat._init_conditions([
        {"type": "multi_ma_bull", "ma_list": [5, 10, 20, 60], "require_all_above": True, "require_up_trend": True},
        {"type": "daily_macd_divergence", "lookback_bars": 60}
    ])
    
    t0 = time.time()
    signals = strat._precompute_signals(conds, "all")
    print(f"Precompute took {time.time() - t0:.2f}s for 1000 bars.")

if __name__ == "__main__":
    test_perf()
