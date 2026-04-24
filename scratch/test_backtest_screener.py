import sys
import os
from pathlib import Path
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider
from src.strategy.backtest.runner import BacktestRunner
from src.strategy.backtest.screener_rule import ScreenerRuleStrategy

def run_test():
    provider = ScreenerDataProvider()
    df = provider.get_daily_ohlcv("000001", days_back=1000, prefer="baostock", market="a")
    
    if df is None or df.empty:
        return
        
    params = {
        "buy_conditions": [
            {"type": "price_above_ma", "ma_period": 20}
        ],
        "sell_conditions": [
            {"type": "price_above_ma", "ma_period": 60}
        ],
        "buy_logic": "all",
        "sell_logic": "any",
        "position_size": 0.95
    }
    
    runner = BacktestRunner(
        strategy_class=ScreenerRuleStrategy,
        data_df=df,
        **params
    )
    res = runner.run()
    
    print("Report:")
    for k, v in res.items():
        print(f"  {k}: {v}")

if __name__ == "__main__":
    run_test()
