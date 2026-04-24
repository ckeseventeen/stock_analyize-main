import sys
import os
from pathlib import Path
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider
from src.screener.conditions import PriceAboveMACondition
from src.analysis.technical.indicators import TechnicalAnalyzer

def run_test():
    provider = ScreenerDataProvider()
    df = provider.get_daily_ohlcv("000001", days_back=1000, prefer="baostock", market="a")
    
    # Correct mapping
    df_bt = df.copy()
    rename_map = {"开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交量": "volume"}
    df_bt.rename(columns={k: v for k, v in rename_map.items() if k in df_bt.columns}, inplace=True)
    df_bt["开盘"] = df_bt["open"]
    df_bt["最高"] = df_bt["high"]
    df_bt["最低"] = df_bt["low"]
    df_bt["收盘"] = df_bt["close"]
    df_bt["成交量"] = df_bt["volume"]
    
    sub_df = df_bt.iloc[:50]
    ta = TechnicalAnalyzer(sub_df)
    ta.add_moving_averages([20])
    df_ta = ta.get_dataframe()
    print("df_ta columns:", df_ta.columns)
    print("last close:", df_ta.iloc[-1]["close"])
    print("last ma_20:", df_ta.iloc[-1]["ma_20"])
    
if __name__ == "__main__":
    run_test()
