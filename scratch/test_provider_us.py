from src.screener.data_provider import ScreenerDataProvider
import pandas as pd
import logging

# Configure logging to see our debug messages
logging.basicConfig(level=logging.DEBUG)

def test_provider():
    provider = ScreenerDataProvider()
    stock_code = "HIMS"
    market = "us"
    days_back = 1000
    
    print(f"Testing provider.get_daily_ohlcv for {stock_code} in {market}...")
    df = provider.get_daily_ohlcv(stock_code, days_back=days_back, market=market)
    
    if df is not None and not df.empty:
        print(f"Success! Rows: {len(df)}")
        print(df.tail(2))
    else:
        print("Failed to get data through provider.")

if __name__ == "__main__":
    test_provider()
