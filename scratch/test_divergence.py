import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider
from src.screener.conditions import WeeklyMACDBottomDivergenceCondition
import pandas as pd

provider = ScreenerDataProvider()
# try a few stocks
codes = ["000001", "600519", "000858", "600036", "601318"]
cond = WeeklyMACDBottomDivergenceCondition(lookback_bars=60)

for code in codes:
    df = provider.get_weekly_ohlcv(code)
    if df is not None and not df.empty:
        # mock spot_row
        res = cond.evaluate_full(pd.Series(), df)
        print(f"{code}: {res}")
    else:
        print(f"{code}: No data")

