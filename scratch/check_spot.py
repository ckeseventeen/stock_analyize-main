import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.screener.data_provider import ScreenerDataProvider

provider = ScreenerDataProvider()
spot_df = provider.get_all_a_shares()

for code in ["000001", "600036"]:
    row = spot_df[spot_df["代码"] == code]
    if not row.empty:
        print(f"{code} Market Cap (亿): {row['总市值'].values[0] / 1e8}")
        print(f"{code} PE (动态): {row['市盈率-动态'].values[0]}")
    else:
        print(f"{code} not found in spot data")
