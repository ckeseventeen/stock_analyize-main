"""
src/data/fetcher/baostock_provider.py - Baostock Provider Wrapper
"""
from __future__ import annotations

import contextlib
import os
import sys
import threading
from datetime import datetime, timedelta

import baostock as bs
import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)
_bs_lock = threading.Lock()
_bs_session_lock = threading.Lock()
_bs_ref_count = 0
_bs_logged_in = False


@contextlib.contextmanager
def _suppress_stdout():
    """Baostock library prints too many error messages to stdout, we swallow them."""
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


def _bs_acquire() -> bool:
    global _bs_ref_count, _bs_logged_in
    with _bs_session_lock:
        if _bs_ref_count == 0 and not _bs_logged_in:
            with _bs_lock, _suppress_stdout():
                lg = bs.login()
                if lg.error_code == "0":
                    _bs_logged_in = True
                    logger.debug("Baostock Session Login Success")
                else:
                    return False
        _bs_ref_count += 1
    return True


def _bs_release() -> None:
    global _bs_ref_count, _bs_logged_in
    with _bs_session_lock:
        _bs_ref_count = max(0, _bs_ref_count - 1)
        if _bs_ref_count == 0 and _bs_logged_in:
            try:
                with _bs_lock, _suppress_stdout():
                    bs.logout()
            except Exception:
                pass
            finally:
                _bs_logged_in = False


def to_bs_code(code: str | None) -> str:
    if not code:
        return ""
    code = str(code).strip().lower()
    if "." in code:
        parts = code.split(".")
        code = parts[0] if len(parts[0]) == 6 else (parts[1] if len(parts[1]) == 6 else code)
    if len(code) != 6 or not code.isdigit():
        return code
    return "sh." + code if code.startswith("6") else "sz." + code


def _rs_to_df(rs) -> pd.DataFrame:
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    return pd.DataFrame(rows, columns=rs.fields) if rows else pd.DataFrame()


class BaostockProvider:
    def __init__(self):
        self._acquired = False

    def login(self):
        if not self._acquired and _bs_acquire():
            self._acquired = True
        return self

    def logout(self):
        if self._acquired:
            _bs_release()
            self._acquired = False

    def __enter__(self):
        return self.login()

    def __exit__(self, t, v, tb):
        self.logout()

    def _safe_query(self, func, **kwargs) -> pd.DataFrame:
        global _bs_logged_in
        with _bs_lock:
            if not _bs_logged_in:
                with _suppress_stdout():
                    lg = bs.login()
                    if lg.error_code == "0":
                        _bs_logged_in = True
                    else:
                        return pd.DataFrame()
            try:
                with _suppress_stdout():
                    return _rs_to_df(func(**kwargs))
            except Exception:
                return pd.DataFrame()

    def get_k_data(self, code, days_back=120, frequency="d", **kwargs) -> pd.DataFrame:
        bs_code = to_bs_code(code)
        if len(bs_code) != 9:
            return pd.DataFrame()
        start = kwargs.get("start_date") or (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        end = kwargs.get("end_date") or datetime.now().strftime("%Y-%m-%d")
        fields = kwargs.get("fields") or (
            "date,open,high,low,close,volume,amount,turn,peTTM,pbMRQ,psTTM,isST"
            if frequency == "d"
            else "date,open,high,low,close,volume,amount,turn,pctChg"
        )
        df = self._safe_query(
            bs.query_history_k_data_plus,
            code=bs_code,
            fields=fields,
            start_date=start,
            end_date=end,
            frequency=frequency,
            adjustflag=kwargs.get("adjust", "2")
        )
        if not df.empty:
            cols = [c for c in df.columns if c not in ("date", "code", "isST")]
            for c in cols:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    def get_all_stocks(self, date=None):
        target_date = date or datetime.now().strftime("%Y-%m-%d")
        df = self._safe_query(bs.query_all_stock, day=target_date)
        if not df.empty:
            return df
        
        # If the query for the specified (or default today's) date returns an empty DataFrame,
        # fallback up to 7 previous days to find the latest valid historical trading day.
        logger.info(f"Baostock query_all_stock for {target_date} returned empty. Trying previous days...")
        try:
            curr = datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            curr = datetime.now()
            
        for i in range(1, 8):
            prev_date = (curr - timedelta(days=i)).strftime("%Y-%m-%d")
            df = self._safe_query(bs.query_all_stock, day=prev_date)
            if not df.empty:
                logger.info(f"Successfully retrieved all stocks from Baostock using date: {prev_date}")
                return df
                
        return pd.DataFrame()

    def get_valuation_history(self, code, days_back=365*5, val_type="pe"):
        f = {"pe": "peTTM", "pb": "pbMRQ", "ps": "psTTM"}.get(val_type, "peTTM")
        df = self.get_k_data(code, days_back=days_back, fields=f"date,{f}")
        if df.empty:
            return df
        v = {"peTTM": "pe_ttm", "pbMRQ": "pb_mrq", "psTTM": "ps_ttm"}.get(f)
        df = df.rename(columns={"date": "trade_date", f: v})
        df[v] = pd.to_numeric(df[v], errors="coerce")
        return df.dropna(subset=[v])[df[v] != 0]

    def get_profit_data(self, code, year=0, quarter=0):
        c = to_bs_code(code)
        if len(c) != 9:
            return pd.DataFrame()
        if year == 0:
            now = datetime.now()
            year, quarter = now.year, (now.month - 1) // 3
            if quarter == 0:
                year -= 1
                quarter = 4
        for _ in range(4):
            df = self._safe_query(bs.query_profit_data, code=c, year=year, quarter=quarter)
            if not df.empty:
                return df
            quarter = (quarter - 1) if quarter > 1 else 4
            if quarter == 4:
                year -= 1
        return pd.DataFrame()

    def get_profit_history(self, code, num_years=5):
        c = to_bs_code(code)
        if len(c) != 9:
            return pd.DataFrame()
        dfs = []
        d_latest = self.get_profit_data(code)
        if not d_latest.empty:
            dfs.append(d_latest)
        for i in range(1, num_years + 1):
            d_y = self._safe_query(bs.query_profit_data, code=c, year=datetime.now().year - i, quarter=4)
            if not d_y.empty:
                dfs.append(d_y)
        if not dfs:
            return pd.DataFrame()
        df = pd.concat(dfs, ignore_index=True)
        cols = [c for c in df.columns if c not in ("code", "pubDate", "statDate")]
        for c in cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.drop_duplicates(subset=["statDate"]).sort_values("statDate", ascending=False)

    def get_latest_price(self, code):
        df = self.get_k_data(code, days_back=10, fields="date,close,volume,amount,turn,peTTM,pbMRQ,psTTM")
        if df.empty:
            return {"price": 0.0}
        last_row = df.iloc[-1]
        return {
            "price": float(last_row.get("close", 0) or 0),
            "peTTM": float(last_row.get("peTTM", 0) or 0),
            "pbMRQ": float(last_row.get("pbMRQ", 0) or 0),
            "psTTM": float(last_row.get("psTTM", 0) or 0),
            "turn": float(last_row.get("turn", 0) or 0),
            "volume": float(last_row.get("volume", 0) or 0),
            "amount": float(last_row.get("amount", 0) or 0),
        }
