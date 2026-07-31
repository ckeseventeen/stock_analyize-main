"""
src/data/providers/eastmoney_spot.py — 东财全A行情直连 Provider（分页并发）

为什么自建而不用 akshare 的 stock_zh_a_spot_em：
  akshare 该接口一次性索要 100+ 字段且不带常规 UA，在本机/多数环境下被东财
  反爬直接掐连接（RemoteDisconnected），触发降级到问财——而问财分页不稳定，
  曾返回 2649/5300 只且 97.9% 市值为 NaN、缺失全部大盘蓝筹，导致筛选恒 0。

本实现只取筛选真正需要的 9 个字段、带常规 UA、按 100 只/页并发翻页：
  实测 5888 只全市场 **1.5 秒**（8 线程 × 59 页），市值有效率 94%，
  比问财（~70s 且残缺）和 Baostock（~20min）都快且完整。

字段映射（东财 clist f-code → 项目统一中文列名）见 _FIELD_MAP。
"""
from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("em_spot")

# 东财行情推送域名（多个子域名互为备份，逐个尝试）
_HOSTS = (
    "82.push2.eastmoney.com",
    "push2.eastmoney.com",
    "1.push2.eastmoney.com",
    "7.push2.eastmoney.com",
)
_PATH = "/api/qt/clist/get"

# 市场范围：沪深主板 + 创业板 + 科创板 + 北交所
_FS = "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048"

# f-code → 统一中文列名（与 akshare stock_zh_a_spot_em 对齐，下游零改动）
_FIELD_MAP = {
    "f12": "代码",
    "f14": "名称",
    "f2": "最新价",
    "f3": "涨跌幅",
    "f8": "换手率",
    "f9": "市盈率-动态",
    "f20": "总市值",
    "f21": "流通市值",
    "f23": "市净率",
}
_FIELDS = ",".join(_FIELD_MAP)

_PAGE_SIZE = 100          # 东财硬限制：单页最多 100 条（pz 传更大也只回 100）
_NUMERIC_COLS = ("最新价", "涨跌幅", "换手率", "市盈率-动态",
                 "总市值", "流通市值", "市净率")


def _session():
    """带常规 UA 且不吃系统代理的 Session（代理策略由 data_provider 全局决定）"""
    import requests

    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0 Safari/537.36"),
        "Referer": "https://quote.eastmoney.com/",
    })
    return s


def _fetch_page(sess, host: str, pn: int, timeout: float = 12.0) -> tuple[list, int]:
    """拉单页，返回 (行列表, 全市场总数)"""
    params = {
        "pn": pn, "pz": _PAGE_SIZE, "po": 1, "np": 1,
        "fltt": 2, "invt": 2, "fid": "f3", "fs": _FS, "fields": _FIELDS,
    }
    r = sess.get(f"https://{host}{_PATH}", params=params, timeout=timeout)
    r.raise_for_status()
    data = (r.json() or {}).get("data") or {}
    return (data.get("diff") or []), int(data.get("total") or 0)


def fetch_all_a_spot(max_workers: int = 8, timeout: float = 12.0) -> pd.DataFrame:
    """
    拉全A股实时行情（含总市值/PE/PB/换手率）。

    Returns:
        DataFrame（列名同 akshare stock_zh_a_spot_em 风格）；失败返回空表。
        已过滤 ST/退市，数值列已转 numeric。
    """
    sess = _session()

    # 第一页兼探测：拿到 total 才知道要翻多少页；同时验证域名可用
    first_rows: list = []
    total = 0
    used_host = None
    for host in _HOSTS:
        try:
            first_rows, total = _fetch_page(sess, host, 1, timeout)
            if first_rows and total:
                used_host = host
                break
            logger.debug(f"东财 {host} 返回空，尝试下一个域名")
        except Exception as e:
            logger.debug(f"东财 {host} 不可用: {type(e).__name__}: {e}")
    if not used_host or not first_rows:
        logger.warning("东财 clist 全部域名不可用")
        return pd.DataFrame()

    pages = math.ceil(total / _PAGE_SIZE)
    rows = list(first_rows)
    failed_pages = 0
    if pages > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_fetch_page, sess, used_host, pn, timeout): pn
                for pn in range(2, pages + 1)
            }
            for fut in as_completed(futures):
                try:
                    page_rows, _ = fut.result()
                    rows.extend(page_rows)
                except Exception as e:
                    failed_pages += 1
                    logger.debug(f"东财第 {futures[fut]} 页失败: {type(e).__name__}: {e}")

    if failed_pages:
        logger.warning(
            f"东财分页有 {failed_pages}/{pages} 页失败，本次仅取到 {len(rows)}/{total} 只"
        )
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).rename(columns=_FIELD_MAP)
    keep = [c for c in _FIELD_MAP.values() if c in df.columns]
    df = df[keep]

    df["代码"] = df["代码"].astype(str).str.zfill(6)
    df = df.drop_duplicates(subset=["代码"], keep="first")
    if "名称" in df.columns:
        df = df[~df["名称"].astype(str).str.contains("ST|退市", na=False)]
    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info(
        f"东财直连拉取成功：全市场 {total} 只 → 过滤ST/退市后 {len(df)} 只"
        f"（域名 {used_host}）"
    )
    return df.reset_index(drop=True)
