"""
src/screener/data_provider.py — 筛选器数据提供层

批量获取A股实时行情和历史K线数据，供筛选器使用。
使用 CacheManager 缓存，减少重复API请求。

数据源优先级：akshare (主，含总市值) → Baostock (fallback，用 amount/turn 推算流通市值)

K 线获取路径：
  - 单只：get_weekly_ohlcv / get_daily_ohlcv
  - 批量并发预取：prefetch_ohlcv_batch(codes, period, max_workers)
    akshare 线程安全（每次独立 HTTP 请求），8 worker 并发可提速 6-8x
"""
import contextlib
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import akshare as ak
import pandas as pd

from src.data.providers.baostock_provider import BaostockProvider
from src.data.providers.cache_manager import CacheManager
from src.utils.logger import get_logger

logger = get_logger("screener_data")


# 全局代理清理标志（一次性）
_PROXY_CLEARED = False


def _ensure_no_proxy_disable():
    """
    一次性禁用 HTTP(S) 代理环境变量（整个进程生命周期内只执行一次）。

    背景：用户报告"不走代理时 akshare 正常"。akshare 内部用 requests，
    会自动读取 HTTP_PROXY/HTTPS_PROXY；当系统代理不通时会直接失败。
    本函数在进程启动时第一次调用后永久清空代理 env，避免各处重复写
    ``with _no_proxy()``。
    """
    global _PROXY_CLEARED
    if _PROXY_CLEARED:
        return
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
              "ALL_PROXY", "all_proxy"):
        os.environ.pop(k, None)
    os.environ["NO_PROXY"] = "*"
    _PROXY_CLEARED = True
    logger.debug("代理环境变量已全局禁用（一次清理，全进程生效）")


class ScreenerDataProvider:
    """
    筛选器数据提供层

    使用示例:
        provider = ScreenerDataProvider()
        all_stocks = provider.get_all_a_shares()      # 全A实时行情
        weekly_df = provider.get_weekly_ohlcv("600519")  # 单只周线
    """

    def __init__(self, cache_dir: str = ".cache/screener", spot_ttl_hours: int = 1, ohlcv_ttl_hours: int = 4):
        self._cache = CacheManager(cache_dir=cache_dir, ttl_hours=ohlcv_ttl_hours)
        self._spot_ttl = spot_ttl_hours
        self._ohlcv_ttl = ohlcv_ttl_hours
        self._bp_session: BaostockProvider | None = None
        self._bp_session_lock = Lock()
        # 全局禁用代理，避免 akshare 调用被系统代理阻塞
        _ensure_no_proxy_disable()

    @contextlib.contextmanager
    def session(self):
        """
        开启一个常驻 Baostock 会话，供批量拉 K 线时复用，避免每只股票都 login/logout。

        使用::
            with provider.session():
                for code in codes:
                    df = provider.get_daily_ohlcv(code)
        """
        if self._bp_session is not None:
            # 已在 session 中，直接 yield
            yield self
            return
        bp = BaostockProvider()
        bp.login()
        self._bp_session = bp
        try:
            yield self
        finally:
            try:
                bp.logout()
            except Exception:
                pass
            self._bp_session = None

    def get_all_a_shares(self) -> pd.DataFrame:
        """
        获取全A股行情数据（约5000只）

        优先级：akshare (一次拉完，含 总市值/PE/PB) →
                akshare 备用API (stock_zh_a_spot) →
                Baostock (逐只拉 K 线，无总市值)

        akshare 有多个行情 API 端点，如果一个挂了可用另一个兜底，
        避免降级到慢 ~100 倍的 Baostock per-stock 查询。

        返回列包含：代码, 名称, 最新价, 总市值, 市盈率-动态, 市净率, 换手率, 涨跌幅 等。
        """

        def _fetch_baostock():
            """通过 Baostock 获取全A行情数据；从 amount/turn 推算流通市值填入'总市值'列"""
            logger.info("正在通过 Baostock 拉取全A股行情数据...")
            with BaostockProvider() as bp:
                # 1. 获取全市场股票列表（快速，一次调用）
                all_stocks = bp.get_all_stocks()
                if all_stocks.empty:
                    return pd.DataFrame()

                # 过滤只保留 A股（sh.6xxx, sz.0xxx, sz.3xxx），排除指数和B股
                a_codes = all_stocks[
                    all_stocks["code"].str.match(r"^(sh\.6|sz\.0|sz\.3)")
                ]["code"].tolist()
                logger.info(f"Baostock 全市场 {len(all_stocks)} 只，A股过滤后 {len(a_codes)} 只")

                # 2. 分批拉取最新日K线（仅含少量字段减小响应）
                today = pd.Timestamp.now().strftime("%Y-%m-%d")
                week_ago = (pd.Timestamp.now() - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
                # Baostock 有全局 _bs_lock，无法并行；通过减少字段数加速
                MINIMAL_FIELDS = "date,close,volume,amount,turn,pctChg,peTTM,pbMRQ"

                rows = []
                total = len(a_codes)
                log_interval = max(1, total // 10)  # 每10%进度日志一次
                for i, bs_code in enumerate(a_codes):
                    try:
                        df_k = bp.get_k_data(
                            bs_code,
                            start_date=week_ago,
                            end_date=today,
                            frequency="d",
                            fields=MINIMAL_FIELDS,
                        )
                        if df_k is not None and not df_k.empty:
                            latest = df_k.iloc[-1]
                            code_name_row = all_stocks[all_stocks["code"] == bs_code]
                            name = code_name_row["code_name"].values[0] if not code_name_row.empty and "code_name" in code_name_row.columns else ""
                            pure_code = bs_code.split(".")[-1]

                            # 从 amount / turn 推算流通市值
                            try:
                                amount = float(latest.get("amount", 0) or 0)
                                turn = float(latest.get("turn", 0) or 0)
                                est_mv = amount * 100 / turn if turn > 0 else 0.0
                            except (ValueError, TypeError):
                                est_mv = 0.0

                            try:
                                close = float(latest.get("close", 0) or 0)
                                close / (1 + float(latest.get("pctChg", 0) or 0) / 100) if float(latest.get("pctChg", 0) or 0) != 0 else close
                                pct_chg = float(latest.get("pctChg", 0) or 0)
                            except (ValueError, TypeError, ZeroDivisionError):
                                pct_chg = 0.0

                            rows.append({
                                "代码": pure_code,
                                "名称": name,
                                "最新价": latest.get("close", 0),
                                "涨跌幅": pct_chg,
                                "总市值": est_mv,
                                "流通市值": est_mv,
                                "市盈率-动态": latest.get("peTTM", 0),
                                "市净率": latest.get("pbMRQ", 0),
                                "换手率": latest.get("turn", 0),
                                "成交量": latest.get("volume", 0),
                                "成交额": latest.get("amount", 0),
                            })
                    except Exception:
                        continue

                    # 进度日志（每10%）
                    if (i + 1) % log_interval == 0 or i == total - 1:
                        logger.info(f"  Baostock 进度: {i+1}/{total} ({((i+1)/total*100):.0f}%)")

                if not rows:
                    return pd.DataFrame()

                result = pd.DataFrame(rows)
                for col in ("最新价", "总市值", "流通市值", "市盈率-动态", "市净率",
                            "换手率", "成交量", "成交额"):
                    if col in result.columns:
                        result[col] = pd.to_numeric(result[col], errors="coerce")

                result = result[~result["名称"].str.contains("ST|退市", na=False)]
                ok_mv = (result["总市值"] > 0).sum() if "总市值" in result.columns else 0
                logger.info(
                    f"Baostock 全A数据拉取完成，共 {len(result)} 只（过滤ST后）；"
                    f"其中 {ok_mv} 只成功推算总市值"
                )
                return result

        def _fetch_akshare():
            """akshare 主路径（代理已全局禁用，无需逐处包裹）"""
            logger.info("正在通过 akshare 拉取全A股实时行情数据...")
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                logger.info(f"akshare 全A股实时行情拉取成功，共 {len(df)} 只股票")
                df = df[~df["名称"].str.contains("ST|退市", na=False)]
                for col in ("总市值", "流通市值", "市盈率-动态", "市净率", "换手率", "最新价", "涨跌幅", "振幅"):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                logger.info(f"过滤ST/退市后剩余 {len(df)} 只")
                return df
            logger.warning("akshare 全A股实时行情数据为空")
            return pd.DataFrame()

        def _fetch_akshare_fallback():
            """akshare 备用 API：stock_zh_a_spot (老接口，数据结构不同但稳定)"""
            logger.info("正在通过 akshare 备用API拉取全A股行情数据...")
            df = ak.stock_zh_a_spot()
            if df is not None and not df.empty:
                logger.info(f"akshare 备用API拉取成功，共 {len(df)} 只股票")
                # 列名映射到统一格式
                col_map = {
                    "code": "代码", "name": "名称", "trade": "最新价",
                    "changepercent": "涨跌幅", "pe": "市盈率-动态",
                    "mktcap": "总市值", "nmc": "流通市值",
                }
                df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
                if "代码" in df.columns:
                    df["代码"] = df["代码"].astype(str).str.zfill(6)
                if "总市值" in df.columns:
                    df["总市值"] = pd.to_numeric(df["总市值"], errors="coerce") * 1e4  # 万元→元
                if "流通市值" in df.columns:
                    df["流通市值"] = pd.to_numeric(df["流通市值"], errors="coerce") * 1e4
                df = df[~df["名称"].str.contains("ST|退市", na=False)] if "名称" in df.columns else df
                logger.info(f"过滤ST/退市后剩余 {len(df)} 只")
                return df
            logger.warning("akshare 备用API数据也为空")
            return pd.DataFrame()

        def _fetch():
            # 优先 akshare（一次调用含总市值/PE/PB/换手率，完全覆盖 Spot 条件）
            try:
                df = _fetch_akshare()
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"akshare 主API拉取失败: {e}")

            # 二级 fallback：akshare 另一套API（比 Baostock 快 100 倍）
            try:
                logger.info("akshare 主API失败，尝试备用API...")
                df = _fetch_akshare_fallback()
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"akshare 备用API也失败: {e}")

            # 三级 fallback 到 Baostock
            try:
                logger.warning("akshare 全部失败，降级到 Baostock（将用 amount/turn 推算总市值）")
                return _fetch_baostock()
            except Exception as e:
                logger.error(f"Baostock fallback 也失败: {e}")
                return pd.DataFrame()

        return self._cache.get_or_fetch("all_a_shares_spot", _fetch, ttl_hours=self._spot_ttl)

    # ========================
    # 板块/指数范围过滤
    # ========================

    # 板块/指数定义：key → (中文标签, 获取成分股的方法)
    SCOPE_DEFINITIONS: dict[str, str] = {
        "全部A股": "all",
        "沪市主板": "sh_main",
        "深市主板": "sz_main",
        "创业板": "chinext",
        "科创板": "star",
        "北交所": "bse",
        "沪深300": "csi300",
        "中证500": "csi500",
        "中证1000": "csi1000",
        "上证50": "sse50",
    }

    def get_scope_codes(self, scope_keys: list[str]) -> set[str] | None:
        """
        获取指定板块/指数的成分股代码集合。

        Args:
            scope_keys: 板块/指数 key 列表，如 ["sh_main", "chinext", "csi300"]
                        传入 ["all"] 或空列表 → 返回 None（不过滤）

        Returns:
            代码集合（纯6位数字），或 None 表示不过滤
        """
        if not scope_keys or "all" in scope_keys:
            return None

        all_codes: set[str] = set()
        for key in scope_keys:
            try:
                codes = self._fetch_scope_codes_single(key)
                if codes:
                    all_codes.update(codes)
                    logger.info(f"板块 [{key}] 获取到 {len(codes)} 只成分股")
            except Exception as e:
                logger.warning(f"板块 [{key}] 获取失败: {e}")
        return all_codes if all_codes else None

    def _fetch_scope_codes_single(self, key: str) -> set[str]:
        """获取单个板块/指数的成分股代码"""

        def _fetch():
            return self._do_fetch_scope(key)

        cache_key = f"scope_{key}"
        result = self._cache.get_or_fetch(cache_key, _fetch, ttl_hours=4)
        if isinstance(result, set):
            return result
        return set(result) if result else set()

    def _do_fetch_scope(self, key: str) -> set[str]:
        """实际调用 akshare 获取板块/指数成分股"""
        codes: set[str] = set()

        # 批量前缀过滤：复用 get_all_a_shares() 的缓存，避免重复 HTTP 请求
        _SCOPE_PREFIXES = {
            "sh_main": ("60",),
            "sz_main": ("00",),
            "chinext": ("30",),
            "star": ("68",),
            "bse": ("8", "4"),
        }

        if key in _SCOPE_PREFIXES:
            df = self.get_all_a_shares()  # 复用已缓存的全市场数据
            if df is not None and not df.empty:
                all_codes = df["代码"].astype(str)
                prefixes = _SCOPE_PREFIXES[key]
                mask = all_codes.str.startswith(prefixes[0])
                for p in prefixes[1:]:
                    mask |= all_codes.str.startswith(p)
                codes = set(all_codes[mask])

        elif key == "csi300":
            # 沪深300成分股
            df = ak.index_stock_cons_csindex(symbol="000300")
            if df is not None and not df.empty:
                code_col = "成分券代码" if "成分券代码" in df.columns else df.columns[0]
                codes = set(df[code_col].astype(str).str.zfill(6))

        elif key == "csi500":
            # 中证500成分股
            df = ak.index_stock_cons_csindex(symbol="000905")
            if df is not None and not df.empty:
                code_col = "成分券代码" if "成分券代码" in df.columns else df.columns[0]
                codes = set(df[code_col].astype(str).str.zfill(6))

        elif key == "csi1000":
            # 中证1000成分股
            df = ak.index_stock_cons_csindex(symbol="000852")
            if df is not None and not df.empty:
                code_col = "成分券代码" if "成分券代码" in df.columns else df.columns[0]
                codes = set(df[code_col].astype(str).str.zfill(6))

        elif key == "sse50":
            # 上证50成分股
            df = ak.index_stock_cons_csindex(symbol="000016")
            if df is not None and not df.empty:
                code_col = "成分券代码" if "成分券代码" in df.columns else df.columns[0]
                codes = set(df[code_col].astype(str).str.zfill(6))

        else:
            logger.warning(f"未知的板块/指数 key: {key}")

        return codes

    # ========================
    # 单只 K 线获取（优先 akshare，线程安全；失败回 Baostock session）
    # ========================

    _COL_MAP_BS = {"date": "日期", "open": "开盘", "high": "最高",
                   "low": "最低", "close": "收盘", "volume": "成交量"}

    def _fetch_k_akshare(self, code: str, days_back: int, frequency: str) -> pd.DataFrame:
        """
        akshare 直连拉 K 线（无 session 概念，每次都是独立 HTTP 请求，**线程安全**）。
        这是并发加速的基石。代理绕开由外层 _no_proxy() 管理。
        """
        end_date = pd.Timestamp.now().strftime("%Y%m%d")
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=days_back)).strftime("%Y%m%d")
        {"d": "daily", "w": "weekly", "m": "monthly"}.get(frequency, "daily")
        try:
            if frequency == "d":
                df = ak.stock_zh_a_hist(
                    symbol=code, period="daily",
                    start_date=start_date, end_date=end_date, adjust="qfq",
                )
            elif frequency == "w":
                df = ak.stock_zh_a_hist(
                    symbol=code, period="weekly",
                    start_date=start_date, end_date=end_date, adjust="qfq",
                )
            else:
                df = ak.stock_zh_a_hist(
                    symbol=code, period="monthly",
                    start_date=start_date, end_date=end_date, adjust="qfq",
                )
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.debug(f"{code} akshare {frequency} 失败: {e}")
        return pd.DataFrame()

    def _fetch_k_hk_akshare(self, code: str, days_back: int, frequency: str) -> pd.DataFrame:
        """akshare 港股历史行情"""
        ak_period = {"d": "daily", "w": "weekly", "m": "monthly"}.get(frequency, "daily")
        end_date = pd.Timestamp.now().strftime("%Y%m%d")
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=days_back)).strftime("%Y%m%d")

        # 1. 尝试 Sina (支持日线，全量获取后可重采样为周线/月线)
        try:
            df = ak.stock_hk_daily(symbol=code, adjust="qfq")
            if df is not None and not df.empty:
                df.rename(columns={"date": "日期", "open": "开盘", "high": "最高",
                                  "low": "最低", "close": "收盘", "volume": "成交量"}, inplace=True)
                df["日期"] = pd.to_datetime(df["日期"])
                df.sort_values("日期", inplace=True)

                # 如果需要周线或月线，进行重采样
                if frequency in ("w", "m"):
                    df.set_index("日期", inplace=True)
                    resample_freq = "W-FRI" if frequency == "w" else "ME"
                    df = df.resample(resample_freq).agg({
                        "开盘": "first",
                        "最高": "max",
                        "最低": "min",
                        "收盘": "last",
                        "成交量": "sum"
                    }).dropna()
                    df.reset_index(inplace=True)

                # 手动日期切片
                mask = (df["日期"] >= pd.to_datetime(start_date)) & (df["日期"] <= pd.to_datetime(end_date))
                res = df.loc[mask]
                if not res.empty:
                    return res
        except Exception as e:
            logger.debug(f"HK {code} Sina {frequency} 失败: {e}")

        # 2. 尝试 Eastmoney
        try:
            # akshare 港股接口：stock_hk_hist
            df = ak.stock_hk_hist(symbol=code, period=ak_period,
                                  start_date=start_date, end_date=end_date, adjust="qfq")
            if df is not None and not df.empty:
                # 转换列名以匹配 A 股统一格式
                df.rename(columns={"日期": "日期", "开盘": "开盘", "最高": "最高",
                                  "最低": "最低", "收盘": "收盘", "成交量": "成交量"}, inplace=True)
                return df
        except Exception as e:
            logger.debug(f"HK {code} akshare {frequency} 失败: {e}")
        return pd.DataFrame()

    def _fetch_k_us_akshare(self, code: str, days_back: int, frequency: str) -> pd.DataFrame:
        """akshare 美股历史行情"""
        ak_period = {"d": "daily", "w": "weekly", "m": "monthly"}.get(frequency, "daily")
        end_date = pd.Timestamp.now().strftime("%Y%m%d")
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=days_back)).strftime("%Y%m%d")

        # 1. 尝试 Sina (支持日线，全量获取后可重采样为周线/月线)
        try:
            df = ak.stock_us_daily(symbol=code, adjust="qfq")
            if df is not None and not df.empty:
                df.rename(columns={"date": "日期", "open": "开盘", "high": "最高",
                                  "low": "最低", "close": "收盘", "volume": "成交量"}, inplace=True)
                df["日期"] = pd.to_datetime(df["日期"])
                df.sort_values("日期", inplace=True)

                # 如果需要周线或月线，进行重采样
                if frequency in ("w", "m"):
                    df.set_index("日期", inplace=True)
                    resample_freq = "W-FRI" if frequency == "w" else "ME"
                    df = df.resample(resample_freq).agg({
                        "开盘": "first",
                        "最高": "max",
                        "最低": "min",
                        "收盘": "last",
                        "成交量": "sum"
                    }).dropna()
                    df.reset_index(inplace=True)

                # 手动日期切片
                mask = (df["日期"] >= pd.to_datetime(start_date)) & (df["日期"] <= pd.to_datetime(end_date))
                res = df.loc[mask]
                if not res.empty:
                    return res
        except Exception as e:
            logger.debug(f"US {code} Sina {frequency} 失败: {e}")

        # 2. 尝试 Eastmoney (需要市场前缀：105=纳斯达克, 106=纽交所, 107=美交所)
        for prefix in ["105.", "106.", "107.", ""]:
            try:
                df = ak.stock_us_hist(symbol=f"{prefix}{code}", period=ak_period,
                                      start_date=start_date, end_date=end_date, adjust="qfq")
                if df is not None and not df.empty:
                    df.rename(columns={"日期": "日期", "开盘": "开盘", "最高": "最高",
                                      "最低": "最低", "收盘": "收盘", "成交量": "成交量"}, inplace=True)
                    return df
            except Exception:
                continue
        return pd.DataFrame()

    def _fetch_k_baostock(self, code: str, days_back: int, frequency: str) -> pd.DataFrame:
        """
        Baostock 拉 K 线。优先复用常驻 session，否则临时登录。

        所有 Baostock 调用已在 baostock_provider 层面通过 _bs_lock 串行化，
        此处额外用 _bp_session_lock 保护 session 对象本身的并发访问。
        """
        try:
            if self._bp_session is not None:
                with self._bp_session_lock:
                    df = self._bp_session.get_k_data(code, days_back=days_back, frequency=frequency)
            else:
                with BaostockProvider() as bp:
                    df = bp.get_k_data(code, days_back=days_back, frequency=frequency)
            if df is not None and not df.empty:
                return df.rename(columns=self._COL_MAP_BS)
        except Exception as e:
            logger.debug(f"{code} Baostock {frequency} 失败: {e}")
        return pd.DataFrame()

    def _fetch_k(self, code: str, days_back: int, frequency: str,
                prefer: str = "akshare", market: str = "a") -> pd.DataFrame:
        """
        按优先级拉单只 K 线：默认 akshare → Baostock (仅A股支持)。

        Args:
            prefer: "akshare" | "baostock"
            market: "a" | "hk" | "us"
        """
        # 归一化代码：移除常见后缀如 .HK, .US, .SS, .SZ
        code = str(code).strip().upper()
        for suffix in (".HK", ".US", ".SS", ".SZ"):
            if code.endswith(suffix):
                code = code[:-len(suffix)]

        if market == "hk":
            return self._fetch_k_hk_akshare(code, days_back, frequency)
        if market == "us":
            return self._fetch_k_us_akshare(code, days_back, frequency)

        # A 股路径
        if prefer == "akshare":
            df = self._fetch_k_akshare(code, days_back, frequency)
            if df is not None and not df.empty:
                return df
            return self._fetch_k_baostock(code, days_back, frequency)
        # prefer == baostock
        df = self._fetch_k_baostock(code, days_back, frequency)
        if df is not None and not df.empty:
            return df
        return self._fetch_k_akshare(code, days_back, frequency)

    def get_weekly_ohlcv(self, code: str, days_back: int = 365 * 3,
                         prefer: str = "akshare", market: str = "a") -> pd.DataFrame:
        cache_key = f"weekly_{market}_{code}_{days_back}"
        df = self._cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_k(code, days_back, "w", prefer=prefer, market=market),
            ttl_hours=self._ohlcv_ttl,
        )
        return df

    def get_monthly_ohlcv(self, code: str, days_back: int = 365 * 5,
                          prefer: str = "akshare", market: str = "a") -> pd.DataFrame:
        cache_key = f"monthly_{market}_{code}_{days_back}"
        df = self._cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_k(code, days_back, "m", prefer=prefer, market=market),
            ttl_hours=self._ohlcv_ttl,
        )
        return df

    def get_yearly_ohlcv(self, code: str, days_back: int = 365 * 10,
                         prefer: str = "akshare", market: str = "a") -> pd.DataFrame:
        """获取年线数据（通过月线重采样）"""
        cache_key = f"yearly_{market}_{code}_{days_back}"

        def _fetch_yearly():
            m_df = self._fetch_k(code, days_back, "m", prefer=prefer, market=market)
            if m_df is None or m_df.empty:
                return pd.DataFrame()

            # 重采样逻辑
            m_df["日期"] = pd.to_datetime(m_df["日期"])
            m_df.set_index("日期", inplace=True)
            y_df = m_df.resample("YE").agg({
                "开盘": "first",
                "最高": "max",
                "最低": "min",
                "收盘": "last",
                "成交量": "sum",
                "成交额": "sum"
            }).dropna()
            y_df.reset_index(inplace=True)
            return y_df

        df = self._cache.get_or_fetch(
            cache_key,
            _fetch_yearly,
            ttl_hours=self._ohlcv_ttl,
        )
        return df

    def get_daily_ohlcv(self, code: str, days_back: int = 120,
                        prefer: str = "akshare", market: str = "a") -> pd.DataFrame:
        cache_key = f"daily_{market}_{code}_{days_back}"
        df = self._cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_k(code, days_back, "d", prefer=prefer, market=market),
            ttl_hours=self._ohlcv_ttl,
        )
        if df is not None and not df.empty:
            # 确保返回长度不超过 days_back（考虑到美股 Sina 获取全量的情况）
            return df.tail(days_back)
        return df

    # ========================
    # 批量并发预取：真正的加速来源
    # akshare 是 HTTP 请求，线程安全 → 多线程并发
    # ========================

    def prefetch_ohlcv_batch(
        self,
        codes: list[str],
        period: str = "daily",   # "daily" | "weekly"
        days_back: int | None = None,
        max_workers: int = 8,
        progress_callback=None,   # callable(done, total, code)
        market: str = "a",
    ) -> dict:
        """
        并发预取一批股票的 K 线，写入本地磁盘缓存。

        akshare 线程安全 + max_workers 并发 HTTP → 速度约 6-8x 于串行。
        Baostock 作为兜底（单个失败的 code 仍可用 Baostock session 补拉；此处保留串行安全）。

        Args:
            codes: 股票代码列表（纯数字）
            period: "daily" | "weekly"
            days_back: 历史天数，默认 weekly=365 / daily=120
            max_workers: 并发线程数（建议 4-12；过高可能触发东方财富限频）
            progress_callback: 进度回调 (done, total, code)

        Returns:
            {"hit": int, "miss": int, "fail": int, "elapsed": float}
        """
        if days_back is None:
            days_back = 365 * 3 if period == "weekly" else 120
        frequency = "w" if period == "weekly" else "d"

        total = len(codes)
        if total == 0:
            return {"hit": 0, "miss": 0, "fail": 0, "elapsed": 0.0}

        # 预先筛出已缓存的 code，避免并发池里还做 cache 检查（也可以让并发池处理，简化逻辑）
        pending: list[str] = []
        hit = 0
        for code in codes:
            cache_key = f"{period}_{market}_{code}_{days_back}"
            if self._cache.get(cache_key) is not None:
                hit += 1
                continue
            pending.append(code)

        logger.info(f"批量预取 {period} K 线: 总 {total} 只, 缓存命中 {hit}, "
                    f"待拉 {len(pending)} 只 (并发 {max_workers})")

        t0 = time.perf_counter()
        done_counter = [hit]  # 借用 list 实现线程安全计数
        ak_failed: list[str] = []
        ak_failed_lock = Lock()

        def _worker(code: str):
            cache_key = f"{period}_{market}_{code}_{days_back}"
            # akshare 线程安全：直接并发调用（代理已全局禁用）
            df = self._fetch_k_akshare(code, days_back, frequency)
            if df is None or df.empty:
                with ak_failed_lock:
                    ak_failed.append(code)
                return code, False
            # 写缓存
            self._cache.set(cache_key, df)
            return code, True

        if pending:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {pool.submit(_worker, c): c for c in pending}
                for fut in as_completed(futures):
                    code, ok = fut.result()
                    done_counter[0] += 1
                    if progress_callback:
                        try:
                            progress_callback(done_counter[0], total, code)
                        except Exception:
                            pass

        # Baostock 兜底：对 akshare 失败的 code 串行补拉（复用 session）
        if ak_failed:
            logger.info(f"akshare 失败 {len(ak_failed)} 只，用 Baostock 串行补拉...")
            with self.session():
                for code in ak_failed:
                    cache_key = f"{period}_{market}_{code}_{days_back}"
                    df = self._fetch_k_baostock(code, days_back, frequency)
                    if df is not None and not df.empty:
                        self._cache.set(cache_key, df)

        elapsed = time.perf_counter() - t0
        miss = total - hit
        real_hit_after = sum(
            1 for c in codes
            if self._cache.get(f"{period}_{market}_{c}_{days_back}") is not None
        )
        final_fail = total - real_hit_after
        rate = (miss - final_fail) / elapsed if elapsed > 0 and miss > 0 else 0

        logger.info(
            f"批量预取完成: 命中 {real_hit_after}/{total}, 失败 {final_fail}, "
            f"耗时 {elapsed:.1f}s, 速率 {rate:.1f} 只/秒"
        )
        return {
            "hit": real_hit_after,
            "miss": miss,
            "fail": final_fail,
            "elapsed": elapsed,
        }

