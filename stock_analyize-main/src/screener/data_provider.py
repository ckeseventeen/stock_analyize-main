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

from src.data.fetcher.baostock_provider import BaostockProvider
from src.data.fetcher.cache_manager import CacheManager
from src.utils.exception_handler import retry
from src.utils.logger import get_logger

logger = get_logger("screener_data")


@contextlib.contextmanager
def _no_proxy():
    """
    临时清空 HTTP(S)_PROXY 环境变量。

    背景：用户报告"不走代理时 akshare 正常"。akshare 内部用 requests，
    会自动读取 HTTP_PROXY/HTTPS_PROXY；当系统代理不通时会直接失败。
    本 context 仅在 akshare 调用期间清代理，退出后恢复，避免影响其他需要代理的地方。
    """
    keys = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
            "ALL_PROXY", "all_proxy")
    saved = {k: os.environ.pop(k, None) for k in keys}
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


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
        # 可选的常驻 Baostock session；用 session() 上下文开启。
        # 一旦开启，get_weekly_ohlcv/get_daily_ohlcv 会复用而不再 login/logout。
        self._bp_session: BaostockProvider | None = None

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

        优先级：akshare (一次拉完，含 总市值/PE/PB) → Baostock (逐只拉 K 线，无总市值)
        返回列包含：代码, 名称, 最新价, 总市值, 市盈率-动态, 市净率, 换手率, 涨跌幅 等。
        """

        def _fetch_baostock():
            """通过 Baostock 获取全A行情数据；从 amount/turn 推算流通市值填入'总市值'列"""
            logger.info("正在通过 Baostock 拉取全A股行情数据...")
            with BaostockProvider() as bp:
                # 1. 获取全市场股票列表
                all_stocks = bp.get_all_stocks()
                if all_stocks.empty:
                    return pd.DataFrame()

                # 过滤只保留 A股（sh.6xxx, sz.0xxx, sz.3xxx），排除指数和B股
                a_codes = all_stocks[
                    all_stocks["code"].str.match(r"^(sh\.6|sz\.0|sz\.3)")
                ]["code"].tolist()
                logger.info(f"Baostock 全市场 {len(all_stocks)} 只，A股过滤后 {len(a_codes)} 只")

                # 2. 批量获取最新一天的K线含估值指标
                today = pd.Timestamp.now().strftime("%Y-%m-%d")
                week_ago = (pd.Timestamp.now() - pd.Timedelta(days=7)).strftime("%Y-%m-%d")

                rows = []
                for bs_code in a_codes:
                    try:
                        df_k = bp.get_k_data(
                            bs_code,
                            start_date=week_ago,
                            end_date=today,
                            frequency="d",
                            fields="date,close,volume,amount,turn,peTTM,pbMRQ,psTTM,isST",
                        )
                        if df_k is not None and not df_k.empty:
                            latest = df_k.iloc[-1]
                            code_name_row = all_stocks[all_stocks["code"] == bs_code]
                            name = code_name_row["code_name"].values[0] if not code_name_row.empty and "code_name" in code_name_row.columns else ""
                            pure_code = bs_code.split(".")[-1]

                            # --- 从 amount / turn 推算"流通市值"作为"总市值"的近似值 ---
                            # 换手率定义: turn(%) = 成交量 / 流通股本 × 100
                            # 因此  流通股本 = 成交量 × 100 / turn
                            # 流通市值 ≈ amount × 100 / turn  (单位: 元)
                            # A 股大部分公司全流通，流通市值 ≈ 总市值（误差可接受）
                            try:
                                amount = float(latest.get("amount", 0) or 0)
                                turn = float(latest.get("turn", 0) or 0)
                                est_mv = amount * 100 / turn if turn > 0 else 0.0
                            except (ValueError, TypeError):
                                est_mv = 0.0

                            rows.append({
                                "代码": pure_code,
                                "名称": name,
                                "最新价": latest.get("close", 0),
                                "总市值": est_mv,       # 推算值（元），供 market_cap 条件使用
                                "流通市值": est_mv,     # 同值
                                "市盈率-动态": latest.get("peTTM", 0),
                                "市净率": latest.get("pbMRQ", 0),
                                "换手率": latest.get("turn", 0),
                                "成交量": latest.get("volume", 0),
                                "成交额": latest.get("amount", 0),
                            })
                    except Exception:
                        continue

                if not rows:
                    return pd.DataFrame()

                result = pd.DataFrame(rows)
                # 数值列转换
                for col in ("最新价", "总市值", "流通市值", "市盈率-动态", "市净率",
                            "换手率", "成交量", "成交额"):
                    if col in result.columns:
                        result[col] = pd.to_numeric(result[col], errors="coerce")

                # 过滤 ST
                result = result[~result["名称"].str.contains("ST|退市", na=False)]
                ok_mv = (result["总市值"] > 0).sum() if "总市值" in result.columns else 0
                logger.info(
                    f"Baostock 全A数据拉取完成，共 {len(result)} 只（过滤ST后）；"
                    f"其中 {ok_mv} 只成功推算总市值（amount/turn 可用）"
                )
                return result

        def _fetch_akshare():
            """akshare 主路径：临时清空代理 env 避免被系统代理拖死"""
            logger.info("正在通过 akshare 拉取全A股实时行情数据（绕过系统代理）...")
            with _no_proxy():
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

        def _fetch():
            # 优先 akshare（一次调用含总市值/PE/PB/换手率，完全覆盖 Spot 条件）
            try:
                df = _fetch_akshare()
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"akshare 全A数据拉取失败: {e}")

            # fallback 到 Baostock；已用 amount/turn 推算出总市值列，market_cap 条件可正常工作
            try:
                logger.warning("akshare 失败，降级到 Baostock（将用 amount/turn 推算总市值）")
                return _fetch_baostock()
            except Exception as e:
                logger.error(f"Baostock fallback 也失败: {e}")
                return pd.DataFrame()

        return self._cache.get_or_fetch("all_a_shares_spot", _fetch, ttl_hours=self._spot_ttl)

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
        ak_period = {"d": "daily", "w": "weekly", "m": "monthly"}.get(frequency, "daily")
        try:
            df = ak.stock_zh_a_hist(
                symbol=code, period=ak_period,
                start_date=start_date, end_date=end_date, adjust="qfq",
            )
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.debug(f"{code} akshare {frequency} 失败: {e}")
        return pd.DataFrame()

    def _fetch_k_baostock(self, code: str, days_back: int, frequency: str) -> pd.DataFrame:
        """
        Baostock 拉 K 线。优先复用常驻 session，否则临时登录。

        注意：Baostock 客户端非线程安全，多线程并发调用需要外部锁。
        """
        try:
            if self._bp_session is not None:
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
                prefer: str = "akshare") -> pd.DataFrame:
        """
        按优先级拉单只 K 线：默认 akshare → Baostock。

        Args:
            prefer: "akshare" | "baostock"  —  决定主路径
        """
        if prefer == "akshare":
            with _no_proxy():
                df = self._fetch_k_akshare(code, days_back, frequency)
            if df is not None and not df.empty:
                return df
            return self._fetch_k_baostock(code, days_back, frequency)
        # prefer == baostock
        df = self._fetch_k_baostock(code, days_back, frequency)
        if df is not None and not df.empty:
            return df
        with _no_proxy():
            return self._fetch_k_akshare(code, days_back, frequency)

    def get_weekly_ohlcv(self, code: str, days_back: int = 365,
                         prefer: str = "akshare") -> pd.DataFrame:
        cache_key = f"weekly_{code}_{days_back}"
        return self._cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_k(code, days_back, "w", prefer=prefer),
            ttl_hours=self._ohlcv_ttl,
        )

    def get_daily_ohlcv(self, code: str, days_back: int = 120,
                        prefer: str = "akshare") -> pd.DataFrame:
        cache_key = f"daily_{code}_{days_back}"
        return self._cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_k(code, days_back, "d", prefer=prefer),
            ttl_hours=self._ohlcv_ttl,
        )

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
            days_back = 365 if period == "weekly" else 120
        frequency = "w" if period == "weekly" else "d"

        total = len(codes)
        if total == 0:
            return {"hit": 0, "miss": 0, "fail": 0, "elapsed": 0.0}

        # 预先筛出已缓存的 code，避免并发池里还做 cache 检查（也可以让并发池处理，简化逻辑）
        pending: list[str] = []
        hit = 0
        for code in codes:
            cache_key = f"{period}_{code}_{days_back}"
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
            cache_key = f"{period}_{code}_{days_back}"
            # akshare 线程安全：直接并发调用
            with _no_proxy():
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
                    cache_key = f"{period}_{code}_{days_back}"
                    df = self._fetch_k_baostock(code, days_back, frequency)
                    if df is not None and not df.empty:
                        self._cache.set(cache_key, df)

        elapsed = time.perf_counter() - t0
        miss = total - hit
        real_hit_after = sum(
            1 for c in codes
            if self._cache.get(f"{period}_{c}_{days_back}") is not None
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

