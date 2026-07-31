"""
src/analysis/screening/kline_mixin.py — K 线获取能力（Mixin）

从 data_provider 拆出：日/周/月/年线 + 分钟线 + 批量并发预取，以及
akshare(东财/新浪) → pytdx → Baostock 的多源降级。

设计为 Mixin 而非独立类：这些方法与 ScreenerDataProvider 共享
`self._cache` / `self._ttl_by_frequency` / `self._bp_session` 等实例状态，
拆成独立对象反而要把状态再传一遍。Mixin 让 `provider.get_daily_ohlcv(...)`
的既有调用方式完全不变。
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import akshare as ak
import pandas as pd

from src.analysis.screening.circuit_breaker import (
    _akshare_circuit_open,
    _akshare_record_fail,
    _akshare_record_success,
)
from src.data.providers.baostock_provider import BaostockProvider
from src.data.providers.pytdx_provider import get_global_pytdx
from src.utils.logger import get_logger

logger = get_logger("screener_kline")


class KlineFetchMixin:
    """K 线获取能力；宿主类需提供 _cache / _ttl_by_frequency / _bp_session(_lock)"""

    _COL_MAP_BS = {"date": "日期", "open": "开盘", "high": "最高",
                   "low": "最低", "close": "收盘", "volume": "成交量"}

    _COL_MAP_SINA = {
        "date": "日期", "open": "开盘", "high": "最高",
        "low": "最低", "close": "收盘", "volume": "成交量",
        "amount": "成交额", "outstanding_share": "流通股本", "turnover": "换手率",
    }

    def _fetch_k_akshare(self, code: str, days_back: int, frequency: str) -> pd.DataFrame:
        """
        akshare 直连拉 K 线（无 session 概念，每次都是独立 HTTP 请求，**线程安全**）。
        这是并发加速的基石。代理绕开由外层 _no_proxy() 管理。

        数据源降级链：
          1. stock_zh_a_hist   (东方财富 push2his，主源，含中文列名)
          2. stock_zh_a_daily  (新浪，备用源，英文列名需转换)
        两个源都失败才返回空 DataFrame。
        """
        end_date = pd.Timestamp.now().strftime("%Y%m%d")
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=days_back)).strftime("%Y%m%d")
        ak_period = {"d": "daily", "w": "weekly", "m": "monthly"}.get(frequency, "daily")

        # ── 1. 东方财富 stock_zh_a_hist（主源）──
        # 熔断器开启时跳过，直接走 Sina
        if not _akshare_circuit_open():
            try:
                df = ak.stock_zh_a_hist(
                    symbol=code, period=ak_period,
                    start_date=start_date, end_date=end_date, adjust="qfq",
                )
                if df is not None and not df.empty:
                    _akshare_record_success()
                    return df
                # 空结果也记一次失败（可能东方财富限制）
                _akshare_record_fail()
            except Exception as e:
                logger.debug(f"{code} akshare EM {frequency} 失败: {e}")
                _akshare_record_fail()
        else:
            logger.debug(f"{code} akshare EM 熔断中，跳过直接走 Sina")

        # ── 2. 新浪 stock_zh_a_daily（降级源）──
        # Sina 只提供日线；周线/月线通过重采样获得
        try:
            sina_symbol = self._to_sina_symbol(code)
            df = ak.stock_zh_a_daily(
                symbol=sina_symbol,
                start_date=start_date, end_date=end_date,
                adjust="qfq",
            )
            if df is not None and not df.empty:
                df = df.rename(columns=self._COL_MAP_SINA)
                # 日期列转 datetime 用于重采样
                if "日期" in df.columns:
                    df["日期"] = pd.to_datetime(df["日期"])

                # 周线/月线重采样
                if frequency in ("w", "m"):
                    resample_freq = "W-FRI" if frequency == "w" else "ME"
                    df = df.set_index("日期").resample(resample_freq).agg({
                        "开盘": "first", "最高": "max", "最低": "min",
                        "收盘": "last", "成交量": "sum",
                        "成交额": "sum" if "成交额" in df.columns else lambda x: x.sum(),
                    }).dropna(subset=["开盘", "收盘"]).reset_index()

                # 日期列统一为字符串格式（与 EM 源一致）
                if "日期" in df.columns:
                    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
                logger.debug(f"{code} Sina {frequency} 降级成功: {len(df)} 行")
                return df
        except Exception as e:
            logger.debug(f"{code} akshare Sina {frequency} 失败: {e}")

        return pd.DataFrame()

    @staticmethod
    def _to_sina_symbol(code: str) -> str:
        """6位纯数字代码 → Sina 格式 (sh/sz 前缀)"""
        code = str(code).strip()
        # 已有前缀直接返回
        if code.lower().startswith(("sh", "sz")):
            return code.lower()
        # 纯数字：6开头=沪市, 0/3开头=深市
        if code.startswith("6"):
            return f"sh{code}"
        return f"sz{code}"

    def _fetch_k_international_akshare(
        self, code: str, days_back: int, frequency: str, market: str
    ) -> pd.DataFrame:
        """
        akshare 国际市场（港股/美股）历史行情，提取公共逻辑消除重复。

        Args:
            market: "hk" 或 "us"
        """
        market_upper = market.upper()
        ak_period = {"d": "daily", "w": "weekly", "m": "monthly"}.get(frequency, "daily")
        end_date = pd.Timestamp.now().strftime("%Y%m%d")
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=days_back)).strftime("%Y%m%d")

        # 1. 尝试 Sina (支持日线，全量获取后可重采样为周线/月线)
        sina_func = ak.stock_hk_daily if market == "hk" else ak.stock_us_daily
        try:
            df = sina_func(symbol=code, adjust="qfq")
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
            logger.debug(f"{market_upper} {code} Sina {frequency} 失败: {e}")

        # 2. 尝试 Eastmoney
        if market == "hk":
            try:
                df = ak.stock_hk_hist(symbol=code, period=ak_period,
                                      start_date=start_date, end_date=end_date, adjust="qfq")
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                logger.debug(f"HK {code} akshare {frequency} 失败: {e}")
        else:
            # 美股需要市场前缀：105=纳斯达克, 106=纽交所, 107=美交所
            for prefix in ["105.", "106.", "107.", ""]:
                try:
                    df = ak.stock_us_hist(symbol=f"{prefix}{code}", period=ak_period,
                                          start_date=start_date, end_date=end_date, adjust="qfq")
                    if df is not None and not df.empty:
                        return df
                except Exception as e:
                    logger.debug(f"_fetch_k_international_akshare 忽略异常: {type(e).__name__}: {e}")
                    continue
        return pd.DataFrame()

    def _fetch_k_hk_akshare(self, code: str, days_back: int, frequency: str) -> pd.DataFrame:
        """akshare 港股历史行情"""
        return self._fetch_k_international_akshare(code, days_back, frequency, market="hk")

    def _fetch_k_us_akshare(self, code: str, days_back: int, frequency: str) -> pd.DataFrame:
        """akshare 美股历史行情"""
        return self._fetch_k_international_akshare(code, days_back, frequency, market="us")

    def _fetch_k_pytdx(self, code: str, days_back: int, frequency: str) -> pd.DataFrame:
        """通达信 pytdx K 线（并发安全）"""
        try:
            pytdx = get_global_pytdx()
            if pytdx.is_available():
                return pytdx.get_k_data(code, days_back=days_back, frequency=frequency)
        except Exception as e:
            logger.debug(f"{code} pytdx {frequency} 失败: {e}")
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

        # A 股路径：akshare → pytdx (并发安全) → Baostock (兜底)
        if prefer == "akshare":
            df = self._fetch_k_akshare(code, days_back, frequency)
            if df is not None and not df.empty:
                return df
            df = self._fetch_k_pytdx(code, days_back, frequency)
            if df is not None and not df.empty:
                return df
            return self._fetch_k_baostock(code, days_back, frequency)
        # prefer == baostock
        df = self._fetch_k_baostock(code, days_back, frequency)
        if df is not None and not df.empty:
            return df
        df = self._fetch_k_pytdx(code, days_back, frequency)
        if df is not None and not df.empty:
            return df
        return self._fetch_k_akshare(code, days_back, frequency)

    def get_weekly_ohlcv(self, code: str, days_back: int = 365 * 3,
                         prefer: str = "akshare", market: str = "a") -> pd.DataFrame:
        cache_key = f"weekly_{market}_{code}_{days_back}"
        df = self._cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_k(code, days_back, "w", prefer=prefer, market=market),
            ttl_hours=self._ttl_by_frequency["weekly"],
        )
        return df

    def get_monthly_ohlcv(self, code: str, days_back: int = 365 * 5,
                          prefer: str = "akshare", market: str = "a") -> pd.DataFrame:
        cache_key = f"monthly_{market}_{code}_{days_back}"
        df = self._cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_k(code, days_back, "m", prefer=prefer, market=market),
            ttl_hours=self._ttl_by_frequency["monthly"],
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
            ttl_hours=self._ttl_by_frequency["monthly"],
        )
        return df

    def get_daily_ohlcv(self, code: str, days_back: int = 120,
                        prefer: str = "akshare", market: str = "a") -> pd.DataFrame:
        cache_key = f"daily_{market}_{code}_{days_back}"
        df = self._cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_k(code, days_back, "d", prefer=prefer, market=market),
            ttl_hours=self._ttl_by_frequency["daily"],
        )
        if df is not None and not df.empty:
            # 确保返回长度不超过 days_back（考虑到美股 Sina 获取全量的情况）
            return df.tail(days_back)
        return df

    MINUTE_FREQ_MAP = {"1m": "1", "5m": "5", "15m": "15", "30m": "30", "60m": "60"}

    _BARS_PER_DAY = {"1m": 240, "5m": 48, "15m": 16, "30m": 8, "60m": 4}

    @staticmethod
    def _normalize_minute_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        分钟线列名归一化：时间 → 日期（datetime），只保留标准 OHLCV 列。
        东财分钟接口返回 时间/开盘/收盘/最高/最低/成交量/成交额(/均价/涨跌幅...)。
        """
        if df is None or df.empty:
            return pd.DataFrame()
        if "时间" in df.columns and "日期" not in df.columns:
            df = df.rename(columns={"时间": "日期"})
        if "日期" not in df.columns:
            return pd.DataFrame()
        df = df.copy()
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
        df = df.dropna(subset=["日期"]).sort_values("日期", ignore_index=True)
        keep = [c for c in ("日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额")
                if c in df.columns]
        return df[keep]

    def _fetch_minute_akshare(self, code: str, freq: str, days_back: int,
                              market: str = "a") -> pd.DataFrame:
        """akshare 东方财富分钟 K 线（A股/港股/美股）。1m 接口不支持复权。"""
        period = self.MINUTE_FREQ_MAP[freq]
        end = pd.Timestamp.now()
        start = end - pd.Timedelta(days=days_back)
        fmt = "%Y-%m-%d %H:%M:%S"
        # 东财 1 分钟接口只支持不复权；5m 以上用前复权与日线口径一致
        adjust = "" if freq == "1m" else "qfq"

        if _akshare_circuit_open():
            logger.debug(f"{code} akshare 分钟接口熔断中，跳过")
            return pd.DataFrame()
        try:
            if market == "hk":
                df = ak.stock_hk_hist_min_em(
                    symbol=code, period=period, adjust=adjust,
                    start_date=start.strftime(fmt), end_date=end.strftime(fmt),
                )
            elif market == "us":
                # 美股分钟接口需要市场前缀：105=纳斯达克, 106=纽交所, 107=美交所
                df = pd.DataFrame()
                for prefix in ("105.", "106.", "107."):
                    try:
                        df = ak.stock_us_hist_min_em(
                            symbol=f"{prefix}{code}",
                            start_date=start.strftime(fmt), end_date=end.strftime(fmt),
                        )
                        if df is not None and not df.empty:
                            break
                    except Exception as e:
                        logger.debug(f"_fetch_minute_akshare 忽略异常: {type(e).__name__}: {e}")
                        continue
                # 美股接口固定返回 1 分钟，需重采样到目标频率
                if df is not None and not df.empty and freq != "1m":
                    df = self._normalize_minute_df(df)
                    rule = {"5m": "5min", "15m": "15min", "30m": "30min", "60m": "60min"}[freq]
                    agg = {"开盘": "first", "最高": "max", "最低": "min",
                           "收盘": "last", "成交量": "sum"}
                    if "成交额" in df.columns:
                        agg["成交额"] = "sum"
                    df = (df.set_index("日期").resample(rule).agg(agg)
                          .dropna(subset=["开盘", "收盘"]).reset_index())
            else:
                df = ak.stock_zh_a_hist_min_em(
                    symbol=code, period=period, adjust=adjust,
                    start_date=start.strftime(fmt), end_date=end.strftime(fmt),
                )
            if df is not None and not df.empty:
                _akshare_record_success()
                return self._normalize_minute_df(df)
            # 空结果不记熔断失败：节假日/超出 1m 历史深度等场景返回空是合法的
        except Exception as e:
            logger.debug(f"{code} akshare 分钟线({freq}) 失败: {e}")
            # 仅 A 股路径记熔断：港/美股分钟接口失败不应连坐 A 股全部东财路径
            if market == "a":
                _akshare_record_fail()
        return pd.DataFrame()

    def _fetch_minute_pytdx(self, code: str, freq: str, days_back: int) -> pd.DataFrame:
        """pytdx 分钟线兜底（仅A股；单次最多 800 根 bar）"""
        try:
            pytdx = get_global_pytdx()
            if pytdx.is_available():
                bars = min(days_back * self._BARS_PER_DAY[freq], 800)
                df = pytdx.get_k_data(code, days_back=bars, frequency=freq)
                return self._normalize_minute_df(df)
        except Exception as e:
            logger.debug(f"{code} pytdx 分钟线({freq}) 失败: {e}")
        return pd.DataFrame()

    def get_minute_ohlcv(self, code: str, freq: str = "5m", days_back: int = 5,
                         market: str = "a") -> pd.DataFrame:
        """
        获取分钟 K 线。

        Args:
            code: 股票代码（纯数字/字母，无市场前缀）
            freq: "1m" | "5m" | "15m" | "30m" | "60m"
            days_back: 回溯日历天数（东财 1m 只保留最近 ~5 个交易日）
            market: "a" | "hk" | "us"

        Returns:
            DataFrame，列: 日期(datetime)/开盘/最高/最低/收盘/成交量/成交额
        """
        if freq not in self.MINUTE_FREQ_MAP:
            raise ValueError(
                f"未知分钟频率: {freq}，支持 {sorted(self.MINUTE_FREQ_MAP)}"
            )
        code = str(code).strip().upper()
        for suffix in (".HK", ".US", ".SS", ".SZ"):
            if code.endswith(suffix):
                code = code[:-len(suffix)]

        def _fetch():
            df = self._fetch_minute_akshare(code, freq, days_back, market=market)
            if df is not None and not df.empty:
                return df
            if market == "a":
                return self._fetch_minute_pytdx(code, freq, days_back)
            return pd.DataFrame()

        cache_key = f"min{freq}_{market}_{code}_{days_back}"
        df = self._cache.get_or_fetch(
            cache_key, _fetch, ttl_hours=self._ttl_by_frequency["minute"],
        )
        return df if df is not None else pd.DataFrame()

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

        # 采样探测：先并发跑前 SAMPLE 只，如果失败率高就跳过剩余 akshare 调用，
        # 避免给东方财富做 4000+ 次空 HTTP（每只 ~20ms，总计可空耗 90s+）
        SAMPLE_SIZE = 30
        SAMPLE_FAIL_THRESHOLD = 0.8  # 失败率 ≥80% 则放弃 akshare
        if pending:
            sample_codes = pending[:SAMPLE_SIZE]
            rest_codes = pending[SAMPLE_SIZE:]

            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {pool.submit(_worker, c): c for c in sample_codes}
                for fut in as_completed(futures):
                    code, ok = fut.result()
                    done_counter[0] += 1
                    if progress_callback:
                        try:
                            progress_callback(done_counter[0], total, code)
                        except Exception as e:
                            logger.debug(f"_worker 忽略异常: {type(e).__name__}: {e}")

            sample_done = len(sample_codes)
            sample_fail = sum(1 for c in sample_codes if c in ak_failed)
            sample_fail_rate = sample_fail / max(sample_done, 1)

            if rest_codes and sample_fail_rate >= SAMPLE_FAIL_THRESHOLD:
                # akshare 不可用，剩下的直接进入 Baostock 兜底队列
                logger.warning(
                    f"akshare K 线探针失败率 {sample_fail_rate:.0%} "
                    f"({sample_fail}/{sample_done})，跳过剩余 {len(rest_codes)} 只 akshare 调用，"
                    f"直接走 Baostock 兜底"
                )
                with ak_failed_lock:
                    ak_failed.extend(rest_codes)
                done_counter[0] += len(rest_codes)
                if progress_callback:
                    for c in rest_codes:
                        try:
                            progress_callback(done_counter[0], total, c)
                        except Exception as e:
                            logger.debug(f"_worker 忽略异常: {type(e).__name__}: {e}")
            elif rest_codes:
                # 探针通过，剩余股票正常走 akshare 并发
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    futures = {pool.submit(_worker, c): c for c in rest_codes}
                    for fut in as_completed(futures):
                        code, ok = fut.result()
                        done_counter[0] += 1
                        if progress_callback:
                            try:
                                progress_callback(done_counter[0], total, code)
                            except Exception as e:
                                logger.debug(f"_worker 忽略异常: {type(e).__name__}: {e}")

        # 兜底阶段：先用 pytdx 并发补拉（快 30 倍），剩余失败再走 Baostock 串行
        bs_failed: list[str] = []
        if ak_failed and market == "a":
            pytdx = get_global_pytdx()
            if pytdx.is_available():
                logger.info(
                    f"akshare 失败 {len(ak_failed)} 只，先用 pytdx 并发补拉 "
                    f"(workers={max_workers})..."
                )
                pytdx_failed_lock = Lock()

                def _pytdx_worker(code: str):
                    cache_key = f"{period}_{market}_{code}_{days_back}"
                    df = pytdx.get_k_data(code, days_back=days_back, frequency=frequency)
                    if df is None or df.empty:
                        with pytdx_failed_lock:
                            bs_failed.append(code)
                        return code, False
                    self._cache.set(cache_key, df)
                    return code, True

                t_pytdx = time.perf_counter()
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    futures = {pool.submit(_pytdx_worker, c): c for c in ak_failed}
                    for fut in as_completed(futures):
                        fut.result()
                logger.info(
                    f"pytdx 补拉完成: 命中 {len(ak_failed) - len(bs_failed)}/{len(ak_failed)}, "
                    f"剩余 {len(bs_failed)} 只交 Baostock, 耗时 {time.perf_counter() - t_pytdx:.1f}s"
                )
            else:
                bs_failed = list(ak_failed)
        else:
            bs_failed = list(ak_failed)

        # 最终兜底：Baostock 串行（复用 session）
        if bs_failed:
            logger.info(f"还有 {len(bs_failed)} 只 pytdx 也失败，用 Baostock 串行补拉...")
            with self.session():
                for code in bs_failed:
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

