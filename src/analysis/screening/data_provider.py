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
from threading import Lock

import akshare as ak
import pandas as pd

from src.core.cache_policy import ttl_for
from src.core.fallback import DataSource, FallbackChain
from src.data.providers.baostock_provider import BaostockProvider
from src.data.providers.cache_manager import CacheManager
from src.data.providers.wencai_provider import WencaiProvider
from src.utils.logger import get_logger

logger = get_logger("screener_data")


# ── 以下能力已拆分到同包的独立模块，这里 re-export 保持向后兼容 ──
# （data_provider 原为 1308 行的"什么都干"模块：熔断/代理/质量校验/行情/K线/板块）
# 注意：这些 re-export 看似"未使用"，实为对外契约（下游与测试通过
# `data_provider.<name>` 访问），**不要让 linter 自动删除**——`_akshare_cb_state`
# 是可变 dict，共享同一引用，测试据此复位熔断状态。
from src.analysis.screening.circuit_breaker import (  # noqa: E402,F401
    _akshare_cb_state,
    _akshare_circuit_open,
    _akshare_record_fail,
    _akshare_record_success,
)
from src.analysis.screening.kline_mixin import KlineFetchMixin  # noqa: E402
from src.analysis.screening.proxy_policy import (  # noqa: E402,F401
    _ensure_no_proxy_disable,
    _probe_em,
)
from src.analysis.screening.spot_quality import (  # noqa: E402,F401
    _SPOT_MIN_MKTCAP_RATIO,
    _SPOT_MIN_ROWS,
    _SPOT_REQUIRED_COLUMNS,
    _SPOT_SENTINEL_CODES,
    validate_spot_quality,
)

# Baostock 全A兜底的时间预算（秒）。串行 5500 只需 ~20min，交互式请求扛不住；
# 可用 STOCK_ANALYZE_BAOSTOCK_BUDGET 覆盖（调度器等离线场景可调大）
_BAOSTOCK_TIME_BUDGET_SEC = int(
    os.environ.get("STOCK_ANALYZE_BAOSTOCK_BUDGET", "300")
)


class ScreenerDataProvider(KlineFetchMixin):
    """
    筛选器数据提供层

    使用示例:
        provider = ScreenerDataProvider()
        all_stocks = provider.get_all_a_shares()      # 全A实时行情
        weekly_df = provider.get_weekly_ohlcv("600519")  # 单只周线
    """

    def __init__(self, cache_dir: str = ".cache/screener",
                 spot_ttl_hours: int | None = None,
                 ohlcv_ttl_hours: int | None = None):
        """TTL 缺省时取 src/core/cache_policy 的统一策略（显式传参仍可覆盖）"""
        ohlcv_ttl_hours = (ohlcv_ttl_hours if ohlcv_ttl_hours is not None
                           else ttl_for("kline_daily"))
        self._cache = CacheManager(cache_dir=cache_dir, ttl_hours=ohlcv_ttl_hours)
        self._spot_ttl = (spot_ttl_hours if spot_ttl_hours is not None
                          else ttl_for("spot"))
        self._ohlcv_ttl = ohlcv_ttl_hours
        # 缓存TTL按周期分档：分钟线最短（盘中滚动更新），月线/年线可缓存更久
        self._ttl_by_frequency = {
            "minute": ttl_for("kline_minute"),
            "daily": ohlcv_ttl_hours,
            "weekly": ttl_for("kline_weekly"),
            "monthly": ttl_for("kline_monthly"),
        }
        self._bp_session: BaostockProvider | None = None
        self._bp_session_lock = Lock()
        self._cache_dir = cache_dir
        # 全局禁用代理，避免 akshare 调用被系统代理阻塞
        _ensure_no_proxy_disable()

    @property
    def cache_dir(self) -> str:
        """磁盘缓存目录（多进程筛选时供子进程构造同源 provider）"""
        return self._cache_dir

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
            except Exception as e:
                logger.debug(f"session 忽略异常: {type(e).__name__}: {e}")
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
                # 时间预算：Baostock 串行 5500 只约 20 分钟，交互式请求扛不住。
                # 超预算即停，已拉部分交由 validate_spot_quality 判定（多半不合格
                # → 走退化告警路径），至少不会把 API 请求卡死 20 分钟。
                deadline = time.time() + _BAOSTOCK_TIME_BUDGET_SEC
                for i, bs_code in enumerate(a_codes):
                    if time.time() > deadline:
                        logger.warning(
                            f"Baostock 兜底超出时间预算 {_BAOSTOCK_TIME_BUDGET_SEC}s，"
                            f"已拉 {len(rows)}/{total} 只，提前结束"
                        )
                        break
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
                    except Exception as e:
                        logger.debug(f"_fetch_baostock 忽略异常: {type(e).__name__}: {e}")
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
            if _akshare_circuit_open():
                logger.info("akshare 主接口处于熔断冷却中，跳过")
                return pd.DataFrame()
            logger.info("正在通过 akshare 拉取全A股实时行情数据...")
            for attempt in range(3):
                try:
                    df = ak.stock_zh_a_spot_em()
                    if df is not None and not df.empty:
                        # 字段完整性校验：缺关键列（如总市值）视为数据源退化
                        missing = [c for c in _SPOT_REQUIRED_COLUMNS if c not in df.columns]
                        if missing:
                            logger.warning(
                                f"akshare 主API返回数据缺关键字段 {missing}，视为失败"
                            )
                            _akshare_record_fail()
                            return pd.DataFrame()
                        logger.info(f"akshare 全A股实时行情拉取成功，共 {len(df)} 只股票")
                        df = df[~df["名称"].str.contains("ST|退市", na=False)]
                        for col in ("总市值", "流通市值", "市盈率-动态", "市净率", "换手率", "最新价", "涨跌幅", "振幅"):
                            if col in df.columns:
                                df[col] = pd.to_numeric(df[col], errors="coerce")
                        logger.info(f"过滤ST/退市后剩余 {len(df)} 只")
                        _akshare_record_success()
                        return df
                except Exception as e:
                    logger.warning(f"akshare 主API拉取尝试 {attempt + 1}/3 失败: {e}")
                    if attempt < 2:
                        time.sleep(2)
            logger.warning("akshare 全A股实时行情数据为空")
            _akshare_record_fail()
            return pd.DataFrame()

        def _fetch_akshare_fallback():
            """akshare 备用 API：stock_zh_a_spot (老接口，数据结构不同但稳定)

            备用 API 通常只返回价格相关字段，**不含 总市值/PE/PB/换手率**。
            为避免下游条件（PE/PB/MarketCap）因字段缺失行为退化，此处要求
            必须含 _SPOT_REQUIRED_COLUMNS 才视为成功，否则返回空让外层兜底到
            _fetch_baostock。
            """
            logger.info("正在通过 akshare 备用API拉取全A股行情数据...")
            for attempt in range(3):
                try:
                    df = ak.stock_zh_a_spot()
                    if df is not None and not df.empty:
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

                        # 字段完整性校验：缺关键列就放弃，让外层走 Baostock
                        missing = [c for c in _SPOT_REQUIRED_COLUMNS if c not in df.columns]
                        if missing:
                            logger.warning(
                                f"akshare 备用API缺关键字段 {missing}（共拉到 {len(df)} 只）"
                                f"，放弃改走 Baostock 以获取完整字段"
                            )
                            return pd.DataFrame()

                        logger.info(f"akshare 备用API拉取成功，过滤ST/退市后剩余 {len(df)} 只")
                        return df
                except Exception as e:
                    logger.warning(f"akshare 备用API拉取尝试 {attempt + 1}/3 失败: {e}")
                    if attempt < 2:
                        time.sleep(2)
            logger.warning("akshare 备用API数据也为空")
            return pd.DataFrame()

        def _fetch_em_direct():
            """
            东财直连（自建分页并发）——现在的**主数据源**。

            akshare 的 stock_zh_a_spot_em 因索要 100+ 字段 + 无常规 UA 被反爬
            掐连接；本路径只取 9 个必要字段并发翻页，实测 5888 只 / 1.5 秒。
            """
            try:
                from src.data.providers.eastmoney_spot import fetch_all_a_spot
                return fetch_all_a_spot()
            except Exception as e:
                logger.warning(f"东财直连失败: {type(e).__name__}: {e}")
                return pd.DataFrame()

        def _fetch_wencai():
            """同花顺问财兜底：一次拉全A股 spot 数据，~40秒，含完整字段"""
            try:
                wp = WencaiProvider()
                if not wp.is_available():
                    return pd.DataFrame()
                df = wp.get_all_a_shares()
                if df is None or df.empty:
                    return pd.DataFrame()
                # 字段完整性校验（与 akshare 路径同标准）
                missing = [c for c in _SPOT_REQUIRED_COLUMNS if c not in df.columns]
                if missing:
                    logger.warning(f"pywencai 返回缺关键字段 {missing}，放弃")
                    return pd.DataFrame()
                return df
            except Exception as e:
                logger.warning(f"WencaiProvider 失败: {type(e).__name__}: {e}")
                return pd.DataFrame()

        def _fetch():
            """
            走统一的 FallbackChain 取数，每一级都必须通过 validate_spot_quality。

            关键语义（由 FallbackChain 统一保证）：不合格的数据不会被直接采用
            （否则会毒害缓存 12 小时，让全站筛选静默归零）；若全链路都拿不到
            合格数据，则返回其中最完整的一份并**打上退化标记**，让上层告知用户
            "数据源退化"而不是假装一切正常。
            """
            chain = FallbackChain(
                name="全A行情",
                sources=[
                    DataSource("东财直连（分页并发，~2s）", _fetch_em_direct),
                    DataSource("akshare 主API", _fetch_akshare),
                    DataSource("akshare 备用API", _fetch_akshare_fallback),
                    DataSource("同花顺问财", _fetch_wencai),
                    DataSource("Baostock（慢，5500只串行 ~20min）", _fetch_baostock),
                ],
                validator=validate_spot_quality,
            )
            result = chain.run()
            df = result.unwrap(pd.DataFrame())
            if result.is_degraded and df is not None and not df.empty:
                # attrs 随 pickle 缓存一起保留，供上层（screener）冒泡给用户
                df.attrs["degraded"] = True
                df.attrs["degrade_reason"] = result.reason
                df.attrs["degrade_source"] = result.source
            return df if df is not None else pd.DataFrame()

        df = self._cache.get_or_fetch("all_a_shares_spot", _fetch,
                                      ttl_hours=self._spot_ttl)
        # 缓存自愈：早期版本可能已缓存残缺数据；读到不合格的就作废重取一次
        if df is not None and not df.empty and not df.attrs.get("degraded"):
            ok, reason = validate_spot_quality(df)
            if not ok:
                logger.warning(f"缓存中的全A行情不合格（{reason}），作废并重新拉取")
                self._cache.invalidate("all_a_shares_spot")
                df = self._cache.get_or_fetch("all_a_shares_spot", _fetch,
                                              ttl_hours=self._spot_ttl)
        return df

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
        result = self._cache.get_or_fetch(cache_key, _fetch,
                                          ttl_hours=ttl_for("index_scope"))
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


    # Sina stock_zh_a_daily 返回的英文列名 → 中文列名（与 stock_zh_a_hist 对齐）













    # ========================
    # 分钟线获取（1m/5m/15m/30m/60m）
    # 主源 akshare 东方财富分钟接口 → pytdx 兜底（仅A股）
    # ========================

    # 分钟频率 → akshare stock_zh_a_hist_min_em 的 period 参数
    # 每个交易日的 bar 数：日历天数 → pytdx bar 数换算用





    # ========================
    # 批量并发预取：真正的加速来源
    # akshare 是 HTTP 请求，线程安全 → 多线程并发
    # ========================


