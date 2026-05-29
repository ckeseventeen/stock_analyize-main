"""
src/data/providers/pytdx_provider.py — 通达信(pytdx) K 线快速并发拉取

为什么需要这个：
  - akshare 主接口（东方财富）在很多机器上被反爬封 IP
  - Baostock Python 库受模块级全局 session 限制，无法多线程并发
  - pytdx 直连券商 TCP 服务器，**真正并发安全**，单只 250 天日 K ~40ms

性能（实测）：
  - 8 worker 并发 50 只: 1.12s (22ms/只)
  - 4342 只外推: ~22 秒（vs Baostock 串行 13 分钟，提速 30x）

输出列名归一化到 akshare 风格（"日期 开盘 最高 最低 收盘 成交量 成交额"），
下游 conditions.py 已兼容。
"""
from __future__ import annotations

import threading
import time
from typing import Optional

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("pytdx_provider")


# K 线周期映射：pytdx category
# 0=5分,1=15分,2=30分,3=60分,4=日,5=周,6=月,7=1分,8=1分,9=日,10=周,11=月,13=年
# 用 9/10/11 系列（更稳定的版本号）
_PERIOD_CATEGORY = {
    "d": 9,
    "w": 10,
    "m": 11,
}

# 通达信服务器（同 core/data_fetcher.py 内的默认列表）
DEFAULT_TDX_SERVERS: list[tuple[str, int]] = [
    ("119.147.212.81", 7709),
    ("114.80.80.222", 7709),
    ("180.153.18.170", 7709),
    ("218.108.98.244", 7709),
    ("218.108.47.69", 7709),
    ("218.75.126.9", 7709),
    ("123.125.108.14", 7709),
    ("114.80.63.12", 7709),
    ("114.80.63.35", 7709),
    ("180.153.39.51", 7709),
]


def _code_to_market(code: str) -> int:
    """A 股代码 → pytdx market 标识。0=SZ, 1=SH."""
    return 1 if str(code).startswith(("6", "9")) else 0


class PytdxProvider:
    """
    通达信 pytdx K 线 Provider（线程级连接池）

    用法:
        provider = PytdxProvider()
        if provider.is_available():
            df = provider.get_k_data("600519", days_back=250, frequency="d")
            # 并发场景：每个 worker 线程自动复用 thread-local 连接
    """

    def __init__(self, servers: Optional[list[tuple[str, int]]] = None,
                 connect_timeout: float = 3.0):
        self._servers = list(servers or DEFAULT_TDX_SERVERS)
        self._connect_timeout = connect_timeout
        self._working_servers: list[tuple[str, int]] = []
        self._working_lock = threading.Lock()
        self._server_round_robin = 0
        self._tlocal = threading.local()
        self._available = self._probe_servers()

    @staticmethod
    def _lazy_pytdx():
        try:
            from pytdx.hq import TdxHq_API
            return TdxHq_API
        except ImportError:
            return None

    def _probe_servers(self) -> bool:
        """探测哪些服务器可达（并发探测，省时）"""
        TdxHq_API = self._lazy_pytdx()
        if TdxHq_API is None:
            logger.info("pytdx 未安装，PytdxProvider 不可用")
            return False
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _try(ip_port):
            ip, port = ip_port
            api = TdxHq_API()
            try:
                ok = api.connect(ip, port, time_out=self._connect_timeout)
                if ok:
                    try:
                        api.disconnect()
                    except Exception:
                        pass
                    return (ip, port)
            except Exception:
                pass
            return None

        t0 = time.perf_counter()
        # 并发探测所有服务器，第一批成功的就用
        with ThreadPoolExecutor(max_workers=len(self._servers)) as pool:
            futures = [pool.submit(_try, s) for s in self._servers]
            for fut in as_completed(futures):
                result = fut.result()
                if result:
                    self._working_servers.append(result)
                if len(self._working_servers) >= 3:
                    # 提前停止剩余探测任务
                    for f in futures:
                        f.cancel()
                    break
        if self._working_servers:
            logger.info(
                f"pytdx 可用服务器: {self._working_servers} "
                f"(探测耗时 {time.perf_counter()-t0:.1f}s)"
            )
            return True
        logger.warning("pytdx 全部服务器不可达，PytdxProvider 不可用")
        return False

    def is_available(self) -> bool:
        return self._available

    def _pick_server(self) -> tuple[str, int]:
        """轮询挑选一个可用服务器"""
        with self._working_lock:
            srv = self._working_servers[self._server_round_robin % len(self._working_servers)]
            self._server_round_robin += 1
        return srv

    def _get_thread_api(self):
        """每个 worker 线程持有独立 TdxHq_API 实例（thread-local）"""
        api = getattr(self._tlocal, "api", None)
        if api is not None:
            return api
        TdxHq_API = self._lazy_pytdx()
        if TdxHq_API is None:
            return None
        api = TdxHq_API()
        # 尝试每个 working server，第一个成功即用
        for ip, port in self._working_servers:
            try:
                if api.connect(ip, port, time_out=self._connect_timeout):
                    self._tlocal.api = api
                    return api
            except Exception:
                continue
        return None

    def get_k_data(self, code: str, days_back: int = 250,
                   frequency: str = "d") -> pd.DataFrame:
        """
        拉单只 K 线。线程安全（每线程独立连接）。

        Args:
            code: 6 位股票代码（无市场前缀）
            days_back: 拉取的 K 线数量（不是日历天数；pytdx 单次最多 800）
            frequency: "d"/"w"/"m"

        Returns:
            DataFrame，列: 日期/开盘/最高/最低/收盘/成交量/成交额
            失败返回空 DataFrame
        """
        if not self._available:
            return pd.DataFrame()
        category = _PERIOD_CATEGORY.get(frequency, 9)
        market = _code_to_market(code)
        count = min(int(days_back), 800)

        api = self._get_thread_api()
        if api is None:
            return pd.DataFrame()

        # 失败时重试一次：换 thread-local 连接
        for attempt in range(2):
            try:
                bars = api.get_security_bars(category, market, str(code), 0, count)
                if bars:
                    return self._bars_to_df(bars)
                # 空结果可能是退市/停牌；不重试
                return pd.DataFrame()
            except Exception as e:
                logger.debug(f"{code} pytdx 第 {attempt+1} 次失败: {e}")
                # 销毁旧连接，下次 _get_thread_api 会重建
                try:
                    api.disconnect()
                except Exception:
                    pass
                self._tlocal.api = None
                if attempt == 0:
                    api = self._get_thread_api()
                    if api is None:
                        return pd.DataFrame()
        return pd.DataFrame()

    @staticmethod
    def _bars_to_df(bars: list) -> pd.DataFrame:
        """把 pytdx 返回的 OrderedDict 列表转成统一格式 DataFrame"""
        df = pd.DataFrame(bars)
        if df.empty:
            return df
        # pytdx 字段: open, close, high, low, vol, amount, datetime, year/month/day...
        rename = {
            "datetime": "日期",
            "open": "开盘",
            "high": "最高",
            "low": "最低",
            "close": "收盘",
            "vol": "成交量",
            "amount": "成交额",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        # 日期统一成 datetime
        if "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"])
        # 按日期升序（与 akshare/Baostock 风格一致）
        df = df.sort_values("日期", ignore_index=True) if "日期" in df.columns else df
        # 只保留下游需要的列
        keep = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额"]
        keep = [c for c in keep if c in df.columns]
        return df[keep]


# 模块级单例（懒初始化），避免每次调用都重新探测服务器
_GLOBAL_PROVIDER: Optional[PytdxProvider] = None
_GLOBAL_PROVIDER_LOCK = threading.Lock()


def get_global_pytdx() -> PytdxProvider:
    """获取全局 PytdxProvider 单例（线程安全延迟初始化）"""
    global _GLOBAL_PROVIDER
    if _GLOBAL_PROVIDER is None:
        with _GLOBAL_PROVIDER_LOCK:
            if _GLOBAL_PROVIDER is None:
                _GLOBAL_PROVIDER = PytdxProvider()
    return _GLOBAL_PROVIDER
