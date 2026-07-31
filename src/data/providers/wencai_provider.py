"""
src/data/providers/wencai_provider.py — 同花顺问财(iWencai) 数据提供层

通过 pywencai (开源 NLP 客户端) 一次性获取全A股的总市值/PE/PB/换手率等字段，
作为 akshare 主接口（东方财富）不可用时的关键兜底。

vs Baostock 串行 5500 只 20 分钟 → 同花顺 NLP 一次拉取 ~40 秒，字段更全。

依赖: pywencai>=0.13.1（懒导入，缺失时返回空 DataFrame 让上层走更下层 fallback）
"""
from __future__ import annotations

import re

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("wencai_provider")

# pywencai 返回的列名形如 "总市值[20260529]" / "市盈率(pe)[20260529]"，
# 把带日期后缀的列归一到统一名（与 akshare 风格一致）
_COL_PATTERNS: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(r"^股票代码$"), "代码"),
    (re.compile(r"^股票简称$"), "名称"),
    (re.compile(r"^最新价$"), "最新价"),
    (re.compile(r"^最新涨跌幅$"), "涨跌幅"),
    # 带日期后缀的字段：取第一个匹配
    (re.compile(r"^总市值\[\d{8}\]$"), "总市值"),
    (re.compile(r"^a股市值\(不含限售股\)\[\d{8}\]$"), "流通市值"),
    (re.compile(r"^市盈率\(pe,ttm\)\[\d{8}\]$"), "市盈率-动态"),
    (re.compile(r"^市盈率\(pe\)\[\d{8}\]$"), "市盈率-动态"),
    (re.compile(r"^市净率\(pb\)\[\d{8}\]$"), "市净率"),
    (re.compile(r"^市销率\(ps\)\[\d{8}\]$"), "市销率"),
    (re.compile(r"^换手率\[\d{8}\]$"), "换手率"),
    (re.compile(r"^涨跌幅:前复权\[\d{8}\]$"), "涨跌幅"),
    (re.compile(r"^成交额\[\d{8}\]$"), "成交额"),
    (re.compile(r"^销售毛利率\[\d{8}\]$"), "销售毛利率"),
    (re.compile(r"^销售净利率\[\d{8}\]$"), "销售净利率"),
    (re.compile(r"^所属同花顺行业$"), "所属行业"),
)

# 默认查询：覆盖筛选器常用的 spot 字段
_DEFAULT_QUERY = "全部A股最新总市值市盈率市净率换手率"

_NUMERIC_COLUMNS = (
    "最新价", "涨跌幅", "总市值", "流通市值",
    "市盈率-动态", "市净率", "市销率", "换手率",
    "成交额", "销售毛利率", "销售净利率",
)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """把 pywencai 返回的带日期后缀的列名映射到统一命名."""
    rename: dict[str, str] = {}
    for col in df.columns:
        for pattern, target in _COL_PATTERNS:
            if pattern.match(str(col)):
                # 同一目标名只保留第一个匹配（避免 pe / pe,ttm 都映射到 市盈率-动态 时冲突）
                if target not in rename.values():
                    rename[col] = target
                break
    return df.rename(columns=rename)


class WencaiProvider:
    """
    同花顺问财 spot 数据提供器（无 token，靠 NLP query）

    使用示例:
        wp = WencaiProvider()
        if wp.is_available():
            df = wp.get_all_a_shares()
    """

    # py_mini_racer(V8) 可用性探测结果缓存（进程级，None=未探测）
    _V8_PROBE_RESULT: bool | None = None

    def __init__(self, query: str = _DEFAULT_QUERY):
        self.query = query
        self._pywencai = self._lazy_import()

    @staticmethod
    def _lazy_import():
        """懒导入 pywencai；缺失时返回 None 而非抛错."""
        try:
            import pywencai  # noqa: F401
            return pywencai
        except ImportError:
            logger.info("pywencai 未安装，WencaiProvider 不可用。pip install pywencai")
            return None

    @classmethod
    def _probe_mini_racer(cls) -> bool:
        """
        在**子进程**里探测 py_mini_racer(V8) 能否初始化。

        背景：pywencai 依赖 py_mini_racer 执行同花顺 JS 加密；某些环境下
        V8 初始化会直接 FATAL（partition_address_space Check failed），
        **杀死整个 Python 进程且无法 try/except 捕获**。必须隔离到子进程
        探测一次，失败则永久禁用问财兜底（走 Baostock）。
        """
        if cls._V8_PROBE_RESULT is not None:
            return cls._V8_PROBE_RESULT

        # v8_guard 已在进程启动时禁用 MiniRacer（见 src/__init__.py）：
        # 此时子进程探测即使通过也没意义——本进程用不了，直接判定不可用，
        # 顺带省掉一次 ~2s 的子进程启动
        try:
            from src.core.v8_guard import guard_status
            if guard_status().get("applied"):
                cls._V8_PROBE_RESULT = False
                logger.info("v8_guard 已禁用 V8，问财兜底不可用（走 Baostock）")
                return False
        except Exception:
            pass

        import subprocess
        import sys
        try:
            proc = subprocess.run(
                [sys.executable, "-c",
                 "import py_mini_racer; py_mini_racer.MiniRacer().eval('1+1')"],
                capture_output=True, timeout=30,
            )
            cls._V8_PROBE_RESULT = proc.returncode == 0
        except Exception:
            cls._V8_PROBE_RESULT = False
        if not cls._V8_PROBE_RESULT:
            logger.warning(
                "py_mini_racer(V8) 在本机无法初始化（会 FATAL 崩进程），"
                "问财兜底已禁用，spot 数据将走 Baostock"
            )
        return cls._V8_PROBE_RESULT

    def is_available(self) -> bool:
        return self._pywencai is not None and self._probe_mini_racer()

    def get_all_a_shares(self, query: str | None = None) -> pd.DataFrame:
        """
        一次性查询全A股 spot 数据（含总市值/PE/PB/换手率）

        返回 DataFrame，列已归一到 akshare 风格 ("代码","名称","总市值","市盈率-动态",...)
        失败/不可用 → 返回空 DataFrame，由上层走更下层 fallback
        """
        if not self.is_available():
            return pd.DataFrame()
        q = query or self.query
        logger.info(f"正在通过同花顺问财(iWencai)拉取全A股数据: query={q!r}")
        try:
            raw = self._pywencai.get(query=q, loop=True)
        except Exception as e:
            logger.warning(f"pywencai 查询失败: {type(e).__name__}: {e}")
            return pd.DataFrame()

        if raw is None or not hasattr(raw, "shape") or raw.empty:
            logger.warning("pywencai 返回空")
            return pd.DataFrame()

        logger.debug(f"pywencai raw columns ({raw.shape}): {list(raw.columns)}")
        df = _normalize_columns(raw)

        # 必须含 代码/名称；否则 query 命中错条目，放弃
        if "代码" not in df.columns or "名称" not in df.columns:
            logger.warning(
                f"pywencai 返回结构异常（无 代码/名称 列），原始列={list(raw.columns)[:15]}"
            )
            return pd.DataFrame()

        # 校验关键 spot 字段是否成功归一化；若未匹配则打印原始列名供调试
        spot_fields = ("总市值", "市盈率-动态", "市净率", "换手率")
        missing_spot = [f for f in spot_fields if f not in df.columns]
        if missing_spot:
            logger.warning(
                f"pywencai 列归一化后缺 {missing_spot}；原始列名={list(raw.columns)}"
            )

        # 代码归一化：去掉 .SH/.SZ/.BJ 后缀，保留 6 位数字
        df["代码"] = df["代码"].astype(str).str.split(".").str[0].str.zfill(6)

        # ST/退市过滤
        df = df[~df["名称"].astype(str).str.contains("ST|退市|退", na=False)]

        # 数值列转 numeric
        for col in _NUMERIC_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 总市值通常以 "元" 返回（pywencai 不会乘万），无需缩放；如发现单位是亿，外层会再处理
        logger.info(
            f"同花顺问财拉取成功，共 {len(df)} 只（过滤ST后），"
            f"含字段: {[c for c in ('总市值','市盈率-动态','市净率','换手率') if c in df.columns]}"
        )
        return df.reset_index(drop=True)
