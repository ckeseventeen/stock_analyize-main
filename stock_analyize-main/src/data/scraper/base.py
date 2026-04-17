"""
src/data/scraper/base.py — 抓取器基类

所有 scraper 继承 BaseScraper，统一：
  - CacheManager 封装（避免重复请求）
  - 已见集合持久化（fetch_new 增量检测）
  - requests Session + UA 伪装（如需 HTML 抓取）
"""
from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests

from src.data.fetcher.cache_manager import CacheManager
from src.utils.logger import get_logger
from src.utils.name_resolver import StockNameResolver

logger = get_logger("scraper")


# 共用 User-Agent（避免部分接口拒绝默认 python-requests/xx）
_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class BaseScraper(ABC):
    """
    抓取器抽象基类。

    子类实现：
      - name: 抓取器名称（用于缓存路径和 CSV 文件名）
      - fetch(**kwargs) -> pd.DataFrame : 拉取数据（带缓存）
    基类统一：
      - fetch_new(**kwargs) : 对比 seen_set，返回新增条目
      - save_csv(df, output_dir) : 以 {name}_{date}.csv 保存
    """

    name: str = "base"

    # 用于 fetch_new 判断唯一性的列名（子类可覆盖）
    primary_key: str = "id"

    def __init__(
        self,
        cache_dir: str = ".cache/scraper",
        cache_ttl_hours: int = 1,
        seen_path: Optional[str | Path] = None,
    ):
        """
        Args:
            cache_dir: 缓存目录
            cache_ttl_hours: 缓存 TTL
            seen_path: 已见条目 JSON 路径（fetch_new 用）
        """
        self._cache = CacheManager(cache_dir=cache_dir, ttl_hours=cache_ttl_hours)
        self._ttl = cache_ttl_hours
        self._seen_path = Path(seen_path) if seen_path else Path(f".cache/scraper/seen_{self.name}.json")
        self._seen_path.parent.mkdir(parents=True, exist_ok=True)

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": _DEFAULT_UA})

    # ------------------------------------------------------------------ #
    # 子类必须实现
    # ------------------------------------------------------------------ #
    @abstractmethod
    def fetch(self, **kwargs) -> pd.DataFrame:
        """
        拉取全量数据（或"最近一批"）。
        推荐使用 self._cache.get_or_fetch() 带缓存。
        """
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # 增量（基于 primary_key 去重）
    # ------------------------------------------------------------------ #
    def fetch_new(self, **kwargs) -> pd.DataFrame:
        """
        返回与上次调用相比的新增条目。

        使用 hash(row[primary_key]) 作为唯一标识，持久化到 seen.json。
        """
        df = self.fetch(**kwargs)
        if df is None or df.empty or self.primary_key not in df.columns:
            return pd.DataFrame(columns=getattr(df, "columns", []))

        seen: set[str] = self._load_seen()

        def _hash(v) -> str:
            return hashlib.md5(str(v).encode("utf-8")).hexdigest()

        df = df.copy()
        df["_hash"] = df[self.primary_key].map(_hash)
        new_mask = ~df["_hash"].isin(seen)
        new_df = df[new_mask].drop(columns=["_hash"])

        # 更新 seen
        seen.update(df["_hash"].tolist())
        # 限制大小（只保留最近 10000 条 hash）
        if len(seen) > 10000:
            seen = set(list(seen)[-10000:])
        self._save_seen(seen)

        if not new_df.empty:
            logger.info(f"[{self.name}] 新增 {len(new_df)} 条 / 总 {len(df)} 条")
        return new_df

    def _load_seen(self) -> set[str]:
        if not self._seen_path.exists():
            return set()
        try:
            with open(self._seen_path, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()

    def _save_seen(self, seen: set[str]) -> None:
        try:
            with open(self._seen_path, "w", encoding="utf-8") as f:
                json.dump(sorted(seen), f)
        except Exception as e:
            logger.warning(f"[{self.name}] seen 持久化失败: {e}")

    # ------------------------------------------------------------------ #
    # CSV 落盘
    # ------------------------------------------------------------------ #
    def save_csv(self, df: pd.DataFrame, output_dir: str | Path) -> Optional[Path]:
        """保存到 output/scraper/{name}_{YYYYMMDD}.csv，返回路径"""
        if df is None or df.empty:
            return None
        out_dir = Path(output_dir) / "scraper"
        out_dir.mkdir(parents=True, exist_ok=True)
        date_str = pd.Timestamp.now().strftime("%Y%m%d")
        path = out_dir / f"{self.name}_{date_str}.csv"
        try:
            df.to_csv(path, index=False, encoding="utf-8-sig")
            logger.info(f"[{self.name}] 已保存 {len(df)} 条 → {path}")
            return path
        except Exception as e:
            logger.error(f"[{self.name}] CSV 保存失败: {e}")
            return None

    # ------------------------------------------------------------------ #
    # 辅助：关键词过滤
    # ------------------------------------------------------------------ #
    @staticmethod
    def filter_by_keywords(df: pd.DataFrame, text_cols: Iterable[str], keywords: list[str]) -> pd.DataFrame:
        """
        按关键词子串匹配过滤（OR 语义，命中任一即保留）。

        Args:
            df: 原始数据
            text_cols: 要匹配的列名列表
            keywords: 关键词列表（空/None 时不过滤）

        Returns:
            过滤后的 DataFrame，额外增加 matched_keyword 列
        """
        if df is None or df.empty or not keywords:
            if df is not None and not df.empty:
                df = df.copy()
                df["matched_keyword"] = ""
            return df

        text_cols = [c for c in text_cols if c in df.columns]
        if not text_cols:
            return df

        # 拼接文本便于一次正则匹配
        combined = df[text_cols].astype(str).agg(" ".join, axis=1).str.lower()
        lower_keywords = [k.lower() for k in keywords if k]

        # 记录每行命中的关键词
        def _match(text: str) -> str:
            hits = [k for k in keywords if k and k.lower() in text]
            return ",".join(hits)

        matched = combined.map(_match)
        out = df[matched != ""].copy()
        out["matched_keyword"] = matched[matched != ""].values
        return out

    def _inject_names(self, df: pd.DataFrame, market: str = "a") -> pd.DataFrame:
        """为 DataFrame 注入 'name' 列（基于 'code' 列）"""
        if df is None or df.empty or "code" not in df.columns:
            return df
        
        resolver = StockNameResolver()
        df = df.copy()
        
        # 如果列中已经有非空的 name，则不覆盖；否则尝试填充
        if "name" not in df.columns:
            df.insert(1, "name", "") # 插在 code 后面
        
        def _get_name(row):
            current = str(row.get("name", "")).strip()
            if current and current != row["code"]:
                return current
            return resolver.get_name(row["code"], market=market)

        df["name"] = df.apply(_get_name, axis=1)
        return df
