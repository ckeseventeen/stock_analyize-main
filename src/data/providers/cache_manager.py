"""
src/data/fetcher/cache_manager.py — 数据缓存管理器

提供基于文件系统的离线数据缓存，避免重复请求 API。
支持：Pickle 格式（DataFrame）、过期自动失效、缓存统计。
"""
import hashlib
import logging
import os
import pickle
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd

logger = logging.getLogger("stock_analyzer")


class CacheManager:
    """
    基于文件系统的数据缓存管理器。

    使用示例:
        cache = CacheManager(cache_dir=".cache", ttl_hours=24)

        # 缓存 DataFrame
        df = cache.get_or_fetch(
            key="a_finance_002156",
            fetch_func=lambda: fetcher.get_financial_abstract("002156"),
            ttl_hours=24
        )
    """

    def __init__(self, cache_dir: str = ".cache", ttl_hours: int = 24, max_mb: int = 200):
        """
        Args:
            cache_dir: 缓存文件存放目录
            ttl_hours: 默认缓存有效期（小时）
            max_mb: 磁盘容量上限（MB），超过时自动淘汰最旧文件
        """
        self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._ttl = timedelta(hours=ttl_hours)
        self._max_bytes = max_mb * 1024 * 1024
        self._hits = 0
        self._misses = 0

    def _cache_path(self, key: str) -> Path:
        """生成缓存文件路径（对 key 做 MD5 防止特殊字符）"""
        hashed = hashlib.md5(key.encode()).hexdigest()
        return self._dir / f"{hashed}.pkl"

    def get(self, key: str, ttl_hours: Optional[int] = None) -> Optional[Any]:
        """
        从缓存读取数据。

        Args:
            key: 缓存键
            ttl_hours: 有效期（小时），None 使用实例默认值

        Returns:
            缓存数据，不存在或已过期返回 None
        """
        path = self._cache_path(key)
        if not path.exists():
            self._misses += 1
            return None

        ttl = timedelta(hours=ttl_hours) if ttl_hours is not None else self._ttl
        modified_time = datetime.fromtimestamp(path.stat().st_mtime)
        if datetime.now() - modified_time > ttl:
            logger.debug(f"缓存已过期: {key}")
            path.unlink(missing_ok=True)
            self._misses += 1
            return None

        try:
            with open(path, 'rb') as f:
                data = pickle.load(f)
            # 命中时更新时间戳，供 LRU 淘汰使用
            now_ts = datetime.now().timestamp()
            os.utime(path, (now_ts, now_ts))
            self._hits += 1
            logger.debug(f"缓存命中: {key}")
            return data
        except Exception as e:
            logger.warning(f"缓存读取失败 [{key}]: {e}")
            self._misses += 1
            return None

    def set(self, key: str, data: Any) -> None:
        """写入缓存（写入后检查容量，超限时淘汰最旧文件）"""
        path = self._cache_path(key)
        try:
            with open(path, 'wb') as f:
                pickle.dump(data, f)
            self._enforce_capacity()
            logger.debug(f"缓存写入成功: {key}")
        except Exception as e:
            logger.warning(f"缓存写入失败 [{key}]: {e}")

    def _enforce_capacity(self) -> None:
        """
        淘汰多余缓存文件（LRU：按最后修改时间从旧到新淘汰），
        直到总大小 <= max_bytes 或只剩一个文件。
        """
        files = sorted(self._dir.glob("*.pkl"), key=lambda f: f.stat().st_mtime)
        total = sum(f.stat().st_size for f in files)
        if total <= self._max_bytes:
            return
        removed = 0
        for f in files:
            if total <= self._max_bytes:
                break
            total -= f.stat().st_size
            f.unlink(missing_ok=True)
            removed += 1
        if removed:
            logger.info(f"缓存容量控制: 淘汰 {removed} 个旧文件, 当前缓存大小 {total / 1e6:.1f}MB")

    def get_or_fetch(
        self,
        key: str,
        fetch_func: Callable,
        ttl_hours: Optional[int] = None,
    ) -> Any:
        """
        缓存穿透模式：先读缓存，未命中则调用 fetch_func 并写入缓存。

        Args:
            key: 缓存键（建议格式："{market}_{type}_{code}"）
            fetch_func: 数据获取函数（无参数可调用对象）
            ttl_hours: 有效期覆盖

        Returns:
            缓存数据或 fetch_func 返回值
        """
        cached = self.get(key, ttl_hours)
        if cached is not None:
            return cached

        logger.debug(f"缓存未命中，正在获取: {key}")
        data = fetch_func()

        # 只缓存非空数据
        if data is not None and (not isinstance(data, pd.DataFrame) or not data.empty):
            self.set(key, data)

        return data

    def invalidate(self, key: str) -> bool:
        """手动失效指定缓存键"""
        path = self._cache_path(key)
        if path.exists():
            path.unlink()
            logger.debug(f"缓存已失效: {key}")
            return True
        return False

    def clear_all(self) -> int:
        """清空所有缓存，返回删除文件数"""
        count = 0
        for f in self._dir.glob("*.pkl"):
            f.unlink()
            count += 1
        logger.info(f"已清空缓存，共删除 {count} 个文件")
        return count

    @property
    def stats(self) -> dict:
        """返回缓存命中统计和容量信息"""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0.0
        files = list(self._dir.glob("*.pkl"))
        total_bytes = sum(f.stat().st_size for f in files)
        return {
            "hits": self._hits,
            "misses": self._misses,
            "total": total,
            "hit_rate": f"{hit_rate:.1%}",
            "cache_files": len(files),
            "disk_usage_mb": round(total_bytes / 1e6, 1),
            "max_mb": round(self._max_bytes / 1e6),
            "usage_pct": f"{total_bytes / self._max_bytes * 100:.1f}%" if self._max_bytes > 0 else "N/A",
        }
