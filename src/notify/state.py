"""
src/automation/alert/state.py — 告警状态存储（去重）

用途：
  - 记录已触发的告警事件，在 cooldown 窗口内相同事件不重复推送
  - 存储为单个 JSON 文件（数据量小、无需引入数据库）

文件路径：./cache/alert_state.json
JSON 结构：
  {
    "600519:price_below_1500:2026-04-15": {"fired_at": "2026-04-15T01:30:00+00:00"},
    ...
  }

并发安全（B2 修复）：
  - 同进程多线程：threading.Lock
  - 多进程：file_lock（fcntl/msvcrt 跨平台），读改写整体原子
  - 时间戳改为 UTC + 时区感知 ISO（B9 修复），避免容器/宿主机 TZ 不一致
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.utils.file_lock import file_lock
from src.utils.logger import get_logger

logger = get_logger("alert")

# 默认状态文件
_DEFAULT_STATE_PATH = Path("./cache/alert_state.json")

# 文件读写锁（同进程多线程安全）
_lock = threading.Lock()


def _utcnow() -> datetime:
    """统一时区感知 UTC 时间，避免本地 TZ 漂移"""
    return datetime.now(timezone.utc)


def _parse_dt(s: str) -> datetime | None:
    """
    向后兼容解析 ISO 时间戳。

    - 新格式：含时区 (e.g. "2026-04-15T01:30:00+00:00") → 直接 fromisoformat
    - 旧格式：naive (e.g. "2026-04-15T09:30:00") → 兜底当作本地时间转 UTC
    - 损坏：返回 None
    """
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            # 旧记录无时区，按本地时区处理后转 UTC
            dt = dt.astimezone(timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


class AlertStateStore:
    """
    告警去重存储。

    典型用法：
        store = AlertStateStore()
        if not store.was_fired(event_key, cooldown_hours=24):
            channel.send(event)
            store.mark_fired(event_key)
    """

    def __init__(self, path: Path | str | None = None):
        """
        Args:
            path: 状态文件路径，None 使用默认 ./cache/alert_state.json
        """
        self._path = Path(path) if path else _DEFAULT_STATE_PATH
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_path = self._path.with_suffix(self._path.suffix + ".lock")

    def _load_unlocked(self) -> dict[str, dict]:
        """从磁盘加载状态；文件不存在或损坏时返回空字典。调用方必须先持锁。"""
        if not self._path.exists():
            return {}
        try:
            with open(self._path, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                logger.warning(f"告警状态文件结构异常，重置: {self._path}")
                return {}
            return data
        except Exception as e:
            logger.warning(f"告警状态文件读取失败，使用空状态: {e}")
            return {}

    def _save_unlocked(self, cache: dict[str, dict]) -> None:
        """原子写：tmp + replace。调用方必须先持锁。"""
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            tmp.replace(self._path)
        except Exception as e:
            logger.error(f"告警状态文件写入失败: {e}", exc_info=True)

    def was_fired(self, event_key: str, cooldown_hours: int = 24) -> bool:
        """
        判断指定事件是否在 cooldown 窗口内已触发过。

        Args:
            event_key: 事件唯一键
            cooldown_hours: 冷却小时数，超过视为可重新推送

        Returns:
            True 表示仍在冷却，应跳过推送；False 表示可推送
        """
        with _lock, file_lock(self._lock_path):
            cache = self._load_unlocked()
            record = cache.get(event_key)
            if not record:
                return False
            fired_at = _parse_dt(record.get("fired_at", ""))
            if fired_at is None:
                return False
            return _utcnow() - fired_at < timedelta(hours=cooldown_hours)

    def mark_fired(self, event_key: str) -> None:
        """
        记录事件已触发（更新时间戳）。

        B2/B9 修复：跨进程串行化 + UTC 时间戳。
        """
        with _lock, file_lock(self._lock_path):
            cache = self._load_unlocked()
            cache[event_key] = {"fired_at": _utcnow().isoformat(timespec="seconds")}
            self._save_unlocked(cache)

    def clear_expired(self, retention_days: int = 30) -> int:
        """
        清理超过 retention_days 天的历史记录，避免文件膨胀。

        Returns:
            清理的条目数
        """
        cutoff = _utcnow() - timedelta(days=retention_days)
        with _lock, file_lock(self._lock_path):
            cache = self._load_unlocked()
            to_remove = []
            for key, record in cache.items():
                fired_at = _parse_dt(record.get("fired_at", ""))
                if fired_at is None or fired_at < cutoff:
                    to_remove.append(key)
            for k in to_remove:
                del cache[k]
            if to_remove:
                self._save_unlocked(cache)
            return len(to_remove)

    def all_records(self) -> dict[str, dict]:
        """返回全量状态快照（供前端告警历史页展示）"""
        with _lock, file_lock(self._lock_path):
            return dict(self._load_unlocked())
