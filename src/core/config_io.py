"""
src/core/config_io.py — YAML 配置读写与路径常量（核心层）

原 src/web/config_ops.py 迁移至此：services / portfolio / trading / report
等非 UI 层不应依赖 web 包。web.config_ops 保留为兼容 shim。
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# -------- sys.path 注入 --------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import yaml  # noqa: E402

# ========================
# 路径常量
# ========================

PROJECT_ROOT: Path = _PROJECT_ROOT
CONFIG_DIR: Path = PROJECT_ROOT / "config"
OUTPUT_DIR: Path = PROJECT_ROOT / "output"
CACHE_DIR: Path = PROJECT_ROOT / "cache"
LOGS_DIR: Path = PROJECT_ROOT / "logs"

# 各功能模块的 YAML 默认路径
PATH_A_STOCK = CONFIG_DIR / "stocks" / "a_stock.yaml"
PATH_HK_STOCK = CONFIG_DIR / "stocks" / "hk_stock.yaml"
PATH_US_STOCK = CONFIG_DIR / "stocks" / "us_stock.yaml"
PATH_SCREEN = CONFIG_DIR / "screen_config.yaml"
PATH_PRICE_ALERTS = CONFIG_DIR / "price_alerts.yaml"
PATH_EARNINGS = CONFIG_DIR / "earnings_monitor.yaml"
PATH_SCRAPER = CONFIG_DIR / "scraper.yaml"
PATH_ALERTS = CONFIG_DIR / "alerts.yaml"
PATH_INDICATORS = CONFIG_DIR / "indicators.yaml"
PATH_FACTORS = CONFIG_DIR / "factors.yaml"
PATH_BACKTEST_PRESETS = CONFIG_DIR / "backtest_presets.yaml"

ALERT_STATE_PATH = CACHE_DIR / "alert_state.json"
ALERT_LOG_PATH = LOGS_DIR / "alerts.log"

# 市场元数据（re-export 入口）
# 实际数据来源是 src/core/market_registry.py；此处仅做 re-export 供各层引用。
# 加新市场请到 market_registry.py，不要改这里。
try:
    from src.core.market_registry import list_markets as _list_markets
    MARKET_LABELS = {spec.key: spec.label for spec in _list_markets()}
    MARKET_CONFIG_PATHS = {spec.key: spec.config_path for spec in _list_markets()}
except Exception:
    # 启动早期或循环 import 时的兜底：保留旧硬编码
    MARKET_LABELS = {"a": "A 股", "hk": "港股", "us": "美股"}
    MARKET_CONFIG_PATHS = {"a": PATH_A_STOCK, "hk": PATH_HK_STOCK, "us": PATH_US_STOCK}


# ========================
# 路径安全（B5/SEC2 修复：防止路径穿越）
# ========================

def validate_user_path(user_path: str | Path, allowed_roots: list[Path] | None = None) -> Path:
    """
    校验用户输入的文件路径，防止路径穿越。

    Args:
        user_path: 用户提供的路径
        allowed_roots: 允许的根目录列表，None 默认仅允许 CONFIG_DIR 和 OUTPUT_DIR

    Returns:
        归一化后的绝对路径

    Raises:
        ValueError: 路径不在白名单根目录内，或包含可疑模式
    """
    if allowed_roots is None:
        allowed_roots = [CONFIG_DIR, OUTPUT_DIR]

    candidate = Path(user_path).expanduser()
    # 拒绝明显恶意输入
    s = str(user_path)
    for bad in ("..\\..", "../..", "/etc/", "C:\\Windows\\", "\x00"):
        if bad in s:
            raise ValueError(f"路径包含可疑模式 '{bad}'，拒绝：{user_path}")

    try:
        resolved = candidate.resolve()
    except OSError as e:
        raise ValueError(f"无法解析路径: {user_path} ({e})")

    # 必须落在白名单根目录内
    for root in allowed_roots:
        try:
            resolved.relative_to(root.resolve())
            return resolved
        except ValueError:
            continue

    allowed_str = ", ".join(str(r) for r in allowed_roots)
    raise ValueError(f"路径必须位于以下目录内: {allowed_str}；用户输入: {user_path}")


# ========================
# YAML 读写
# ========================

def load_yaml(path: Path | str, ttl: int = 0) -> dict:
    """
    读取 YAML 文件为字典。文件不存在或格式错误时返回空 dict，避免前端崩溃。

    Args:
        path: YAML 文件路径
        ttl: 缓存秒数（0=不缓存，默认）。前端页面可传 ttl>0 启用缓存，
             避免每次 Streamlit rerun 都重新读取磁盘。
    """
    p = Path(path)
    if not p.exists():
        return {}
    if ttl > 0:
        return _cached_load_yaml(str(p), ttl)
    return _read_yaml_file(p)


_YAML_CACHE: dict = {}  # path -> (ts, data)


def _cached_load_yaml(path_str: str, ttl: int) -> dict:
    """进程级 TTL 缓存的 YAML 读取（原为 Streamlit 缓存，界面下线后自管理）"""
    import time
    hit = _YAML_CACHE.get(path_str)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    data = _read_yaml_file(Path(path_str))
    _YAML_CACHE[path_str] = (time.time(), data)
    return data


def _read_yaml_file(p: Path) -> dict:
    """内部：实际读取 YAML 文件"""
    try:
        with open(p, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except yaml.YAMLError as e:
        import logging
        logging.getLogger("web.utils").warning(f"YAML 解析失败 [{p}]: {e}")
        return {}
    except Exception as e:
        import logging
        logging.getLogger("web.utils").warning(f"YAML 读取失败 [{p}]: {e}")
        return {}


def save_yaml(path: Path | str, data: dict, preserve_comments: bool = True) -> bool:
    """
    写入 YAML。优先使用 ruamel.yaml 保留注释与结构，降级到 PyYAML。

    Args:
        path: 目标文件路径
        data: 待写入字典
        preserve_comments: 是否尝试保留注释（仅当 ruamel.yaml 可用时生效）

    Returns:
        True 表示写入成功
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    if preserve_comments:
        try:
            from ruamel.yaml import YAML
            yaml_rw = YAML()
            yaml_rw.preserve_quotes = True
            yaml_rw.indent(mapping=2, sequence=4, offset=2)
            with open(p, "w", encoding="utf-8") as f:
                yaml_rw.dump(data, f)
            return True
        except ImportError:
            # ruamel 未安装，降级到 PyYAML
            pass
        except Exception:
            # 保留注释失败，降级
            pass

    try:
        with open(p, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False, indent=2)
        return True
    except Exception:
        return False


def atomic_save_yaml(path: Path | str, data: dict, preserve_comments: bool = True,
                     keep_backups: int = 3) -> bool:
    """
    原子写入 YAML：先写 .tmp 再 os.replace；同时保留 .bak 备份（轮转 keep_backups 份）。

    避免写入过程中断导致配置损坏；多 tab 并发编辑也更安全。
    """
    import os
    import shutil

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    # 备份当前文件（如果存在）
    if p.exists() and keep_backups > 0:
        try:
            # 滚动旧备份 .bak.2 -> .bak.3, .bak.1 -> .bak.2
            for i in range(keep_backups - 1, 0, -1):
                src = p.with_suffix(p.suffix + f".bak.{i}")
                dst = p.with_suffix(p.suffix + f".bak.{i + 1}")
                if src.exists():
                    shutil.copy2(src, dst)
            # 当前文件 -> .bak.1
            shutil.copy2(p, p.with_suffix(p.suffix + ".bak.1"))
        except Exception:
            pass  # 备份失败不影响主流程

    tmp_path = p.with_suffix(p.suffix + ".tmp")
    try:
        if preserve_comments:
            try:
                from ruamel.yaml import YAML
                yaml_rw = YAML()
                yaml_rw.preserve_quotes = True
                yaml_rw.indent(mapping=2, sequence=4, offset=2)
                with open(tmp_path, "w", encoding="utf-8") as f:
                    yaml_rw.dump(data, f)
                os.replace(tmp_path, p)
                return True
            except ImportError:
                pass
            except Exception:
                pass

        with open(tmp_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False, indent=2)
        os.replace(tmp_path, p)
        return True
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return False


# ========================
# 通用工具
# ========================

def ensure_project_dirs() -> None:
    """确保 cache/logs/output 目录存在（避免读写抛 FileNotFoundError）"""
    for d in (CACHE_DIR, LOGS_DIR, OUTPUT_DIR):
        d.mkdir(parents=True, exist_ok=True)


def safe_import(module: str, attr: str | None = None) -> Any:
    """
    安全动态 import；用于 Streamlit 页面对可选依赖（如 akshare、yfinance）做降级。
    失败时返回 None 而非抛异常，让页面自行决定降级提示。
    """
    try:
        import importlib
        mod = importlib.import_module(module)
        return getattr(mod, attr) if attr else mod
    except Exception:
        return None


def setup_matplotlib_chinese() -> None:
    """
    配置 matplotlib 使用中文字体 + Agg 后端。
    解决 Streamlit 多线程环境下 TkAgg 崩溃问题，同时确保图表中文不乱码。
    每个使用 matplotlib 的页面调用一次即可。
    """
    import os
    os.environ.setdefault("MPLBACKEND", "Agg")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    for _fn in ("SimHei", "Microsoft YaHei", "WenQuanYi Micro Hei", "Arial Unicode MS"):
        try:
            plt.rcParams["font.sans-serif"] = [_fn] + plt.rcParams.get("font.sans-serif", [])
            break
        except Exception:
            continue
    plt.rcParams["axes.unicode_minus"] = False


# ========================
# 通用 YAML 列表 CRUD（供 scraper.yaml / 任何 YAML 下的 list 字段使用）
# 以点号路径定位字段，例如 "news.keywords" / "announcements.watchlist"
# ========================

def _resolve_dotted(obj: dict, dotted_key: str, create_missing: bool = False):
    """
    按点号路径在 dict 中下钻。返回 (parent_dict, leaf_key)。
    create_missing=True 时自动创建中间 dict；否则缺失返回 (None, None)。
    """
    if not dotted_key:
        return None, None
    parts = dotted_key.split(".")
    cur = obj
    for p in parts[:-1]:
        if not isinstance(cur, dict):
            return None, None
        if p not in cur or cur[p] is None:
            if create_missing:
                cur[p] = {}
            else:
                return None, None
        cur = cur[p]
    if not isinstance(cur, dict):
        return None, None
    return cur, parts[-1]


def list_yaml_list(path: Path | str, dotted_key: str) -> list[str]:
    """
    读取 YAML 下 dotted_key 对应的 list（强制 str 化，防御数字被错解析）。
    不存在时返回 []。
    """
    cfg = load_yaml(path) or {}
    parent, leaf = _resolve_dotted(cfg, dotted_key, create_missing=False)
    if parent is None or leaf not in parent:
        return []
    raw = parent[leaf] or []
    if not isinstance(raw, list):
        return []
    return [str(x).strip() for x in raw if x not in (None, "")]


def add_to_yaml_list(path: Path | str, dotted_key: str, value: str) -> tuple[bool, str]:
    """
    向 YAML 下 dotted_key 指向的 list 追加一个值（去重）。
    自动创建缺失的中间 dict 与 list。成功返回 (True, '添加成功')。
    """
    v = str(value).strip()
    if not v:
        return False, "不能为空"
    cfg = load_yaml(path) or {}
    parent, leaf = _resolve_dotted(cfg, dotted_key, create_missing=True)
    if parent is None:
        return False, f"路径无效: {dotted_key}"
    existing = parent.get(leaf)
    if not isinstance(existing, list):
        existing = []
    # 去重（忽略前后空白）
    existing_norm = [str(x).strip() for x in existing]
    if v in existing_norm:
        return False, f"'{v}' 已存在"
    existing.append(v)
    parent[leaf] = existing
    ok = atomic_save_yaml(path, cfg)
    return ok, ("添加成功" if ok else "写入失败")


def remove_from_yaml_list(path: Path | str, dotted_key: str, value: str) -> tuple[bool, str]:
    """从 YAML 下 dotted_key 的 list 移除值"""
    v = str(value).strip()
    cfg = load_yaml(path) or {}
    parent, leaf = _resolve_dotted(cfg, dotted_key, create_missing=False)
    if parent is None or leaf not in parent:
        return False, f"路径不存在: {dotted_key}"
    existing = parent.get(leaf)
    if not isinstance(existing, list):
        return False, "目标不是 list"
    # 按 str 化后的值匹配，避免 YAML 混入 int/float 时找不到
    new_list = [x for x in existing if str(x).strip() != v]
    if len(new_list) == len(existing):
        return False, f"'{v}' 不存在"
    parent[leaf] = new_list
    ok = atomic_save_yaml(path, cfg)
    return ok, ("已删除" if ok else "写入失败")


def list_stocks_from_market_config(market: str, ttl: int = 300) -> list[dict]:
    """
    从市场 YAML 提取所有股票（展平 categories）。

    原 src/web/utils.py 迁入（Streamlit 界面删除后归位核心层）。

    Returns:
        [{"code", "name", "category", "valuation", ...}, ...]
    """
    cfg_path = MARKET_CONFIG_PATHS.get(market)
    if not cfg_path:
        return []
    cfg = load_yaml(cfg_path, ttl=ttl)
    if not cfg:
        return []
    stocks: list[dict] = []
    for cat_key, cat_data in (cfg.get("categories") or {}).items():
        if not cat_data:
            continue
        cat_name = cat_data.get("name", cat_key)
        for stock in cat_data.get("stocks", []) or []:
            entry = dict(stock)
            entry["category"] = cat_name
            stocks.append(entry)
    return stocks
