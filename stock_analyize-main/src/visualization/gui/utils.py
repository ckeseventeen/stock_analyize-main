"""
src/visualization/gui/utils.py — Streamlit 前端共享工具

统一封装：
  - sys.path 注入（确保可从 project root 导入）
  - YAML 读写（读用 PyYAML，写优先用 ruamel.yaml 保留注释）
  - DataFrame 展示格式化
  - 配置路径常量 + 缓存装饰器
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# -------- sys.path 注入：确保从 streamlit 运行时也能找到项目根目录 --------
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd  # noqa: E402
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
PATH_A_STOCK = CONFIG_DIR / "a_stock.yaml"
PATH_HK_STOCK = CONFIG_DIR / "hk_stock.yaml"
PATH_US_STOCK = CONFIG_DIR / "us_stock.yaml"
PATH_SCREEN = CONFIG_DIR / "screen_config.yaml"
PATH_PRICE_ALERTS = CONFIG_DIR / "price_alerts.yaml"
PATH_EARNINGS = CONFIG_DIR / "earnings_monitor.yaml"
PATH_SCRAPER = CONFIG_DIR / "scraper.yaml"
PATH_ALERTS = CONFIG_DIR / "alerts.yaml"

ALERT_STATE_PATH = CACHE_DIR / "alert_state.json"
ALERT_LOG_PATH = LOGS_DIR / "alerts.log"


# ========================
# YAML 读写
# ========================

def load_yaml(path: Path | str) -> dict:
    """
    读取 YAML 文件为字典。文件不存在或格式错误时返回空 dict，避免前端崩溃。
    """
    p = Path(path)
    if not p.exists():
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
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


# ========================
# DataFrame 工具
# ========================

def format_numeric_columns(df: pd.DataFrame, decimals: int = 2) -> pd.DataFrame:
    """
    对 DataFrame 的数值列做保留小数格式化（返回副本，不改原数据）。
    用于前端表格展示，避免科学计数法/过多小数位。
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in out.select_dtypes(include=["float", "float64", "float32"]).columns:
        out[col] = out[col].round(decimals)
    return out


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """DataFrame 转 UTF-8-SIG 编码的 CSV bytes，用于 st.download_button"""
    return df.to_csv(index=False).encode("utf-8-sig")


# ========================
# 市场元数据
# ========================

MARKET_LABELS = {
    "a": "A 股",
    "hk": "港股",
    "us": "美股",
}

MARKET_CONFIG_PATHS = {
    "a": PATH_A_STOCK,
    "hk": PATH_HK_STOCK,
    "us": PATH_US_STOCK,
}


def list_stocks_from_market_config(market: str) -> list[dict]:
    """
    从市场 YAML 提取所有股票（展平 categories）供下拉选择。

    Returns:
        [{"code": "...", "name": "...", "category": "...", "valuation": "pe", ...}, ...]
    """
    cfg_path = MARKET_CONFIG_PATHS.get(market)
    if not cfg_path:
        return []
    cfg = load_yaml(cfg_path)
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


# ========================
# 通用渲染
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
