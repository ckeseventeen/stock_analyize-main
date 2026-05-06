"""
src/web/utils.py — Streamlit 前端共享工具

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
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
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


# ========================
# 原子写 + 备份
# ========================

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
# 关注标的（watchlist）CRUD
# ========================

def add_stock_to_market(market: str, category_key: str, stock: dict) -> tuple[bool, str]:
    """
    向市场 YAML 追加股票。若 (market, code) 已存在则拒绝。

    Returns:
        (成功标志, 消息)
    """
    cfg_path = MARKET_CONFIG_PATHS.get(market)
    if not cfg_path:
        return False, f"未知市场: {market}"

    cfg = load_yaml(cfg_path) or {}
    cfg.setdefault("categories", {})
    cfg["categories"].setdefault(category_key, {"name": category_key, "stocks": []})
    cat = cfg["categories"][category_key]
    cat.setdefault("stocks", [])

    code = str(stock.get("code", "")).strip()
    if not code:
        return False, "股票代码不能为空"

    # 全市场去重（检查所有 category 下的 code）
    for cat_data in cfg["categories"].values():
        for existing in (cat_data.get("stocks") or []):
            if str(existing.get("code", "")).strip() == code:
                return False, f"股票 {code} 已存在于市场 {market}"

    cat["stocks"].append(stock)
    ok = atomic_save_yaml(cfg_path, cfg)
    return ok, ("添加成功" if ok else "写入失败")


def remove_stock_from_market(market: str, code: str) -> tuple[bool, str]:
    """从市场 YAML 移除指定 code 的股票（所有 category 都搜索）"""
    cfg_path = MARKET_CONFIG_PATHS.get(market)
    if not cfg_path:
        return False, f"未知市场: {market}"

    cfg = load_yaml(cfg_path) or {}
    code_str = str(code).strip()
    removed = False

    for cat_data in (cfg.get("categories") or {}).values():
        stocks = cat_data.get("stocks") or []
        new_stocks = [s for s in stocks if str(s.get("code", "")).strip() != code_str]
        if len(new_stocks) != len(stocks):
            cat_data["stocks"] = new_stocks
            removed = True

    if not removed:
        return False, f"未找到代码 {code}"

    ok = atomic_save_yaml(cfg_path, cfg)
    return ok, ("已删除" if ok else "写入失败")


def update_stock_in_market(market: str, code: str, updates: dict) -> tuple[bool, str]:
    """
    更新市场 YAML 中指定 code 的股票字段（合并更新，不替换整条记录）。

    Args:
        market: 'a' / 'hk' / 'us'
        code: 股票代码
        updates: 要更新的字段 dict，例如 {"name": "xxx", "pe_range": [10, 20, 30]}

    Returns:
        (成功标志, 消息)
    """
    cfg_path = MARKET_CONFIG_PATHS.get(market)
    if not cfg_path:
        return False, f"未知市场: {market}"

    cfg = load_yaml(cfg_path) or {}
    code_str = str(code).strip()
    found = False

    for cat_data in (cfg.get("categories") or {}).values():
        for stock in (cat_data.get("stocks") or []):
            if str(stock.get("code", "")).strip() == code_str:
                stock.update(updates)
                found = True
                break
        if found:
            break

    if not found:
        return False, f"未找到代码 {code}"

    ok = atomic_save_yaml(cfg_path, cfg)
    return ok, ("更新成功" if ok else "写入失败")


def move_stock_category(market: str, code: str, new_category_key: str) -> tuple[bool, str]:
    """将股票从当前分类移动到另一个分类"""
    cfg_path = MARKET_CONFIG_PATHS.get(market)
    if not cfg_path:
        return False, f"未知市场: {market}"

    cfg = load_yaml(cfg_path) or {}
    code_str = str(code).strip()
    cats = cfg.get("categories") or {}

    if new_category_key not in cats:
        return False, f"目标分类 {new_category_key} 不存在"

    # 找到并移除
    stock_entry = None
    for cat_key, cat_data in cats.items():
        stocks = cat_data.get("stocks") or []
        for i, s in enumerate(stocks):
            if str(s.get("code", "")).strip() == code_str:
                stock_entry = stocks.pop(i)
                break
        if stock_entry:
            break

    if not stock_entry:
        return False, f"未找到代码 {code}"

    # 追加到目标分类
    cats[new_category_key].setdefault("stocks", [])
    cats[new_category_key]["stocks"].append(stock_entry)

    ok = atomic_save_yaml(cfg_path, cfg)
    return ok, ("移动成功" if ok else "写入失败")


def add_category_to_market(market: str, category_key: str, category_name: str) -> tuple[bool, str]:
    """新增板块分类"""
    cfg_path = MARKET_CONFIG_PATHS.get(market)
    if not cfg_path:
        return False, f"未知市场: {market}"

    key = str(category_key).strip()
    if not key:
        return False, "分类 key 不能为空"

    cfg = load_yaml(cfg_path) or {}
    cfg.setdefault("categories", {})
    if key in cfg["categories"]:
        return False, f"分类 {key} 已存在"

    cfg["categories"][key] = {"name": category_name or key, "stocks": []}
    ok = atomic_save_yaml(cfg_path, cfg)
    return ok, ("添加成功" if ok else "写入失败")


def remove_category_from_market(market: str, category_key: str) -> tuple[bool, str]:
    """删除板块分类（连同下属所有股票）"""
    cfg_path = MARKET_CONFIG_PATHS.get(market)
    if not cfg_path:
        return False, f"未知市场: {market}"

    cfg = load_yaml(cfg_path) or {}
    cats = cfg.get("categories") or {}
    if category_key not in cats:
        return False, f"分类 {category_key} 不存在"

    del cats[category_key]
    cfg["categories"] = cats
    ok = atomic_save_yaml(cfg_path, cfg)
    return ok, ("已删除" if ok else "写入失败")


def list_market_categories(market: str) -> list[tuple[str, str]]:
    """返回 [(key, name), ...]"""
    cfg_path = MARKET_CONFIG_PATHS.get(market)
    if not cfg_path:
        return []
    cfg = load_yaml(cfg_path) or {}
    return [(k, (v or {}).get("name", k)) for k, v in (cfg.get("categories") or {}).items()]


# ========================
# 技术指标 profile
# ========================

_DEFAULT_INDICATOR_PROFILE = {
    "macd": {"fast": 12, "slow": 26, "signal": 9},
    "rsi": {"period": 14},
    "kdj": {"n": 9, "m1": 3, "m2": 3},
    "bollinger": {"period": 20, "std_dev": 2.0},
    "moving_averages": {"periods": [5, 10, 20, 60, 120, 250]},
}


def list_indicator_profiles() -> list[str]:
    cfg = load_yaml(PATH_INDICATORS) or {}
    return list((cfg.get("profiles") or {}).keys())


def load_indicator_profile(name: str) -> dict:
    """读取指定 profile；不存在则返回默认值副本（不抛错）"""
    cfg = load_yaml(PATH_INDICATORS) or {}
    profiles = cfg.get("profiles") or {}
    if name in profiles and isinstance(profiles[name], dict):
        # 合并默认值，防止某个指标配置缺失
        merged = {k: dict(v) if isinstance(v, dict) else v
                  for k, v in _DEFAULT_INDICATOR_PROFILE.items()}
        for k, v in profiles[name].items():
            if isinstance(v, dict) and isinstance(merged.get(k), dict):
                merged[k].update(v)
            else:
                merged[k] = v
        return merged
    return {k: dict(v) if isinstance(v, dict) else v
            for k, v in _DEFAULT_INDICATOR_PROFILE.items()}


def save_indicator_profile(name: str, params: dict) -> bool:
    cfg = load_yaml(PATH_INDICATORS) or {}
    cfg.setdefault("profiles", {})
    cfg["profiles"][name] = params
    cfg.setdefault("active_profile", name)
    return atomic_save_yaml(PATH_INDICATORS, cfg)


def delete_indicator_profile(name: str) -> bool:
    if name == "default":
        return False  # 默认 profile 不允许删除
    cfg = load_yaml(PATH_INDICATORS) or {}
    if name in (cfg.get("profiles") or {}):
        del cfg["profiles"][name]
        if cfg.get("active_profile") == name:
            cfg["active_profile"] = "default"
        return atomic_save_yaml(PATH_INDICATORS, cfg)
    return False


def set_active_indicator_profile(name: str) -> bool:
    cfg = load_yaml(PATH_INDICATORS) or {}
    cfg.setdefault("profiles", {})
    if name not in cfg["profiles"]:
        return False
    cfg["active_profile"] = name
    return atomic_save_yaml(PATH_INDICATORS, cfg)


def get_active_indicator_profile() -> dict:
    cfg = load_yaml(PATH_INDICATORS) or {}
    name = cfg.get("active_profile", "default")
    return load_indicator_profile(name)


# ========================
# 因子 profile
# ========================

def list_factor_profiles() -> list[str]:
    cfg = load_yaml(PATH_FACTORS) or {}
    return list((cfg.get("profiles") or {}).keys())


def load_factor_profile(name: str) -> list[dict]:
    """返回 [{type, enabled, params?}, ...]"""
    cfg = load_yaml(PATH_FACTORS) or {}
    profiles = cfg.get("profiles") or {}
    prof = profiles.get(name) or {}
    return list(prof.get("factors") or [])


def save_factor_profile(name: str, factors: list[dict]) -> bool:
    cfg = load_yaml(PATH_FACTORS) or {}
    cfg.setdefault("profiles", {})
    cfg["profiles"][name] = {"factors": factors}
    cfg.setdefault("active_profile", name)
    return atomic_save_yaml(PATH_FACTORS, cfg)


def delete_factor_profile(name: str) -> bool:
    if name == "default":
        return False
    cfg = load_yaml(PATH_FACTORS) or {}
    if name in (cfg.get("profiles") or {}):
        del cfg["profiles"][name]
        if cfg.get("active_profile") == name:
            cfg["active_profile"] = "default"
        return atomic_save_yaml(PATH_FACTORS, cfg)
    return False


def set_active_factor_profile(name: str) -> bool:
    cfg = load_yaml(PATH_FACTORS) or {}
    cfg.setdefault("profiles", {})
    if name not in cfg["profiles"]:
        return False
    cfg["active_profile"] = name
    return atomic_save_yaml(PATH_FACTORS, cfg)


def get_active_factor_config() -> list[dict]:
    cfg = load_yaml(PATH_FACTORS) or {}
    name = cfg.get("active_profile", "default")
    return load_factor_profile(name)


# ========================
# 回测预设
# ========================

def list_backtest_presets() -> list[str]:
    cfg = load_yaml(PATH_BACKTEST_PRESETS) or {}
    return list((cfg.get("presets") or {}).keys())


def load_backtest_preset(name: str) -> dict:
    cfg = load_yaml(PATH_BACKTEST_PRESETS) or {}
    return dict((cfg.get("presets") or {}).get(name) or {})


def save_backtest_preset(name: str, preset: dict) -> bool:
    cfg = load_yaml(PATH_BACKTEST_PRESETS) or {}
    cfg.setdefault("presets", {})
    cfg["presets"][name] = preset
    return atomic_save_yaml(PATH_BACKTEST_PRESETS, cfg)


def delete_backtest_preset(name: str) -> bool:
    cfg = load_yaml(PATH_BACKTEST_PRESETS) or {}
    presets = cfg.get("presets") or {}
    if name in presets:
        del presets[name]
        cfg["presets"] = presets
        return atomic_save_yaml(PATH_BACKTEST_PRESETS, cfg)
    return False


# ========================
# 财报关注列表（earnings_monitor.yaml 的 watchlist）
# 结构不同于 a_stock.yaml：
#   watchlist:
#     a: ["600519", "000001"]
#     hk: ["00700"]
#     us: ["AAPL"]
# ========================

def list_earnings_watchlist(market: str) -> list[str]:
    """
    读取财报关注名单；强制 str 化，防御 YAML 把无引号数字解析成 int/float
    （否则后续 `, '.join(...)` 会崩 "expected str instance, float found"）。
    """
    cfg = load_yaml(PATH_EARNINGS) or {}
    raw = (cfg.get("watchlist") or {}).get(market) or []
    return [str(c).strip() for c in raw if c not in (None, "")]


def add_code_to_earnings_watchlist(market: str, code: str) -> tuple[bool, str]:
    """向 earnings_monitor.yaml 的 watchlist[market] 追加代码（去重）"""
    code_s = str(code).strip()
    if not code_s:
        return False, "代码不能为空"
    if market not in ("a", "hk", "us"):
        return False, f"未知市场: {market}"

    cfg = load_yaml(PATH_EARNINGS) or {}
    wl = cfg.setdefault("watchlist", {})
    wl.setdefault(market, [])
    if code_s in wl[market]:
        return False, f"{code_s} 已在关注列表"
    wl[market].append(code_s)
    ok = atomic_save_yaml(PATH_EARNINGS, cfg)
    return ok, ("添加成功" if ok else "写入失败")


def remove_code_from_earnings_watchlist(market: str, code: str) -> tuple[bool, str]:
    code_s = str(code).strip()
    cfg = load_yaml(PATH_EARNINGS) or {}
    wl = cfg.get("watchlist", {}) or {}
    if market not in wl or code_s not in wl[market]:
        return False, f"{code_s} 不在关注列表"
    wl[market].remove(code_s)
    cfg["watchlist"] = wl
    ok = atomic_save_yaml(PATH_EARNINGS, cfg)
    return ok, ("已删除" if ok else "写入失败")


# ========================
# 可复用 UI 组件：紧凑式"加入关注"表单
# 供 1_估值分析 / 2_股票筛选 / 3_价格预警 / 7_策略回测 五页就地嵌入
# ========================

def quick_add_stock_widget(
    key_prefix: str,
    default_market: str = "a",
    default_code: str = "",
    default_name: str = "",
    default_valuation: str = "pe",
    default_range: list | None = None,
    expanded: bool = False,
    label: str = "➕ 加入关注列表",
) -> bool:
    """
    嵌入页面的紧凑式 "加入关注列表" 表单（st.expander 折叠）。

    用法::
        quick_add_stock_widget("page1", default_market=market,
                                default_code=code, default_name=name)

    每个 key_prefix 必须唯一，避免多组件在同页冲突。
    Returns:
        True 表示本次提交触发了写入（调用方可决定是否 st.rerun()）
    """
    import streamlit as st

    default_range = default_range or [10, 20, 30]
    markets = list(MARKET_LABELS.keys())
    try:
        default_market_idx = markets.index(default_market)
    except ValueError:
        default_market_idx = 0

    triggered = False
    with st.expander(label, expanded=expanded):
        st.caption("直接写入 a_stock.yaml / hk_stock.yaml / us_stock.yaml，无需手动编辑配置。")

        col_mkt, col_cat = st.columns([1, 2])
        with col_mkt:
            market = st.selectbox(
                "市场",
                options=markets,
                format_func=lambda k: MARKET_LABELS[k],
                index=default_market_idx,
                key=f"{key_prefix}_qa_market",
            )
        cats = list_market_categories(market)
        with col_cat:
            cat_opt_labels = ["<新建分类>"] + [f"{k} ({n})" for k, n in cats]
            cat_sel = st.selectbox("分类板块", options=cat_opt_labels,
                                   index=min(1, len(cat_opt_labels) - 1),
                                   key=f"{key_prefix}_qa_cat_sel")

        # 新建分类的补充字段
        if cat_sel == "<新建分类>":
            ck_col, cn_col = st.columns([1, 1])
            with ck_col:
                cat_key_input = st.text_input(
                    "新分类 key（英文）", placeholder="tech",
                    key=f"{key_prefix}_qa_new_cat_key",
                )
            with cn_col:
                cat_name_input = st.text_input(
                    "新分类 名称", placeholder="科技板块",
                    key=f"{key_prefix}_qa_new_cat_name",
                )
        else:
            cat_key_input, cat_name_input = None, None

        c1, c2 = st.columns([1, 1])
        with c1:
            code = st.text_input("股票代码", value=default_code,
                                 key=f"{key_prefix}_qa_code")
        with c2:
            name = st.text_input("股票名称", value=default_name,
                                 key=f"{key_prefix}_qa_name")

        c3 = st.columns([1, 1, 1, 1])
        with c3[0]:
            val_type = st.radio(
                "估值方式", options=["pe", "ps"],
                index=0 if default_valuation == "pe" else 1,
                horizontal=True, key=f"{key_prefix}_qa_val",
            )
        with c3[1]:
            r_low = st.number_input("低档位", value=float(default_range[0]),
                                    step=0.5, key=f"{key_prefix}_qa_low")
        with c3[2]:
            r_mid = st.number_input("中档位", value=float(default_range[1]),
                                    step=0.5, key=f"{key_prefix}_qa_mid")
        with c3[3]:
            r_high = st.number_input("高档位", value=float(default_range[2]),
                                     step=0.5, key=f"{key_prefix}_qa_high")

        if st.button("✅ 确认加入", key=f"{key_prefix}_qa_submit", type="primary"):
            code_s = code.strip()
            if not code_s:
                st.error("股票代码不能为空")
                return False

            # 确定 category_key：新建时先创建
            if cat_sel == "<新建分类>":
                ck = (cat_key_input or "").strip()
                cn = (cat_name_input or "").strip() or ck
                if not ck:
                    st.error("新分类 key 不能为空")
                    return False
                ok_cat, msg_cat = add_category_to_market(market, ck, cn)
                # "已存在" 时允许继续往该分类追加
                if not ok_cat and "已存在" not in msg_cat:
                    st.error(f"新建分类失败: {msg_cat}")
                    return False
                cat_key_final = ck
            else:
                # 已选已有分类，去掉 " (xxx)" 取 key
                cat_key_final = cats[cat_opt_labels.index(cat_sel) - 1][0]

            stock_entry = {
                "code": code_s,
                "name": (name or code_s).strip(),
                "valuation": val_type,
                f"{val_type}_range": [float(r_low), float(r_mid), float(r_high)],
            }
            ok, msg = add_stock_to_market(market, cat_key_final, stock_entry)
            if ok:
                st.success(
                    f"✅ 已添加 {stock_entry['name']} ({code_s}) → "
                    f"{MARKET_LABELS[market]} / {cat_key_final}"
                )
                triggered = True
            else:
                st.error(f"添加失败: {msg}")
    return triggered


def quick_add_earnings_widget(
    key_prefix: str,
    default_market: str = "a",
    default_code: str = "",
    expanded: bool = False,
    label: str = "➕ 加入财报关注列表",
) -> bool:
    """
    紧凑式 "加入财报关注" 表单 —— 写入 earnings_monitor.yaml 的 watchlist[market]。

    与 quick_add_stock_widget 分开：财报 watchlist 是简单 list[code] 而不是 categories。
    """
    import streamlit as st

    triggered = False
    markets = ["a", "hk", "us"]
    try:
        default_market_idx = markets.index(default_market)
    except ValueError:
        default_market_idx = 0

    with st.expander(label, expanded=expanded):
        st.caption(f"写入 `{PATH_EARNINGS}` 的 watchlist 段。")
        cols = st.columns([1, 2, 1])
        with cols[0]:
            market = st.selectbox(
                "市场", options=markets,
                format_func=lambda k: MARKET_LABELS.get(k, k),
                index=default_market_idx,
                key=f"{key_prefix}_ea_market",
            )
        with cols[1]:
            code = st.text_input(
                "股票代码", value=default_code,
                placeholder="A股6位/港股5位/美股ticker",
                key=f"{key_prefix}_ea_code",
            )
        with cols[2]:
            st.markdown("&nbsp;")  # 对齐
            if st.button("✅ 添加", key=f"{key_prefix}_ea_submit", type="primary"):
                ok, msg = add_code_to_earnings_watchlist(market, code)
                if ok:
                    st.success(f"{msg}: {code}")
                    triggered = True
                else:
                    st.error(msg)

        # 顺便展示该市场当前关注（供用户确认无重复）
        current = list_earnings_watchlist(market)
        if current:
            st.caption(f"当前 {MARKET_LABELS.get(market, market)} 关注: {', '.join(current)}")
    return triggered


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


def quick_edit_list_widget(
    *,
    path: Path | str,
    dotted_key: str,
    key_prefix: str,
    label: str = "✏️ 管理此列表",
    placeholder: str = "",
    expanded: bool = False,
    help_text: str = "",
) -> bool:
    """
    折叠式的"列表增删"小组件，适用于 scraper.yaml 这类"每段 = 一个字符串列表"配置。

    Args:
        path: YAML 文件路径
        dotted_key: 点号路径（如 "news.keywords" / "announcements.watchlist"）
        key_prefix: Streamlit 组件 key 前缀（必须在当前页面唯一）
        label: expander 标题
        placeholder: 输入框占位文本
        expanded: 是否默认展开
        help_text: expander 顶部说明（可留空）

    Returns:
        True 表示本次发生了写入（调用方决定是否 st.rerun()）
    """
    import streamlit as st

    triggered = False
    current = list_yaml_list(path, dotted_key)

    with st.expander(label, expanded=expanded):
        if help_text:
            st.caption(help_text)

        # 显示当前项（如果较多则做成 caption，避免撑开太多）
        if current:
            st.caption(f"当前 {len(current)} 项: {', '.join(current)}")
        else:
            st.caption("（当前为空）")

        # 添加
        c_add1, c_add2 = st.columns([3, 1])
        with c_add1:
            new_val = st.text_input(
                "新增项",
                value="",
                placeholder=placeholder,
                key=f"{key_prefix}_qel_add",
                label_visibility="collapsed",
            )
        with c_add2:
            if st.button("➕ 添加", key=f"{key_prefix}_qel_add_btn",
                         width='stretch'):
                ok, msg = add_to_yaml_list(path, dotted_key, new_val)
                if ok:
                    st.success(f"{msg}: {new_val}")
                    triggered = True
                else:
                    st.error(msg)

        # 移除
        if current:
            c_rm1, c_rm2 = st.columns([3, 1])
            with c_rm1:
                rm_val = st.selectbox(
                    "删除项",
                    options=current,
                    key=f"{key_prefix}_qel_rm",
                    label_visibility="collapsed",
                )
            with c_rm2:
                if st.button("🗑 删除", key=f"{key_prefix}_qel_rm_btn",
                             width='stretch'):
                    ok, msg = remove_from_yaml_list(path, dotted_key, rm_val)
                    if ok:
                        st.success(f"{msg}: {rm_val}")
                        triggered = True
                    else:
                        st.error(msg)

    return triggered
