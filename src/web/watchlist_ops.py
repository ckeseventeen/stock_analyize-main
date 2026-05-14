"""
src/web/watchlist_ops.py — 关注标的 CRUD 操作

从 utils.py 拆出，封装 watchlist（关注列表）的增删改查、分类管理、
技术指标 profile、因子 profile、回测预设、财报关注等数据操作。
"""
from __future__ import annotations

from src.web.config_ops import (
    MARKET_CONFIG_PATHS,
    PATH_BACKTEST_PRESETS,
    PATH_EARNINGS,
    PATH_FACTORS,
    PATH_INDICATORS,
    atomic_save_yaml,
    load_yaml,
)

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
    for _cat_key, cat_data in cats.items():
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
    （否则后续 ', '.join(...) 会崩 "expected str instance, float found"）。
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
