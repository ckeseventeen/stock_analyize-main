"""
src/web/app.py — Streamlit 前端入口

启动命令：
    streamlit run src/web/app.py --server.port=8501

pages/ 目录下的文件会自动出现在侧边栏（按文件名前缀数字排序）。
本文件作为首页，展示系统概览与快速入口。
"""
from __future__ import annotations

import sys
from pathlib import Path

# sys.path 注入：必须在其他项目模块 import 之前
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st  # noqa: E402

from src.web.utils import (  # noqa: E402
    ALERT_STATE_PATH,
    CONFIG_DIR,
    OUTPUT_DIR,
    PATH_A_STOCK,
    PATH_HK_STOCK,
    PATH_PRICE_ALERTS,
    PATH_US_STOCK,
    ensure_project_dirs,
    load_yaml,
    setup_matplotlib_chinese,
)

# 初始化 matplotlib 中文渲染（全局一次即可）
setup_matplotlib_chinese()

# 确保 cache/logs/output 目录存在
ensure_project_dirs()

# 自动启动后台调度器（幂等：进程级单例，Streamlit rerun 不会重复启动）
# B24/SEC6 修复：settings.scheduler_disabled 为 True 时跳过，
# 避免与独立 `python -m src.automation.scheduler` 进程双启动（典型场景：docker-compose 部署）
from src.core.settings import settings as _settings  # noqa: E402

if _settings.scheduler_disabled:
    import logging as _logging
    _logging.getLogger("scheduler_mgr").info("SCHEDULER_DISABLED=1，跳过 Streamlit 内嵌调度器自动启动")
else:
    try:
        from src.automation.scheduler_manager import start as _start_scheduler
        _start_scheduler()
    except Exception as _e:
        import logging as _logging
        _logging.getLogger("scheduler_mgr").warning(f"调度器自动启动失败（不影响前端使用）: {_e}")

# Streamlit 页面全局设置
st.set_page_config(
    page_title="股票估值分析平台",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ========================
# 首页：系统概览
# ========================

st.title("📊 多市场股票估值分析平台")
st.caption("A 股 · 港股 · 美股  |  估值分析 · 筛选 · 预警 · 财报监控 · 资讯抓取")

# ────────────────────────────────────────────────────────
# 🔍 全局命令栏 - 输入股票代码/名称跳转到分析页
# ────────────────────────────────────────────────────────

with st.container():
    cmd_cols = st.columns([1, 4, 1, 2])
    with cmd_cols[0]:
        cmd_market = st.selectbox(
            "市场", options=["a", "hk", "us"],
            format_func=lambda k: {"a": "A 股", "hk": "港股", "us": "美股"}[k],
            key="cmd_market",
            label_visibility="collapsed",
        )
    with cmd_cols[1]:
        cmd_query = st.text_input(
            "搜索",
            placeholder="🔍 输入股票代码或名称（如 600519 / 贵州茅台）后按 Enter 直达分析页",
            key="cmd_query",
            label_visibility="collapsed",
        )
    with cmd_cols[2]:
        cmd_action = st.selectbox(
            "动作", options=["分析", "回测", "加关注"],
            key="cmd_action",
            label_visibility="collapsed",
        )
    with cmd_cols[3]:
        cmd_go = st.button(
            "🚀 跳转", type="primary", width="stretch",
            disabled=not cmd_query.strip(),
        )

    if cmd_go and cmd_query.strip():
        # 解析输入：纯数字 = 代码；含中文 = 名称（去关注列表找）
        q = cmd_query.strip()
        resolved_code = q
        resolved_name = ""

        # 名称 → 代码 反查（从关注列表）
        try:
            from src.web.utils import list_stocks_from_market_config
            stocks_list = list_stocks_from_market_config(cmd_market) or []
            # 优先精确匹配代码
            match = next((s for s in stocks_list if str(s.get("code", "")) == q), None)
            if not match:
                # 再按名称模糊匹配
                match = next((s for s in stocks_list if q in str(s.get("name", ""))), None)
            if match:
                resolved_code = str(match.get("code", q))
                resolved_name = str(match.get("name", ""))
        except Exception:
            pass

        # 写入全局焦点
        st.session_state["focus_stock"] = {
            "code": resolved_code,
            "name": resolved_name,
            "market": cmd_market,
        }

        # 跳转
        target_page = {
            "分析": "pages/1_估值分析.py",
            "回测": "pages/4_策略回测.py",
            "加关注": "pages/12_关注标的.py",
        }[cmd_action]
        try:
            st.switch_page(target_page)
        except Exception:
            st.toast(f"已设焦点 {resolved_code}，请手动点击侧栏 {target_page}", icon="🎯")

# 显示当前焦点
_current_focus = st.session_state.get("focus_stock")
if _current_focus and _current_focus.get("code"):
    fc = _current_focus
    name_str = f"{fc.get('name', '')} ({fc['code']})" if fc.get("name") else fc["code"]
    st.caption(
        f"🎯 **当前焦点**：{name_str} · 市场 "
        f"{ {'a': 'A 股', 'hk': '港股', 'us': '美股'}.get(fc.get('market', 'a'), '?') } "
        f"（其他页会沿用此焦点）"
    )

st.markdown("---")

# --- 快速指标卡：统计当前配置中的股票数 ---
col1, col2, col3, col4 = st.columns(4)


def _count_stocks(cfg_path: Path) -> int:
    """统计某市场配置中的股票总数（累加所有 category.stocks）"""
    cfg = load_yaml(cfg_path, ttl=300)
    if not cfg:
        return 0
    total = 0
    for cat in (cfg.get("categories") or {}).values():
        if cat and isinstance(cat, dict):
            total += len(cat.get("stocks") or [])
    return total


with col1:
    st.metric("A 股关注股票", _count_stocks(PATH_A_STOCK))
with col2:
    st.metric("港股关注股票", _count_stocks(PATH_HK_STOCK))
with col3:
    st.metric("美股关注股票", _count_stocks(PATH_US_STOCK))
with col4:
    # 价格预警规则条数
    rules = (load_yaml(PATH_PRICE_ALERTS, ttl=300) or {}).get("rules", []) or []
    st.metric("价格预警规则", len(rules))


st.markdown("---")

# --- 功能导航 ---
st.subheader("🗂 功能导航")

left, right = st.columns(2)

with left:
    st.markdown(
        """
        ### 📈 分析与筛选
        - **估值分析**：对单只股票跑 4 格估值图（营收利润、历史分位、目标价、汇总）
        - **策略配置**：可视化编辑筛选策略，支持 20+ 条件类型、回测一体化配置
        - **股票筛选**：执行策略批量筛选 A 股，支持多策略组合
        - **策略回测**：基于 Backtrader 的量化策略历史验证
        """
    )
    st.markdown(
        """
        ### 🔔 预警与监控
        - **价格预警**：管理价格阈值/涨跌幅/均线突破等规则，推送至手机
        - **财报披露**：跟踪 A/港/美三市场未来 30 天披露日历与业绩预告
        """
    )

with right:
    st.markdown(
        """
        ### 🌐 资讯抓取
        - **资讯抓取**：财经新闻、公司公告、股东持仓、研报评级 4 类数据批量拉取
        """
    )
    st.markdown(
        """
        ### 📜 系统管理
        - **关注标的**：多市场关注列表增删改查、板块分类、搜索筛选
        - **告警历史**：查看历史告警事件、冷却状态、推送日志
        - **配置管理**：管理关注列表、指标参数、回测预设等全局配置
        - **调度管理**：定时任务调度与监控
        """
    )

st.info("👈 使用左侧 **侧边栏导航** 切换到具体功能页面。")


# ========================
# 系统信息
# ========================

st.markdown("---")
st.subheader("⚙️ 系统信息")

info_col1, info_col2 = st.columns(2)

with info_col1:
    st.markdown("**配置目录**")
    st.code(str(CONFIG_DIR), language="text")

    st.markdown("**告警状态文件**")
    if ALERT_STATE_PATH.exists():
        @st.cache_data(ttl=60, show_spinner=False)
        def _count_alert_records(p: str) -> int:
            import json
            try:
                with open(p, encoding="utf-8") as f:
                    state = json.load(f)
                return len(state)
            except Exception:
                return -1
        n = _count_alert_records(str(ALERT_STATE_PATH))
        st.caption(f"当前 {n} 条记录" if n >= 0 else "(读取失败)")
    else:
        st.caption("暂无记录")

with info_col2:
    st.markdown("**输出目录**")
    st.code(str(OUTPUT_DIR), language="text")

    # 展示输出目录下的文件数（缓存 60 秒，避免每次 rerun 递归扫描）
    if OUTPUT_DIR.exists():
        @st.cache_data(ttl=60, show_spinner=False)
        def _count_output_files(d: str) -> int:
            return sum(1 for f in Path(d).rglob("*") if f.is_file())
        st.caption(f"共 {_count_output_files(str(OUTPUT_DIR))} 个文件")

# 页脚
st.markdown("---")
st.caption("💡 数据源：akshare / pytdx / yfinance。推送通道：Server酱 / Bark / PushPlus / Console。")
