"""
src/web/app.py — Streamlit 前端入口（分组导航路由）

启动命令：
    streamlit run src/web/app.py --server.port=8501

架构（Streamlit 1.36+ st.navigation）：
  - 本文件是唯一路由：统一 set_page_config + 注入全局 CSS + 分组侧栏导航
  - pages/ 下的页面文件不再各自 set_page_config（由本路由统一）
  - 首页是仪表盘：关键指标 + 快捷入口，不再是文字墙
"""
from __future__ import annotations

import sys
from pathlib import Path

# sys.path 注入：必须在其他项目模块 import 之前
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st  # noqa: E402

from src.web.theme import inject_global_css  # noqa: E402
from src.web.utils import (  # noqa: E402
    PATH_A_STOCK,
    PATH_HK_STOCK,
    PATH_PRICE_ALERTS,
    PATH_US_STOCK,
    ensure_project_dirs,
    load_yaml,
    setup_matplotlib_chinese,
)

# ── 全局初始化（路由级，只跑一次语义）──
st.set_page_config(
    page_title="量化投研平台",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_global_css()
setup_matplotlib_chinese()
ensure_project_dirs()

# 自动启动后台调度器（幂等：进程级单例）
from src.core.settings import settings as _settings  # noqa: E402

if _settings.scheduler_disabled:
    import logging as _logging
    _logging.getLogger("scheduler_mgr").info("SCHEDULER_DISABLED=1，跳过内嵌调度器")
else:
    try:
        from src.automation.scheduler_manager import start as _start_scheduler
        _start_scheduler()
    except Exception as _e:
        import logging as _logging
        _logging.getLogger("scheduler_mgr").warning(f"调度器自动启动失败（不影响前端）: {_e}")


# ============================================================================
# 首页：仪表盘
# ============================================================================

def _count_stocks(cfg_path: Path) -> int:
    cfg = load_yaml(cfg_path, ttl=300)
    if not cfg:
        return 0
    return sum(
        len(cat.get("stocks") or [])
        for cat in (cfg.get("categories") or {}).values()
        if isinstance(cat, dict)
    )


def _home() -> None:
    st.title("📊 量化投研平台")
    st.caption("A 股 · 港股 · 美股 | 估值 · 筛选 · 回测 · 持仓 · 预警 · AI 周报")

    # ── 快捷搜索条 ──
    cmd_cols = st.columns([1, 4, 1.2, 1])
    with cmd_cols[0]:
        cmd_market = st.selectbox(
            "市场", options=["a", "hk", "us"],
            format_func=lambda k: {"a": "A 股", "hk": "港股", "us": "美股"}[k],
            key="cmd_market", label_visibility="collapsed",
        )
    with cmd_cols[1]:
        cmd_query = st.text_input(
            "搜索", placeholder="🔍 输入代码或名称（如 600519 / 贵州茅台）直达分析",
            key="cmd_query", label_visibility="collapsed",
        )
    with cmd_cols[2]:
        cmd_action = st.selectbox(
            "动作", options=["分析", "回测", "加关注"],
            key="cmd_action", label_visibility="collapsed",
        )
    with cmd_cols[3]:
        cmd_go = st.button("🚀 直达", type="primary", use_container_width=True,
                           disabled=not cmd_query.strip())

    if cmd_go and cmd_query.strip():
        q = cmd_query.strip()
        resolved_code, resolved_name = q, ""
        try:
            from src.web.utils import list_stocks_from_market_config
            stocks_list = list_stocks_from_market_config(cmd_market) or []
            match = next((s for s in stocks_list if str(s.get("code", "")) == q), None) \
                or next((s for s in stocks_list if q in str(s.get("name", ""))), None)
            if match:
                resolved_code = str(match.get("code", q))
                resolved_name = str(match.get("name", ""))
        except Exception:
            pass
        st.session_state["focus_stock"] = {
            "code": resolved_code, "name": resolved_name, "market": cmd_market,
        }
        target = {"分析": "pages/1_估值分析.py", "回测": "pages/4_策略回测.py",
                  "加关注": "pages/12_关注标的.py"}[cmd_action]
        st.switch_page(target)

    _focus = st.session_state.get("focus_stock")
    if _focus and _focus.get("code"):
        name_str = f"{_focus.get('name', '')} ({_focus['code']})" if _focus.get("name") else _focus["code"]
        st.caption(f"🎯 当前焦点：**{name_str}**（其他页会沿用）")

    st.markdown("")

    # ── 关键指标行 ──
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("🇨🇳 A 股关注", _count_stocks(PATH_A_STOCK))
    m2.metric("🇭🇰 港股关注", _count_stocks(PATH_HK_STOCK))
    m3.metric("🇺🇸 美股关注", _count_stocks(PATH_US_STOCK))
    _rules = (load_yaml(PATH_PRICE_ALERTS, ttl=300) or {})
    _n_rules = len(_rules.get("buy_alerts") or []) + len(_rules.get("sell_alerts") or []) \
        or len(_rules.get("rules") or [])
    m4.metric("🔔 预警规则", _n_rules)
    _holdings = (load_yaml(_ROOT / "config" / "holdings.yaml", ttl=300) or {})
    m5.metric("💼 持仓数", len(_holdings.get("holdings") or []))

    st.markdown("")

    # ── 快捷入口卡片 ──
    def _card(col, page_path: str, icon: str, title: str, desc: str) -> None:
        with col:
            st.markdown(
                f'<div class="nav-card"><div class="nav-title">{icon} {title}</div>'
                f'<div class="nav-desc">{desc}</div></div>',
                unsafe_allow_html=True,
            )
            st.page_link(page_path, label=f"进入 {title} →", use_container_width=True)

    row1 = st.columns(3)
    _card(row1[0], "pages/1_估值分析.py", "🎯", "个股中心",
          "一次选股：估值 · 基本面 · 回测 · 财报 · 资讯 · 买卖信号 · 预警")
    _card(row1[1], "pages/23_策略中心.py", "🧠", "策略中心",
          "8 条基本面+技术双层策略：管理 · 筛选 · 共享一页完成")
    _card(row1[2], "pages/4_策略回测.py", "🧪", "策略回测",
          "多策略一键对比 KPI / 收益曲线 / 买卖点标注")

    row2 = st.columns(3)
    _card(row2[0], "pages/16_持仓监控.py", "💼", "持仓监控",
          "L1 止损/止盈 + L2 信号 + L4 大盘守门员综合判定")
    _card(row2[1], "pages/20_持仓体检.py", "🩺", "持仓体检",
          "四维健康分：信号 / 集中度 / 盈亏结构 / 大盘环境")
    _card(row2[2], "pages/21_AI周报.py", "📰", "AI 周报",
          "组合诊断 + 筛选结果 → Claude 深度解读 → 一键导出")


# ============================================================================
# 分组导航
# ============================================================================

_P = "pages/"

nav = st.navigation({
    "": [
        st.Page(_home, title="总览", icon="🏠", default=True),
    ],
    "分析": [
        st.Page(_P + "1_估值分析.py", title="个股中心", icon="🎯"),
        st.Page(_P + "23_策略中心.py", title="策略中心", icon="🧠"),
        st.Page(_P + "2_策略配置.py", title="策略编辑器", icon="⚙️"),
        st.Page(_P + "13_因子库.py", title="因子库", icon="🧬"),
        st.Page(_P + "18_荐股逆向.py", title="荐股逆向", icon="🕵️"),
    ],
    "回测与模型": [
        st.Page(_P + "4_策略回测.py", title="策略回测", icon="🧪"),
        st.Page(_P + "17_ML训练.py", title="ML 训练", icon="🤖"),
    ],
    "持仓与交易": [
        st.Page(_P + "16_持仓监控.py", title="持仓监控", icon="💼"),
        st.Page(_P + "20_持仓体检.py", title="持仓体检", icon="🩺"),
        st.Page(_P + "15_卖点扫描.py", title="卖点扫描", icon="📉"),
        st.Page(_P + "22_交易台.py", title="交易台", icon="💹"),
        st.Page(_P + "21_AI周报.py", title="AI 周报", icon="📰"),
    ],
    "监控与资讯": [
        st.Page(_P + "5_价格预警.py", title="价格预警", icon="🔔"),
        st.Page(_P + "6_财报披露.py", title="财报披露", icon="📅"),
        st.Page(_P + "8_告警历史.py", title="告警历史", icon="🕘"),
        st.Page(_P + "7_资讯抓取.py", title="资讯抓取", icon="🌐"),
    ],
    "系统": [
        st.Page(_P + "12_关注标的.py", title="关注标的", icon="⭐"),
        st.Page(_P + "9_指标参数.py", title="指标参数", icon="🎛️"),
        st.Page(_P + "10_调度管理.py", title="调度管理", icon="⏰"),
    ],
}, expanded=True)

nav.run()
