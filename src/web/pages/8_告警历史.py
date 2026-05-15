"""
pages/8_告警历史.py — 告警历史查看（PR 4/5 重构示范）

PR 4/5 重构示范：
  - 原 132 行 → 现 65 行（砍 51%）
  - 使用 Page 框架 + stats_row widget
  - 业务逻辑（读 json / 解析 key）下沉到 Page 类方法，与 UI 解耦
"""
from __future__ import annotations

import json
from datetime import datetime

from src.web.framework import Page, inject_project_path

inject_project_path()

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.web.utils import ALERT_LOG_PATH, ALERT_STATE_PATH, df_to_csv_bytes  # noqa: E402
from src.web.widgets import stats_row  # noqa: E402


class AlertHistoryPage(Page):
    title = "告警历史"
    icon = "📜"
    caption = f"状态文件：`{ALERT_STATE_PATH}` | 日志文件：`{ALERT_LOG_PATH}`"

    def render(self) -> None:
        self._render_events_section()
        st.markdown("---")
        self._render_log_section()

    # ---------------- 已触发事件 ----------------

    def _render_events_section(self) -> None:
        st.subheader("🗂 已触发事件 (alert_state.json)")
        df = self._load_events_df()
        if df is None or df.empty:
            st.info("尚无告警记录")
            return

        # KPI 卡片：总数 / 今日 / 7 天
        today = datetime.now().date().isoformat()
        stats_row([
            ("总事件数", len(df), None),
            ("今日新增", int((df["日期"] == today).sum()), None),
            ("近 7 天", int(df["日期"].apply(lambda d: d >= str(today)[:7]).sum()), None),
        ])

        # 过滤表单
        with st.form("alert_filter_form"):
            c1, c2 = st.columns(2)
            with c1:
                code_filter = st.text_input("按股票代码过滤", value="")
            with c2:
                rule_filter = st.text_input("按规则类型过滤", value="")
            st.form_submit_button("应用过滤")

        view = df.copy()
        if code_filter.strip():
            view = view[view["股票代码"].str.contains(code_filter.strip(), na=False)]
        if rule_filter.strip():
            view = view[view["规则类型"].str.contains(rule_filter.strip(), na=False)]

        st.dataframe(view, width="stretch", hide_index=True)

        col_dl, col_clean = st.columns(2)
        with col_dl:
            st.download_button(
                "💾 下载全部记录 CSV",
                data=df_to_csv_bytes(df),
                file_name=f"alert_state_{datetime.now():%Y%m%d_%H%M%S}.csv",
                mime="text/csv",
            )
        with col_clean:
            days = st.number_input("清理超过 N 天的记录", min_value=1, max_value=365, value=30)
            if st.button("🧹 执行清理"):
                from src.automation.alert.state import AlertStateStore
                removed = AlertStateStore(ALERT_STATE_PATH).clear_expired(int(days))
                st.success(f"已清理 {removed} 条过期记录")
                st.rerun()

    @staticmethod
    def _load_events_df() -> pd.DataFrame | None:
        """读 alert_state.json → DataFrame；不存在或损坏返回 None / 空 df"""
        if not ALERT_STATE_PATH.exists():
            return None
        try:
            with open(ALERT_STATE_PATH, encoding="utf-8") as f:
                state = json.load(f)
        except Exception as e:
            st.error(f"读取状态文件失败: {e}")
            return pd.DataFrame()
        if not state:
            return pd.DataFrame()
        rows = []
        for key, rec in state.items():
            parts = key.split(":")
            rows.append({
                "event_key": key,
                "股票代码": parts[0] if parts else "",
                "规则类型": parts[1] if len(parts) > 1 else "",
                "日期": parts[2] if len(parts) > 2 else "",
                "fired_at": rec.get("fired_at", ""),
            })
        return pd.DataFrame(rows).sort_values("fired_at", ascending=False)

    # ---------------- 原始日志 ----------------

    def _render_log_section(self) -> None:
        st.subheader("📃 Console 通道原始日志")
        if not ALERT_LOG_PATH.exists():
            st.info("尚无 alerts.log 文件")
            return
        tail_n = st.slider("显示最新 N 行", min_value=20, max_value=500, value=100, step=20)
        try:
            with open(ALERT_LOG_PATH, encoding="utf-8") as f:
                lines = f.readlines()
            tail = lines[-tail_n:] if len(lines) > tail_n else lines
            st.code("".join(tail), language="text")
        except Exception as e:
            st.error(f"读取日志失败: {e}")


AlertHistoryPage.run()
