"""
pages/6_告警历史.py — 告警历史查看

功能：
  - 读取 cache/alert_state.json → 展示已触发事件 + cooldown 状态
  - 读取 logs/alerts.log → 展示 console 通道原始日志（尾部 N 行）
  - 支持按股票代码/日期过滤
  - 清理过期记录按钮
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.web.utils import (  # noqa: E402
    ALERT_LOG_PATH,
    ALERT_STATE_PATH,
    df_to_csv_bytes,
)

st.set_page_config(page_title="告警历史", page_icon="📜", layout="wide")
st.title("📜 告警历史")
st.caption(f"状态文件：`{ALERT_STATE_PATH}`  |  日志文件：`{ALERT_LOG_PATH}`")


# ========================
# 已触发事件表
# ========================

st.subheader("🗂 已触发事件（alert_state.json）")

if not ALERT_STATE_PATH.exists():
    st.info("尚无告警状态文件，说明从未推送过告警")
else:
    try:
        with open(ALERT_STATE_PATH, encoding="utf-8") as f:
            state = json.load(f)
    except Exception as e:
        st.error(f"读取状态文件失败: {e}")
        state = {}

    if not state:
        st.info("状态文件为空")
    else:
        # 转 DataFrame
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
        df_state = pd.DataFrame(rows).sort_values("fired_at", ascending=False)

        # 过滤
        colf1, colf2 = st.columns(2)
        with colf1:
            code_filter = st.text_input("🔍 按股票代码过滤（留空=全部）", value="")
        with colf2:
            rule_filter = st.text_input("🔍 按规则类型过滤（留空=全部）", value="")

        df_show = df_state
        if code_filter.strip():
            df_show = df_show[df_show["股票代码"].str.contains(code_filter.strip(), na=False)]
        if rule_filter.strip():
            df_show = df_show[df_show["规则类型"].str.contains(rule_filter.strip(), na=False)]

        st.dataframe(df_show, width='stretch', hide_index=True)

        col_dl, col_clean = st.columns(2)
        with col_dl:
            st.download_button(
                label="💾 下载全部记录 CSV",
                data=df_to_csv_bytes(df_state),
                file_name=f"alert_state_{datetime.now():%Y%m%d_%H%M%S}.csv",
                mime="text/csv",
            )
        with col_clean:
            retention_days = st.number_input(
                "清理超过 N 天的记录", min_value=1, max_value=365, value=30, step=1,
            )
            if st.button("🧹 执行清理", type="secondary"):
                from src.automation.alert.state import AlertStateStore
                store = AlertStateStore(ALERT_STATE_PATH)
                removed = store.clear_expired(int(retention_days))
                st.success(f"已清理 {removed} 条过期记录")
                st.rerun()


# ========================
# 原始日志尾部
# ========================

st.markdown("---")
st.subheader("📃 Console 通道原始日志")

if not ALERT_LOG_PATH.exists():
    st.info("尚无 alerts.log 文件")
else:
    tail_n = st.slider("显示最新 N 行", min_value=20, max_value=500, value=100, step=20)
    try:
        with open(ALERT_LOG_PATH, encoding="utf-8") as f:
            lines = f.readlines()
        tail = lines[-tail_n:] if len(lines) > tail_n else lines
        st.code("".join(tail), language="text")
    except Exception as e:
        st.error(f"读取日志失败: {e}")


st.markdown("---")
st.caption("💡 状态文件用于 cooldown 去重；日志文件记录所有 console 通道输出。")
