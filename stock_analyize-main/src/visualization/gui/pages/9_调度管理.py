"""
pages/9_调度管理.py — 后台调度器管理面板

显示调度器运行状态、Job 列表、执行历史，并提供启停与手动触发控制。
"""
from __future__ import annotations

import streamlit as st

from src.automation.scheduler_manager import (
    get_job_history,
    get_status,
    is_running,
    pause_job,
    resume_job,
    start,
    stop,
    trigger_job,
)

st.set_page_config(page_title="调度管理", page_icon="⏰", layout="wide")

st.title("⏰ 后台调度管理")
st.caption("APScheduler 后台守护线程 · 自动周期执行监控与抓取任务")

# ========================
# 状态概览卡
# ========================

status = get_status()
running = status["running"]

col1, col2, col3 = st.columns(3)
with col1:
    if running:
        st.success("🟢 调度器运行中", icon="✅")
    else:
        st.error("🔴 调度器已停止", icon="⛔")
with col2:
    st.metric("注册 Job 数", status["job_count"])
with col3:
    st.metric("累计执行次数", status["total_executions"])

st.markdown("---")

# ========================
# 控制按钮
# ========================

ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([1, 1, 4])

with ctrl_col1:
    if running:
        if st.button("⏹ 停止调度器", type="secondary", width='stretch'):
            stop()
            st.rerun()
    else:
        if st.button("▶️ 启动调度器", type="primary", width='stretch'):
            ok = start()
            if ok:
                st.toast("✅ 调度器已启动", icon="🚀")
            else:
                st.toast("❌ 启动失败，请检查日志", icon="⚠️")
            st.rerun()

with ctrl_col2:
    if st.button("🔄 刷新状态", width='stretch'):
        st.rerun()


# ========================
# Job 列表
# ========================

st.subheader("📋 已注册任务")

if status["jobs"]:
    # 展示 Job 表格
    for job in status["jobs"]:
        with st.container():
            jcol1, jcol2, jcol3, jcol4 = st.columns([2, 3, 3, 2])
            with jcol1:
                paused = job["next_run"] == "暂停中"
                icon = "⏸️" if paused else "✅"
                st.markdown(f"**{icon} {job['id']}**")
            with jcol2:
                st.caption(f"触发器: `{job['trigger']}`")
            with jcol3:
                st.caption(f"下次执行: {job['next_run']}")
            with jcol4:
                btn_col_a, btn_col_b = st.columns(2)
                with btn_col_a:
                    if st.button("▶ 触发", key=f"trigger_{job['id']}", width='stretch'):
                        if trigger_job(job["id"]):
                            st.toast(f"✅ 已触发: {job['id']}", icon="🚀")
                        else:
                            st.toast(f"❌ 触发失败: {job['id']}", icon="⚠️")
                        st.rerun()
                with btn_col_b:
                    paused = job["next_run"] == "暂停中"
                    if paused:
                        if st.button("▶ 恢复", key=f"resume_{job['id']}", width='stretch'):
                            resume_job(job["id"])
                            st.rerun()
                    else:
                        if st.button("⏸ 暂停", key=f"pause_{job['id']}", width='stretch'):
                            pause_job(job["id"])
                            st.rerun()
            st.divider()
else:
    if running:
        st.info("调度器运行中但无已注册 Job。请检查 `config/scheduler.yaml`。")
    else:
        st.warning("调度器未启动，点击上方「启动调度器」按钮。")


# ========================
# 执行历史
# ========================

st.subheader("📜 最近执行记录")

history = get_job_history(limit=50)

if history:
    import pandas as pd

    df = pd.DataFrame(history)
    # 状态列上色
    def _color_status(val):
        if val == "success":
            return "color: #22c55e"
        if val == "error":
            return "color: #ef4444"
        return ""

    col_config = {
        "job_id": st.column_config.TextColumn("Job ID", width="medium"),
        "run_time": st.column_config.TextColumn("执行时间", width="medium"),
        "status": st.column_config.TextColumn("状态", width="small"),
        "duration": st.column_config.TextColumn("耗时", width="small"),
        "error": st.column_config.TextColumn("错误信息", width="large"),
    }

    st.dataframe(
        df.style.applymap(_color_status, subset=["status"]),
        column_config=col_config,
        width='stretch',
        hide_index=True,
        height=min(len(df) * 40 + 50, 600),
    )
else:
    st.info("暂无执行记录。调度器启动后，Job 执行结果会自动记录在此。")


# ========================
# 配置预览
# ========================

st.subheader("⚙️ 调度配置")

with st.expander("查看 scheduler.yaml", expanded=False):
    config_path = "./config/scheduler.yaml"
    try:
        with open(config_path, encoding="utf-8") as f:
            st.code(f.read(), language="yaml")
    except FileNotFoundError:
        st.warning(f"配置文件不存在: {config_path}")

st.markdown("---")
st.caption("💡 修改 `config/scheduler.yaml` 后，需要重启调度器使配置生效。")
