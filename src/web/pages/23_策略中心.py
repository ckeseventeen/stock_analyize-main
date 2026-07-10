"""
pages/23_策略中心.py — 策略一站式管理（合并原 股票筛选 / 回测预设 / 策略市场）

三个 Tab：
  📋 我的策略：列表 + 重命名 / 复制 / 删除 / 发布共享（深度编辑 → 策略编辑器）
  🔍 执行筛选：选策略 + 板块范围 → 全市场扫描
  🌐 共享市场：浏览 / 导入 / 上传策略包
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.services import screening_service as svc  # noqa: E402
from src.services import strategy_market as market  # noqa: E402
from src.web.utils import df_to_csv_bytes  # noqa: E402

st.title("🧠 策略中心")
st.caption("管理 · 筛选 · 共享，一页完成 | 深度编辑条件请用「⚙️ 策略编辑器」")

tab_manage, tab_run, tab_share = st.tabs(["📋 我的策略", "🔍 执行筛选", "🌐 共享市场"])


# ============================================================================
# Tab 1：我的策略（管理）
# ============================================================================
with tab_manage:
    strategies = svc.load_all_strategies()
    if not strategies:
        st.info("暂无策略。到「⚙️ 策略编辑器」创建，或去「🌐 共享市场」导入。")
    else:
        # 概览表
        rows = []
        for sid, body in strategies.items():
            conds = body.get("conditions", [])
            fundamental = sum(1 for c in conds if svc.is_spot_only(c.get("type", "")))
            rows.append({
                "ID": sid,
                "名称": body.get("name", sid),
                "基本面条件": fundamental,
                "技术条件": len(conds) - fundamental,
                "可回测": "✅" if body.get("backtest") else "—",
            })
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

        st.markdown("---")
        mc1, mc2 = st.columns([1, 2])
        with mc1:
            sel_sid = st.selectbox(
                "选择策略", options=list(strategies.keys()),
                format_func=lambda s: f"{strategies[s].get('name', s)} ({s})",
                key="mgr_sel",
            )
        with mc2:
            st.markdown("&nbsp;")
            try:
                st.page_link("pages/2_策略配置.py",
                             label="✏️ 去策略编辑器深度编辑该策略 →")
            except Exception:
                # 单页独立运行（无导航注册）时降级为文字提示
                st.caption("✏️ 深度编辑条件请到「策略编辑器」页")

        op1, op2, op3, op4 = st.columns(4)

        with op1:
            with st.popover("✏️ 重命名", use_container_width=True):
                new_id = st.text_input("新 ID（英文）", value=sel_sid, key="mgr_new_id")
                new_name = st.text_input(
                    "新显示名", value=strategies[sel_sid].get("name", ""),
                    key="mgr_new_name")
                if st.button("确认重命名", key="mgr_rename_ok", type="primary"):
                    body = svc.get_strategy(sel_sid)
                    body["name"] = new_name.strip() or body.get("name", "")
                    ok, msg = svc.upsert_strategy(sel_sid, body)
                    if ok and new_id.strip() != sel_sid:
                        ok, msg = svc.rename_strategy(sel_sid, new_id.strip())
                    (st.success if ok else st.error)(msg)
                    if ok:
                        st.rerun()

        with op2:
            if st.button("📋 复制", use_container_width=True, key="mgr_dup"):
                mem = dict(strategies)
                new_sid = svc.duplicate_strategy_in_memory(mem, sel_sid)
                ok, msg = svc.upsert_strategy(new_sid, mem[new_sid])
                (st.success if ok else st.error)(f"已复制为 {new_sid}" if ok else msg)
                if ok:
                    st.rerun()

        with op3:
            with st.popover("🚀 发布共享", use_container_width=True):
                pub_author = st.text_input("作者", key="mgr_pub_author")
                pub_ver = st.text_input("版本", value="1.0.0", key="mgr_pub_ver")
                pub_desc = st.text_area("说明", key="mgr_pub_desc", height=68)
                if st.button("发布到本地市场", key="mgr_pub_ok", type="primary"):
                    ok, msg, path = market.publish_strategy(
                        sel_sid, author=pub_author, version=pub_ver,
                        description=pub_desc)
                    (st.success if ok else st.error)(msg)
                pkg = market.build_package(
                    sel_sid, strategies.get(sel_sid, {}),
                    author=pub_author, version=pub_ver, description=pub_desc)
                st.download_button(
                    "💾 下载策略包分享", key="mgr_pub_dl",
                    data=json.dumps(pkg, ensure_ascii=False, indent=2),
                    file_name=f"{sel_sid}@{pub_ver}.json", mime="application/json")

        with op4:
            with st.popover("🗑️ 删除", use_container_width=True):
                st.warning(f"删除策略 **{sel_sid}**？不可撤销。")
                if st.button("确认删除", key="mgr_del_ok", type="primary"):
                    ok, msg = svc.delete_strategy(sel_sid)
                    (st.success if ok else st.error)(msg)
                    if ok:
                        st.rerun()


# ============================================================================
# Tab 2：执行筛选（原 股票筛选 页的精简版）
# ============================================================================
with tab_run:
    names = svc.list_strategy_names()
    if not names:
        st.info("暂无策略可执行。")
    else:
        rc1, rc2 = st.columns([2, 2])
        with rc1:
            run_ids = st.multiselect(
                "策略（多选 = 条件 AND 组合）",
                options=list(names.keys()),
                default=[next(iter(names))],
                format_func=lambda s: names[s],
                key="runner_ids",
            )
        with rc2:
            scope_opts = svc.scope_options()
            scope_labels = st.multiselect(
                "板块/指数范围", options=list(scope_opts.keys()),
                default=["全部A股"], key="runner_scope",
            )
        with st.expander("⚙️ 高级参数"):
            max_workers = st.slider("并行 worker", 1, 16, 8, key="runner_workers")
            clear_cache = st.checkbox("忽略 K 线缓存", key="runner_nocache")

        if st.button("▶️ 开始筛选", type="primary", key="runner_go",
                     disabled=not run_ids):
            with st.spinner("全市场扫描中（视条件 1-5 分钟）..."):
                try:
                    run = svc.run_screening(
                        run_ids,
                        scope_keys=[scope_opts[label] for label in scope_labels],
                        scope_labels=scope_labels,
                        max_workers=int(max_workers),
                        clear_cache=clear_cache,
                    )
                    st.session_state["center_run"] = run
                    st.session_state["_screen_last_results"] = run.df
                except Exception as e:
                    st.error(f"筛选失败: {e}")
                    st.exception(e)

        run = st.session_state.get("center_run")
        if run is not None:
            for w in run.warnings:
                st.warning(w)
            st.caption(
                f"⏱ {run.elapsed_seconds:.1f}s · 策略 {','.join(run.strategy_ids)}"
                + (f" · 范围 {run.scope_size} 只" if run.scope_size else " · 全市场"))
            if run.df.empty:
                st.warning("无符合条件的股票（条件可能太严）")
            else:
                st.success(f"命中 {len(run.df)} 只")
                st.dataframe(run.df, width="stretch", hide_index=True)
                st.download_button(
                    "💾 下载 CSV", data=df_to_csv_bytes(run.df),
                    file_name=f"screen_{pd.Timestamp.now():%Y%m%d_%H%M}.csv",
                    mime="text/csv", key="runner_dl")


# ============================================================================
# Tab 3：共享市场（原 策略市场 页的精简版）
# ============================================================================
with tab_share:
    up = st.file_uploader("导入他人分享的策略包 (.json)", type=["json"],
                          key="share_upload")
    if up is not None:
        pkg, errors = market.load_package_text(up.read().decode("utf-8"))
        if errors:
            st.error("包校验失败：" + "；".join(errors))
        else:
            st.success(f"✅ {pkg.get('name')} v{pkg.get('version')} "
                       f"by {pkg.get('author') or '匿名'}")
            uc1, uc2 = st.columns([2, 1])
            with uc1:
                up_sid = st.text_input("导入为 ID", value=pkg.get("id", ""),
                                       key="share_up_sid")
            with uc2:
                st.markdown("&nbsp;")
                if st.button("📥 导入", type="primary", key="share_up_go"):
                    ok, msg = market.import_package(
                        pkg, new_sid=up_sid,
                        overwrite=st.session_state.get("share_up_ow", False))
                    (st.success if ok else st.error)(msg)
            st.checkbox("覆盖已有同名策略", key="share_up_ow")

    st.markdown("---")
    entries = market.list_packages()
    st.caption(f"本地市场目录 `{market.MARKET_DIR}` · {len(entries)} 个包")
    for entry in entries:
        pkg = entry.package
        icon = "✅" if entry.valid else "⚠️"
        with st.expander(f"{icon} {entry.display_name} — {pkg.get('author') or '匿名'}"):
            if not entry.valid:
                st.error("；".join(entry.errors))
                continue
            if pkg.get("description"):
                st.markdown(f"> {pkg['description']}")
            st.caption(f"条件 {len(pkg['strategy'].get('conditions', []))} 个 · "
                       f"发布于 {(pkg.get('created_at') or '?')[:10]}")
            ic1, ic2 = st.columns([2, 1])
            with ic1:
                imp_sid = st.text_input("导入为 ID", value=pkg.get("id", ""),
                                        key=f"share_sid_{entry.path.name}")
            with ic2:
                st.markdown("&nbsp;")
                if st.button("📥 导入", key=f"share_go_{entry.path.name}"):
                    ok, msg = market.import_package(pkg, new_sid=imp_sid,
                                                    overwrite=True)
                    (st.success if ok else st.error)(msg)
