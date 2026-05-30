"""
pages/17_ML训练.py — ML 自学习模型训练 + 监控

功能：
  - 显示当前模型状态（CV IC / RMSE / 训练时间 / 特征重要性）
  - 触发训练（后台 subprocess 启动 src.ml.cli）
  - 实时 tail 训练日志
  - 训练完成后展示新模型指标
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

st.set_page_config(page_title="ML 训练", page_icon="🧠", layout="wide")
st.title("🧠 ML 自学习模型训练")
st.caption("基本面 + 技术指标 → LightGBM 预测未来 20 日超额收益 → ml_top_k / ML 策略 / 持仓评分")


# ========================
# 路径常量
# ========================
MODEL_PATH = _ROOT / "cache" / "ml_models" / "lgbm_latest.joblib"
META_PATH = _ROOT / "cache" / "ml_models" / "lgbm_latest.meta.json"
DATASET_PATH = _ROOT / "cache" / "ml_dataset" / "dataset_latest.parquet"
LOG_PATH = _ROOT / "output" / "ml_training.log"


# ========================
# 当前模型状态卡片
# ========================
st.subheader("📊 当前模型状态")

if not MODEL_PATH.exists():
    st.warning(
        "⚠️ **模型尚未训练** — 下方触发训练，~5 分钟（30 只验证集）/ ~20-30 分钟（沪深 300 全量）"
    )
else:
    meta = {}
    if META_PATH.exists():
        try:
            meta = json.loads(META_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            st.error(f"读取 metadata 失败: {e}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("CV IC", f"{meta.get('cv_ic_mean', 0):.4f}",
               help="信息系数：> 0.03 弱有效 / > 0.05 正常 / > 0.08 良好")
    c2.metric("IC 稳定性", f"±{meta.get('cv_ic_std', 0):.4f}",
               help="标准差越小越稳定，理想 < IC 的 50%")
    c3.metric("CV RMSE", f"{meta.get('cv_rmse_mean', 0):.4f}",
               help="预测误差，跟 y 分布对比")
    c4.metric("样本数", f"{meta.get('n_samples', 0):,}",
               help=f"覆盖 {meta.get('n_features', 0)} 个特征")

    trained_at = meta.get("trained_at", "")
    if trained_at:
        st.caption(f"🕐 训练时间：{trained_at} | 版本：{meta.get('version', '?')}")

    # 健康度判定
    ic = meta.get("cv_ic_mean", 0)
    if ic >= 0.05:
        st.success(f"✅ 模型健康度：**良好** (IC={ic:.4f})。可用于实盘策略。")
    elif ic >= 0.03:
        st.info(f"🟡 模型健康度：**弱有效** (IC={ic:.4f})。可参考，建议补特征或扩股票池。")
    elif ic >= 0:
        st.warning(f"⚠️ 模型健康度：**接近随机** (IC={ic:.4f})。不建议实盘。")
    else:
        st.error(f"❌ 模型健康度：**负 IC** (IC={ic:.4f})，反向预测。检查 dataset/特征工程。")

    # 特征重要性
    fi = meta.get("feature_importance", [])
    if fi:
        with st.expander(f"🌳 Top 15 特征重要性（共 {len(fi)} 个特征）", expanded=False):
            df_fi = pd.DataFrame(fi).head(15)
            st.bar_chart(df_fi.set_index("feature")["gain"], horizontal=True)

st.divider()


# ========================
# 训练参数 + 启动
# ========================
st.subheader("🚀 启动训练")

# 检测训练进程是否在跑
def _is_training_running() -> tuple[bool, dict]:
    """
    通过 session_state 中的 PID 检测进程，且校验日志最近 30 秒有更新。
    """
    pid = st.session_state.get("ml_train_pid")
    if pid is None:
        return False, {}
    # 1. PID 检测
    try:
        import os
        # Windows / Unix 通用
        if os.name == "nt":
            import subprocess as sp
            result = sp.run(["tasklist", "/FI", f"PID eq {pid}"],
                              capture_output=True, text=True, timeout=3)
            alive = str(pid) in result.stdout
        else:
            try:
                os.kill(pid, 0)
                alive = True
            except (OSError, ProcessLookupError):
                alive = False
    except Exception:
        alive = False

    # 2. 日志最近活动
    log_active = False
    log_mtime = None
    if LOG_PATH.exists():
        log_mtime = LOG_PATH.stat().st_mtime
        log_active = (time.time() - log_mtime) < 120  # 2 分钟内有更新

    return alive or log_active, {
        "pid": pid,
        "log_mtime": log_mtime,
    }

is_running, run_info = _is_training_running()

if is_running:
    st.warning(f"⏳ **训练正在进行中** — PID `{run_info.get('pid', '?')}`")
    if run_info.get("log_mtime"):
        last_update = time.time() - run_info["log_mtime"]
        st.caption(f"日志最近更新于 {last_update:.0f} 秒前")
    if st.button("🔄 刷新页面查看进度"):
        st.rerun()
    if st.button("🛑 终止训练", type="secondary"):
        pid = st.session_state.get("ml_train_pid")
        if pid:
            try:
                import os
                if os.name == "nt":
                    subprocess.run(["taskkill", "/PID", str(pid), "/F"], timeout=5)
                else:
                    os.kill(pid, 9)
                st.session_state.pop("ml_train_pid", None)
                st.success("已发送终止信号")
                st.rerun()
            except Exception as e:
                st.error(f"终止失败: {e}")
else:
    with st.form("ml_train_form"):
        st.markdown("**训练参数**")
        c1, c2, c3 = st.columns(3)
        max_codes = c1.number_input(
            "股票数上限", min_value=0, value=30, step=10,
            help="0 = 全沪深 300（~300 只，20-30 分钟）；调试用 30",
        )
        start_date = c2.date_input("数据起点", value=pd.Timestamp("2022-01-01"))
        end_date = c3.date_input(
            "数据终点（None=今天）",
            value=pd.Timestamp.now(),
        )
        d1, d2, d3 = st.columns(3)
        horizon = d1.number_input("标签时间窗（天）", 5, 60, 20,
                                    help="预测未来 N 日超额收益。20 ≈ 1 月（默认），60 = 3 月")
        n_splits = d2.number_input("CV 折数", 2, 10, 5,
                                    help="Purged Walk-Forward 切分数")
        num_rounds = d3.number_input("LightGBM 轮数", 50, 2000, 200,
                                       help="200 调试 / 500 正式 / >1000 容易过拟合")

        submitted = st.form_submit_button("▶️ 开始训练", type="primary")

        if submitted:
            # 构造命令
            cmd = [
                sys.executable, "-X", "utf8",
                "-m", "src.ml.cli", "train",
                "--start-date", start_date.strftime("%Y-%m-%d"),
                "--end-date", end_date.strftime("%Y-%m-%d"),
                "--horizon", str(int(horizon)),
                "--n-splits", str(int(n_splits)),
                "--num-rounds", str(int(num_rounds)),
            ]
            if max_codes > 0:
                cmd.extend(["--max-codes", str(int(max_codes))])

            # 清空旧日志
            LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            LOG_PATH.write_text("", encoding="utf-8")

            # 后台启动
            try:
                env = dict(__import__("os").environ)
                env["PYTHONIOENCODING"] = "utf-8"
                with open(LOG_PATH, "wb") as flog:
                    proc = subprocess.Popen(
                        cmd, stdout=flog, stderr=subprocess.STDOUT,
                        cwd=str(_ROOT), env=env,
                    )
                st.session_state["ml_train_pid"] = proc.pid
                st.session_state["ml_train_start"] = time.time()
                st.success(f"✅ 训练已启动 (PID {proc.pid})。每隔几秒刷新查看进度。")
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"启动失败: {e}")

st.divider()


# ========================
# 实时训练日志
# ========================
st.subheader("📜 训练日志")

if LOG_PATH.exists() and LOG_PATH.stat().st_size > 0:
    log_size = LOG_PATH.stat().st_size
    log_mtime = LOG_PATH.stat().st_mtime
    st.caption(f"📂 `{LOG_PATH.relative_to(_ROOT)}` | 大小 {log_size / 1024:.1f} KB | "
                f"最近更新 {(time.time() - log_mtime):.0f} 秒前")

    # 读最后 2000 行（避免大日志卡死）
    try:
        with open(LOG_PATH, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        tail_lines = lines[-200:] if len(lines) > 200 else lines
        log_text = "".join(tail_lines)
        st.code(log_text or "（空）", language="text")
    except Exception as e:
        st.error(f"读取日志失败: {e}")

    if is_running and st.checkbox("自动刷新（每 5 秒）", value=True):
        time.sleep(5)
        st.rerun()
else:
    st.info("还没有训练日志。点击「开始训练」生成。")


# ========================
# 数据集状态
# ========================
st.divider()
st.subheader("📦 训练数据集")

if DATASET_PATH.exists():
    size_mb = DATASET_PATH.stat().st_size / 1024 / 1024
    mtime = pd.Timestamp.fromtimestamp(DATASET_PATH.stat().st_mtime)
    st.caption(f"📂 `{DATASET_PATH.relative_to(_ROOT)}` | "
                f"大小 {size_mb:.1f} MB | "
                f"构建于 {mtime:%Y-%m-%d %H:%M:%S}")

    if st.button("📊 查看数据集摘要", type="secondary"):
        try:
            df = pd.read_parquet(DATASET_PATH)
            c1, c2, c3 = st.columns(3)
            c1.metric("样本数", f"{len(df):,}")
            c2.metric("覆盖股票", df["code"].nunique() if "code" in df.columns else "?")
            date_range = ""
            if "date" in df.columns:
                date_range = f"{df['date'].min().date()} ~ {df['date'].max().date()}"
            c3.metric("日期范围", date_range)
            st.dataframe(df.head(10), use_container_width=True)
        except Exception as e:
            st.error(f"加载失败: {e}")
else:
    st.info("尚无数据集。首次训练时会自动构建。")
