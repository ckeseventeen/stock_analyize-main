"""
src/api/main.py — FastAPI 应用：服务层的 REST 出口 + 新版单页前端

启动：
    uvicorn src.api.main:app --reload --port 8600
浏览器打开 http://localhost:8600 即新版界面；/docs 为自动 API 文档。
"""
from __future__ import annotations

import math
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

os.environ.setdefault("SCHEDULER_DISABLED", "1")  # API 进程不内嵌调度器

from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.responses import FileResponse  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402

from src.trading import OrderSide, PaperBroker  # noqa: E402

app = FastAPI(title="量化投研平台 API", version="0.1.0")


@app.on_event("startup")
def _warm_caches() -> None:
    """后台预热全市场名称表（3-4 分钟的首次拉取不能落在用户第一次点击上）"""
    import threading

    def _warm():
        try:
            from src.services.stock_service import _a_share_names
            _a_share_names()
        except Exception:
            pass

    threading.Thread(target=_warm, daemon=True, name="warm-names").start()

_STATIC_DIR = Path(__file__).parent / "static"

# 模拟盘单例（进程级）
_broker = PaperBroker()


# ============================================================================
# 工具：JSON 安全化
# ============================================================================

def _jsonable(v: Any) -> Any:
    """把服务层返回值里的 NaN/numpy/DataFrame 等清洗成 JSON 可序列化"""
    import numpy as np
    import pandas as pd

    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()
                if not isinstance(x, pd.DataFrame)}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, pd.DataFrame):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        return None if (v is None or math.isnan(float(v)) or math.isinf(float(v))) else float(v)
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    return v


# stock_valuation 的 low/mid/high 缺省值。用于判断"调用方是否自定义过区间"——
# 没自定义时把 PE 档位套到 PS 上是错的量纲，需要按标的自身 PS 重算
_PE_RANGE_DEFAULT = (10.0, 20.0, 30.0)


def _unusable_multiple(v: Any) -> bool:
    """
    估值倍数是否不可用（None / NaN / 非正）。

    必须显式判 NaN：分析器对亏损股给出的 current_pe 是 **NaN 而非 None**，
    而 `NaN <= 0` 恒为 False——只写 `v is None or v <= 0` 会漏掉全部亏损股，
    页面于是显示一排负目标价。
    """
    if v is None:
        return True
    try:
        f = float(v)
    except (TypeError, ValueError):
        return True
    return math.isnan(f) or f <= 0


def _df_records(df) -> list[dict]:
    import pandas as pd
    if df is None or df.empty:
        return []
    return _jsonable(df.replace({pd.NA: None}).to_dict(orient="records"))


def _build_valuation_charts(result: dict, run, val_type: str) -> dict:
    """从估值分析结果中提取图表就绪数据（供前端 ECharts 渲染四幅图）"""
    import numpy as np
    import pandas as pd

    charts = {}

    # ── 图1：年度营收与净利润 + 毛利率 ──
    annual_df = result.get("annual_df")
    if annual_df is not None and not annual_df.empty:
        years = [str(d.year) if hasattr(d, "year") else str(d)[:4]
                 for d in annual_df.index]
        revenue = [round(float(v) / 1e8, 2) for v in annual_df.get("营业总收入", [])]
        net_profit = [round(float(v) / 1e8, 2) for v in annual_df.get("归母净利润", [])]
        gross_margin = []
        gm_col = annual_df.get("毛利率", None)
        if gm_col is not None:
            gross_margin = [round(float(v) * 100, 2) if v else 0 for v in gm_col]
        charts["annual"] = {"years": years, "revenue": revenue,
                            "net_profit": net_profit, "gross_margin": gross_margin}
    else:
        charts["annual"] = None

    # ── 图2：历史估值走势 ──
    hist_val = result.get("hist_val")
    val_col = "pe_ttm" if val_type == "pe" else "ps_ttm"
    current_val = result.get("current_pe" if val_type == "pe" else "current_ps", 0)
    if hist_val is not None and not hist_val.empty and val_col in hist_val.columns:
        hist_series = pd.to_numeric(hist_val[val_col], errors="coerce").dropna()
        if not hist_series.empty:
            # 降采样：超过 500 个点时按月取均值，避免前端渲染卡顿
            if len(hist_series) > 500:
                try:
                    hist_series = hist_series.resample("ME").mean().dropna()
                except Exception:
                    try:
                        hist_series = hist_series.resample("1M").mean().dropna()
                    except Exception:
                        pass  # 保留原始数据
            dates = [str(d)[:10] for d in hist_series.index]
            values = [round(float(v), 2) for v in hist_series.values]
            median_val = round(float(hist_series.median()), 2)
            charts["hist_val"] = {
                "dates": dates, "values": values,
                "current": round(float(current_val), 2) if current_val and not (isinstance(current_val, float) and np.isnan(current_val)) else None,
                "median": median_val,
                "val_name": val_type.upper(),
            }
        else:
            charts["hist_val"] = None
    else:
        charts["hist_val"] = None

    # ── 图3：情景假设估值推演 ──
    scenarios = result.get("scenarios", [0, 0, 0])
    price = result.get("price", 0)
    val_range = run.stock_config.get(f"{val_type}_range", [0, 0, 0])
    charts["scenarios"] = {
        "labels": [f"保守({val_type.upper()}={val_range[0]})",
                   f"中性({val_type.upper()}={val_range[1]})",
                   f"乐观({val_type.upper()}={val_range[2]})"],
        "values": [round(float(v), 2) for v in scenarios],
        "current_price": round(float(price), 2) if price else 0,
        "val_name": val_type.upper(),
    }

    return charts


# ============================================================================
# 个股
# ============================================================================

@app.get("/api/health")
def health():
    """健康检查（含 V8 守卫状态——它决定问财数据源是否可用）"""
    from src.core.v8_guard import guard_status

    return {"status": "ok", "v8_guard": guard_status()}


@app.get("/api/config/validate")
def config_validate():
    """
    配置自检：返回各 YAML 的 schema 问题清单。

    配置写错（未知条件类型、未知 job type、非法因子表达式…）过去只会静默
    失效，用户看到的是"结果不对"却无从定位；这里把问题显式暴露出来。
    """
    from src.core.config_validation import validate_all

    report = validate_all()
    return {
        "ok": not report,
        "files_with_issues": len(report),
        "total_issues": sum(len(v) for v in report.values()),
        "issues": report,
    }


@app.get("/api/stocks/search")
def stocks_search(q: str, market: str = "a", limit: int = 10):
    """代码前缀或名称子串搜索 → [{code, name}]"""
    from src.services import stock_service as ssvc
    return ssvc.search_stocks(q, market, limit=min(limit, 30))


@app.get("/api/stocks/{market}/{code}")
def stock_basic(market: str, code: str):
    """代码 → 名称 + 最新价"""
    from src.services import stock_service as ssvc
    try:
        name = ssvc.resolve_name(code, market)
        price = ssvc.current_price(code, market)
    except Exception as e:
        raise HTTPException(502, f"基本信息获取失败: {e}")
    return {"code": code, "market": market, "name": name, "price": price}


@app.get("/api/stocks/{market}/{code}/valuation")
def stock_valuation(market: str, code: str,
                    valuation: str = "pe",
                    low: float = 10, mid: float = 20, high: float = 30):
    """估值分析（精简 JSON：核心指标 + 目标价 + 汇总行）"""
    from src.services import valuation_service as vsvc
    try:
        cfg = vsvc.build_stock_config(
            code, stock_basic(market, code)["name"] or code,
            market, valuation, [low, mid, high])
        run = vsvc.run_valuation(market, cfg)
    except KeyError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(502, f"数据源失败: {e}")
    if not run.result:
        # 区分"数据源没给财务数据"与"确实查不到这只股票"——
        # 一律说"请检查代码"会让用户以为是自己输错了，实际多是数据源限流
        if run.fin_df is None or run.fin_df.empty:
            raise HTTPException(
                502, f"{code} 的财务数据暂时取不到（数据源限流或不可达），请稍后重试")
        raise HTTPException(404, f"{code} 分析结果为空，请确认代码与市场是否匹配")

    # 亏损股用 PE 估值毫无意义（负 EPS 会推出负目标价），自动改用 PS。
    # 美股成长股大量处于这种状态，不回退的话页面只能显示一排负数。
    basis, basis_note = valuation, ""
    if valuation == "pe" and _unusable_multiple(run.result.get("current_pe")):
        try:
            cur_ps = run.result.get("current_ps")
            # 调用方没自定义区间时，PE 的 10/20/30 套到 PS 上是错的量纲
            # （Roblox 当前 PS 仅 5，会推出 2x/4x/6x 现价的假目标），
            # 改成围绕该股自身当前 PS 取档
            if (low, mid, high) == _PE_RANGE_DEFAULT and not _unusable_multiple(cur_ps):
                ps_range = [round(cur_ps * k, 2) for k in (0.7, 1.0, 1.3)]
            else:
                ps_range = [low, mid, high]
            run_ps = vsvc.run_valuation(market, vsvc.build_stock_config(
                code, cfg["name"], market, "ps", ps_range))
            if run_ps.result and not _unusable_multiple(run_ps.result.get("current_ps")):
                run, basis = run_ps, "ps"
                basis_note = (f"该股 TTM 净利润为负，PE 估值不适用，已自动改用 PS（市销率）；"
                              f"情景区间取自当前 PS 的 {ps_range[0]}/{ps_range[1]}/{ps_range[2]} 倍档")
            else:
                basis_note = "该股 TTM 净利润为负且无有效 PS，估值指标暂不适用"
        except Exception as e:
            basis_note = f"该股 TTM 净利润为负，PE 估值不适用；PS 回退亦失败：{e}"

    r = run.result
    label, level = vsvc.percentile_badge(r.get("hist_percentile", 0))

    # ── 图表就绪数据 ──
    charts = _build_valuation_charts(r, run, basis)

    return {
        "core": _jsonable({k: r.get(k) for k in (
            "price", "current_pe", "current_ps", "hist_percentile",
            "ttm_revenue", "ttm_net_profit", "scenarios")}),
        "badge": {"label": label, "level": level},
        "basis": basis,
        "basis_note": basis_note,
        "targets": vsvc.target_price_rows(r),
        "summary": vsvc.summary_rows(r, run.fin_df, basis),
        "charts": _jsonable(charts),
    }


# /kline days 缺省（days<=0）时按周期自动选择的回溯天数：
# 周线/月线需要足够长的窗口，否则 MA60 全为 null
_KLINE_AUTO_DAYS = {"d": 250, "w": 1095, "m": 1825, "y": 3650}


@app.get("/api/stocks/{market}/{code}/kline")
def stock_kline(market: str, code: str, days: int = 0, freq: str = "d"):
    """K 线 + MA20/60 + 成交量（ECharts 蜡烛图格式 [开,收,低,高]）

    freq: d/w/m/y 或分钟周期 1m/5m/15m/30m/60m。
    days 缺省（<=0）时按周期自动取合理窗口（日 250 / 周 1095 / 月 1825 / 分钟 30 或 5）。
    """
    from src.services import backtest_service as btsvc
    from src.services import market_data_service as mdsvc

    is_minute = mdsvc.is_minute_freq(freq)
    if not is_minute and freq not in _KLINE_AUTO_DAYS:
        raise HTTPException(422, f"未知周期: {freq}")
    minute_cap = 30 if freq != "1m" else 5
    if days <= 0:
        days = minute_cap if is_minute else _KLINE_AUTO_DAYS[freq]
    elif is_minute:
        # 东财分钟接口历史深度有限，限制回溯窗口防止空转
        days = min(days, minute_cap)
    try:
        df = btsvc.fetch_ohlcv(code, market, days, frequency=freq)
        if df is None or df.empty:
            raise HTTPException(404, "未能获取 K 线数据")
        df = btsvc._normalize_for_charts(df)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"K线数据获取失败: {e}")
    close = df["close"]
    # 分钟周期保留时分（MM-DD HH:MM），日线以上只保留日期
    date_fmt = (lambda d: str(d)[5:16]) if is_minute else (lambda d: str(d)[:10])
    return {
        "dates": [date_fmt(d) for d in df.index],
        "k": _jsonable([[float(o), float(c), float(lo), float(h)]
                        for o, c, lo, h in zip(df["open"], close, df["low"], df["high"])]),
        "volume": _jsonable([float(v) for v in df.get("volume", [0] * len(df))]),
        "ma20": _jsonable(list(close.rolling(20).mean().round(2))),
        "ma60": _jsonable(list(close.rolling(60).mean().round(2))),
    }


@app.get("/api/stocks/{market}/{code}/fcf")
def stock_fcf(market: str, code: str, annual: bool = True):
    """自由现金流分析：五维评分卡 + 年度序列（亿元）"""
    from src.services import valuation_service as vsvc
    try:
        run = vsvc.run_fcf(market, code, is_annual=annual)
    except Exception as e:
        raise HTTPException(502, f"FCF 数据源失败: {e}")
    if run.analyzed_df.empty or not run.score_res:
        # 数据源故障（Yahoo 限流/不可达）不能报成"新股/无记录"——
        # 那会让用户以为代码输错了，实际上过几秒重试就有数据
        if run.fetch_status == "failed":
            raise HTTPException(502, run.fetch_reason or "FCF 数据源暂时不可用，请稍后重试")
        raise HTTPException(404, run.fetch_reason or "FCF 财务数据为空（新股/已退市/数据源无记录）")
    df = run.analyzed_df
    series = {}
    for col in ("operating_cash_flow", "capex", "fcf", "net_profit"):
        if col in df.columns:
            series[col] = _jsonable([round(float(v) / 1e8, 2) for v in df[col]])
    if "fcf_margin" in df.columns:
        series["fcf_margin"] = _jsonable([round(float(v), 1) for v in df["fcf_margin"]])
    return {
        "dates": [str(i)[:10] for i in df.index],
        "series": series,
        "scores": _jsonable(run.score_res.get("scores", {})),
        "summary": _jsonable(run.score_res.get("summary", {})),
    }


@app.get("/api/stocks/{market}/{code}/strategy-signals")
def stock_strategy_signals(market: str, code: str):
    """逐策略扫描该股的买入点/卖出点（策略买卖两侧分别判定）"""
    from src.services import stock_service as ssvc
    try:
        return ssvc.scan_strategy_signals(code, market)
    except Exception as e:
        raise HTTPException(502, f"策略信号扫描失败: {e}")


@app.get("/api/stocks/{market}/{code}/signals")
def stock_signals(market: str, code: str):
    """买点条件扫描 + 卖出引擎判定"""
    from src.services import stock_service as ssvc
    try:
        hits = ssvc.scan_buy_signals(code, market)
        ev = ssvc.sell_verdict_for_code(code, "", market)
    except Exception as e:
        raise HTTPException(502, f"信号扫描失败: {e}")
    return {
        "buy": [{"type": h.cond_type, "label": h.label,
                 "period": h.period, "hit": h.hit} for h in hits],
        "sell": None if ev is None else _jsonable({
            "risk_pct": ev.verdict.risk_pct,
            "risk_level": ev.verdict.risk_level,
            "action": ev.verdict.action,
            "advice": ev.verdict.advice,
            "signals": ev.verdict.signals,
        }),
    }


# ============================================================================
# 策略与筛选
# ============================================================================

@app.get("/api/strategies")
def strategies():
    from src.services import screening_service as svc
    out = []
    for sid, body in svc.load_all_strategies().items():
        conds = body.get("conditions", [])
        fundamental = sum(1 for c in conds if svc.is_spot_only(c.get("type", "")))
        out.append({
            "id": sid, "name": body.get("name", sid),
            "fundamental": fundamental,
            "technical": len(conds) - fundamental,
            "has_backtest": bool(body.get("backtest")),
        })
    return out


class StrategyPatch(BaseModel):
    new_id: Optional[str] = None
    new_name: Optional[str] = None


@app.patch("/api/strategies/{sid}")
def strategy_patch(sid: str, req: StrategyPatch):
    """重命名策略（ID 和/或显示名）"""
    from src.services import screening_service as svc
    if req.new_name:
        body = svc.get_strategy(sid)
        if not body:
            raise HTTPException(404, f"策略不存在: {sid}")
        body["name"] = req.new_name.strip()
        ok, msg = svc.upsert_strategy(sid, body)
        if not ok:
            raise HTTPException(400, msg)
    if req.new_id and req.new_id.strip() != sid:
        ok, msg = svc.rename_strategy(sid, req.new_id.strip())
        if not ok:
            raise HTTPException(400, msg)
        sid = req.new_id.strip()
    return {"id": sid, "ok": True}


@app.delete("/api/strategies/{sid}")
def strategy_delete(sid: str):
    from src.services import screening_service as svc
    ok, msg = svc.delete_strategy(sid)
    if not ok:
        raise HTTPException(404, msg)
    return {"deleted": sid}


@app.post("/api/strategies/{sid}/duplicate")
def strategy_duplicate(sid: str):
    from src.services import screening_service as svc
    strategies = svc.load_all_strategies()
    if sid not in strategies:
        raise HTTPException(404, f"策略不存在: {sid}")
    new_sid = svc.duplicate_strategy_in_memory(strategies, sid)
    ok, msg = svc.upsert_strategy(new_sid, strategies[new_sid])
    if not ok:
        raise HTTPException(400, msg)
    return {"id": new_sid}


class StrategyBacktestRequest(BaseModel):
    code: str = ""
    market: str = "a"
    days: int = 0


@app.post("/api/strategies/{sid}/backtest")
def strategy_backtest(sid: str, req: StrategyBacktestRequest):
    """按策略回测单标的：技术条件买入 + backtest 段卖出，KPI 含最大回撤 + 资金曲线"""
    from src.services import backtest_service as btsvc
    try:
        report, err = btsvc.run_strategy_backtest(
            sid, req.code, req.market, req.days)
    except Exception as e:
        raise HTTPException(502, f"回测失败: {e}")
    if report is None:
        raise HTTPException(400, err)

    curve = {}
    eq = report.get("equity_curve")
    if eq is not None and not eq.empty and "value" in eq.columns:
        curve = {
            "dates": [str(d)[:10] for d in eq["datetime"]],
            "values": _jsonable(list(eq["value"])),
        }
    out = _jsonable({k: v for k, v in report.items() if k != "trades"})
    out["curve"] = curve
    return out


@app.get("/api/factors/alpha158")
def factors_alpha158():
    """Alpha 158 表达式因子清单（名称/表达式/说明/方向）"""
    from src.services import factor_service as fsvc
    return fsvc.list_alpha158()


@app.get("/api/conditions")
def conditions_schema():
    """条件 schema：分类 → [{type,label,params:[{key,kind,choices,default}]}]（编辑器用）"""
    from src.services import screening_service as svc
    cats = {}
    for cat, types in svc.condition_categories().items():
        cats[cat] = [{
            "type": t,
            "label": svc.condition_label(t),
            "params": [{"key": s.yaml_key, "kind": s.kind,
                        "choices": s.choices, "default": s.default}
                       for s in svc.condition_param_specs(t)],
        } for t in types]
    # 条件默认值里可能有 float('inf')（如 market_cap.max）→ 统一清洗为 None
    return _jsonable({"categories": cats,
                      "sellable": svc.sellable_condition_types()})


@app.get("/api/strategies/{sid}/detail")
def strategy_detail(sid: str):
    from src.services import screening_service as svc
    body = svc.get_strategy(sid)
    if not body:
        raise HTTPException(404, f"策略不存在: {sid}")
    return {"id": sid, "body": body}


class StrategyBody(BaseModel):
    body: dict


class StrategyCreate(BaseModel):
    name: str = ""
    id: str = ""


@app.post("/api/strategies")
def strategy_create(req: StrategyCreate):
    """从零新建策略（此前前端只能克隆已有策略，无法新建）"""
    from src.services import screening_service as svc

    sid, err = svc.create_strategy(name=req.name, sid=req.id)
    if err:
        raise HTTPException(400, err)
    return {"id": sid, "ok": True}


@app.put("/api/strategies/{sid}")
def strategy_put(sid: str, req: StrategyBody):
    """整体更新策略（带条件校验）"""
    from src.services import screening_service as svc
    ok, msg = svc.upsert_strategy(sid, req.body)
    if not ok:
        raise HTTPException(400, msg)
    return {"id": sid, "ok": True}


class ScreeningRequest(BaseModel):
    strategy_ids: list[str] = Field(min_length=1)
    scope_keys: Optional[list[str]] = None
    max_workers: int = 8
    use_processes: bool = False   # Pass2 进程池并行（CPU 密集条件多时提速）


@app.post("/api/screening/run")
def screening_run(req: ScreeningRequest):
    from src.services import screening_service as svc
    try:
        run = svc.run_screening(
            req.strategy_ids, scope_keys=req.scope_keys,
            max_workers=req.max_workers, use_processes=req.use_processes)
    except Exception as e:
        raise HTTPException(502, f"筛选失败: {e}")
    return {
        "elapsed": round(run.elapsed_seconds, 1),
        "warnings": run.warnings,
        "count": len(run.df),
        "rows": _df_records(run.df),
    }


# ============================================================================
# 回测
# ============================================================================

class CompareRequest(BaseModel):
    code: str
    market: str = "a"
    days: int = 1000


@app.post("/api/backtest/compare")
def backtest_compare(req: CompareRequest):
    from src.services import backtest_service as btsvc
    try:
        result, _df = btsvc.run_compare(req.code, req.market, req.days)
    except Exception as e:
        raise HTTPException(502, f"回测失败: {e}")
    if result is None:
        raise HTTPException(404, "未能获取行情数据")

    rows = []
    for r in result.results:
        rep = {k: v for k, v in (r.report or {}).items() if k != "trades"}
        rows.append(_jsonable({
            "key": r.key, "label": r.label, "success": r.success,
            "skip_reason": getattr(r, "skip_reason", None) or getattr(r, "error", None),
            **rep,
        }))
    eq = result.equity_curves_df()
    curves = {}
    if eq is not None and not eq.empty:
        curves = {
            "dates": [str(d)[:10] for d in eq.index],
            "series": {col: _jsonable(list(eq[col].fillna(0))) for col in eq.columns},
        }
    return {"results": rows, "curves": curves}


class OptimizeRequest(BaseModel):
    code: str
    market: str = "a"
    days: int = 750
    strategy: str = "ma_crossover"
    method: str = "bayesian"          # grid | random | bayesian
    n_trials: int = 100
    metric: str = "总收益率(%)"
    walk_forward: bool = False         # 附带 Walk-Forward 过拟合检测


@app.post("/api/backtest/optimize")
def backtest_optimize(req: OptimizeRequest):
    """策略参数寻优（向量化引擎，秒级数百 trial）+ 可选 Walk-Forward 验证"""
    from src.services import backtest_service as btsvc

    try:
        r = btsvc.run_optimization(
            req.strategy, req.code, req.market, req.days,
            method=req.method, n_trials=req.n_trials,
            metric=req.metric, walk_forward=req.walk_forward)
    except KeyError as e:
        raise HTTPException(404, str(e).strip("'"))
    except LookupError as e:
        raise HTTPException(404, str(e))
    except (ValueError, RuntimeError) as e:
        raise HTTPException(422, str(e))
    except Exception as e:
        raise HTTPException(502, f"寻优失败: {e}")

    wf = r.get("walk_forward")
    return {
        "strategy": r["strategy"],
        "method": r["method"],
        "metric": r["metric"],
        "n_trials": r["n_trials"],
        "elapsed": r["elapsed"],
        "best_params": _jsonable(r["best_params"]),
        "best_score": _jsonable(r["best_score"]),
        "top_trials": _df_records(r["top_trials"]),
        "walk_forward": ({"summary": _jsonable(wf["summary"]),
                          "folds": _df_records(wf["folds"])} if wf else None),
    }


@app.get("/api/backtest/optimizable")
def backtest_optimizable():
    """可寻优策略清单：[{key, label, space}]"""
    from src.services import backtest_service as btsvc
    return btsvc.list_optimizable_strategies()


# ============================================================================
# ML 模型（B 路径自学习）
# ============================================================================

@app.get("/api/ml/status")
def ml_status():
    """检查 ML 模型是否已训练"""
    from src.services import ml_service

    return ml_service.model_status()


@app.post("/api/ml/train")
def ml_train():
    """触发 ML 模型训练/重训（后台异步执行）"""
    from src.services import ml_service

    started, message = ml_service.start_training()
    return {"message": message, "started": started,
            "status": ml_service.training_status()}


@app.get("/api/ml/training-status")
def ml_training_status():
    """
    训练进度轮询端点。

    训练要跑 5-15 分钟且是后台线程，前端只拿到"已启动"就没了下文——
    用户无从判断是在跑还是挂了。这里把进程级训练状态暴露出去供轮询。
    """
    from src.services import ml_service

    return {"training": ml_service.training_status(),
            "model": ml_service.model_status()}


# ============================================================================
# 持仓
# ============================================================================

@app.get("/api/portfolio/diagnosis")
def portfolio_diagnosis():
    from src.services import portfolio_service as pfsvc
    try:
        return _jsonable(pfsvc.run_portfolio_diagnosis())
    except Exception as e:
        raise HTTPException(502, f"体检失败: {e}")


# ============================================================================
# 交易（模拟盘）
# ============================================================================

@app.get("/api/trading/account")
def trading_account():
    s = _broker.summary()
    positions = [{
        "code": p.code, "name": p.name, "qty": p.qty,
        "avg_cost": round(p.avg_cost, 3),
        "price": round(p.market_price, 2),
        "market_value": round(p.market_value, 0),
        "pnl_pct": round(p.pnl_pct, 2),
    } for p in _broker.get_positions()]
    return {"summary": _jsonable(s), "positions": positions}


@app.get("/api/trading/orders")
def trading_orders(limit: int = 50):
    return [o.to_dict() for o in _broker.list_orders(limit=limit)]


class OrderRequest(BaseModel):
    code: str
    side: str = Field(pattern="^(buy|sell)$")
    qty: int = Field(gt=0)
    price: float = 0.0
    name: str = ""


@app.post("/api/trading/orders")
def place_order(req: OrderRequest):
    order = _broker.place_order(
        req.code, OrderSide(req.side), req.qty, req.price,
        name=req.name, note="api")
    return order.to_dict()


@app.delete("/api/trading/orders/{order_id}")
def cancel_order(order_id: str):
    if not _broker.cancel_order(order_id):
        raise HTTPException(404, "订单不存在或不可撤销")
    return {"cancelled": order_id}


# ============================================================================
# 市场监控
# ============================================================================

@app.get("/api/market/sentiment")
def market_sentiment():
    """市场情绪面板 — 六大维度实时数据"""
    from src.services import market_monitor_service as mms
    try:
        return _jsonable(mms.get_full_panel())
    except Exception as e:
        raise HTTPException(502, f"市场情绪面板获取失败: {e}")


@app.get("/api/market/us-summary")
def market_us_summary():
    """美股盘前总结 — 三大指数最近交易日表现"""
    from src.services import market_monitor_service as mms
    try:
        return _jsonable(mms.us_market_summary())
    except Exception as e:
        raise HTTPException(502, f"美股数据获取失败: {e}")


@app.get("/api/altdata/sources")
def altdata_sources():
    """另类数据源清单：[{key, label, params, optional}]"""
    from src.services import market_data_service as mdsvc
    return mdsvc.list_alt_sources()


@app.get("/api/altdata/{source}")
def altdata_query(source: str, code: str = "", symbol: str = "",
                  date: str = "", days: int = 5, limit: int = 200):
    """通用另类数据查询：龙虎榜/北向/融资融券/大宗/解禁/股东人数/分红/概念成分"""
    from src.services import market_data_service as mdsvc

    if not mdsvc.is_alt_source(source):
        raise HTTPException(404, f"未知数据源: {source}")
    params: dict = {}
    if code:
        params["code"] = code.strip()
    if symbol:
        params["symbol"] = symbol.strip()
    if date:
        params["date"] = date.strip()
    if source == "lhb":
        params["days"] = max(1, min(int(days), 30))
    try:
        df = mdsvc.fetch_alt(source, **params)
    except ValueError as e:
        raise HTTPException(422, str(e))
    except Exception as e:
        raise HTTPException(502, f"另类数据获取失败: {e}")
    if df is None or df.empty:
        return {"count": 0, "columns": [], "rows": []}
    df = df.tail(int(limit)) if source in ("northbound_stock", "holder_number",
                                           "block_trade") else df.head(int(limit))
    return {
        "count": int(len(df)),
        "columns": [str(c) for c in df.columns],
        "rows": _df_records(df),
    }


@app.get("/api/fundamental/sources")
def fundamental_sources():
    """基本面深度数据源清单：[{key, label, params, optional}]"""
    from src.services import market_data_service as mdsvc
    return mdsvc.list_fundamental_sources()


@app.get("/api/fundamental/{source}")
def fundamental_query(source: str, code: str = "", period: str = "",
                      start_year: str = "", limit: int = 200):
    """通用基本面查询：三大报表 / 财务指标 / 业绩预告 / 业绩快报"""
    from src.services import market_data_service as mdsvc

    if not mdsvc.is_fundamental_source(source):
        raise HTTPException(404, f"未知数据源: {source}")
    params: dict = {}
    if code:
        params["code"] = code.strip()
    if period:
        params["period"] = period.strip()
    if start_year:
        params["start_year"] = start_year.strip()
    try:
        df = mdsvc.fetch_fundamental(source, **params)
    except ValueError as e:
        raise HTTPException(422, str(e))
    except Exception as e:
        raise HTTPException(502, f"基本面数据获取失败: {e}")
    if df is None or df.empty:
        return {"count": 0, "columns": [], "rows": []}
    df = df.head(int(limit))
    return {
        "count": int(len(df)),
        "columns": [str(c) for c in df.columns],
        "rows": _df_records(df),
    }


@app.get("/api/market/trading-status")
def market_trading_status():
    """当前交易时段状态（是否交易时间 / 是否盘前）"""
    from src.services import market_monitor_service as mms
    now = datetime.now()
    return {
        "is_trading": mms.is_a_share_trading_time(now),
        "is_pre_market": mms.is_pre_market_time(now),
        "now": now.strftime("%Y-%m-%d %H:%M:%S"),
        "weekday": now.weekday(),
    }


# ============================================================================
# 前端（静态单页）
# ============================================================================

@app.get("/", include_in_schema=False)
def index():
    return FileResponse(_STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")
