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


def _df_records(df) -> list[dict]:
    import pandas as pd
    if df is None or df.empty:
        return []
    return _jsonable(df.replace({pd.NA: None}).to_dict(orient="records"))


# ============================================================================
# 个股
# ============================================================================

@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/stocks/search")
def stocks_search(q: str, market: str = "a", limit: int = 10):
    """代码前缀或名称子串搜索 → [{code, name}]"""
    from src.services import stock_service as ssvc
    return ssvc.search_stocks(q, market, limit=min(limit, 30))


@app.get("/api/stocks/{market}/{code}")
def stock_basic(market: str, code: str):
    """代码 → 名称 + 最新价"""
    from src.services import stock_service as ssvc
    name = ssvc.resolve_name(code, market)
    price = ssvc.current_price(code, market)
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
        raise HTTPException(404, "分析结果为空，请检查代码")
    r = run.result
    label, level = vsvc.percentile_badge(r.get("hist_percentile", 0))
    return {
        "core": _jsonable({k: r.get(k) for k in (
            "price", "current_pe", "current_ps", "hist_percentile",
            "ttm_revenue", "ttm_net_profit", "scenarios")}),
        "badge": {"label": label, "level": level},
        "targets": vsvc.target_price_rows(r),
        "summary": vsvc.summary_rows(r, run.fin_df, valuation),
    }


@app.get("/api/stocks/{market}/{code}/kline")
def stock_kline(market: str, code: str, days: int = 250):
    """日线 K 线 + MA20/60 + 成交量（ECharts 蜡烛图格式 [开,收,低,高]）"""
    from src.services import backtest_service as btsvc
    df = btsvc.fetch_ohlcv(code, market, days)
    if df is None or df.empty:
        raise HTTPException(404, "未能获取 K 线数据")
    df = btsvc._normalize_for_charts(df)
    close = df["close"]
    return {
        "dates": [str(d)[:10] for d in df.index],
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
        raise HTTPException(404, "FCF 财务数据为空（新股/数据源无记录）")
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
    hits = ssvc.scan_buy_signals(code, market)
    ev = ssvc.sell_verdict_for_code(code, "", market)
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


@app.post("/api/screening/run")
def screening_run(req: ScreeningRequest):
    from src.services import screening_service as svc
    try:
        run = svc.run_screening(
            req.strategy_ids, scope_keys=req.scope_keys,
            max_workers=req.max_workers)
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


# ============================================================================
# 持仓
# ============================================================================

@app.get("/api/portfolio/diagnosis")
def portfolio_diagnosis():
    from src.portfolio.diagnostics import run_diagnosis
    try:
        return _jsonable(run_diagnosis().to_report_dict())
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
# 前端（静态单页）
# ============================================================================

@app.get("/", include_in_schema=False)
def index():
    return FileResponse(_STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")
