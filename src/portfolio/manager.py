"""
src/portfolio/manager.py — 持仓 CRUD + YAML 持久化

封装对 config/holdings.yaml 的所有读写。原子写 + 备份，避免数据损坏。
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from src.portfolio.models import Holding, Portfolio, Transaction
from src.utils.logger import get_logger
from src.core.config_io import atomic_save_yaml, load_yaml

logger = get_logger("portfolio_mgr")


DEFAULT_HOLDINGS_PATH = Path("config/holdings.yaml")


class PortfolioManager:
    """
    持仓管理器。所有 CRUD 走这里，确保数据一致。

    用法:
        mgr = PortfolioManager()
        pf = mgr.load()
        mgr.add_holding(Holding(code="603005", name="晶方", qty=1000, avg_cost=28.5))
        mgr.save(pf)
    """

    def __init__(self, path: Path | str = DEFAULT_HOLDINGS_PATH):
        self.path = Path(path)

    # ── 加载 ──

    def load(self) -> Portfolio:
        """从 YAML 加载持仓组合；文件不存在或空返回空组合。"""
        raw = load_yaml(self.path) or {}
        return self._deserialize(raw)

    @staticmethod
    def _deserialize(raw: dict) -> Portfolio:
        default_alerts = raw.get("default_alerts") or {}
        cash = float(raw.get("cash", 0) or 0)
        holdings_raw = raw.get("holdings") or []
        holdings: list[Holding] = []
        for h in holdings_raw:
            if not isinstance(h, dict):
                continue
            tx_list = []
            for tx in (h.get("transactions") or []):
                if not isinstance(tx, dict):
                    continue
                try:
                    tx_list.append(Transaction(
                        action=str(tx.get("action", "buy")),
                        date=str(tx.get("date", "")),
                        qty=int(tx.get("qty", 0) or 0),
                        price=float(tx.get("price", 0) or 0),
                        note=str(tx.get("note", "")),
                    ))
                except (ValueError, TypeError) as e:
                    logger.warning(f"持仓 {h.get('code')} 跳过非法 transaction: {e}")
            holding = Holding(
                code=str(h.get("code", "")).strip().zfill(6) if str(h.get("code", "")).isdigit() else str(h.get("code", "")).strip(),
                name=str(h.get("name", "")),
                market=str(h.get("market", "a")),
                qty=int(h.get("qty", 0) or 0),
                avg_cost=float(h.get("avg_cost", 0) or 0),
                buy_date=str(h.get("buy_date", "")),
                notes=str(h.get("notes", "")),
                transactions=tx_list,
                alerts=dict(h.get("alerts") or {}),
                tag=str(h.get("tag", "")),
            )
            holdings.append(holding)
        return Portfolio(holdings=holdings, cash=cash, default_alerts=default_alerts)

    # ── 保存 ──

    def save(self, pf: Portfolio) -> bool:
        data = self._serialize(pf)
        ok = atomic_save_yaml(self.path, data)
        if ok:
            logger.info(f"持仓已保存: {len(pf.holdings)} 只 -> {self.path}")
        else:
            logger.error(f"持仓保存失败: {self.path}")
        return ok

    @staticmethod
    def _serialize(pf: Portfolio) -> dict:
        holdings_out = []
        for h in pf.holdings:
            entry = {
                "code": h.code,
                "name": h.name,
                "market": h.market,
                "qty": h.qty,
                "avg_cost": round(h.avg_cost, 4),
                "buy_date": h.buy_date,
            }
            if h.tag:
                entry["tag"] = h.tag
            if h.notes:
                entry["notes"] = h.notes
            if h.alerts:
                entry["alerts"] = dict(h.alerts)
            if h.transactions:
                entry["transactions"] = [
                    {"action": t.action, "date": t.date, "qty": t.qty,
                     "price": round(t.price, 4), "note": t.note}
                    for t in h.transactions
                ]
            holdings_out.append(entry)
        return {
            "default_alerts": dict(pf.default_alerts),
            "cash": round(pf.cash, 2),
            "holdings": holdings_out,
        }

    # ── 高层 CRUD ──

    def add_holding(self, holding: Holding) -> tuple[bool, str]:
        """添加持仓；如代码已存在则拒绝"""
        pf = self.load()
        if pf.find(holding.code):
            return False, f"持仓已存在：{holding.code}"
        pf.holdings.append(holding)
        if self.save(pf):
            return True, "已添加"
        return False, "保存失败"

    def remove_holding(self, code: str) -> tuple[bool, str]:
        pf = self.load()
        before = len(pf.holdings)
        pf.holdings = [h for h in pf.holdings if h.code != code]
        if len(pf.holdings) == before:
            return False, f"未找到代码 {code}"
        return (True, "已删除") if self.save(pf) else (False, "保存失败")

    def update_holding(self, code: str, updates: dict) -> tuple[bool, str]:
        """更新持仓的部分字段（不动 transactions / alerts，需单独 API）"""
        pf = self.load()
        h = pf.find(code)
        if not h:
            return False, f"未找到代码 {code}"
        for key, val in updates.items():
            if hasattr(h, key) and key not in ("code", "transactions"):
                try:
                    setattr(h, key, val)
                except Exception:
                    pass
        return (True, "已更新") if self.save(pf) else (False, "保存失败")

    def add_transaction(self, code: str, tx: Transaction,
                        recompute: bool = True) -> tuple[bool, str]:
        """给指定持仓加一笔交易记录；可选自动重算 qty/avg_cost"""
        pf = self.load()
        h = pf.find(code)
        if not h:
            return False, f"未找到代码 {code}"
        h.transactions.append(tx)
        if recompute:
            h.recompute_from_transactions()
        return (True, "已添加交易") if self.save(pf) else (False, "保存失败")
