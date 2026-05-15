"""
src/web/widgets/market_stock_picker.py — 市场 + 选股 + 手动输入三合一选择器

取代 1_估值分析 / 4_策略回测 / 5_价格预警 / 6_财报披露 / 11_FCF分析
等页面里手写的 6 份重复"市场下拉 + 从 yaml 选股 + 手动输入"代码。

用法：
    picker = MarketStockPicker(default_market="a", key="page1")
    market, code, name, extra = picker.render(sidebar=True)
    # extra 包含 yaml 里的 valuation / pe_range 等额外字段
"""
from __future__ import annotations

from typing import Any

from src.core.market_registry import get_market, market_keys, market_labels


class MarketStockPicker:
    """市场 + 选股 + 手动覆盖一体化"""

    def __init__(
        self,
        *,
        default_market: str = "a",
        key: str = "picker",
        sidebar: bool = True,
        allow_manual: bool = True,
        show_market_select: bool = True,
    ):
        self.default_market = default_market
        self.key = key
        self.sidebar = sidebar
        self.allow_manual = allow_manual
        self.show_market_select = show_market_select

    def render(self) -> tuple[str, str, str, dict]:
        """
        渲染选择器。

        Returns:
            (market, code, name, extra_dict)
            extra_dict 含 yaml 中该股票的额外字段（valuation/pe_range 等）
        """
        import streamlit as st

        container = st.sidebar if self.sidebar else st
        labels = market_labels()
        keys = market_keys()

        # 1. 市场选择
        if self.show_market_select:
            idx = keys.index(self.default_market) if self.default_market in keys else 0
            market = container.selectbox(
                "市场",
                options=keys,
                format_func=lambda k: labels.get(k, k),
                index=idx,
                key=f"{self.key}_market",
            )
        else:
            market = self.default_market

        # 2. 从 yaml 加载该市场的股票
        stocks = self._load_stocks_for_market(market)

        # 3. 选股
        default_code = ""
        default_name = ""
        extra: dict[str, Any] = {}

        if stocks:
            options = [f"{s.get('name', '?')} ({s.get('code', '?')})" for s in stocks]
            options = ["<手动输入>"] + options if self.allow_manual else options
            choice = container.selectbox(
                "选择股票",
                options=range(len(options)),
                format_func=lambda i: options[i],
                key=f"{self.key}_stock",
            )
            if self.allow_manual and choice == 0:
                # 手动输入分支
                pass
            else:
                stock = stocks[choice - 1 if self.allow_manual else choice]
                default_code = str(stock.get("code", ""))
                default_name = str(stock.get("name", ""))
                # 保留其他字段（valuation/pe_range 等）
                extra = {
                    k: v for k, v in stock.items()
                    if k not in ("code", "name")
                }
        else:
            # 该市场 yaml 为空，用 registry 的 default
            try:
                spec = get_market(market)
                default_code = spec.default_picker_code
                default_name = spec.default_picker_name
            except KeyError:
                pass

        # 4. 手动覆盖输入框
        if self.allow_manual:
            code = container.text_input(
                "股票代码", value=default_code,
                key=f"{self.key}_code",
            )
            name = container.text_input(
                "股票名称", value=default_name,
                key=f"{self.key}_name",
            )
        else:
            code = default_code
            name = default_name

        return market, code.strip(), name.strip(), extra

    @staticmethod
    def _load_stocks_for_market(market: str) -> list[dict]:
        """从 yaml 读该市场的股票列表（带 Streamlit 缓存）"""
        try:
            from src.web.utils import list_stocks_from_market_config
            return list_stocks_from_market_config(market) or []
        except Exception:
            return []
