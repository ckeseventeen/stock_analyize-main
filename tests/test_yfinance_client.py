"""
tests/test_yfinance_client.py — yfinance 取数重试与健康度归因

线上报障：美股个股页的「估值分析」与「现金流(FCF)」两块同时报错，文案分别是
"分析结果为空，请检查代码" 和 "FCF 财务数据为空（新股/数据源无记录）"——
两条都在暗示用户输错了代码。实际上同一个 RBLX 隔几秒重试就 4 年数据齐全。

根因：Yahoo 限流时 `tk.cashflow` **返回空表且不抛异常**，旧代码把空表直接
当成"这只股票没有财务数据"。于是数据源故障被误报成用户输入错误。
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data.providers import yfinance_client as yc


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """把退避等待去掉，测试不该真的睡 5 秒"""
    monkeypatch.setattr(yc.time, "sleep", lambda *_: None)


class _FakeTicker:
    """按预设脚本逐次返回报表；每次构造消费一步，模拟"重试第几次成功" """

    def __init__(self, script):
        self._script = script

    def __call__(self, symbol):
        step = self._script.pop(0) if self._script else ("empty", "empty")
        if step == "raise":
            raise ConnectionError("Yahoo 不可达")
        cf, inc = step
        self.cashflow = pd.DataFrame() if cf == "empty" else pd.DataFrame({"c": [1.0]})
        self.financials = pd.DataFrame() if inc == "empty" else pd.DataFrame({"i": [2.0]})
        self.quarterly_cashflow = self.cashflow
        self.quarterly_financials = self.financials
        return self


def _install(monkeypatch, script):
    fake = _FakeTicker(list(script))

    class _FakeYF:
        Ticker = staticmethod(fake)

    monkeypatch.setitem(sys.modules, "yfinance", _FakeYF)


@pytest.mark.unit
class TestRetry:
    def test_first_attempt_success(self, monkeypatch):
        _install(monkeypatch, [("ok", "ok")])
        res = yc.fetch_statements("RBLX")
        assert res.is_ok
        cf, inc = res.data
        assert not cf.empty and not inc.empty

    def test_recovers_from_transient_empty(self, monkeypatch):
        """这条就是线上现象的回归：首次空表、重试拿到数据"""
        _install(monkeypatch, [("empty", "empty"), ("empty", "empty"), ("ok", "ok")])
        assert yc.fetch_statements("RBLX").is_ok

    def test_recovers_from_transient_exception(self, monkeypatch):
        _install(monkeypatch, ["raise", ("ok", "ok")])
        assert yc.fetch_statements("RBLX").is_ok

    def test_one_empty_table_is_not_enough(self, monkeypatch):
        """只有一张表有数据也不算成功——合并后必然缺列"""
        _install(monkeypatch, [("ok", "empty"), ("ok", "empty"), ("ok", "empty")])
        assert not yc.fetch_statements("RBLX").is_ok


@pytest.mark.unit
class TestHealthAttribution:
    def test_persistent_empty_is_empty_not_failed(self, monkeypatch):
        """重试到底仍空 → 判 EMPTY（新股/退市/代码不存在）"""
        _install(monkeypatch, [("empty", "empty")] * 3)
        res = yc.fetch_statements("NOSUCH")
        assert res.is_empty and not res.is_failed
        # 空表在"限流"与"真无记录"下同形，不可武断归因——两种可能都要讲
        assert "限流" in res.reason and "新股" in res.reason

    def test_persistent_exception_is_failed_not_empty(self, monkeypatch):
        """持续抛异常 → 判 FAILED（数据源故障，提示可重试）"""
        _install(monkeypatch, ["raise"] * 3)
        res = yc.fetch_statements("RBLX")
        assert res.is_failed and not res.is_empty
        assert "重试" in res.reason

    def test_failed_and_empty_are_distinguishable(self, monkeypatch):
        """两者必须可区分——这正是旧代码做不到、导致误报的地方"""
        _install(monkeypatch, ["raise"] * 3)
        failed = yc.fetch_statements("X")
        _install(monkeypatch, [("empty", "empty")] * 3)
        empty = yc.fetch_statements("Y")
        assert failed.status is not empty.status


@pytest.mark.unit
class TestCompatShim:
    def test_statements_or_empty_returns_frames(self, monkeypatch):
        _install(monkeypatch, [("ok", "ok")])
        cf, inc = yc.statements_or_empty("RBLX")
        assert not cf.empty and not inc.empty

    def test_statements_or_empty_swallows_failure(self, monkeypatch):
        _install(monkeypatch, ["raise"] * 3)
        cf, inc = yc.statements_or_empty("RBLX")
        assert cf.empty and inc.empty
