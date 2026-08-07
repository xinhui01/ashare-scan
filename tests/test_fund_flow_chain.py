"""资金流兜底链（东财→同花顺）的行为契约。

覆盖 0807 重写修掉的缺陷：东财返回空表时必须继续下探同花顺（此前直接
break 白白放弃）；东财真实成功后翻回 _FF_EM_REACHABLE（此前置 False 后
进程内永无恢复路径）。
"""
import pandas as pd
import pytest

import stock_data
from stock_data import StockDataFetcher
from src.sources.eastmoney import fund_flow as em_fund_flow


def _em_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "日期": "2026-08-07",
                "收盘价": 10.0,
                "涨跌幅": 2.0,
                "主力净流入-净额": 1_000_000.0,
                "主力净流入-净占比": 5.0,
                "大单净流入-净额": 600_000.0,
                "大单净流入-净占比": 3.0,
                "超大单净流入-净额": 400_000.0,
                "超大单净流入-净占比": 2.0,
            }
        ]
    )


def _ths_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [{"日期": "2026-08-07", "收盘价": 10.0, "主力净额": 800_000.0, "大单净额": 800_000.0}]
    )


def _fetcher() -> StockDataFetcher:
    instance = StockDataFetcher.__new__(StockDataFetcher)
    instance._log = None
    instance._notify = None
    return instance


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(stock_data, "_FF_EM_REACHABLE", True)
    monkeypatch.setattr(stock_data, "_eastmoney_circuit_breaker_open", lambda: False)
    monkeypatch.setattr(stock_data, "_save_fund_flow_store", lambda *a, **k: None)


def test_eastmoney_empty_frame_falls_through_to_ths(monkeypatch):
    ths_called = []

    monkeypatch.setattr(em_fund_flow, "fetch_individual_fund_flow", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(
        stock_data, "_fetch_ths_fund_flow_frame",
        lambda code: ths_called.append(code) or _ths_frame(),
    )

    df = _fetcher().get_fund_flow_data("000938", days=5, force_refresh=True, source="auto")

    assert ths_called == ["000938"]
    assert df is not None and not df.empty
    assert float(df.iloc[-1]["main_force_amount"]) == 800_000.0


def test_eastmoney_success_skips_ths(monkeypatch):
    ths_called = []

    monkeypatch.setattr(em_fund_flow, "fetch_individual_fund_flow", lambda *a, **k: _em_frame())
    monkeypatch.setattr(
        stock_data, "_fetch_ths_fund_flow_frame",
        lambda code: ths_called.append(code) or _ths_frame(),
    )

    df = _fetcher().get_fund_flow_data("000938", days=5, force_refresh=True, source="auto")

    assert ths_called == []
    assert df is not None
    assert float(df.iloc[-1]["big_order_amount"]) == 600_000.0


def test_eastmoney_both_paths_fail_falls_to_ths(monkeypatch):
    def _raise(*a, **k):
        raise ConnectionError("blocked")

    monkeypatch.setattr(em_fund_flow, "fetch_individual_fund_flow", _raise)
    monkeypatch.setattr(stock_data, "_retry_ak_call", _raise)
    monkeypatch.setattr(stock_data, "_fetch_ths_fund_flow_frame", lambda code: _ths_frame())

    df = _fetcher().get_fund_flow_data("000938", days=5, force_refresh=True, source="auto")

    assert df is not None and not df.empty


def test_em_success_restores_reachable_flag(monkeypatch):
    # 探针曾判死东财 → plan 为同花顺优先；同花顺失败、东财真实成功后标志翻回
    monkeypatch.setattr(stock_data, "_FF_EM_REACHABLE", False)

    def _ths_fail(code):
        raise ConnectionError("ths down")

    monkeypatch.setattr(stock_data, "_fetch_ths_fund_flow_frame", _ths_fail)
    monkeypatch.setattr(em_fund_flow, "fetch_individual_fund_flow", lambda *a, **k: _em_frame())

    df = _fetcher().get_fund_flow_data("000938", days=5, force_refresh=True, source="auto")

    assert df is not None
    assert stock_data._FF_EM_REACHABLE is True


def test_all_sources_fail_notifies_and_returns_none(monkeypatch):
    def _raise(*a, **k):
        raise ConnectionError("all down")

    notified = []
    monkeypatch.setattr(em_fund_flow, "fetch_individual_fund_flow", _raise)
    monkeypatch.setattr(stock_data, "_retry_ak_call", _raise)
    monkeypatch.setattr(stock_data, "_fetch_ths_fund_flow_frame", _raise)

    fetcher = _fetcher()
    fetcher._notify = lambda title, msg: notified.append(title)

    df = fetcher.get_fund_flow_data("000938", days=5, force_refresh=True, source="auto")

    assert df is None
    assert notified == ["资金流数据缺失"]
