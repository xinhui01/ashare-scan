from __future__ import annotations

import pandas as pd

from src.services import relative_strength_service as svc


def _history(closes, *, start="2026-05-01") -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=len(closes)).strftime("%Y-%m-%d")
    return pd.DataFrame({"date": dates, "close": closes})


def test_load_index_history_uses_fallback_and_caches(monkeypatch):
    saved = {}

    def fail_fetcher(_symbol, _start_date, _end_date):
        raise RuntimeError("eastmoney down")

    def sina_fetcher(_symbol, _start_date, _end_date):
        return _history([100, 101, 102, 103])

    monkeypatch.setattr(svc.stock_store, "load_app_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(svc.stock_store, "save_app_config", lambda key, value: saved.setdefault(key, value))

    result = svc.load_index_history(
        "sh000001",
        target_date="20260506",
        fetchers=[("eastmoney", fail_fetcher), ("sina", sina_fetcher)],
    )

    assert result["ok"] is True
    assert result["source"] == "sina"
    assert result["history"].iloc[-1]["date"] == "2026-05-06"
    assert "index_history_sh000001" in saved


def test_load_index_history_rejects_stale_target_date(monkeypatch):
    def stale_fetcher(_symbol, _start_date, _end_date):
        return _history([100, 101, 102, 103])

    monkeypatch.setattr(svc.stock_store, "load_app_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(svc.stock_store, "save_app_config", lambda *_args, **_kwargs: None)

    result = svc.load_index_history(
        "sz399001",
        target_date="20260508",
        fetchers=[("sina", stale_fetcher)],
    )

    assert result["ok"] is False
    assert result["history"] is None
    assert "未覆盖目标日" in result["warning"]


def test_score_stock_relative_strength_rewards_real_relative_leadership():
    stock = _history(
        [
            10.0, 10.1, 10.0, 10.3, 10.5,
            10.8, 11.0, 11.2, 11.4, 11.8,
            12.0, 12.4, 12.7, 13.1, 13.5,
            13.8, 14.1, 14.5, 14.8, 15.2,
            15.6, 16.0,
        ]
    )
    index = _history(
        [
            100.0, 99.8, 99.7, 99.9, 100.1,
            100.0, 99.6, 99.5, 99.8, 100.0,
            100.2, 100.1, 99.9, 99.7, 99.8,
            100.0, 100.2, 100.0, 99.9, 100.1,
            100.0, 100.2,
        ]
    )

    result = svc.score_stock_relative_strength(
        "600001",
        stock,
        index,
        category="cont",
        boards=2,
    )

    assert result["available"] is True
    assert result["score"] >= 8
    assert result["benchmark"] == "sh000001"
    assert any("启动前强" in reason or "强弱线" in reason for reason in result["reasons"])


def test_score_stock_relative_strength_requires_matching_index_date():
    stock = _history([10, 10.2, 10.4, 10.8, 11.2, 11.5])
    index = _history([100, 101, 102, 103, 104], start="2026-05-01")

    result = svc.score_stock_relative_strength("000001", stock, index, category="fresh")

    assert result["available"] is False
    assert result["score"] is None
    assert "指数历史未覆盖目标日" in result["warning"]
