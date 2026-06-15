import main


def test_no_command_starts_gui(monkeypatch):
    called = []
    monkeypatch.setattr(main, "_run_gui", lambda: called.append("gui") or 0)

    assert main.main([]) == 0
    assert called == ["gui"]


def test_update_cache_command_calls_fetcher(monkeypatch):
    calls = {}

    class FakeFetcher:
        def update_history_cache(self, **kwargs):
            calls.update(kwargs)
            return {"total": 2, "updated": 1, "failed": 0, "skipped": 1}

    class FakeStockFilter:
        def __init__(self):
            self.fetcher = FakeFetcher()

        def set_history_source_preference(self, source):
            calls["source_preference"] = source

    monkeypatch.setattr(main, "ensure_store_ready", lambda: None)
    monkeypatch.setattr(main, "StockFilter", FakeStockFilter)

    rc = main.main([
        "update-cache",
        "--max-stocks", "2",
        "--days", "80",
        "--workers", "4",
        "--source", "sina",
        "--refresh-universe",
        "--board", "main",
        "--board", "gem",
    ])

    assert rc == 0
    assert calls["source_preference"] == "sina"
    assert calls["max_stocks"] == 2
    assert calls["days"] == 80
    assert calls["workers"] == 4
    assert calls["source"] == "sina"
    assert calls["refresh_universe"] is True
    assert calls["allowed_boards"] == ["main", "gem"]


def test_predict_today_command_calls_predictor(monkeypatch):
    calls = {}

    class FakeStockFilter:
        def predict_limit_up_candidates(self, trade_date, **kwargs):
            calls["trade_date"] = trade_date
            calls.update(kwargs)
            return {
                "trade_date": trade_date,
                "continuation_candidates": [1, 2],
                "first_board_candidates": [1],
                "fresh_first_board_candidates": [],
                "broken_board_wrap_candidates": [1],
                "trend_limit_up_candidates": [],
            }

    monkeypatch.setattr(main, "ensure_store_ready", lambda: None)
    monkeypatch.setattr(main, "StockFilter", FakeStockFilter)
    monkeypatch.setattr(main, "_default_predict_trade_date", lambda: "20260612")

    rc = main.main(["predict-today", "--lookback", "7"])

    assert rc == 0
    assert calls["trade_date"] == "20260612"
    assert calls["lookback_days"] == 7
    assert calls["historical_mode"] is False


def test_sentiment_command_prints_summary_on_success(monkeypatch, capsys):
    import src.services.market_sentiment_service as mss

    def fake_analyze(target, **kwargs):
        return {
            "trade_date": target,
            "score": 72,
            "position_suggest": {"label": "7 成", "ratio": 0.7},
            "signals": [{"name": "涨停数", "value": "42", "delta": 5, "note": "强"}],
            "summary": "涨停 42 只。综合 72 分 → 建议 7 成。",
            "market_state": {
                "label": "接力日",
                "confidence": 0.85,
                "strategy": {"label": "连板接力", "notes": "重点 2-4 板"},
            },
        }

    monkeypatch.setattr(main, "ensure_store_ready", lambda: None)
    monkeypatch.setattr(mss, "analyze_market_sentiment", fake_analyze)

    rc = main.main(["sentiment", "--date", "20260612"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "市场情绪 20260612" in out
    assert "接力日" in out


def test_sentiment_command_prints_prerequisites_on_missing_data(monkeypatch, capsys):
    import src.services.market_sentiment_service as mss

    def fake_analyze(target, **kwargs):
        # 数据缺失分支：服务返回的 dict 不含 market_state
        return {
            "trade_date": target,
            "score": 50,
            "signals": [],
            "summary": "本地无 limit_up_pool 缓存，无法判断情绪。",
            "raw": {"missing_pool_dates": ["20260612"]},
        }

    monkeypatch.setattr(main, "ensure_store_ready", lambda: None)
    monkeypatch.setattr(mss, "analyze_market_sentiment", fake_analyze)

    rc = main.main(["sentiment", "--date", "20260612"])
    out = capsys.readouterr().out

    assert rc == 1
    assert "前置步骤" in out
    assert "无法获取" in out


def test_update_and_predict_runs_prediction_after_cache_failures(monkeypatch):
    calls = []

    monkeypatch.setattr(main, "_run_update_cache", lambda args: calls.append("cache") or 1)
    monkeypatch.setattr(main, "_run_predict_today", lambda args: calls.append("predict") or 0)

    rc = main.main(["update-and-predict"])

    assert rc == 1
    assert calls == ["cache", "predict"]
