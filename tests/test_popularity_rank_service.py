from src.services import popularity_rank_service as svc


def test_eastmoney_stock_rank_symbol_normalizes_a_share_codes():
    assert svc.eastmoney_stock_rank_symbol("002585") == "SZ002585"
    assert svc.eastmoney_stock_rank_symbol("600000") == "SH600000"
    assert svc.eastmoney_stock_rank_symbol("688001") == "SH688001"


def test_get_stock_popularity_rank_matches_target_trade_date():
    def fake_fetch(symbol):
        assert symbol == "SZ002585"
        return [
            {"trade_date": "20260602", "rank": 88, "source": "fake"},
            {"trade_date": "2026-06-03", "rank": 16, "source": "fake"},
        ]

    out = svc.get_stock_popularity_rank(
        "002585", "20260603", fetch_history_func=fake_fetch,
    )

    assert out == {
        "trade_date": "20260603",
        "rank": 16,
        "symbol": "SZ002585",
        "source": "fake",
    }


def test_enrich_wrap_candidates_adds_rank_bonus_and_reason():
    candidates = [
        {"code": "002585", "score": 72, "reasons": "断板反包"},
        {"code": "600000", "score": 75, "reasons": "放量承接"},
    ]

    def fake_fetch(symbol):
        mapping = {
            "SZ002585": [{"trade_date": "20260603", "rank": 18, "source": "fake"}],
            "SH600000": [{"trade_date": "20260603", "rank": 260, "source": "fake"}],
        }
        return mapping[symbol]

    stats = svc.enrich_wrap_candidates_with_popularity(
        candidates, "2026-06-03", fetch_history_func=fake_fetch, max_workers=1,
    )

    by_code = {row["code"]: row for row in candidates}
    assert stats == {"total": 2, "hit": 2, "missing": 0, "bonus": 1}
    assert by_code["002585"]["popularity_rank"] == 18
    assert by_code["002585"]["popularity_bonus"] == 8
    assert by_code["002585"]["score"] == 80
    assert "人气18名+8" in by_code["002585"]["reasons"]
    assert by_code["600000"]["popularity_rank"] == 260
    assert by_code["600000"]["score"] == 75
