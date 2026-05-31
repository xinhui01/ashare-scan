from datetime import datetime

import pandas as pd

from src.services.opening_confirmation_service import confirm_candidate_lists


class FakeFetcher:
    def __init__(self, auctions=None, intraday=None):
        self.auctions = auctions or {}
        self.intraday = intraday or {}
        self.auction_calls = []
        self.intraday_calls = []

    def get_auction_snapshot(self, code):
        code = str(code).zfill(6)
        self.auction_calls.append(code)
        return self.auctions.get(code)

    def get_intraday_data(self, code, **kwargs):
        code = str(code).zfill(6)
        self.intraday_calls.append((code, kwargs))
        return self.intraday.get(code)


def test_confirms_buy_when_auction_gap_and_amount_are_healthy():
    fetcher = FakeFetcher(
        auctions={
            "600000": {
                "price": 10.30,
                "amount": 30_000_000,
                "trade_date": "2026-06-01",
                "source": "sina",
            }
        }
    )
    candidate_lists = {
        "fresh": [
            {
                "code": "600000",
                "name": "A",
                "close": 10.0,
                "score": 76,
            }
        ]
    }

    result = confirm_candidate_lists(
        candidate_lists,
        fetcher=fetcher,
        now=datetime(2026, 6, 1, 9, 26),
    )

    confirmation = candidate_lists["fresh"][0]["opening_confirmation"]
    assert confirmation["status"] == "可买"
    assert round(confirmation["auction_gap_pct"], 2) == 3.0
    assert confirmation["auction_source"] == "sina"
    assert result["status_counts"]["可买"] == 1


def test_marks_risk_when_main_board_auction_is_too_close_to_limit_up():
    fetcher = FakeFetcher(
        auctions={"600000": {"price": 10.92, "amount": 50_000_000}}
    )
    candidate_lists = {
        "fresh": [{"code": "600000", "close": 10.0, "score": 80}]
    }

    confirm_candidate_lists(candidate_lists, fetcher=fetcher, now=datetime(2026, 6, 1, 9, 26))

    confirmation = candidate_lists["fresh"][0]["opening_confirmation"]
    assert confirmation["status"] == "风险过高"
    assert "接近涨停" in confirmation["reason"]


def test_abandons_when_auction_opens_too_weak():
    fetcher = FakeFetcher(
        auctions={"600000": {"price": 9.60, "amount": 15_000_000}}
    )
    candidate_lists = {
        "wrap": [{"code": "600000", "close": 10.0, "score": 75}]
    }

    confirm_candidate_lists(candidate_lists, fetcher=fetcher, now=datetime(2026, 6, 1, 9, 26))

    confirmation = candidate_lists["wrap"][0]["opening_confirmation"]
    assert confirmation["status"] == "放弃"
    assert "低开过多" in confirmation["reason"]


def test_missing_auction_data_stays_observation():
    fetcher = FakeFetcher()
    candidate_lists = {
        "first": [{"code": "600000", "close": 10.0, "score": 70}]
    }

    confirm_candidate_lists(candidate_lists, fetcher=fetcher, now=datetime(2026, 6, 1, 9, 26))

    confirmation = candidate_lists["first"][0]["opening_confirmation"]
    assert confirmation["status"] == "观察"
    assert "缺竞价" in confirmation["reason"]


def test_skips_network_outside_opening_confirmation_window():
    fetcher = FakeFetcher(
        auctions={"600000": {"price": 10.30, "amount": 30_000_000}},
        intraday={"600000": pd.DataFrame([{"time": pd.Timestamp("2026-05-29 09:30:00"), "open": 10.1}])},
    )
    candidate_lists = {
        "fresh": [{"code": "600000", "close": 10.0, "score": 76}]
    }

    result = confirm_candidate_lists(
        candidate_lists,
        fetcher=fetcher,
        now=datetime(2026, 5, 31, 22, 58),
    )

    assert fetcher.auction_calls == []
    assert fetcher.intraday_calls == []
    confirmation = candidate_lists["fresh"][0]["opening_confirmation"]
    assert confirmation["status"] == "观察"
    assert "非交易日" in confirmation["reason"]
    assert result["skipped_reason"] == "非交易日"
    assert result["fetched_auction"] is False
    assert result["fetched_intraday"] is False


def test_skips_network_before_auction_match_time():
    fetcher = FakeFetcher(
        auctions={"600000": {"price": 10.30, "amount": 30_000_000}}
    )
    candidate_lists = {
        "fresh": [{"code": "600000", "close": 10.0, "score": 76}]
    }

    result = confirm_candidate_lists(
        candidate_lists,
        fetcher=fetcher,
        now=datetime(2026, 6, 1, 9, 24),
    )

    assert fetcher.auction_calls == []
    assert fetcher.intraday_calls == []
    confirmation = candidate_lists["fresh"][0]["opening_confirmation"]
    assert confirmation["status"] == "观察"
    assert "尚未到09:25" in confirmation["reason"]
    assert result["skipped_reason"] == "尚未到09:25"


def test_reuses_one_auction_fetch_for_duplicate_code_across_categories():
    fetcher = FakeFetcher(
        auctions={"600000": {"price": 10.20, "amount": 30_000_000}}
    )
    candidate_lists = {
        "fresh": [{"code": "600000", "close": 10.0, "score": 76}],
        "wrap": [{"code": "600000", "close": 10.0, "score": 78}],
    }

    confirm_candidate_lists(candidate_lists, fetcher=fetcher, now=datetime(2026, 6, 1, 9, 26))

    assert fetcher.auction_calls == ["600000"]
    assert candidate_lists["fresh"][0]["opening_confirmation"]["status"] == "可买"
    assert candidate_lists["wrap"][0]["opening_confirmation"]["status"] == "可买"


def test_after_930_weak_open_downgrades_buy_signal():
    intraday = pd.DataFrame(
        [
            {
                "time": pd.Timestamp("2026-06-01 09:30:00"),
                "open": 9.82,
                "close": 9.80,
            }
        ]
    )
    fetcher = FakeFetcher(
        auctions={"600000": {"price": 10.30, "amount": 35_000_000}},
        intraday={"600000": intraday},
    )
    candidate_lists = {
        "fresh": [{"code": "600000", "close": 10.0, "score": 82}]
    }

    confirm_candidate_lists(candidate_lists, fetcher=fetcher, now=datetime(2026, 6, 1, 9, 31))

    confirmation = candidate_lists["fresh"][0]["opening_confirmation"]
    assert confirmation["status"] == "观察"
    assert "开盘转弱" in confirmation["reason"]
