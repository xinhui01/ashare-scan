from datetime import datetime

from src.gui.tabs.predict import _normalize_predict_trade_date


def test_normalize_single_day_uses_current_month():
    now = datetime(2026, 6, 5, 10, 30)

    assert _normalize_predict_trade_date("4", now=now) == "20260604"
    assert _normalize_predict_trade_date("04", now=now) == "20260604"


def test_normalize_short_month_day_uses_current_year():
    now = datetime(2026, 6, 5, 10, 30)

    assert _normalize_predict_trade_date("6/4", now=now) == "20260604"
    assert _normalize_predict_trade_date("6-4", now=now) == "20260604"


def test_normalize_full_date_keeps_yyyymmdd():
    now = datetime(2026, 6, 5, 10, 30)

    assert _normalize_predict_trade_date("2026-06-04", now=now) == "20260604"
    assert _normalize_predict_trade_date("20260604", now=now) == "20260604"


def test_normalize_future_day_falls_back_to_previous_month():
    # 用户必指过去：6/10 输 "25" 应理解为 5 月 25 日而非未来的 6/25
    now = datetime(2026, 6, 10, 10, 30)

    assert _normalize_predict_trade_date("25", now=now) == "20260525"


def test_normalize_future_day_falls_back_across_year():
    # 1 月输 "5" 且 5 号还没到 → 回退到去年 12 月
    now = datetime(2026, 1, 3, 10, 30)

    assert _normalize_predict_trade_date("5", now=now) == "20251205"


def test_normalize_future_month_day_falls_back_to_last_year():
    # 1 月输 "12/31" 必指去年年底，而非今年年底
    now = datetime(2026, 1, 5, 10, 30)

    assert _normalize_predict_trade_date("12/31", now=now) == "20251231"
    assert _normalize_predict_trade_date("1231", now=now) == "20251231"


def test_normalize_invalid_dates_return_empty():
    now = datetime(2026, 6, 10, 10, 30)

    # 6 月没有 31 日（上月 5 月构造前已先抛 ValueError）
    assert _normalize_predict_trade_date("31", now=now) == ""
    assert _normalize_predict_trade_date("2/30", now=now) == ""
    # 8 位直通必须通过 strptime 校验
    assert _normalize_predict_trade_date("20260631", now=now) == ""
    # 三段式优先校验，不能拼成 "20261345" 放行
    assert _normalize_predict_trade_date("2026-13-45", now=now) == ""


def test_normalize_chinese_date_formats():
    now = datetime(2026, 6, 10, 10, 30)

    assert _normalize_predict_trade_date("2026年6月4日", now=now) == "20260604"
    assert _normalize_predict_trade_date("6月4号", now=now) == "20260604"


def test_normalize_empty_input_returns_empty():
    now = datetime(2026, 6, 10, 10, 30)

    assert _normalize_predict_trade_date("", now=now) == ""
    assert _normalize_predict_trade_date(None, now=now) == ""
