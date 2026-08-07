"""T+1 卖点建议：连板与其余类别的最优卖点相反，必须分开给结论。"""
from src.services import exit_timing
from src.services.exit_timing import (
    HOLD_TO_CLOSE,
    SELL_AT_AUCTION,
    compute_exit_hints,
)


def _row(category, t_close, t1_open, t1_close):
    return (category, t_close, t1_open, t1_close)


def _rows(category, n, *, open_gap_pct, close_gap_pct):
    """造 n 条样本：T+1 竞价价与收盘价相对 T 日收盘的涨幅固定。"""
    base = 10.0
    return [
        _row(category, base, base * (1 + open_gap_pct / 100), base * (1 + close_gap_pct / 100))
        for _ in range(n)
    ]


def test_hold_to_close_when_afternoon_still_rises():
    # 竞价 -0.6%，收盘 +0.3% → 留到收盘多赚 0.9 个点（二波的真实形态）
    rows = _rows("first", 50, open_gap_pct=-0.6, close_gap_pct=0.3)
    hints = compute_exit_hints(load_rows_fn=lambda _d: rows)

    hint = hints["first"]
    assert hint.advice == HOLD_TO_CLOSE
    assert hint.samples == 50
    assert abs(hint.edge_pct - 0.9) < 0.01


def test_sell_at_auction_when_open_is_the_high():
    # 竞价 +2.8%，收盘 +2.2% → 开盘后倒亏 0.6 个点（连板的真实形态）
    rows = _rows("cont", 50, open_gap_pct=2.8, close_gap_pct=2.2)
    hints = compute_exit_hints(load_rows_fn=lambda _d: rows)

    hint = hints["cont"]
    assert hint.advice == SELL_AT_AUCTION
    assert hint.edge_pct < 0


def test_opposite_conclusions_coexist_in_one_run():
    """核心契约：同一轮里连板和二波必须给出相反建议。"""
    rows = (
        _rows("first", 40, open_gap_pct=-0.6, close_gap_pct=0.3)
        + _rows("cont", 40, open_gap_pct=2.8, close_gap_pct=2.2)
    )
    hints = compute_exit_hints(load_rows_fn=lambda _d: rows)

    assert hints["first"].advice == HOLD_TO_CLOSE
    assert hints["cont"].advice == SELL_AT_AUCTION


def test_flat_edge_defaults_to_holding():
    """收益差在噪音带内不该建议多卖一次（省一次冲击成本）。"""
    rows = _rows("wrap", 50, open_gap_pct=1.0, close_gap_pct=1.05)
    hints = compute_exit_hints(load_rows_fn=lambda _d: rows)

    assert hints["wrap"].advice == HOLD_TO_CLOSE


def test_thin_sample_falls_back_to_default():
    rows = _rows("first", 5, open_gap_pct=5.0, close_gap_pct=-5.0)  # 极端但样本不足
    hints = compute_exit_hints(load_rows_fn=lambda _d: rows)

    hint = hints["first"]
    assert hint.samples == 0
    assert hint.from_data is False
    assert hint.advice == HOLD_TO_CLOSE          # 用兜底默认，不被 5 条样本带跑
    assert hint.describe().endswith("(默认)")


def test_db_failure_falls_back_to_defaults():
    def _boom(_days):
        raise RuntimeError("db down")

    hints = compute_exit_hints(load_rows_fn=_boom)

    assert hints["cont"].advice == SELL_AT_AUCTION
    assert hints["first"].advice == HOLD_TO_CLOSE
    assert all(h.samples == 0 for h in hints.values())


def test_bad_rows_are_skipped_not_fatal():
    rows = [
        ("first", 0, 10.0, 10.0),        # t_close=0
        ("first", 10.0, None, 10.0),     # 缺竞价价
        ("first", 10.0, 10.0),           # 字段不全
        (None, 10.0, 10.0, 10.0),        # 无类别
    ] + _rows("first", 40, open_gap_pct=-0.6, close_gap_pct=0.3)

    hints = compute_exit_hints(load_rows_fn=lambda _d: rows)

    assert hints["first"].samples == 40


def test_real_db_query_shape_is_valid():
    """真实库跑一次，确保 SQL 与表结构没脱节（不断言具体结论）。"""
    hints = exit_timing.compute_exit_hints(lookback_days=120)

    assert set(hints) == {"cont", "first", "wrap", "fresh", "trend"}
    for hint in hints.values():
        assert hint.advice in (HOLD_TO_CLOSE, SELL_AT_AUCTION)
