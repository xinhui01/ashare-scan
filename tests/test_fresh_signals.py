"""资金接入型首板的两个单日信号 helper 测试：止跌企稳 + 地量启动。"""
import pandas as pd

from src.services.scoring.helpers import detect_stop_falling, detect_volume_ignition


def _df(rows):
    return pd.DataFrame(rows)


def test_stop_falling_prior_weak_plus_hammer_is_stabilizing():
    # 11 天下行（prior_weak）+ 今日长下影锤子
    closes = [110, 108, 106, 104, 103, 102, 101, 100, 99, 98, 97.5, 97]
    rows = []
    for i, c in enumerate(closes[:-1]):
        rows.append({"date": f"2026-05-{i+1:02d}", "open": c + 1, "close": c,
                     "high": c + 1.2, "low": c - 1})
    # 今日：open=99 close=97 low=92.5 high=99 → 锤子（下影4.5≥2×实体2），非十字
    rows.append({"date": "2026-05-12", "open": 99, "close": 97, "high": 99, "low": 92.5})
    out = detect_stop_falling(_df(rows))
    assert out["prior_weak"] is True
    assert out["hammer"] is True
    assert out["doji"] is False
    assert out["stabilizing"] is True
    assert "长下影止跌" in out["label"]


def test_stop_falling_uptrend_not_stabilizing():
    # 持续上行，无前期走弱 → 即便今天是十字也不算止跌（prior_weak=False）
    closes = [90, 92, 94, 96, 98, 100, 102, 104, 106, 108, 110, 112]
    rows = []
    for i, c in enumerate(closes):
        rows.append({"date": f"2026-05-{i+1:02d}", "open": c, "close": c,
                     "high": c + 0.2, "low": c - 0.2})
    out = detect_stop_falling(_df(rows))
    assert out["prior_weak"] is False
    assert out["stabilizing"] is False


def test_stop_falling_short_history_returns_empty():
    rows = [{"date": f"2026-05-{i+1:02d}", "open": 10, "close": 10, "high": 10, "low": 10}
            for i in range(4)]
    out = detect_stop_falling(_df(rows))
    assert out["stabilizing"] is False
    assert out["label"] == ""


def test_volume_ignition_dry_then_spike():
    vols = [2000] * 16 + [800] * 5 + [2000]   # 22 天：前段常量 → 近5日地量 → 今日放量
    rows = [{"date": f"2026-05-{i+1:02d}", "close": 10, "volume": v} for i, v in enumerate(vols)]
    out = detect_volume_ignition(_df(rows))
    assert out["was_dry"] is True
    assert out["ignited"] is True
    assert out["today_ratio"] == 2.5


def test_volume_ignition_no_dry_phase():
    vols = [2000] * 22   # 一直放量，无地量蓄势
    rows = [{"date": f"2026-05-{i+1:02d}", "close": 10, "volume": v} for i, v in enumerate(vols)]
    out = detect_volume_ignition(_df(rows))
    assert out["was_dry"] is False
    assert out["ignited"] is False


def test_volume_ignition_short_history_returns_empty():
    rows = [{"date": f"2026-05-{i+1:02d}", "close": 10, "volume": 1000} for i in range(5)]
    out = detect_volume_ignition(_df(rows))
    assert out["ignited"] is False
    assert out["today_ratio"] is None
