import sqlite3

import pandas as pd

from src.services import derived_pool_service as svc


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE history (
            code TEXT,
            trade_date TEXT,
            close REAL,
            open REAL,
            high REAL,
            low REAL,
            change_pct REAL,
            amount REAL,
            volume REAL,
            turnover_rate REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE universe (
            code TEXT,
            name TEXT,
            industry TEXT
        )
        """
    )
    return conn


def test_derived_pool_st_threshold_changes_from_20260706(monkeypatch):
    conn = _make_conn()
    conn.executemany(
        "INSERT INTO universe (code, name, industry) VALUES (?, ?, ?)",
        [
            ("000001", "ST旧规", "测试"),
            ("000002", "ST新规低涨幅", "测试"),
            ("000003", "ST新规涨停", "测试"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO history
        (code, trade_date, close, open, high, low, change_pct, amount, volume, turnover_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("000001", "2026-07-03", 10.5, 10.0, 10.5, 10.0, 5.0, 80_000_000, 1_000_000, 4.0),
            ("000002", "2026-07-06", 10.5, 10.0, 10.5, 10.0, 5.0, 80_000_000, 1_000_000, 4.0),
            ("000003", "2026-07-06", 11.0, 10.0, 11.0, 10.0, 10.0, 80_000_000, 1_000_000, 4.0),
        ],
    )

    class _Conn:
        def __enter__(self):
            return conn

        def __exit__(self, *_exc):
            return False

    monkeypatch.setattr(svc.stock_store, "_connect", lambda: _Conn())

    before = svc.derive_pool_for_date("2026-07-03")
    after = svc.derive_pool_for_date("2026-07-06")

    assert before is not None
    assert before["代码"].tolist() == ["000001"]
    assert after is not None
    assert after["代码"].tolist() == ["000003"]
