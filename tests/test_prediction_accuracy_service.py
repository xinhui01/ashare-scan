import unittest
from unittest.mock import patch

import pandas as pd

from src.services import prediction_accuracy_service as svc


class TestPredictionAccuracyService(unittest.TestCase):
    def test_evaluate_falls_back_to_next_history_date_when_calendar_degraded(self):
        payload = {
            "trade_date": "20260430",
            "continuation_candidates": [
                {
                    "code": "000001",
                    "name": "测试股A",
                    "industry": "机器人",
                    "score": 80,
                    "predict_type": "保留涨停",
                    "consecutive_boards": 1,
                }
            ],
            "first_board_candidates": [
                {
                    "code": "000002",
                    "name": "测试股B",
                    "industry": "算力",
                    "score": 70,
                    "predict_type": "二波接力",
                }
            ],
        }
        history_a = pd.DataFrame({
            "date": ["2026-04-30", "2026-05-06"],
            "open": [10.0, 10.2],
            "high": [10.0, 11.0],
            "low": [10.0, 10.1],
            "close": [10.0, 11.0],
            "change_pct": [10.0, 10.0],
        })
        history_b = pd.DataFrame({
            "date": ["2026-04-30", "2026-05-06"],
            "open": [8.0, 8.1],
            "high": [8.0, 8.4],
            "low": [8.0, 8.0],
            "close": [8.0, 8.3],
            "change_pct": [10.0, 3.75],
        })
        saved_rows = {}

        def _load_history(code):
            return {"000001": history_a, "000002": history_b}.get(code)

        def _save_rows(rows):
            saved_rows["rows"] = rows
            return len(rows)

        with (
            patch.object(svc.stock_store, "load_limit_up_prediction_by_date", return_value=payload),
            patch.object(svc.stock_store, "load_history", side_effect=_load_history),
            patch.object(svc.stock_store, "save_prediction_accuracy_records", side_effect=_save_rows),
            patch.object(svc, "_next_trading_day_yyyymmdd", return_value="20260501"),
        ):
            result = svc.evaluate("20260430")

        self.assertEqual(result["reason"], "ok")
        self.assertEqual(result["verify_date"], "20260506")
        self.assertGreater(result["written"], 0)
        rows = saved_rows["rows"]
        self.assertTrue(rows)
        self.assertTrue(all(str(r["verify_date"]) == "20260506" for r in rows))
        main_rows = [r for r in rows if r["category"] in {"cont", "first"}]
        self.assertEqual(len(main_rows), 2)


if __name__ == "__main__":
    unittest.main()
