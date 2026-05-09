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


class TestEvaluateCandidateOpenCloseCriteria(unittest.TestCase):
    """新成功口径：开盘买、收盘卖。"""

    def _eval(self, t1_open, t1_high, t1_low, t1_close, t1_change_pct, code="000100"):
        df = pd.DataFrame({
            "date": ["2026-04-30", "2026-05-06"],
            "open": [10.0, t1_open],
            "high": [10.0, t1_high],
            "low": [10.0, t1_low],
            "close": [10.0, t1_close],
            "change_pct": [10.0, t1_change_pct],
        })
        return svc._evaluate_candidate(
            code=code, name="测试股", history_df=df,
            trade_date_dash="2026-04-30", verify_date_dash="2026-05-06",
        )

    def test_open_high_close_low_is_failure(self):
        # 开盘 +8% (10.8)，收盘 +6% (10.6) —— 按开盘买入收盘卖出实际亏损 ~1.85%
        r = self._eval(t1_open=10.8, t1_high=10.9, t1_low=10.5, t1_close=10.6, t1_change_pct=6.0)
        self.assertAlmostEqual(r["t1_open_close_pct"], (10.6 - 10.8) / 10.8 * 100.0, places=4)
        self.assertFalse(r["hit_loose"], "开盘买亏损时不应记为弱命中")
        self.assertFalse(r["hit_strict"])  # 没有涨停

    def test_open_low_close_high_is_success(self):
        # 开盘 +1% (10.1)，收盘 +7% (10.7) —— 开盘买入收盘卖出 ~5.94%，命中
        r = self._eval(t1_open=10.1, t1_high=10.8, t1_low=10.0, t1_close=10.7, t1_change_pct=7.0)
        self.assertGreaterEqual(r["t1_open_close_pct"], 5.0)
        self.assertTrue(r["hit_loose"])
        self.assertFalse(r["hit_strict"])

    def test_limit_up_still_hit_strict_regardless_of_open(self):
        # 涨停（+9.95%）虽然开盘已经 +9%，仍是 hit_strict
        r = self._eval(t1_open=10.9, t1_high=11.0, t1_low=10.7, t1_close=10.995, t1_change_pct=9.95)
        self.assertTrue(r["hit_strict"])
        self.assertTrue(r["hit_loose"])

    def test_one_word_not_buyable(self):
        # 一字板：开高低收都是涨停价，hit_buyable=False
        r = self._eval(t1_open=11.0, t1_high=11.0, t1_low=11.0, t1_close=11.0, t1_change_pct=10.0)
        self.assertTrue(r["t1_one_word"])
        self.assertFalse(r["hit_buyable"])
        self.assertFalse(r["hit_strict"])
        self.assertFalse(r["hit_loose"])


class TestRowIsHit(unittest.TestCase):
    """按类别选取成功口径。"""

    def test_cont_uses_strict(self):
        # 保留涨停类：必须 hit_strict 才算成功
        row_strict = {"category": "cont", "hit_buyable": 1, "hit_strict": 1, "hit_loose": 1}
        row_loose_only = {"category": "cont", "hit_buyable": 1, "hit_strict": 0, "hit_loose": 1}
        self.assertTrue(svc._row_is_hit(row_strict))
        self.assertFalse(svc._row_is_hit(row_loose_only))

    def test_cont_subcategory_also_uses_strict(self):
        row = {"category": "cont_2to3", "hit_buyable": 1, "hit_strict": 0, "hit_loose": 1}
        self.assertFalse(svc._row_is_hit(row))

    def test_other_categories_use_loose(self):
        for cat in ("first", "fresh", "wrap", "trend"):
            row = {"category": cat, "hit_buyable": 1, "hit_strict": 0, "hit_loose": 1}
            self.assertTrue(svc._row_is_hit(row), f"{cat} 应当走 hit_loose")

    def test_unbuyable_never_hit(self):
        row = {"category": "first", "hit_buyable": 0, "hit_strict": 1, "hit_loose": 1}
        self.assertFalse(svc._row_is_hit(row))


if __name__ == "__main__":
    unittest.main()
