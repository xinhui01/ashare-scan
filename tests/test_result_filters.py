"""结果表客户端过滤谓词测试。纯 Python，无 Tk 依赖。"""
import unittest

from src.gui import result_filters as rf


def _item(
    code="000001",
    name="平安银行",
    *,
    score=60,
    five_day_return=5.0,
    volume_expand_ratio=1.5,
    limit_up_streak=0,
    latest_close=10.0,
    limit_up=False,
    broken_limit_up=False,
    volume_expand=False,
    strong_ft=False,
):
    return {
        "code": code,
        "name": name,
        "data": {
            "analysis": {
                "score": score,
                "five_day_return": five_day_return,
                "volume_expand_ratio": volume_expand_ratio,
                "limit_up_streak": limit_up_streak,
                "latest_close": latest_close,
                "limit_up": limit_up,
                "broken_limit_up": broken_limit_up,
                "volume_expand": volume_expand,
                "strong_followthrough": {"has_strong_followthrough": strong_ft},
            },
        },
    }


class TestSearch(unittest.TestCase):
    def test_empty_needle_keeps_all(self):
        self.assertTrue(rf.matches_search(_item(), ""))
        self.assertTrue(rf.matches_search(_item(), "   "))

    def test_match_by_code(self):
        self.assertTrue(rf.matches_search(_item(code="600036"), "600"))

    def test_match_by_name(self):
        self.assertTrue(rf.matches_search(_item(name="贵州茅台"), "茅台"))

    def test_case_insensitive(self):
        self.assertTrue(rf.matches_search(_item(name="iFlytek"), "IFLY"))

    def test_no_match(self):
        self.assertFalse(rf.matches_search(_item(code="000001", name="平安"), "xyz"))


class TestMinScore(unittest.TestCase):
    def test_none_threshold_keeps_all(self):
        self.assertTrue(rf.at_least_score(_item(score=10), None))

    def test_meets_threshold(self):
        self.assertTrue(rf.at_least_score(_item(score=70), 70))

    def test_below_threshold(self):
        self.assertFalse(rf.at_least_score(_item(score=50), 70))

    def test_none_score_filtered_out(self):
        self.assertFalse(rf.at_least_score(_item(score=None), 10))


class TestMinFiveDayReturn(unittest.TestCase):
    def test_none_threshold(self):
        self.assertTrue(rf.at_least_five_day_return(_item(), None))

    def test_meets(self):
        self.assertTrue(rf.at_least_five_day_return(_item(five_day_return=12.0), 10.0))

    def test_below(self):
        self.assertFalse(rf.at_least_five_day_return(_item(five_day_return=3.0), 10.0))

    def test_negative_return(self):
        self.assertFalse(rf.at_least_five_day_return(_item(five_day_return=-5.0), 0.0))
        self.assertTrue(rf.at_least_five_day_return(_item(five_day_return=-5.0), -10.0))


class TestMinVolumeRatio(unittest.TestCase):
    def test_none_threshold(self):
        self.assertTrue(rf.at_least_volume_ratio(_item(), None))

    def test_meets(self):
        self.assertTrue(rf.at_least_volume_ratio(_item(volume_expand_ratio=2.5), 2.0))

    def test_below(self):
        self.assertFalse(rf.at_least_volume_ratio(_item(volume_expand_ratio=1.2), 2.0))


class TestMinStreak(unittest.TestCase):
    def test_none_or_zero_threshold(self):
        self.assertTrue(rf.at_least_limit_up_streak(_item(limit_up_streak=0), None))
        self.assertTrue(rf.at_least_limit_up_streak(_item(limit_up_streak=0), 0))

    def test_meets(self):
        self.assertTrue(rf.at_least_limit_up_streak(_item(limit_up_streak=3), 2))

    def test_below(self):
        self.assertFalse(rf.at_least_limit_up_streak(_item(limit_up_streak=1), 2))


class TestOnlyWatchlist(unittest.TestCase):
    def test_disabled_keeps_all(self):
        self.assertTrue(rf.only_in_watchlist(_item(code="000001"), False, set()))

    def test_in_watchlist_passes(self):
        self.assertTrue(rf.only_in_watchlist(_item(code="000001"), True, {"000001"}))

    def test_not_in_watchlist_rejected(self):
        self.assertFalse(rf.only_in_watchlist(_item(code="000002"), True, {"000001"}))

    def test_code_is_padded(self):
        # item code="1" → padded to "000001"
        self.assertTrue(rf.only_in_watchlist(_item(code="1"), True, {"000001"}))


class TestOnlyFlags(unittest.TestCase):
    def test_only_limit_up(self):
        self.assertTrue(rf.only_limit_up(_item(limit_up=True), True))
        self.assertFalse(rf.only_limit_up(_item(limit_up=False), True))
        self.assertTrue(rf.only_limit_up(_item(limit_up=False), False))  # disabled

    def test_only_broken_limit_up(self):
        self.assertTrue(rf.only_broken_limit_up(_item(broken_limit_up=True), True))
        self.assertFalse(rf.only_broken_limit_up(_item(broken_limit_up=False), True))

    def test_only_volume_expand(self):
        self.assertTrue(rf.only_volume_expand(_item(volume_expand=True), True))
        self.assertFalse(rf.only_volume_expand(_item(volume_expand=False), True))

    def test_only_strong_followthrough(self):
        self.assertTrue(rf.only_strong_followthrough(_item(strong_ft=True), True))
        self.assertFalse(rf.only_strong_followthrough(_item(strong_ft=False), True))
        self.assertTrue(rf.only_strong_followthrough(_item(strong_ft=False), False))


class TestWithinPriceRange(unittest.TestCase):
    def test_both_none_keeps_all(self):
        self.assertTrue(rf.within_price_range(_item(latest_close=10.0), None, None))

    def test_within_bounds(self):
        self.assertTrue(rf.within_price_range(_item(latest_close=10.0), 5.0, 15.0))

    def test_below_min(self):
        self.assertFalse(rf.within_price_range(_item(latest_close=4.0), 5.0, None))

    def test_above_max(self):
        self.assertFalse(rf.within_price_range(_item(latest_close=20.0), None, 15.0))

    def test_missing_close_rejected_when_filter_active(self):
        self.assertFalse(rf.within_price_range(_item(latest_close=None), 5.0, None))

    def test_missing_close_kept_when_no_filter(self):
        self.assertTrue(rf.within_price_range(_item(latest_close=None), None, None))


class TestIntegrationChain(unittest.TestCase):
    """模拟 stock_gui 里的 chain：过完板块 + 价格 + 快速过滤 后剩什么。"""

    def test_multiple_predicates_all_must_pass(self):
        item = _item(
            score=80, five_day_return=15.0, volume_expand_ratio=3.0,
            limit_up_streak=2, latest_close=12.0, limit_up=True,
        )
        # 每条都满足
        self.assertTrue(rf.at_least_score(item, 70))
        self.assertTrue(rf.at_least_five_day_return(item, 10))
        self.assertTrue(rf.at_least_volume_ratio(item, 2.0))
        self.assertTrue(rf.at_least_limit_up_streak(item, 2))
        self.assertTrue(rf.within_price_range(item, 5, 20))
        self.assertTrue(rf.only_limit_up(item, True))

    def test_one_fail_rejects(self):
        item = _item(score=80, five_day_return=5.0)  # 5 日涨幅不足
        self.assertTrue(rf.at_least_score(item, 70))
        self.assertFalse(rf.at_least_five_day_return(item, 10))


if __name__ == "__main__":
    unittest.main()
