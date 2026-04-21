"""扫描结果列注册表：路径解析、格式化、排序键、完整性验证。"""
import unittest

from src.gui.result_columns import (
    RESULT_COLUMNS,
    ResultColumn,
    all_column_ids,
    columns_by_id,
    default_visible_ids,
    desc_by_default_ids,
    _walk_path,
)


def _make_item(**overrides):
    base = {
        "code": "000001",
        "name": "平安银行",
        "data": {
            "board": "主板",
            "exchange": "SZ",
            "analysis": {
                "score": 72,
                "latest_close": 12.34,
                "latest_ma": 11.56,
                "five_day_return": 8.45,
                "limit_up_streak": 2,
                "broken_limit_up": True,
                "volume_expand_ratio": 2.15,
                "volume_expand": True,
                "volume_break_limit_up": False,
                "after_two_limit_up": True,
                "limit_up": False,
                "recent_closes": [10.1, 10.5, 11.2, 11.8, 12.34],
                "strong_followthrough": {
                    "has_strong_followthrough": True,
                    "limit_up_date": "2026-04-15",
                    "pullback_pct": 2.35,
                    "pullback_volume_ratio": 0.62,
                    "hold_days": 3,
                },
            },
        },
    }
    base.update(overrides)
    return base


class TestWalkPath(unittest.TestCase):
    def test_single_level(self):
        self.assertEqual(_walk_path({"a": 1}, "a"), 1)

    def test_nested(self):
        self.assertEqual(_walk_path({"a": {"b": {"c": 42}}}, "a.b.c"), 42)

    def test_missing_returns_none(self):
        self.assertIsNone(_walk_path({"a": 1}, "a.b"))

    def test_none_midway(self):
        self.assertIsNone(_walk_path({"a": None}, "a.b"))

    def test_non_dict_midway(self):
        self.assertIsNone(_walk_path({"a": [1, 2]}, "a.b"))


class TestFormatCell(unittest.TestCase):
    def setUp(self):
        self.ctx = {"watchlist_items": {}}
        self.item = _make_item()
        self.by_id = columns_by_id()

    def test_code_formatting(self):
        self.assertEqual(self.by_id["code"].format_cell(self.item, self.ctx), "000001")

    def test_code_pads_with_zeros(self):
        item = _make_item(code="1")
        self.assertEqual(self.by_id["code"].format_cell(item, self.ctx), "000001")

    def test_score_int_format(self):
        self.assertEqual(self.by_id["score"].format_cell(self.item, self.ctx), "72")

    def test_score_none_shows_placeholder(self):
        item = _make_item()
        item["data"]["analysis"]["score"] = None
        self.assertEqual(self.by_id["score"].format_cell(item, self.ctx), "0")

    def test_float_with_suffix(self):
        self.assertEqual(
            self.by_id["volume_expand_ratio"].format_cell(self.item, self.ctx),
            "2.15x",
        )

    def test_float_pct(self):
        self.assertEqual(
            self.by_id["five_day_return"].format_cell(self.item, self.ctx),
            "8.45%",
        )

    def test_bool_cn(self):
        self.assertEqual(self.by_id["broken_limit_up"].format_cell(self.item, self.ctx), "是")
        self.assertEqual(self.by_id["limit_up"].format_cell(self.item, self.ctx), "否")

    def test_board_falls_back_to_exchange(self):
        item = _make_item()
        item["data"]["board"] = ""
        self.assertEqual(self.by_id["board"].format_cell(item, self.ctx), "SZ")

    def test_watch_column(self):
        ctx = {"watchlist_items": {"000001": {}}}
        self.assertEqual(self.by_id["watch"].format_cell(self.item, ctx), "自选")
        self.assertEqual(self.by_id["watch"].format_cell(self.item, {"watchlist_items": {}}), "")

    def test_recent_closes_joined(self):
        self.assertEqual(
            self.by_id["recent_closes"].format_cell(self.item, self.ctx),
            "10.10, 10.50, 11.20, 11.80, 12.34",
        )

    def test_recent_closes_handles_none_in_list(self):
        item = _make_item()
        item["data"]["analysis"]["recent_closes"] = [10.0, None, 11.0]
        self.assertEqual(
            self.by_id["recent_closes"].format_cell(item, self.ctx),
            "10.00, -, 11.00",
        )

    def test_strong_ft_columns(self):
        self.assertEqual(
            self.by_id["strong_followthrough"].format_cell(self.item, self.ctx), "是"
        )
        self.assertEqual(
            self.by_id["strong_ft_limit_up_date"].format_cell(self.item, self.ctx),
            "2026-04-15",
        )
        self.assertEqual(
            self.by_id["strong_ft_pullback_pct"].format_cell(self.item, self.ctx),
            "2.35%",
        )
        # pullback_volume_ratio 用 "%" 显示(0.62 → "62%"),与过滤失败原因
        # 里的 `:.0%` 单位保持一致,避免同指标两种写法。
        self.assertEqual(
            self.by_id["strong_ft_volume_ratio"].format_cell(self.item, self.ctx),
            "62%",
        )
        self.assertEqual(
            self.by_id["strong_ft_hold_days"].format_cell(self.item, self.ctx), "3"
        )

    def test_missing_nested_path_shows_placeholder(self):
        item = _make_item()
        item["data"]["analysis"]["strong_followthrough"] = {}
        self.assertEqual(
            self.by_id["strong_ft_pullback_pct"].format_cell(item, self.ctx), "-"
        )


class TestSortKey(unittest.TestCase):
    def setUp(self):
        self.ctx = {"watchlist_items": {}}
        self.by_id = columns_by_id()

    def test_float_sort_key(self):
        item = _make_item()
        self.assertEqual(
            self.by_id["latest_close"].sort_key(item, self.ctx), 12.34
        )

    def test_none_float_sorts_to_bottom(self):
        item = _make_item()
        item["data"]["analysis"]["latest_close"] = None
        self.assertEqual(
            self.by_id["latest_close"].sort_key(item, self.ctx), float("-inf")
        )

    def test_bool_sort_key(self):
        item_true = _make_item()
        item_false = _make_item()
        item_false["data"]["analysis"]["broken_limit_up"] = False
        self.assertEqual(self.by_id["broken_limit_up"].sort_key(item_true, self.ctx), 1)
        self.assertEqual(self.by_id["broken_limit_up"].sort_key(item_false, self.ctx), 0)

    def test_watch_sort_reads_from_context(self):
        item = _make_item()
        self.assertEqual(self.by_id["watch"].sort_key(item, self.ctx), 0)
        ctx = {"watchlist_items": {"000001": {}}}
        self.assertEqual(self.by_id["watch"].sort_key(item, ctx), 1)

    def test_recent_closes_sort_is_tuple(self):
        item = _make_item()
        result = self.by_id["recent_closes"].sort_key(item, self.ctx)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 5)


class TestRegistryIntegrity(unittest.TestCase):
    def test_ids_are_unique(self):
        ids = [col.id for col in RESULT_COLUMNS]
        self.assertEqual(len(ids), len(set(ids)))

    def test_columns_by_id_covers_all(self):
        mapping = columns_by_id()
        self.assertEqual(len(mapping), len(RESULT_COLUMNS))

    def test_all_column_ids_matches_registry_order(self):
        self.assertEqual(
            all_column_ids(),
            tuple(col.id for col in RESULT_COLUMNS),
        )

    def test_default_visible_is_subset(self):
        visible = set(default_visible_ids())
        all_ids = set(all_column_ids())
        self.assertTrue(visible.issubset(all_ids))

    def test_desc_by_default_is_subset(self):
        desc = desc_by_default_ids()
        all_ids = set(all_column_ids())
        self.assertTrue(desc.issubset(all_ids))

    def test_each_column_either_has_path_or_extract(self):
        for col in RESULT_COLUMNS:
            with self.subTest(col=col.id):
                self.assertTrue(
                    col.path is not None or col.extract is not None,
                    f"{col.id} has neither path nor extract",
                )


if __name__ == "__main__":
    unittest.main()
