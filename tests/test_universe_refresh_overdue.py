"""股票池自动刷新：universe_refresh_overdue 的判定逻辑。"""
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import stock_data


class UniverseRefreshOverdueTests(unittest.TestCase):
    def setUp(self):
        self.fetcher = stock_data.StockDataFetcher()

    def _with_marker(self, value):
        return patch.object(
            stock_data, "load_app_config_store", lambda key, default=None: value
        )

    def test_missing_marker_is_overdue(self):
        with self._with_marker(""):
            self.assertTrue(self.fetcher.universe_refresh_overdue())

    def test_fresh_marker_not_overdue(self):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._with_marker(now):
            self.assertFalse(self.fetcher.universe_refresh_overdue())

    def test_old_marker_is_overdue(self):
        old = (datetime.now() - timedelta(days=4)).strftime("%Y-%m-%d %H:%M:%S")
        with self._with_marker(old):
            self.assertTrue(self.fetcher.universe_refresh_overdue(max_age_days=3))

    def test_within_threshold_not_overdue(self):
        recent = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        with self._with_marker(recent):
            self.assertFalse(self.fetcher.universe_refresh_overdue(max_age_days=3))

    def test_unparsable_marker_is_overdue(self):
        with self._with_marker("not-a-date"):
            self.assertTrue(self.fetcher.universe_refresh_overdue())


if __name__ == "__main__":
    unittest.main()
