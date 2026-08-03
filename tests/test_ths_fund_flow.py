import unittest
from unittest import mock

import pandas as pd

from src.sources import ths


def _board_row(code="002165", name="红宝丽"):
    """同花顺 ggzjl 榜单页的单行（列名带「(元)」后缀，与真实页面一致）。"""
    return pd.DataFrame(
        [
            {
                "序号": 1,
                "股票代码": code,
                "股票简称": name,
                "最新价": 8.88,
                "涨跌幅": 3.21,
                "换手率": 5.6,
                "流入资金(元)": 9876543,
                "流出资金(元)": 8641976,
                "净额(元)": 1234567,
                "成交额(元)": 55667788,
            }
        ]
    )


class THSFundFlowTests(unittest.TestCase):
    @mock.patch("src.sources.ths._ths_locate_code_row")
    def test_fetch_fund_flow_frame_maps_fields_for_target_code(self, mock_locate):
        mock_locate.return_value = _board_row()

        result = ths.fetch_fund_flow_frame("002165")

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["股票代码"], "002165")
        self.assertEqual(result.iloc[0]["大单净额"], 1234567)
        self.assertEqual(result.iloc[0]["主力净额"], 1234567)
        self.assertEqual(result.iloc[0]["收盘价"], 8.88)
        self.assertIn("日期", result.columns)

    @mock.patch("src.sources.ths._ths_locate_code_row")
    def test_fetch_fund_flow_frame_retries_once_after_vcode_reset(self, mock_locate):
        mock_locate.side_effect = [pd.DataFrame(), _board_row()]
        ths._THS_VCODE_CACHE["v"] = "stale-vcode"

        result = ths.fetch_fund_flow_frame("002165")

        self.assertEqual(mock_locate.call_count, 2)
        self.assertIsNone(ths._THS_VCODE_CACHE["v"])  # 首次未命中应清空 vcode 缓存
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["股票代码"], "002165")

    @mock.patch("src.sources.ths._ths_locate_code_row")
    def test_fetch_fund_flow_frame_returns_empty_when_not_found(self, mock_locate):
        mock_locate.return_value = pd.DataFrame()

        result = ths.fetch_fund_flow_frame("002165")

        self.assertTrue(result.empty)


if __name__ == "__main__":
    unittest.main()
