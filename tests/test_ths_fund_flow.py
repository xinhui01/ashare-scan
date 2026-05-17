import unittest
from unittest import mock

import pandas as pd

from src.sources import ths


class THSFundFlowTests(unittest.TestCase):
    @mock.patch("src.sources.ths.ak.stock_fund_flow_individual")
    def test_fetch_fund_flow_frame_maps_fields_for_target_code(self, mock_fetch):
        mock_fetch.return_value = pd.DataFrame(
            [
                {
                    "股票代码": "000001",
                    "股票简称": "平安银行",
                    "最新价": 12.34,
                    "涨跌幅": 1.23,
                    "净额": 4567890,
                    "成交额": 99887766,
                },
                {
                    "股票代码": "002165",
                    "股票简称": "红宝丽",
                    "最新价": 8.88,
                    "涨跌幅": 3.21,
                    "净额": 1234567,
                    "成交额": 55667788,
                },
            ]
        )

        result = ths.fetch_fund_flow_frame("002165")

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["股票代码"], "002165")
        self.assertEqual(result.iloc[0]["大单净额"], 1234567)
        self.assertEqual(result.iloc[0]["主力净额"], 1234567)
        self.assertEqual(result.iloc[0]["收盘价"], 8.88)
        self.assertIn("日期", result.columns)


if __name__ == "__main__":
    unittest.main()
