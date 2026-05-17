import unittest

from stock_data import StockDataFetcher


class TestLimitUpReasonParser(unittest.TestCase):
    def test_parse_limit_up_reason_page(self):
        html = """
        <html><body>
        <table>
          <tr><th>时间</th><th>股票名称</th><th>异动标记</th><th>板块题材</th><th>盘面信息</th></tr>
          <tr>
            <td>09:25</td><td>利仁科技</td><td>大单一字</td><td>股权转让</td>
            <td>股权转让利仁科技大单一字、连续加速，五连板</td>
          </tr>
          <tr>
            <td>09:30</td><td>多氟多</td><td>直线拉升</td><td>氢氟酸</td>
            <td>氢氟酸多氟多直线拉升首板涨停</td>
          </tr>
        </table>
        <div>ID 股票名称 涨幅%</div>
        </body></html>
        """
        parsed = StockDataFetcher._parse_limit_up_reason_page(html)
        self.assertEqual(parsed["利仁科技"]["reason"], "股权转让")
        self.assertIn("五连板", parsed["利仁科技"]["detail"])
        self.assertEqual(parsed["多氟多"]["reason"], "氢氟酸")

    def test_enrich_limit_up_reason_fields(self):
        fetcher = StockDataFetcher()
        fetcher._limit_up_reason_cache["auto:20260515"] = {
            "利仁科技": {"reason": "股权转让", "detail": "股权转让利仁科技大单一字、连续加速，五连板"},
        }
        fetcher.get_limit_up_strong_tag = lambda code, trade_date: "60日新高且近期多次涨停"  # type: ignore[method-assign]

        records = [{"code": "001259", "name": "利仁科技"}]
        enriched = fetcher.enrich_limit_up_reason_fields(records, "20260515")
        self.assertEqual(enriched[0]["limit_up_reason"], "股权转让")
        self.assertIn("五连板", enriched[0]["limit_up_reason_detail"])
        self.assertEqual(enriched[0]["strong_tag"], "60日新高且近期多次涨停")


if __name__ == "__main__":
    unittest.main()
