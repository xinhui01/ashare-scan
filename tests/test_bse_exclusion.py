"""北交所（4/8/92 开头）全链路排除：股票池 + 涨停池。

用户不交易北交所。这些代码既不该进历史抓取（多数源不提供北交所数据，每轮失败刷屏），
也不该进涨停池/预测。这里覆盖三层：
  1. is_bse_code 判定（含沪市 B 股 900xxx 不误伤）
  2. _drop_bse_universe 从股票池剔除
  3. _sanitize_limit_up_pool 从涨停池剔除（兼容历史脏缓存）
"""
import pandas as pd

from src.utils.codes import is_bse_code
from stock_data import StockDataFetcher, _drop_bse_universe


def test_is_bse_code_matches_only_bse():
    # 北交所 / 新三板转板
    for c in ["920225", "920012", "830799", "871981", "889999", "430139"]:
        assert is_bse_code(c), c
    # 沪深主板 / 创业板 / 科创板 / 沪市 B 股 900xxx —— 都不是北交所
    for c in ["600000", "601318", "603259", "000001", "002594", "300750", "688981", "900901"]:
        assert not is_bse_code(c), c


def test_drop_bse_universe_removes_bse_rows():
    df = pd.DataFrame(
        {
            "code": ["600000", "920225", "000001", "830799", "300750"],
            "name": ["浦发银行", "北交A", "平安银行", "北交B", "宁德时代"],
        }
    )
    out = _drop_bse_universe(df)
    assert list(out["code"]) == ["600000", "000001", "300750"]


def test_drop_bse_universe_noop_when_clean():
    df = pd.DataFrame({"code": ["600000", "000001"], "name": ["a", "b"]})
    out = _drop_bse_universe(df)
    assert list(out["code"]) == ["600000", "000001"]


def test_sanitize_limit_up_pool_drops_bse():
    # 模拟东财涨停池：北交所涨停股（30% 阈值）也会出现，必须被剔除
    df = pd.DataFrame(
        {
            "代码": ["600000", "920225", "300750"],
            "名称": ["浦发银行", "北交涨停", "宁德时代"],
            "涨跌幅": [10.0, 29.9, 19.9],
            "最新价": [10.0, 5.0, 200.0],
        }
    )
    out = StockDataFetcher._sanitize_limit_up_pool(df, drop_missing_seal_time=False)
    assert list(out["代码"]) == ["600000", "300750"]
