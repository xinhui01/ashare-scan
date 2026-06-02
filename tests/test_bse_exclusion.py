"""北交所（4/8/92 开头）全链路排除：股票池 + 涨停池。

用户不交易北交所。这些代码既不该进历史抓取（多数源不提供北交所数据，每轮失败刷屏），
也不该进涨停池/预测。这里覆盖三层：
  1. is_bse_code 判定（含沪市 B 股 900xxx 不误伤）
  2. _drop_bse_universe 从股票池剔除
  3. _sanitize_limit_up_pool 从涨停池剔除（兼容历史脏缓存）
"""
import pandas as pd

from data_source_models import HistoryRequestPlan
from src.services.scoring import first_board
from src.services.scoring.predict import _check_prerequisites, _drop_bse_rows
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


def test_get_history_data_skips_bse_network(monkeypatch):
    fetcher = StockDataFetcher.__new__(StockDataFetcher)
    fetcher._log = lambda msg: None
    fetcher._default_history_source = "auto"

    monkeypatch.setattr("stock_data._load_history_store", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "stock_data._fetch_sina_hist_frame",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("BSE history must not hit network providers")
        ),
    )

    plan = HistoryRequestPlan(
        mode="network",
        provider_sequence=("sina",),
        reason="test-sina",
    )

    assert fetcher.get_history_data("920225", days=1, force_refresh=True, request_plan=plan) is None


def test_prediction_drops_bse_spot_and_pool_rows():
    df = pd.DataFrame(
        {
            "代码": ["000001", "920225", "830799", "300750"],
            "名称": ["平安银行", "北交A", "北交B", "宁德时代"],
        }
    )

    out = _drop_bse_rows(df)

    assert list(out["代码"]) == ["000001", "300750"]


def test_prediction_parse_spot_record_skips_bse():
    row = {
        "代码": "920225",
        "名称": "北交测试",
        "最新价": 10.0,
        "涨跌幅": 5.0,
        "成交额": 10000_0000,
    }

    assert first_board.parse_spot_record(row, set()) is None


def test_prediction_prereq_does_not_prefetch_bse_history():
    calls = []

    class _Fetcher:
        def get_history_data(self, code, *args, **kwargs):
            calls.append(code)
            return None

    missing = _check_prerequisites(
        historical_mode=False,
        pool_source="cache_db",
        concept_themes_count=1,
        board_strength={"银行": 1.0},
        sentiment_degraded=False,
        zt_codes={"920225"},
        fetcher=_Fetcher(),
        build_local_cache_history_plan_fn=lambda **kwargs: object(),
        log_fn=lambda *_: None,
    )

    assert missing == []
    assert calls == []
