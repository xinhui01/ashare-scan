import pandas as pd

from src.services import concept_hype_service as svc


def test_local_concept_tags_are_primary_main_line_without_llm(monkeypatch):
    dates = ["20260616", "20260617", "20260618"]
    pools = {
        d: pd.DataFrame(
            [
                {
                    "代码": "300001",
                    "名称": "机甲一号",
                    "所属行业": "通用设备",
                    "连板数": 2,
                    "涨跌幅": 20.0,
                    "最新价": 10.0,
                    "换手率": 8.0,
                },
                {
                    "代码": "300002",
                    "名称": "机甲二号",
                    "所属行业": "通用设备",
                    "连板数": 1,
                    "涨跌幅": 20.0,
                    "最新价": 11.0,
                    "换手率": 7.0,
                },
            ]
        )
        for d in dates
    }

    monkeypatch.setattr(
        svc.stock_store,
        "list_limit_up_pool_trade_dates",
        lambda: dates,
    )
    monkeypatch.setattr(
        svc.stock_store,
        "load_limit_up_pool",
        lambda d: pools[d],
    )
    monkeypatch.setattr(
        svc.stock_store,
        "lookup_concepts_batch",
        lambda codes, per_code_limit=12, sources=None: {
            "300001": ["机器人"],
            "300002": ["机器人"],
        },
    )

    def fail_if_llm_is_required(_date):
        raise RuntimeError("model url unavailable")

    monkeypatch.setattr(svc, "load_cached_themes", fail_if_llm_is_required)

    result = svc.analyze_concept_hype("20260618", lookback=3)

    assert result["main_line"]["name"] == "机器人"
    assert result["main_line"]["source"] == "概念"
    assert result["stats"]["primary_source"] == "概念"
    assert result["stats"]["concept_pairs"] == 2
    assert result["stats"]["llm_cache_days"] == 0
