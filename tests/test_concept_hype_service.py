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


def test_sustained_semiconductor_line_is_not_demoted_as_stale_industry(monkeypatch):
    dates = [
        "20260610", "20260611", "20260612", "20260615", "20260616",
        "20260617", "20260618", "20260622", "20260623", "20260624",
    ]
    semiconductor_counts = [8, 8, 1, 3, 1, 3, 2, 3, 2, 4]
    pharma_counts = [0, 0, 1, 0, 0, 1, 1, 2, 8, 4]

    def rows(prefix: str, industry: str, count: int, base: int):
        return [
            {
                "代码": f"{base + i:06d}",
                "名称": f"{prefix}{i}",
                "所属行业": industry,
                "连板数": 1,
                "涨跌幅": 10.0,
                "最新价": 10.0,
                "换手率": 8.0,
            }
            for i in range(count)
        ]

    pools = {}
    for idx, d in enumerate(dates):
        pools[d] = pd.DataFrame(
            rows("芯片", "半导体", semiconductor_counts[idx], 300000 + idx * 20)
            + rows("药", "化学制药", pharma_counts[idx], 600000 + idx * 20)
        )

    monkeypatch.setattr(svc.stock_store, "list_limit_up_pool_trade_dates", lambda: dates)
    monkeypatch.setattr(svc.stock_store, "load_limit_up_pool", lambda d: pools[d])
    monkeypatch.setattr(svc.stock_store, "lookup_concepts_batch", lambda *args, **kwargs: {})
    monkeypatch.setattr(svc, "load_cached_themes", lambda _date: {})

    result = svc.analyze_concept_hype("20260624", lookback=10)
    concepts = result["concepts"]
    semiconductor = next(c for c in concepts if c["name"] == "半导体")

    assert semiconductor["phase"] == "主升"
    assert semiconductor["opportunity_score"] >= 60
    assert result["main_line"]["name"] == "半导体"


def test_default_concept_hype_uses_auto_theme_cycle_window(monkeypatch):
    assert svc.DEFAULT_THEME_LOOKBACK_DAYS == 25

    dates = [f"2026{i:04d}" for i in range(1, 76)]
    pools = {
        d: pd.DataFrame(
            [
                {
                    "代码": f"300{i:03d}",
                    "名称": f"芯片{i}",
                    "所属行业": "半导体",
                    "连板数": 1,
                    "涨跌幅": 10.0,
                    "最新价": 10.0,
                    "换手率": 8.0,
                }
            ]
        )
        for i, d in enumerate(dates, start=1)
    }

    monkeypatch.setattr(svc.stock_store, "list_limit_up_pool_trade_dates", lambda: dates)
    monkeypatch.setattr(svc.stock_store, "load_limit_up_pool", lambda d: pools[d])
    monkeypatch.setattr(svc.stock_store, "lookup_concepts_batch", lambda *args, **kwargs: {})
    monkeypatch.setattr(svc, "load_cached_themes", lambda _date: {})

    result = svc.analyze_concept_hype(dates[-1])

    assert result["lookback_mode"] == "auto"
    assert result["lookback_days"] == svc.DEFAULT_THEME_LOOKBACK_DAYS
    assert result["lookback_label"] == "自动题材周期(25日)"
    assert result["stats"]["lookback_label"] == "自动题材周期(25日)"
    assert len(result["trade_dates"]) == svc.DEFAULT_THEME_LOOKBACK_DAYS
    assert result["trade_dates"] == dates[-svc.DEFAULT_THEME_LOOKBACK_DAYS:]
    assert result["start_date"] == dates[-svc.DEFAULT_THEME_LOOKBACK_DAYS]
