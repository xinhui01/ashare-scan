"""反包评分的板块信号必须经 em_industry_map 映射后再查。

候选 industry 是 spot 的证监会粗命名（"计算机应用"），hot_industries 的 key
是涨停池的东财窄命名（"教育"），两者实测 0% 对得上——fresh/first/trend 早已
用 em_industry_map 修好，wrap 一直漏着，那两行加分是死代码。
"""
import pandas as pd
import pytest

from src.services.scoring import wrap as wrap_scoring


class _FakeFetcher:
    def __init__(self, df):
        self._df = df

    def get_history_data(self, code, days=120, force_refresh=False, request_plan=None, **_kw):
        return self._df


def _wrap_history() -> pd.DataFrame:
    """造一段满足反包硬性条件的日线：3 连板 → 断板大阴 → 今日仍低于前涨停价。"""
    rows = []
    price = 10.0
    for i in range(20):  # 平台期
        rows.append({"date": f"2026-07-{i + 1:02d}", "open": price, "close": price,
                     "high": price * 1.01, "low": price * 0.99, "volume": 5000})
    for i in range(3):   # 3 连板（每日 +10%）
        price = round(price * 1.1, 2)
        rows.append({"date": f"2026-07-{21 + i:02d}", "open": price, "close": price,
                     "high": price, "low": price * 0.98, "volume": 20000})
    prior_lu_close = price
    price = round(price * 0.92, 2)  # 断板大阴 -8%
    rows.append({"date": "2026-07-24", "open": prior_lu_close, "close": price,
                 "high": prior_lu_close, "low": price, "volume": 18000})
    price = round(price * 0.98, 2)  # 今日小跌，仍低于前涨停价 → 有反包缺口
    rows.append({"date": "2026-07-25", "open": price, "close": price,
                 "high": price * 1.01, "low": price * 0.98, "volume": 9000})
    return pd.DataFrame(rows)


def _rec() -> dict:
    # industry 用证监会粗命名，与 hot_industries 的东财窄命名对不上
    return {"code": "003032", "name": "传智教育", "change_pct": -2.0,
            "turnover": 7.0, "industry": "计算机应用"}


def _score(hot_industries: dict, compare_context: dict) -> dict:
    out = wrap_scoring.score_broken_board_wrap(
        _rec(), hot_industries, compare_context, fetcher=_FakeFetcher(_wrap_history()),
    )
    assert out is not None, "测试用的日线未能通过反包硬性条件，用例本身失效"
    assert "score" in out and "link_hot_count" in out
    return out


@pytest.fixture
def baseline() -> dict:
    """板块无涨停时的基准分，联动加分应相对它计算。"""
    return _score({}, {})


def test_hot_sector_adds_10_after_em_industry_map_translation(baseline):
    """修复前：候选 industry='计算机应用' 查不到 hot_industries['教育']，加分永远是 0。"""
    out = _score({"教育": 4}, {"em_industry_map": {"003032": "教育"}})

    assert out["link_hot_count"] == 4
    assert out["score"] - baseline["score"] == 10


def test_linked_sector_adds_5(baseline):
    out = _score({"教育": 2}, {"em_industry_map": {"003032": "教育"}})

    assert out["link_hot_count"] == 2
    assert out["score"] - baseline["score"] == 5


def test_falls_back_to_raw_industry_when_map_missing(baseline):
    """映射缺失时回退原 industry，命名恰好一致的场景仍要能触发。"""
    out = _score({"计算机应用": 2}, {})

    assert out["link_hot_count"] == 2
    assert out["score"] - baseline["score"] == 5


def test_cold_sector_adds_nothing(baseline):
    out = _score({"教育": 1}, {"em_industry_map": {"003032": "教育"}})

    assert out["link_hot_count"] == 1
    assert out["score"] == baseline["score"]


def test_unmapped_candidate_gets_nothing_without_the_map(baseline):
    """回归锁定：给东财口径的热板块但不给映射，raw industry 对不上就是 0 分。

    这正是修复前的实际处境（信号死）。修好后该场景行为不变，两者的差异由
    上面的用例锁定——有映射拿 10 分，没映射拿 0 分。
    """
    out = _score({"教育": 4}, {})

    assert out["link_hot_count"] == 0
    assert out["score"] == baseline["score"]
