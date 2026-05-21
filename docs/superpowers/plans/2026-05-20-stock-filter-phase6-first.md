# stock_filter Phase 6: first scorer (二波接力) 迁移到 scoring/first.py

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** 把 `_scan_followthrough_candidates_cached` + `_score_followthrough_candidate` 2 个二波接力相关方法迁移到 `src/services/scoring/first.py`。

**Architecture:** 1 commit。同 P5 cont 模式。

**Spec:** `docs/superpowers/specs/2026-05-20-stock-filter-modularization-design.md`

**模板：** `src/services/scoring/cont.py`（P5，最近完成）

---

## 目标方法

| 方法 | 行号 | 约行数 |
|---|---|---|
| `_scan_followthrough_candidates_cached` | 1858 | ~40 |
| `_score_followthrough_candidate` | 1897 | ~440 |

---

## Task 1 — 创建 src/services/scoring/first.py

```python
"""二波接力（first）评分。

2 个函数：
- scan_followthrough_candidates_cached: 从强势股池扫候选
- score_followthrough_candidate: 主评分（含历史命中、量比、距 MA5 等多维度）
"""
from __future__ import annotations
# imports: pandas, typing, logging, shared, helpers._count_historical_followthrough

def scan_followthrough_candidates_cached(
    fetcher,
    spot_df,
    zt_codes,
    hot_industries,
    compare_context,
    *,
    lookback_days=5,
    progress_callback=None,
    log_fn=None,
    limit_up_threshold_fn=None,
    build_local_cache_history_plan_fn=None,
    filter_strong_stocks_fn=None,  # 注入 _filter_strong_stocks（P10 才迁到 first_board）
) -> List[Dict[str, Any]]:
    """完整搬 stock_filter.py:1858 函数体"""

def score_followthrough_candidate(
    rec, hot_industries, compare_context,
    *,
    fetcher,
    lookback_days=5,
    log_fn=None,
    limit_up_threshold_fn=None,
    build_local_cache_history_plan_fn=None,
) -> Optional[Dict[str, Any]]:
    """完整搬 stock_filter.py:1897 函数体（~440 行）。
    
    self.fetcher → fetcher
    self._log → log_fn
    self._theme_bonus → _shared.theme_bonus
    self._capital_flow_bonus → _shared.capital_flow_bonus
    self._vol_ratio_with_baseline → _shared.vol_ratio_with_baseline
    self._limit_up_threshold_pct → 内联或注入
    self._build_local_cache_history_plan → build_local_cache_history_plan_fn
    _count_historical_followthrough → from helpers import
    """
```

---

## Task 2 — stock_filter.py thin delegate

```python
from src.services.scoring import first as _scoring_first

def _scan_followthrough_candidates_cached(self, spot_df, zt_codes, hot_industries, compare_context, *, lookback_days=5, progress_callback=None):
    return _scoring_first.scan_followthrough_candidates_cached(
        self.fetcher, spot_df, zt_codes, hot_industries, compare_context,
        lookback_days=lookback_days,
        progress_callback=progress_callback,
        log_fn=self._log,
        limit_up_threshold_fn=self._limit_up_threshold,
        build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
        filter_strong_stocks_fn=self._filter_strong_stocks,
    )

def _score_followthrough_candidate(self, rec, hot_industries, compare_context, *, lookback_days=5):
    return _scoring_first.score_followthrough_candidate(
        rec, hot_industries, compare_context,
        fetcher=self.fetcher,
        lookback_days=lookback_days,
        log_fn=self._log,
        limit_up_threshold_fn=self._limit_up_threshold,
        build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
    )
```

注意：签名按原方法**完整展开**（grep 各原方法的 def 头）。

---

## Task 3 — 验证 + commit

- pytest 318 不下降（特别 test_strong_followthrough / test_limit_up_prediction / test_historical_pattern_count）
- import check
- GUI 6s
- 行数：stock_filter.py ~3530 → ~3050，first.py ~480
- Commit
