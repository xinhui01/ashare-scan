# stock_filter Phase 5: cont scorer 迁移到 scoring/cont.py

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** 把 `_score_continuation`（保留涨停核心评分，~160 行）+ `_score_continuation_by_compare`（基于涨停对比的辅助评分，~130 行）2 个方法迁移到 `src/services/scoring/cont.py`。

**Architecture:** 1 commit。同 P3/P4 模式：模块级函数 + 参数注入。

**Spec:** [`docs/superpowers/specs/2026-05-20-stock-filter-modularization-design.md`](../specs/2026-05-20-stock-filter-modularization-design.md)

**模板：** `src/services/scoring/profile.py`（P4，最近完成）

---

## Task 0

- pytest 318 passed
- stock_filter.py ~4121 行
- 目标方法行号：
  - `_score_continuation` (1827)
  - `_score_continuation_by_compare` (1989)

---

## Task 1 — 创建 src/services/scoring/cont.py

模块级函数：

```python
"""保留涨停（cont）评分。

2 个函数：
- score_continuation: 主评分（涨停股技术形态 + 板块热度 + 量价 + 历史命中）
- score_continuation_by_compare: 基于涨停对比的辅助评分（环境定盘）
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from src.services.scoring import shared as _shared
from src.services.scoring.helpers import _count_historical_continuation

logger = logging.getLogger(__name__)


def score_continuation(
    rec: Dict[str, Any],
    hot_industries: Dict[str, int],
    *,
    fetcher,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_fn: Optional[Callable[[str, str], float]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[[str], Any]] = None,
    # 如果方法内部还引用 self.xxx，按需添加参数
) -> Dict[str, Any]:
    """完整搬 stock_filter.py:1827 函数体（约 160 行）。
    
    替换：
    - self.fetcher → fetcher
    - self._log → log_fn (if log_fn else 无操作)
    - self._theme_bonus(...) → _shared.theme_bonus(...)
    - self._capital_flow_bonus(...) → _shared.capital_flow_bonus(...)
    - self._vol_ratio_with_baseline(...) → _shared.vol_ratio_with_baseline(...)
    - self._limit_up_threshold(...) → limit_up_threshold_fn(...) or fallback
    - self._build_local_cache_history_plan(...) → build_local_cache_history_plan_fn(...) if injected
    - _count_historical_continuation(...) 直接 from helpers import 调用
    """


def score_continuation_by_compare(
    rec: Dict[str, Any],
    compare_context: Dict[str, Any],
    *,
    fetcher,
    log_fn: Optional[Callable[[str], None]] = None,
    # 视 self.xxx 引用添加
) -> Dict[str, Any]:
    """完整搬 stock_filter.py:1989 函数体。"""
```

---

## Task 2 — stock_filter.py thin delegate

```python
from src.services.scoring import cont as _scoring_cont

def _score_continuation(self, rec, hot_industries):
    return _scoring_cont.score_continuation(
        rec, hot_industries,
        fetcher=self.fetcher,
        log_fn=self._log,
        limit_up_threshold_fn=self._limit_up_threshold,
        build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
    )

def _score_continuation_by_compare(self, rec, compare_context):
    return _scoring_cont.score_continuation_by_compare(
        rec, compare_context,
        fetcher=self.fetcher,
        log_fn=self._log,
        # ... 按需
    )
```

注意：原方法签名展开（不要 *args）。

---

## Task 3 — 验证 + commit

- pytest 318 不下降（特别看 test_scoring / test_limit_up_prediction）
- import check
- GUI 6s
- 行数：stock_filter.py ~4121 → ~3830，cont.py ~330
- Commit：

```
重构（stock_filter P5）：抽 cont scorer 到 scoring/cont.py

score_continuation（160 行）+ score_continuation_by_compare（130 行）
2 个方法迁到 scoring/cont.py。模块级函数 + 参数注入，复用 P2 shared 模块
的 theme_bonus / capital_flow_bonus / vol_ratio_with_baseline。
StockFilter 主类保留 thin delegate。零行为变化，pytest 318 全绿。
```

## 风险点

1. **`_score_continuation` 内部对 self.xxx 引用密度高**：grep 一遍，按需添加注入参数
2. **历史形态加分逻辑**（`_count_historical_continuation`）已在 P1 提取，直接 import 调用
3. **`_score_continuation_by_compare` 可能内部调 `self._score_continuation`**：若有，改 `score_continuation(rec, hot_industries, fetcher=fetcher, ...)`
4. **score_continuation 跟 shared.theme_bonus 等的调用顺序**：保持原代码顺序，不要重排
