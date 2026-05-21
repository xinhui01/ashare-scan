# stock_filter Phase 3: classifier 迁移到 scoring/classifiers.py

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** 把 `classify_limit_up_pattern` / `_prefetch_history_for_pool` / `classify_limit_up_pool` 3 个涨停分类相关方法迁移到 `src/services/scoring/classifiers.py`，模式：模块级函数 + 参数注入（fetcher/log_fn 等通过参数传入）。

**Architecture:** 1 commit。新建 classifiers.py 含 3 个模块级函数，stock_filter.py 中保留 thin delegate 方法。

**Spec:** [`docs/superpowers/specs/2026-05-20-stock-filter-modularization-design.md`](../specs/2026-05-20-stock-filter-modularization-design.md)

**模板：** `src/services/scoring/shared.py`（P2，纯函数）+ `src/services/scoring/helpers.py`（P1，模块级）

---

## Task 0 — 基线

- pytest 318 passed
- stock_filter.py 当前 ~4680 行

---

## Task 1 — 创建 src/services/scoring/classifiers.py

**目标函数（从 stock_filter.py 迁移）：**

| 旧（StockFilter 方法） | 新（模块级函数） | 行号 | 约行数 |
|---|---|---|---|
| `classify_limit_up_pattern(self, stock_code, board, stock_name)` | `classify_limit_up_pattern(fetcher, stock_code, *, board, stock_name, log_fn=None, limit_up_threshold_fn=None)` | 1096 | 220 |
| `_prefetch_history_for_pool(self, codes, days, progress_callback, cache_only)` | `prefetch_history_for_pool(fetcher, codes, days, progress_callback, cache_only, *, log_fn=None)` | 1316 | 82 |
| `classify_limit_up_pool(self, pool_records, progress_callback)` | `classify_limit_up_pool(fetcher, pool_records, progress_callback, *, log_fn=None, limit_up_threshold_fn=None)` | 1398 | 60 |

**写入 classifiers.py：**

```python
"""涨停形态分类与批量分类编排。

3 个模块级函数（参数注入模式）：
- classify_limit_up_pattern: 单股技术形态分类（连板/反包/突破/超跌等）
- prefetch_history_for_pool: 批量预取涨停池股票历史数据到本地缓存
- classify_limit_up_pool: 涨停池每只股票批量分类

依赖：StockDataFetcher（通过 fetcher 参数注入）+ 可选 log_fn / limit_up_threshold_fn。
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.utils.daemon_executor import DaemonThreadPoolExecutor

logger = logging.getLogger(__name__)


def classify_limit_up_pattern(
    fetcher,
    stock_code: str,
    *,
    board: str = "",
    stock_name: str = "",
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_fn: Optional[Callable[[str, str], float]] = None,
) -> Dict[str, Any]:
    """对涨停股进行技术形态分类（迁自 StockFilter.classify_limit_up_pattern）。"""
    # 完整搬 stock_filter.py:1096-1314 函数体
    # 把 self.fetcher → fetcher, self._log → log_fn, self._limit_up_threshold(board, name) → limit_up_threshold_fn(board, name) or 10.0


def prefetch_history_for_pool(
    fetcher,
    codes: List[str],
    days: int = 65,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    cache_only: bool = False,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> None:
    """批量预取涨停池股票的历史数据（迁自 StockFilter._prefetch_history_for_pool）。"""
    # 完整搬 stock_filter.py:1316-1396 函数体
    # self.fetcher → fetcher, self._log → log_fn


def classify_limit_up_pool(
    fetcher,
    pool_records: List[Dict[str, Any]],
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_fn: Optional[Callable[[str, str], float]] = None,
) -> List[Dict[str, Any]]:
    """涨停池批量分类（迁自 StockFilter.classify_limit_up_pool）。"""
    # 完整搬 stock_filter.py:1398-1450 函数体
    # 注意：内部调用 self._prefetch_history_for_pool / self.classify_limit_up_pattern 改为
    # prefetch_history_for_pool(fetcher, ...) / classify_limit_up_pattern(fetcher, ...)
```

---

## Task 2 — stock_filter.py 改 thin delegate

```python
# 顶部 import
from src.services.scoring import classifiers as _scoring_classifiers

# 方法体替换
def classify_limit_up_pattern(
    self, stock_code: str, board: str = "", stock_name: str = "",
) -> Dict[str, Any]:
    return _scoring_classifiers.classify_limit_up_pattern(
        self.fetcher, stock_code,
        board=board, stock_name=stock_name,
        log_fn=self._log,
        limit_up_threshold_fn=self._limit_up_threshold,
    )

def _prefetch_history_for_pool(
    self, codes: List[str], days: int = 65,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    cache_only: bool = False,
) -> None:
    return _scoring_classifiers.prefetch_history_for_pool(
        self.fetcher, codes, days, progress_callback, cache_only,
        log_fn=self._log,
    )

def classify_limit_up_pool(
    self, pool_records: List[Dict[str, Any]],
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> List[Dict[str, Any]]:
    return _scoring_classifiers.classify_limit_up_pool(
        self.fetcher, pool_records, progress_callback,
        log_fn=self._log,
        limit_up_threshold_fn=self._limit_up_threshold,
    )
```

---

## Task 3 — 验证 + commit

- [ ] pytest 318 不下降
- [ ] import check
- [ ] GUI 6s 启动
- [ ] 行数：stock_filter.py ~4680 → ~4310，classifiers.py ~360
- [ ] Commit

```powershell
git -C D:\code\python\gupiao add src/services/scoring/classifiers.py stock_filter.py
git -C D:\code\python\gupiao commit -m @'
重构（stock_filter P3）：抽涨停分类到 scoring/classifiers.py

按 stock_filter.py 模块化拆分 spec 的 Phase 3，把 classify_limit_up_pattern
（220 行）/ _prefetch_history_for_pool（82 行）/ classify_limit_up_pool 3 个
涨停形态分类相关方法迁移到 src/services/scoring/classifiers.py。

模式：模块级函数 + 参数注入（fetcher / log_fn / limit_up_threshold_fn 通过
参数传入），与 P2 shared.py 一致。StockFilter 主类保留 thin delegate
方法保持向后兼容。

零行为变化，pytest 318 全绿。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```
