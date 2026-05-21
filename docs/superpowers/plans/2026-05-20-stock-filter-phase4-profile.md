# stock_filter Phase 4: profile 迁移到 scoring/profile.py

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** 把 `_extract_pre_limit_up_features` / `analyze_pre_limit_up_profile` / `_aggregate_profile` 3 个 pre-limit-up profile 分析方法迁移到 `src/services/scoring/profile.py`。

**Architecture:** 1 commit。同 P3 模式：模块级函数 + 参数注入。

**Spec:** [`docs/superpowers/specs/2026-05-20-stock-filter-modularization-design.md`](../specs/2026-05-20-stock-filter-modularization-design.md)

**模板：** `src/services/scoring/classifiers.py`（P3，参数注入模式）

---

## Task 0 — 基线

- pytest 318 passed
- stock_filter.py 当前 ~4387 行

---

## Task 1 — 创建 src/services/scoring/profile.py

**目标函数（从 stock_filter.py 迁移）：**

| 旧（StockFilter 方法） | 新（模块级函数） | 行号 |
|---|---|---|
| `_extract_pre_limit_up_features(self, code, ...)` | `extract_pre_limit_up_features(fetcher, code, *, log_fn=None)` | 1242 |
| `analyze_pre_limit_up_profile(self, ...)` | `analyze_pre_limit_up_profile(fetcher, *, log_fn=None)` | 1372 |
| `_aggregate_profile(samples)` @staticmethod | `aggregate_profile(samples)` | 1514 |

写入 profile.py：

```python
"""Pre-limit-up 特征提取与 profile 聚合。

3 个模块级函数（参数注入）：
- extract_pre_limit_up_features: 单股涨停日前特征提取
- analyze_pre_limit_up_profile: 批量历史涨停股 profile 分析
- aggregate_profile: 批量样本聚合统计

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn。
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def extract_pre_limit_up_features(
    fetcher,
    code: str,
    # ... 其他参数照搬
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Optional[Dict[str, Any]]:
    """完整搬 stock_filter.py:1242 函数体，self.fetcher → fetcher, self._log → log_fn。"""


def analyze_pre_limit_up_profile(
    fetcher,
    # ... 其他参数
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """完整搬 stock_filter.py:1372 函数体。内部调 self._extract_pre_limit_up_features →
    extract_pre_limit_up_features(fetcher, ...); 调 self._aggregate_profile → aggregate_profile()。"""


def aggregate_profile(samples: List[Dict[str, Any]]) -> Dict[str, Any]:
    """完整搬 stock_filter.py:1514 函数体（@staticmethod 在新模块去掉装饰器）。"""
```

---

## Task 2 — stock_filter.py thin delegate

```python
# 顶部 import
from src.services.scoring import profile as _scoring_profile

# 替换 3 个原方法为 delegate
def _extract_pre_limit_up_features(self, code: str, *args, **kwargs):
    return _scoring_profile.extract_pre_limit_up_features(
        self.fetcher, code, *args, log_fn=self._log, **kwargs,
    )

def analyze_pre_limit_up_profile(self, *args, **kwargs):
    return _scoring_profile.analyze_pre_limit_up_profile(
        self.fetcher, *args, log_fn=self._log, **kwargs,
    )

@staticmethod
def _aggregate_profile(samples):
    return _scoring_profile.aggregate_profile(samples)
```

注意：保留原方法签名（_extract / analyze 参数列表照原样），上面用 *args/**kwargs 是简化示意；**实际实施时按原签名展开**。

---

## Task 3 — 验证 + commit

- pytest 318 不下降
- import check
- GUI 6s
- 行数：stock_filter.py ~4387 → ~4080，profile.py ~300
- Commit：

```
重构（stock_filter P4）：抽 pre-limit-up profile 到 scoring/profile.py

extract_pre_limit_up_features / analyze_pre_limit_up_profile /
aggregate_profile 3 个方法迁到 scoring/profile.py，参数注入模式。
StockFilter 主类保留 thin delegate。零行为变化，pytest 318 全绿。
```

## 风险点

- 这 3 个方法的参数列表可能较长，**按原签名完整展开**
- `analyze_pre_limit_up_profile` 内部可能调 `self._extract_pre_limit_up_features` 多次（批量循环），要全部改为模块级 `extract_pre_limit_up_features(fetcher, ...)` 调用
- `_aggregate_profile` 是 @staticmethod，迁到模块去掉装饰器
