# stock_filter Phase 2: 共享 helper 迁移到 scoring/shared.py 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** 把 stock_filter.py 中 5 个跨 scorer 复用的共享 helper（`_parse_full_pool`、`_count_pool_industries`、`_theme_bonus`、`_capital_flow_bonus`、`_vol_ratio_with_baseline`）迁移到 `src/services/scoring/shared.py`，StockFilter 主类保留 thin delegate 方法以兼容现有调用方。

**Architecture:** 1 commit。新建 shared.py 含 5 个模块级函数（无 self 依赖，纯函数 / 静态方法），stock_filter.py 中保留同名方法体调 shared 模块。

**Tech Stack:** Python 3.12 + pandas

**Spec:** [`docs/superpowers/specs/2026-05-20-stock-filter-modularization-design.md`](../specs/2026-05-20-stock-filter-modularization-design.md)

**模板参考：** `src/services/scoring/helpers.py`（P1 已建立）

---

## Task 0 — 基线

- [ ] `git status` 干净
- [ ] pytest `318 passed`
- [ ] stock_filter.py 当前 ~4411 行

---

## Task 1 — 创建 src/services/scoring/shared.py

**Files:**
- Create: `D:\code\python\gupiao\src\services\scoring\shared.py`

5 个目标函数（stock_filter.py 行号）：
- `_parse_full_pool` (2351-2373) → `parse_full_pool` (去 `_` 前缀)
- `_count_pool_industries` (2375-2380) → `count_pool_industries`（已是 @staticmethod）
- `_theme_bonus` (2382-2416) → `theme_bonus`
- `_capital_flow_bonus` (2418-2542) → `capital_flow_bonus`
- `_vol_ratio_with_baseline` (2544-2566) → `vol_ratio_with_baseline`

**写入 shared.py：**

```python
"""跨 scorer 复用的评分调节因子。

5 个无状态函数：
- parse_full_pool: 把涨停池 DataFrame 转 records 列表
- count_pool_industries: 涨停池行业分布
- theme_bonus: AI 题材聚类热度加分
- capital_flow_bonus: 龙虎榜 + 北向资金 + 板块涨跌幅加分
- vol_ratio_with_baseline: 5/20 日量比双口径计算

设计：纯函数 / 静态方法，无 self.fetcher 依赖。所需上下文（compare_context 等）以参数注入。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def parse_full_pool(pool_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """将涨停池 DataFrame 解析为完整记录列表（包含所有连板数）。"""
    # 完整搬 stock_filter.py:2353-2372 函数体


def count_pool_industries(pool_df: pd.DataFrame) -> Dict[str, int]:
    """涨停池按行业计数（≥1 只）。"""
    # 完整搬 stock_filter.py:2377-2380 函数体


def theme_bonus(
    code: str,
    industry: str,
    compare_context: Dict[str, Any],
) -> Tuple[float, Optional[str]]:
    """根据 AI 题材聚类缓存返回题材热度加分。"""
    # 完整搬 stock_filter.py:2395-2416 函数体


def capital_flow_bonus(
    code: str,
    compare_context: Dict[str, Any],
    *,
    industry: str = "",
    boards: int = 0,
) -> Tuple[float, List[str]]:
    """龙虎榜 + 北向资金 + 板块涨跌幅加分（含 LHB 解读字段细分）。"""
    # 完整搬 stock_filter.py:2425-2542 函数体


def vol_ratio_with_baseline(
    volume: pd.Series,
    t: int,
) -> Tuple[Optional[float], Optional[float]]:
    """同时计算 5 日量比与 20 日量比。"""
    # 完整搬 stock_filter.py:2555-2566 函数体
```

注意：去掉 `self` 参数，函数体内部如果引用 `self.xxx` 全部改为参数（理论上这 5 个函数都不需要 self）。

---

## Task 2 — stock_filter.py 中保留 thin delegate

**Files:**
- Modify: `D:\code\python\gupiao\stock_filter.py`

- [ ] **Step 1: 顶部 import 新模块**

在 `from src.services.scoring.helpers import ...` 后加：

```python
from src.services.scoring import shared as _scoring_shared
```

- [ ] **Step 2: 替换 5 个原方法为 thin delegate**

把 `_parse_full_pool` / `_count_pool_industries` / `_theme_bonus` / `_capital_flow_bonus` / `_vol_ratio_with_baseline` 的方法体替换为：

```python
def _parse_full_pool(self, pool_df: pd.DataFrame) -> List[Dict[str, Any]]:
    return _scoring_shared.parse_full_pool(pool_df)

@staticmethod
def _count_pool_industries(pool_df: pd.DataFrame) -> Dict[str, int]:
    return _scoring_shared.count_pool_industries(pool_df)

def _theme_bonus(
    self, code: str, industry: str, compare_context: Dict[str, Any],
) -> Tuple[float, Optional[str]]:
    return _scoring_shared.theme_bonus(code, industry, compare_context)

def _capital_flow_bonus(
    self, code: str, compare_context: Dict[str, Any],
    *, industry: str = "", boards: int = 0,
) -> Tuple[float, List[str]]:
    return _scoring_shared.capital_flow_bonus(
        code, compare_context, industry=industry, boards=boards,
    )

def _vol_ratio_with_baseline(
    self, volume: pd.Series, t: int,
) -> Tuple[Optional[float], Optional[float]]:
    return _scoring_shared.vol_ratio_with_baseline(volume, t)
```

注意：保留原方法签名（`self` 参数仍在），保持兼容。

---

## Task 3 — 验证 + commit

- [ ] **pytest 318 不下降**
- [ ] **import check**：
  ```powershell
  .venv\Scripts\python -c "from src.services.scoring import shared; from stock_filter import StockFilter; print('OK')"
  ```
- [ ] **GUI 启动 6s**
- [ ] **行数对比**：stock_filter.py ~4411 → ~4250（减 ~160），shared.py ~150
- [ ] **Commit**：

```powershell
git -C D:\code\python\gupiao add src/services/scoring/shared.py stock_filter.py
git -C D:\code\python\gupiao commit -m @'
重构（stock_filter P2）：抽 5 个跨 scorer 共享 helper 到 scoring/shared.py

按 stock_filter.py 模块化拆分 spec 的 Phase 2，把 parse_full_pool /
count_pool_industries / theme_bonus / capital_flow_bonus /
vol_ratio_with_baseline 5 个无 self.fetcher 依赖的纯函数迁移到
src/services/scoring/shared.py。

StockFilter 主类保留 thin delegate 方法（同名 + 同签名），保证外部
（5 个 scorer + 测试）老调用方无需改 import / 调用方式。

零行为变化，pytest 318 全绿。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## 自检表

| 检查 | 状态 |
|---|---|
| shared.py 5 个函数无 self 依赖 / 纯函数式 | ✅ |
| StockFilter thin delegate 保持原签名 | ✅ |
| 外部调用（5 个 scorer + 测试）无需改动 | ✅ |
| pytest 不下降 | ✅ |
| GUI 启动正常 | ✅ |
