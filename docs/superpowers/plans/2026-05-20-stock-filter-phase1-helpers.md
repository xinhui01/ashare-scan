# stock_filter Phase 1: scoring 包骨架 + 模块级 helper 迁移 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** 建立 `src/services/scoring/` 包骨架，把 stock_filter.py 顶部 3 个模块级 helper（`_count_historical_continuation` / `_count_historical_followthrough` / `_count_historical_wrap`）迁移到 `src/services/scoring/helpers.py`，stock_filter.py 通过 re-export 保持向后兼容。

**Architecture:** 1 commit。创建 scoring 包 + helpers.py 含 3 个函数 + stock_filter.py 顶部改为 re-export。零行为改动，单测继续从 stock_filter 导入。

**Tech Stack:** Python 3.12 + pandas

**Spec:** [`docs/superpowers/specs/2026-05-20-stock-filter-modularization-design.md`](../specs/2026-05-20-stock-filter-modularization-design.md)

---

## Task 0 — 基线

- [ ] `git status` 干净
- [ ] pytest `318 passed`
- [ ] stock_filter.py 当前 ~4539 行

---

## Task 1 — 创建 src/services/scoring/ 包

**Files:**
- Create: `D:\code\python\gupiao\src\services\scoring\__init__.py`
- Create: `D:\code\python\gupiao\src\services\scoring\helpers.py`

- [ ] **Step 1: 创建 __init__.py**

写入：

```python
"""scoring 包：涨停预测评分模块化拆分。

按 stock_filter.py 模块化 spec 拆出的子模块：
- helpers.py — 模块级 K 线历史形态统计 helper
- shared.py — 跨 scorer 复用的评分调节因子（theme/capital flow/vol baseline）
- classifiers.py — 涨停形态分类
- profile.py — pre-limit-up 特征提取与 profile 聚合
- cont.py / first.py / fresh.py / wrap.py / trend.py / first_board.py — 5 个主类别 scorer
- predict.py — predict_limit_up_candidates 主编排
"""
```

- [ ] **Step 2: 创建 helpers.py 含 3 个函数**

从 stock_filter.py 复制 3 个模块级函数（约 27-167 行）的**完整实现**到 `src/services/scoring/helpers.py`，保留所有签名、docstring、注释。

文件顶部需要的 import：

```python
"""K 线历史形态统计 helper：识别"成功二波接力/连板/反包"形态。

3 个函数无状态，输入是 history DataFrame + 配置参数，输出是 (occurrence_count, days_since_last_hit) 元组。
"""
from __future__ import annotations

from typing import Callable, Optional, Tuple

import pandas as pd
```

然后整体复制 3 个函数：
- `_count_historical_continuation(history_df, code, lookback_days=90, threshold_fn=None) -> Tuple[int, Optional[int]]`
- `_count_historical_followthrough(history_df, code, lookback_days=90, window=5, threshold_fn=None) -> Tuple[int, Optional[int]]`
- `_count_historical_wrap(history_df, code, lookback_days=90, window=5, drop_threshold=-3.0, threshold_fn=None) -> Tuple[int, Optional[int]]`

---

## Task 2 — 修改 stock_filter.py：删除原函数 + 顶部 re-export

**Files:**
- Modify: `D:\code\python\gupiao\stock_filter.py`

- [ ] **Step 1: 删除 stock_filter.py 中 3 个函数的实现**

定位 stock_filter.py 约 27-167 行的 3 个 `def _count_historical_*(...)` 定义。**整体删除这 3 个函数体**（连同它们的 docstring + 函数间空行）。

- [ ] **Step 2: 在 stock_filter.py 顶部 import 段后插入 re-export**

在 `from data_source_models import ...` 之类的 import 段后（约第 25 行附近），插入：

```python
# 模块级 K 线历史形态 helper 已迁移到 src/services/scoring/helpers.py
# 保留 re-export 以兼容 `from stock_filter import _count_historical_*` 老调用方
from src.services.scoring.helpers import (
    _count_historical_continuation,
    _count_historical_followthrough,
    _count_historical_wrap,
)
```

- [ ] **Step 3: 跑测试看 import 仍正常**

Run: `.venv\Scripts\python -c "from stock_filter import _count_historical_continuation, _count_historical_followthrough, _count_historical_wrap; from src.services.scoring.helpers import _count_historical_continuation as h1; assert _count_historical_continuation is h1; print('OK')"`
Expected: `OK`

---

## Task 3 — 验证 + commit

- [ ] **Step 1: pytest 318 passed 不下降**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `318 passed`

- [ ] **Step 2: import check**

Run: `.venv\Scripts\python -c "import stock_filter; from src.services.scoring import helpers; from src.services.scoring.helpers import _count_historical_continuation; print('OK')"`
Expected: `OK`

- [ ] **Step 3: GUI 启动 6s**

```powershell
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 6
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```

- [ ] **Step 4: 行数对比**

```powershell
(Get-Content D:\code\python\gupiao\stock_filter.py | Measure-Object -Line).Lines
(Get-Content D:\code\python\gupiao\src\services\scoring\helpers.py | Measure-Object -Line).Lines
```

Expected：stock_filter.py ~4539 → ~4400（减 ~140），helpers.py ~150。

- [ ] **Step 5: Commit**

```powershell
git -C D:\code\python\gupiao add src/services/scoring/__init__.py src/services/scoring/helpers.py stock_filter.py
git -C D:\code\python\gupiao commit -m @'
重构（stock_filter P1）：建 scoring 包 + 抽 3 个模块级 K 线形态 helper

按 stock_filter.py 模块化拆分 spec 的 Phase 1，建立 src/services/scoring/
包骨架，把 _count_historical_continuation / followthrough / wrap 三个模块级
helper 迁移到 src/services/scoring/helpers.py。

stock_filter.py 顶部 re-export 保持向后兼容（外部 from stock_filter import
_count_historical_* 老调用方无需改动）。

零行为变化，pytest 318 全绿。

作为后续 12 phase 拆分（5 scorer + shared + classifiers + profile + predict +
scanning + facade）的模板。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## 自检表

| 检查 | 状态 |
|---|---|
| scoring 包 + __init__.py docstring 描述各子模块 | ✅ |
| helpers.py 含 3 个函数完整实现（不改逻辑） | ✅ |
| stock_filter.py re-export 保持 `from stock_filter import _count_historical_*` 兼容 | ✅ |
| 测试 `from stock_filter import _count_historical_* is from src.services.scoring.helpers ...` 等价性 | ✅ |
| pytest 不下降 / GUI 启动正常 | ✅ |
| commit message 中文 + Co-Authored-By | ✅ |
