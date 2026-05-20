# 历史同类形态命中加分 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在涨停预测 cont / first / wrap 3 个评分函数里新增"近 90 日同类形态成功命中次数"加分维度；同时把全部 5 个 sub-tab 评分函数（+ first 辅助评分）的 history 加载从 65 日**统一**改为 120 日。

**Architecture:** TDD 流程。先写 3 个 helper 的单测（约 18-20 个 case），让测试驱动 helper 实现。helper 完成后集成到 3 个评分函数。最后把所有 6 处评分函数的 history days=65 统一改 120。3 个 commit：Commit 1 = TDD helpers，Commit 2 = 集成评分 + reason 文案，Commit 3 = history days 统一。

**Tech Stack:** Python 3.12 + pandas + pytest

**Spec:** [`docs/superpowers/specs/2026-05-20-predict-historical-pattern-bonus-design.md`](../specs/2026-05-20-predict-historical-pattern-bonus-design.md)

---

## Task 0 — 起跑基线

- [ ] **Step 1: 工作树干净**

Run: `git -C D:\code\python\gupiao status`
Expected: `nothing to commit, working tree clean`

- [ ] **Step 2: pytest baseline**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `274 passed, 20 subtests passed`

---

## Task 1 — TDD：3 个 helper + 单测（Commit 1）

**Files:**
- Create: `D:\code\python\gupiao\tests\test_historical_pattern_count.py`
- Modify: `D:\code\python\gupiao\stock_filter.py`（新增 3 个 helper）

> 严格 TDD：先写测试 → 跑测试看 FAIL → 写最小实现让测试 PASS → refactor。

- [ ] **Step 1: 先写测试文件 `tests/test_historical_pattern_count.py`**

完整内容：

```python
"""测试 stock_filter 中 3 个 _count_historical_* helper。

定义复述（来自 spec）：
- _count_historical_continuation：涨停日 → 次日继续涨停 → 计 1 次连板成功
- _count_historical_followthrough：涨停日 → 后续 window=5 日内出现另一次涨停 → 计 1 次
- _count_historical_wrap：涨停日 → window=5 日内至少一根 ≤ drop% 阴线 → 之后再涨停 → 计 1 次
"""
from __future__ import annotations

import pandas as pd
import pytest

from stock_filter import (
    _count_historical_continuation,
    _count_historical_followthrough,
    _count_historical_wrap,
)


def _make_df(rows):
    """rows: [(date_str, close_float), ...]"""
    return pd.DataFrame({
        "date": [r[0] for r in rows],
        "close": [r[1] for r in rows],
        "low": [r[1] for r in rows],
    })


def _threshold_main_board(_code: str) -> float:
    return 10.0


def _threshold_growth_board(_code: str) -> float:
    return 20.0


# ============== _count_historical_continuation ==============

class TestHistoricalContinuation:
    def test_empty_df_returns_zero(self):
        df = pd.DataFrame()
        cnt, last = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0
        assert last is None

    def test_single_limit_up_no_followup(self):
        # 一次涨停但次日下跌，不算连板成功
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10% 涨停
            ("2024-01-03", 10.8),  # -1.8% 不算连板
            ("2024-01-04", 10.5),
        ])
        cnt, last = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0
        assert last is None

    def test_two_consecutive_limit_ups_counts_one(self):
        # 涨停 + 次日继续涨停 = 1 次连板成功
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10% 涨停
            ("2024-01-03", 12.1),  # +10% 连板成功
            ("2024-01-04", 12.0),  # 跳过 today
        ])
        cnt, last = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 1
        assert last is not None

    def test_three_consecutive_limit_ups_counts_two(self):
        # 3 连板：D1↑ D2↑ D3↑ → D1→D2 + D2→D3 共 2 次成功
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),   # +10%
            ("2024-01-03", 12.1),   # +10%
            ("2024-01-04", 13.31),  # +10%
            ("2024-01-05", 13.0),   # 跳过 today
        ])
        cnt, last = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 2

    def test_gap_day_does_not_count(self):
        # 涨停 + 间隔一日下跌 + 再涨停 — 不算连板（不是 T+1）
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10%
            ("2024-01-03", 10.0),  # 跌
            ("2024-01-04", 11.0),  # +10%（但与前涨停隔了一天，非连板）
            ("2024-01-05", 11.0),
        ])
        cnt, last = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0

    def test_growth_board_20pct_threshold(self):
        # 创业板 20% 阈值：+11% 不算涨停，+20% 才算
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.1),   # +11% 主板算涨停，创业板不算
            ("2024-01-03", 12.21),  # +10% 主板算涨停
            ("2024-01-04", 12.0),
        ])
        cnt, last = _count_historical_continuation(
            df, "300001", lookback_days=90, threshold_fn=_threshold_growth_board,
        )
        assert cnt == 0  # 创业板下两个 +10% 都不算涨停

        # 创业板真涨停（+20%）
        df2 = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 12.0),   # +20% 涨停
            ("2024-01-03", 14.4),   # +20% 连板
            ("2024-01-04", 14.0),
        ])
        cnt2, _ = _count_historical_continuation(
            df2, "300001", lookback_days=90, threshold_fn=_threshold_growth_board,
        )
        assert cnt2 == 1

    def test_lookback_filter(self):
        # 100 日前的涨停不计入（超出 lookback=90）
        dates = pd.date_range("2024-01-01", periods=105, freq="D")
        closes = [10.0] * 105
        # 在第 0/1 天造一次连板（距 today 即第 104 天有 103 天差距 > 90）
        closes[1] = 11.0  # +10%
        closes[2] = 12.1  # +10% 连板
        df = _make_df([(d.strftime("%Y-%m-%d"), c) for d, c in zip(dates, closes)])
        cnt, _ = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0


# ============== _count_historical_followthrough ==============

class TestHistoricalFollowthrough:
    def test_empty_df(self):
        cnt, last = _count_historical_followthrough(
            pd.DataFrame(), "000001", lookback_days=90, window=5,
            threshold_fn=_threshold_main_board,
        )
        assert cnt == 0
        assert last is None

    def test_single_limit_up_no_followup_window(self):
        # 涨停后 7 日内无再涨停 → 不计入
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10%
            ("2024-01-03", 10.8),
            ("2024-01-04", 10.6),
            ("2024-01-05", 10.4),
            ("2024-01-06", 10.2),
            ("2024-01-07", 10.0),
            ("2024-01-08", 10.0),  # today
        ])
        cnt, _ = _count_historical_followthrough(
            df, "000001", lookback_days=90, window=5,
            threshold_fn=_threshold_main_board,
        )
        assert cnt == 0

    def test_limit_up_then_within_5d_another_limit_up(self):
        # 涨停后 3 日内出现另一次涨停 → 1 次接力成功
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10%
            ("2024-01-03", 10.5),
            ("2024-01-04", 10.0),
            ("2024-01-05", 11.0),  # +10% 在 window=5 内
            ("2024-01-06", 11.0),  # today
        ])
        cnt, last = _count_historical_followthrough(
            df, "000001", lookback_days=90, window=5,
            threshold_fn=_threshold_main_board,
        )
        assert cnt == 1
        assert last is not None

    def test_limit_up_then_6d_later_does_not_count(self):
        # 涨停后第 6 日才涨停 — 超出 window=5
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),   # +10%
            ("2024-01-03", 10.5),
            ("2024-01-04", 10.4),
            ("2024-01-05", 10.3),
            ("2024-01-06", 10.2),
            ("2024-01-07", 10.1),
            ("2024-01-08", 11.11),  # +10% 但距前涨停 6 日
            ("2024-01-09", 11.11),  # today
        ])
        cnt, _ = _count_historical_followthrough(
            df, "000001", lookback_days=90, window=5,
            threshold_fn=_threshold_main_board,
        )
        assert cnt == 0


# ============== _count_historical_wrap ==============

class TestHistoricalWrap:
    def test_empty_df(self):
        cnt, last = _count_historical_wrap(
            pd.DataFrame(), "000001", lookback_days=90, window=5,
            drop_threshold=-3.0, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0

    def test_consecutive_limit_ups_not_wrap(self):
        # 涨停直接再涨停（无阴线打回）→ 不算反包
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10%
            ("2024-01-03", 12.1),  # +10% 直接连板，不算反包
            ("2024-01-04", 12.0),  # today
        ])
        cnt, _ = _count_historical_wrap(
            df, "000001", lookback_days=90, window=5,
            drop_threshold=-3.0, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0

    def test_limit_up_drop_then_wrap_counts(self):
        # 涨停 → -4% 阴线 → 再涨停 → 1 次反包
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),    # +10%
            ("2024-01-03", 10.56),   # -4%
            ("2024-01-04", 11.62),   # +10% 反包
            ("2024-01-05", 11.5),    # today
        ])
        cnt, last = _count_historical_wrap(
            df, "000001", lookback_days=90, window=5,
            drop_threshold=-3.0, threshold_fn=_threshold_main_board,
        )
        assert cnt == 1
        assert last is not None

    def test_limit_up_minor_drop_then_wrap_does_not_count(self):
        # 涨停 → -2% 阴线（未达 -3% 阈值）→ 再涨停 → 不算反包
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),    # +10%
            ("2024-01-03", 10.78),   # -2%（未达阈值）
            ("2024-01-04", 11.86),   # +10% 但不算反包
            ("2024-01-05", 11.8),    # today
        ])
        cnt, _ = _count_historical_wrap(
            df, "000001", lookback_days=90, window=5,
            drop_threshold=-3.0, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0

    def test_multiple_wraps_counted(self):
        # 2 次反包：[涨停 → 阴线 → 涨停] × 2
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),    # +10% (1st 涨停)
            ("2024-01-03", 10.56),   # -4% 阴线
            ("2024-01-04", 11.62),   # +10% (1st 反包)
            ("2024-01-05", 11.15),   # -4%
            ("2024-01-06", 12.27),   # +10% (2nd 反包)
            ("2024-01-07", 12.2),    # today
        ])
        cnt, _ = _count_historical_wrap(
            df, "000001", lookback_days=90, window=5,
            drop_threshold=-3.0, threshold_fn=_threshold_main_board,
        )
        assert cnt == 2
```

- [ ] **Step 2: 跑测试看 FAIL（红）**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest tests/test_historical_pattern_count.py -v --tb=short 2>&1 | Select-Object -Last 15`
Expected: 所有测试 FAIL（因为 helper 还不存在），错误是 `ImportError: cannot import name '_count_historical_continuation'`

- [ ] **Step 3: 写 3 个 helper 在 stock_filter.py 顶部（imports 之后，class 之前）**

定位 stock_filter.py 顶部 imports 结束的位置，在 `class StockFilter:` 之前插入：

```python
def _count_historical_continuation(
    history_df: "pd.DataFrame",
    code: str,
    lookback_days: int = 90,
    threshold_fn=None,
):
    """扫历史 K 线统计成功连板次数（涨停 → T+1 继续涨停）。

    跳过最后一行（today），避免今日数据自计。

    返回 (occurrence_count, days_since_last_hit)。
    """
    if history_df is None or len(history_df) < 3:
        return (0, None)
    if threshold_fn is None:
        def threshold_fn(_c):
            return 10.0
    df = history_df.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    n = len(df)
    t = n - 1  # today index (skip)
    threshold = float(threshold_fn(code))
    cutoff_idx = max(1, t - int(lookback_days))
    occ = 0
    last_hit_idx = None
    for i in range(cutoff_idx, t - 1):
        # i: 涨停日? i+1: 次日继续涨停?
        if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]):
            continue
        if float(close.iloc[i - 1]) <= 0:
            continue
        chg_i = (float(close.iloc[i]) / float(close.iloc[i - 1]) - 1) * 100
        if chg_i < threshold - 0.3:
            continue
        if pd.isna(close.iloc[i + 1]) or float(close.iloc[i]) <= 0:
            continue
        chg_next = (float(close.iloc[i + 1]) / float(close.iloc[i]) - 1) * 100
        if chg_next >= threshold - 0.3:
            occ += 1
            last_hit_idx = i + 1
    last_days = (t - last_hit_idx) if last_hit_idx is not None else None
    return (occ, last_days)


def _count_historical_followthrough(
    history_df: "pd.DataFrame",
    code: str,
    lookback_days: int = 90,
    window: int = 5,
    threshold_fn=None,
):
    """扫历史 K 线统计成功二波接力次数（涨停 → window 日内另一次涨停）。

    跳过最后一行（today）。返回 (occurrence_count, days_since_last_hit)。
    """
    if history_df is None or len(history_df) < 3:
        return (0, None)
    if threshold_fn is None:
        def threshold_fn(_c):
            return 10.0
    df = history_df.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    n = len(df)
    t = n - 1
    threshold = float(threshold_fn(code))
    cutoff_idx = max(1, t - int(lookback_days))
    # 找出所有涨停日 idx
    lu_indices = []
    for i in range(cutoff_idx, t):
        if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]) or float(close.iloc[i - 1]) <= 0:
            continue
        chg_i = (float(close.iloc[i]) / float(close.iloc[i - 1]) - 1) * 100
        if chg_i >= threshold - 0.3:
            lu_indices.append(i)
    # 对每个涨停日 i，看 [i+1, min(i+window, t-1)] 内是否有再次涨停（i 之外的）
    occ = 0
    last_hit_idx = None
    for i in lu_indices:
        end = min(i + int(window), t - 1)
        for j in range(i + 1, end + 1):
            if j in lu_indices:
                occ += 1
                last_hit_idx = j
                break
    last_days = (t - last_hit_idx) if last_hit_idx is not None else None
    return (occ, last_days)


def _count_historical_wrap(
    history_df: "pd.DataFrame",
    code: str,
    lookback_days: int = 90,
    window: int = 5,
    drop_threshold: float = -3.0,
    threshold_fn=None,
):
    """扫历史 K 线统计成功反包次数（涨停 → window 日内 ≤drop 阴线 → 再涨停）。

    跳过最后一行（today）。返回 (occurrence_count, days_since_last_hit)。
    """
    if history_df is None or len(history_df) < 3:
        return (0, None)
    if threshold_fn is None:
        def threshold_fn(_c):
            return 10.0
    df = history_df.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    n = len(df)
    t = n - 1
    threshold = float(threshold_fn(code))
    cutoff_idx = max(1, t - int(lookback_days))
    # 找出所有涨停日 idx
    lu_indices = []
    for i in range(cutoff_idx, t):
        if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]) or float(close.iloc[i - 1]) <= 0:
            continue
        chg_i = (float(close.iloc[i]) / float(close.iloc[i - 1]) - 1) * 100
        if chg_i >= threshold - 0.3:
            lu_indices.append(i)
    if len(lu_indices) < 2:
        return (0, None)
    # 对每对相邻涨停日 (a, b)，b - a ≤ window，且 (a, b) 之间至少一根 ≤ drop_threshold 阴线
    occ = 0
    last_hit_idx = None
    for k in range(len(lu_indices) - 1):
        a = lu_indices[k]
        b = lu_indices[k + 1]
        if b - a > int(window):
            continue
        # a 与 b 之间有阴线 ≤ drop_threshold
        has_drop = False
        for j in range(a + 1, b):
            if pd.isna(close.iloc[j]) or pd.isna(close.iloc[j - 1]) or float(close.iloc[j - 1]) <= 0:
                continue
            chg_j = (float(close.iloc[j]) / float(close.iloc[j - 1]) - 1) * 100
            if chg_j <= drop_threshold:
                has_drop = True
                break
        if has_drop:
            occ += 1
            last_hit_idx = b
    last_days = (t - last_hit_idx) if last_hit_idx is not None else None
    return (occ, last_days)
```

注意：
- 用现有 `pd.to_numeric` / `pd.isna` 防御（与现有 scoring 代码一致风格）
- threshold_fn 可空，默认 10.0%

- [ ] **Step 4: 跑测试看 PASS（绿）**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest tests/test_historical_pattern_count.py -v --tb=short 2>&1 | Select-Object -Last 30`
Expected: 所有测试通过（约 15 个 case 全 pass）。如果有 FAIL，逐个 debug helper 实现。

- [ ] **Step 5: 跑全量 pytest 确保无回归**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: 274 + 新增数 = 约 289-295 passed

- [ ] **Step 6: Commit 1**

```powershell
git -C D:\code\python\gupiao add tests/test_historical_pattern_count.py stock_filter.py
git -C D:\code\python\gupiao commit -m @'
新增：3 个 K 线历史形态统计 helper + 单测

新增 stock_filter._count_historical_continuation / followthrough / wrap，
扫候选股 K 线统计近 90 日内"涨停 → T+1 连板/接力/反包"成功次数，
供后续 cont/first/wrap 评分函数加分使用。

测试覆盖：空 DataFrame、单次涨停、连续涨停、隔日涨停、超窗口、
创业板 20% 阈值、阴线阈值边界、多次反包、lookback 截断 共 15 个 case。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## Task 2 — 集成 3 个 helper 到评分函数（Commit 2）

**Files:**
- Modify: `D:\code\python\gupiao\stock_filter.py`（3 个评分函数末尾追加 bonus 逻辑）

- [ ] **Step 1: 在 `_score_continuation` 末尾追加 bonus**

Run: `grep -n "def _score_continuation" stock_filter.py`

定位约 2567 行。在该函数 `return` 前（约 2700 行附近，看具体 return 位置）追加：

```python
# === 历史同类形态加分：近 90 日内的连板成功次数 ===
occ_count, last_hit_days = _count_historical_continuation(
    history, code, lookback_days=90,
    threshold_fn=self._limit_up_threshold_pct,
)
if occ_count >= 3:
    bonus = 8
elif occ_count >= 2:
    bonus = 5
elif occ_count >= 1:
    bonus = 2
else:
    bonus = 0

if bonus > 0:
    if last_hit_days is not None and last_hit_days <= 30:
        bonus = min(bonus + 2, 10)
        reasons.append(f"近90日{occ_count}次连板成功(最近{last_hit_days}日内)+{bonus}")
    else:
        reasons.append(f"近90日{occ_count}次连板成功+{bonus}")
    score += bonus
```

**关键**：`_score_continuation` 内已有 `history` 变量（约 2654 行加载），直接复用。把这段插在 `history` 已被使用的所有现有 score 加分后、return 之前。

- [ ] **Step 2: 跑 pytest，确保未回归**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: 全绿。

- [ ] **Step 3: 在 `_score_followthrough_candidate` 末尾追加 bonus**

Run: `grep -n "def _score_followthrough_candidate" stock_filter.py`

定位约 2869 行。在该函数 `return` 前追加：

```python
# === 历史同类形态加分：近 90 日内的二波接力成功次数 ===
occ_count, last_hit_days = _count_historical_followthrough(
    history, code, lookback_days=90, window=5,
    threshold_fn=self._limit_up_threshold_pct,
)
if occ_count >= 3:
    bonus = 8
elif occ_count >= 2:
    bonus = 5
elif occ_count >= 1:
    bonus = 2
else:
    bonus = 0

if bonus > 0:
    if last_hit_days is not None and last_hit_days <= 30:
        bonus = min(bonus + 2, 10)
        reasons.append(f"近90日{occ_count}次二波接力成功(最近{last_hit_days}日内)+{bonus}")
    else:
        reasons.append(f"近90日{occ_count}次二波接力成功+{bonus}")
    score += bonus
```

- [ ] **Step 4: 在 `_score_broken_board_wrap` 末尾追加 bonus（两路径共用）**

Run: `grep -n "def _score_broken_board_wrap" stock_filter.py`

定位约 3611 行。在该函数 `return` 前追加：

```python
# === 历史同类形态加分：近 90 日内的反包/承接成功次数 ===
occ_count, last_hit_days = _count_historical_wrap(
    history, code, lookback_days=90, window=5, drop_threshold=-3.0,
    threshold_fn=self._limit_up_threshold_pct,
)
if occ_count >= 3:
    bonus = 8
elif occ_count >= 2:
    bonus = 5
elif occ_count >= 1:
    bonus = 2
else:
    bonus = 0

if bonus > 0:
    if last_hit_days is not None and last_hit_days <= 30:
        bonus = min(bonus + 2, 10)
        _label = "反包" if pattern_kind == "wrap" else "承接"
        reasons.append(f"近90日{occ_count}次{_label}成功(最近{last_hit_days}日内)+{bonus}")
    else:
        _label = "反包" if pattern_kind == "wrap" else "承接"
        reasons.append(f"近90日{occ_count}次{_label}成功+{bonus}")
    score += bonus
```

注意：`pattern_kind` 是 `_score_broken_board_wrap` 内已存在的变量（"wrap" 或 "hold_strong"），区分反包 vs 承接文案。

- [ ] **Step 5: 跑 pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: 全绿。

- [ ] **Step 6: import / GUI 启动 / 静态验证**

```powershell
.venv\Scripts\python -c "import stock_filter; print('OK')"
```
Expected: `OK`

GUI 静态启动：
```powershell
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 6
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```
Expected: `OK`

- [ ] **Step 7: Commit 2**

```powershell
git -C D:\code\python\gupiao add stock_filter.py
git -C D:\code\python\gupiao commit -m @'
新功能：cont/first/wrap 评分加入"历史同类形态命中"加分

在 _score_continuation / _score_followthrough_candidate /
_score_broken_board_wrap 末尾追加：调 helper 统计近 90 日内
同类形态成功次数（连板/二波接力/反包），按 3/2/1 次分别给
+8/+5/+2 分；近 30 日内命中再 +2；封顶 +10。

预测依据列自动展示新 reason，让用户一眼看出"这只股有此性格"。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## Task 3 — 5+1 处评分函数 history days 统一改 120（Commit 3）

**Files:**
- Modify: `D:\code\python\gupiao\stock_filter.py`（6 处 `days=65` → `days=120`）

> 单独一个 commit，便于将来如需回滚仅 days 改动。

- [ ] **Step 1: grep 6 处目标位置**

Run: `grep -n "self.fetcher.get_history_data(code, days=65" stock_filter.py`

应命中约 6 处（spec 表格中列出的行号附近）：
- ~2654 `_score_continuation`
- ~2907 `_score_followthrough_candidate`
- ~3371 `_score_fresh_first_board`
- ~3641 `_score_broken_board_wrap`
- ~3966 `_score_trend_limit_up`
- ~4610 `_score_first_board_by_profile`

**只改这 6 处**，其他 `days=65`（如 `_prefetch_history_for_pool` 调用、其他非评分路径）**不动**。

- [ ] **Step 2: 用 Edit 工具逐个改**

对每个目标位置：用足够上下文的 `old_string` 确保仅匹配 1 处（function name 在上下文中区分），把 `days=65` 改成 `days=120`。

可用的辨识上下文：reason 字符串中的 cache-only 标记区分得清楚：
- `reason="predict-continuation-cache-only"` → cont
- `reason="predict-followthrough-cache-only"` → first
- `reason="predict-fresh-first-board-cache-only"` → fresh
- `reason="predict-broken-wrap-cache-only"` → wrap
- `reason="predict-trend-cache-only"` → trend
- `reason="predict-first-board-cache-only"` → first 辅助评分

- [ ] **Step 3: grep 验证**

Run: `grep -n "self.fetcher.get_history_data(code, days=" stock_filter.py`

预期所有评分函数行均显示 `days=120`，仅 `_prefetch_history_for_pool` 等其他路径仍是 `days=65` 或其他值。

具体：
```
self.fetcher.get_history_data(code, days=history_days,  # 这些是 _prefetch 动态参数，跳过
self.fetcher.get_history_data(code, days=120,           # 评分函数应都是 120
self.fetcher.get_history_data(code, days=20,            # 这条是其他路径，跳过
```

- [ ] **Step 4: pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: 全绿。

- [ ] **Step 5: GUI 启动**

```powershell
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 6
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```
Expected: `OK`

- [ ] **Step 6: Commit 3**

```powershell
git -C D:\code\python\gupiao add stock_filter.py
git -C D:\code\python\gupiao commit -m @'
重构：5 个 sub-tab 评分函数 + first 辅助评分，history days 统一 120

把 _score_continuation / _score_followthrough_candidate /
_score_fresh_first_board / _score_broken_board_wrap /
_score_trend_limit_up / _score_first_board_by_profile 共 6 处的
self.fetcher.get_history_data(code, days=65, ...) 统一改成 days=120。

理由：cont/first/wrap 的新增"历史同类形态命中"加分需要 90 日 lookback
+ 余量；fresh/trend 暂不加 bonus 但也统一为 120 避免参数漂移。
本地 SQLite history 表通常已有 ≥250 日，不会触发额外网络拉取。

_prefetch_history_for_pool 等批量预热路径的 days=65 保持不变
（与本次评分维度无关）。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## Task 4 — 终验

- [ ] **Step 1: 看 3 个 commit**

Run: `git -C D:\code\python\gupiao log --oneline -5`
Expected: 看到 Commit 3 + Commit 2 + Commit 1 + 已有 plan/spec commit。

- [ ] **Step 2: 最终 pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: 274 + 新增数 = 约 289-295 passed。

- [ ] **Step 3: 关键 grep 验证**

```powershell
grep -n "_count_historical_continuation\|_count_historical_followthrough\|_count_historical_wrap" stock_filter.py
```
Expected: 每个 helper 应有 1 def + 1 调用 = 共 6 处命中（再加单测里多次）。

```powershell
grep -n "self.fetcher.get_history_data(code, days=120" stock_filter.py
```
Expected: 至少 6 处命中（6 个评分函数全部改成 120）。

- [ ] **Step 4: 报告**

向用户报告：3 个 commit 落地，pytest + GUI 验证通过，等用户确认是否 push。

---

## 自检表

| 检查 | 状态 |
|---|---|
| spec 覆盖：cont/first/wrap 各加 bonus + 6 处 history days → 3 个 commit 覆盖 | ✅ |
| TDD：先单测红 → 再写 helper → 单测绿（Commit 1） | ✅ |
| 评分集成放在已加载 history 之后，return 之前，不破坏现有 reason 顺序 | ✅ |
| 封顶 +10 / 近 30 日 +2 / 阶梯 +8/+5/+2 落实 | ✅ |
| _score_broken_board_wrap 区分 wrap/承接 文案（用 pattern_kind 变量） | ✅ |
| history days 仅改评分函数路径，不动 _prefetch_history_for_pool | ✅ |
| 每个 commit 后跑 pytest + import + GUI smoke | ✅ |
| 不开新分支，直接 main | ✅ |
| commit message 中文 + Co-Authored-By | ✅ |
