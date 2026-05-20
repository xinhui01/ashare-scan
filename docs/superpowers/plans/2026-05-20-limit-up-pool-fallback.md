# 涨停池 spot 兜底 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 给 `EODData.get_limit_up_pool` / `get_previous_limit_up_pool` 加全市场 spot 兜底（含新浪兜底），用昨日 SQLite pool 递推连板数。

**Architecture:** 2 个 commit。Commit 1: TDD 写 helper（`_fetch_spot_with_fallback` + `_derive_limit_up_pool_from_spot`）+ 单测。Commit 2: 串接到 `get_limit_up_pool` / `get_previous_limit_up_pool`，验证 GUI / pytest。

**Tech Stack:** Python 3.12 + pandas + akshare + pytest

**Spec:** [`docs/superpowers/specs/2026-05-20-limit-up-pool-fallback-design.md`](../specs/2026-05-20-limit-up-pool-fallback-design.md)

---

## Task 0 — 基线

- [ ] **Step 1:** `git status` 干净
- [ ] **Step 2:** `pytest -q --tb=no` 290 passed
- [ ] **Step 3:** import OK

---

## Task 1 — TDD: 写 2 个 helper + 单测（Commit 1）

**Files:**
- Create: `D:\code\python\gupiao\tests\test_limit_up_pool_fallback.py`
- Modify: `D:\code\python\gupiao\stock_data.py`（在 EODData 类内新增 2 个方法）

- [ ] **Step 1: 先写单测 `tests/test_limit_up_pool_fallback.py`**

```python
"""测试 EODData._derive_limit_up_pool_from_spot 派生今日涨停池逻辑。

兜底链：东财涨停池失败 → 全市场 spot（含新浪兜底）→ 过滤涨停股 → 递推连板数。
本测试只覆盖派生逻辑（_derive_limit_up_pool_from_spot），不联网。
"""
from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from stock_data import EODData


@pytest.fixture
def eod():
    """构造一个不联网的 EODData 实例。"""
    instance = EODData.__new__(EODData)
    instance._log = lambda msg: None
    return instance


def _make_spot(rows):
    """rows: [(代码, 涨跌幅, 最新价, 换手率, 所属行业, 名称), ...]"""
    return pd.DataFrame([
        {"代码": r[0], "涨跌幅": r[1], "最新价": r[2],
         "换手率": r[3], "所属行业": r[4], "名称": r[5]}
        for r in rows
    ])


class TestDeriveFromSpot:
    def test_none_spot_returns_empty(self, eod):
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=None):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        assert df.empty

    def test_empty_spot_returns_empty(self, eod):
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=pd.DataFrame()):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        assert df.empty

    def test_filter_to_limit_up_only(self, eod):
        # 主板：+10% 阈值；3 只达 / 不达
        spot = _make_spot([
            ("600000", 10.0, 5.5, 3.0, "银行", "浦发银行"),     # +10%, 主板涨停
            ("600001", 9.7, 5.2, 2.5, "钢铁", "邯郸钢铁"),      # +9.7%, 边界（>= threshold-0.3=9.7）算涨停
            ("600002", 5.0, 5.0, 2.0, "钢铁", "齐鲁石化"),      # +5%, 不算
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        assert len(df) == 2
        assert set(df["代码"].astype(str).tolist()) == {"600000", "600001"}

    def test_growth_board_20pct_threshold(self, eod):
        # 创业板 300xxx 阈值 20%，+11% 不算
        spot = _make_spot([
            ("300001", 11.0, 22.0, 5.0, "电子", "ABC"),
            ("300002", 20.0, 24.0, 8.0, "电子", "DEF"),
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        assert len(df) == 1
        assert df.iloc[0]["代码"] == "300002"

    def test_beijing_board_30pct_threshold(self, eod):
        # 北交所 8xxxxx 阈值 30%
        spot = _make_spot([
            ("830001", 29.0, 13.0, 5.0, "材料", "BJ1"),
            ("830002", 30.0, 13.0, 5.0, "材料", "BJ2"),
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        assert len(df) == 1
        assert df.iloc[0]["代码"] == "830002"

    def test_consecutive_boards_inferred_from_prev_pool(self, eod):
        # 昨日 pool：A 连板=2, B 连板=1。今日 A、B、C 都涨停 → A=3 / B=2 / C=1
        prev_pool = pd.DataFrame([
            {"代码": "600100", "连板数": 2, "名称": "A"},
            {"代码": "600200", "连板数": 1, "名称": "B"},
        ])
        spot = _make_spot([
            ("600100", 10.0, 11.0, 5.0, "X", "A"),
            ("600200", 10.0, 11.0, 5.0, "X", "B"),
            ("600300", 10.0, 11.0, 5.0, "X", "C"),
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520", prev_pool_df=prev_pool)
        df_indexed = df.set_index("代码")
        assert int(df_indexed.loc["600100", "连板数"]) == 3
        assert int(df_indexed.loc["600200", "连板数"]) == 2
        assert int(df_indexed.loc["600300", "连板数"]) == 1

    def test_no_prev_pool_defaults_to_one(self, eod):
        spot = _make_spot([
            ("600100", 10.0, 11.0, 5.0, "X", "A"),
            ("600200", 10.0, 11.0, 5.0, "X", "B"),
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520", prev_pool_df=None)
        assert (df["连板数"] == 1).all()

    def test_required_columns_present(self, eod):
        spot = _make_spot([
            ("600100", 10.0, 11.0, 5.0, "银行", "A"),
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        required = {"代码", "名称", "最新价", "涨跌幅", "换手率", "连板数", "所属行业"}
        assert required.issubset(set(df.columns))
```

- [ ] **Step 2: 跑测试看 FAIL**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest tests/test_limit_up_pool_fallback.py -v --tb=short 2>&1 | Select-Object -Last 15`
Expected: 全部 FAIL（AttributeError `_derive_limit_up_pool_from_spot`）

- [ ] **Step 3: 在 stock_data.py 的 EODData 类中新增 2 个 helper**

在 `EODData` 类内合适位置（如 `get_limit_up_pool` 方法之前）新增：

```python
def _fetch_spot_with_fallback(self) -> Optional[pd.DataFrame]:
    """全市场实时行情快照，东财→新浪自动兜底。"""
    import akshare as ak
    if not _eastmoney_circuit_breaker_open():
        try:
            if self._log:
                self._log("全市场 spot 快照：东财...")
            return _retry_ak_call(ak.stock_zh_a_spot_em)
        except Exception as exc:
            if self._log:
                self._log(f"全市场 spot 东财失败: {exc}，尝试新浪兜底")
    try:
        if self._log:
            self._log("全市场 spot 快照：新浪兜底（约 30s）...")
        df = _retry_ak_call(ak.stock_zh_a_spot)
        if df is not None and not df.empty:
            if "代码" in df.columns:
                df["代码"] = (
                    df["代码"].astype(str)
                    .str.replace(r"^(sh|sz|bj)", "", regex=True)
                    .str.strip().str.zfill(6)
                )
            return df
    except Exception as exc:
        if self._log:
            self._log(f"全市场 spot 新浪兜底也失败: {exc}")
    return None


def _derive_limit_up_pool_from_spot(
    self,
    trade_date: str,
    prev_pool_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """从全市场 spot 派生今日涨停池（东财涨停池失败时的兜底）。

    步骤：
    1. _fetch_spot_with_fallback 拿全市场快照
    2. 按代码前缀算涨停阈值（主板 10/创业板 20/北交所 30）
    3. 过滤涨幅 >= threshold - 0.3 的股票
    4. 用 prev_pool_df 递推 连板数：昨日涨停连板=N → 今日涨停连板=N+1；昨日未涨停 → 连板=1
    5. 合成兼容 ak.stock_zt_pool_em 列名的 DataFrame
    """
    spot = self._fetch_spot_with_fallback()
    if spot is None or spot.empty:
        return pd.DataFrame()

    def _threshold_for(code: str) -> float:
        c = str(code).strip().zfill(6)
        if c.startswith(("300", "301", "688")):
            return 20.0
        if c.startswith(("8", "9")):
            # 北交所 8xx / 9xx
            return 30.0
        return 10.0

    # 标准化代码列
    spot = spot.copy()
    if "代码" in spot.columns:
        spot["代码"] = spot["代码"].astype(str).str.strip().str.zfill(6)
    # 过滤涨停
    if "涨跌幅" not in spot.columns:
        return pd.DataFrame()
    rows = []
    for _, row in spot.iterrows():
        code = str(row.get("代码", "")).strip()
        if not code:
            continue
        try:
            chg = float(row.get("涨跌幅") or 0)
        except (TypeError, ValueError):
            continue
        thresh = _threshold_for(code)
        if chg < thresh - 0.3:
            continue
        rows.append(row)
    if not rows:
        return pd.DataFrame()

    # 递推连板数
    prev_lookup: Dict[str, int] = {}
    if prev_pool_df is not None and not prev_pool_df.empty and "代码" in prev_pool_df.columns:
        prev_pool_df = prev_pool_df.copy()
        prev_pool_df["代码"] = prev_pool_df["代码"].astype(str).str.strip().str.zfill(6)
        if "连板数" in prev_pool_df.columns:
            for _, r in prev_pool_df.iterrows():
                c = str(r.get("代码") or "").strip()
                try:
                    n = int(r.get("连板数") or 0)
                except (TypeError, ValueError):
                    n = 0
                if c and n > 0:
                    prev_lookup[c] = n

    out = []
    for row in rows:
        code = str(row.get("代码", "")).strip()
        prev_n = prev_lookup.get(code, 0)
        boards = prev_n + 1 if prev_n > 0 else 1
        out.append({
            "代码": code,
            "名称": str(row.get("名称", "") or ""),
            "最新价": row.get("最新价"),
            "涨跌幅": row.get("涨跌幅"),
            "换手率": row.get("换手率"),
            "流通市值": row.get("流通市值"),
            "总市值": row.get("总市值"),
            "连板数": boards,
            "首次封板时间": "",
            "最后封板时间": "",
            "炸板次数": 0,
            "所属行业": str(row.get("所属行业", "") or ""),
            "涨停统计": "",
            "涨停原因": "",
        })
    return pd.DataFrame(out)
```

注意：
- `_eastmoney_circuit_breaker_open` 和 `_retry_ak_call` 都已在 stock_data.py 顶部 import
- `Optional[pd.DataFrame]` / `Dict[str, int]` 需要确认 typing import（应该已有）

- [ ] **Step 4: 跑测试看 PASS**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest tests/test_limit_up_pool_fallback.py -v --tb=short 2>&1 | Select-Object -Last 30`
Expected: 8 个 case 全 pass

- [ ] **Step 5: 全量 pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: 290 + 8 = ~298 passed

- [ ] **Step 6: Commit 1**

```powershell
git -C D:\code\python\gupiao add tests/test_limit_up_pool_fallback.py stock_data.py
git -C D:\code\python\gupiao commit -m @'
新增：涨停池 spot 兜底 helper + 单测

新增 EODData._fetch_spot_with_fallback（东财→新浪）和
_derive_limit_up_pool_from_spot（从 spot 派生涨停池，含连板数递推）。
单测覆盖主板/创业板/北交所阈值、连板数递推、列名兼容性等 8 个 case。

本 commit 仅新增 helper 与单测，不接入 get_limit_up_pool。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## Task 2 — 接入 get_limit_up_pool / get_previous_limit_up_pool（Commit 2）

**Files:**
- Modify: `D:\code\python\gupiao\stock_data.py`（get_limit_up_pool / get_previous_limit_up_pool 的网络分支改造）

- [ ] **Step 1: 改造 get_limit_up_pool 网络分支**

定位约 1434-1456 行的"3. 网络请求（涨停池目前仅东财有接口）"块，整体替换为：

```python
# 3. 东财涨停池接口
em_ok = not _eastmoney_circuit_breaker_open()
if em_ok:
    try:
        df = _retry_ak_call(ak.stock_zt_pool_em, date=date_key)
        if df is not None and not df.empty:
            raw_count = len(df)
            df = self._sanitize_limit_up_pool(df)
            dropped = raw_count - len(df)
            self._limit_up_pool_cache[date_key] = df
            save_limit_up_pool(date_key, df)
            if self._log:
                drop_note = f"，过滤 {dropped} 条脏数据" if dropped > 0 else ""
                self._log(f"涨停池 {date_key} 东财 {len(df)} 只{drop_note}，已保存")
            return df
    except Exception as exc:
        if self._log:
            self._log(f"涨停池 {date_key} 东财失败: {exc}，尝试 spot 兜底")
else:
    if self._log:
        self._log(f"涨停池 {date_key} 东财熔断中，尝试 spot 兜底")

# 4. spot 兜底
from src.utils.trade_calendar import _previous_trading_day
prev_date = _previous_trading_day(date_key) or ""
prev_pool = None
if prev_date:
    prev_pool = load_limit_up_pool(prev_date)
try:
    derived = self._derive_limit_up_pool_from_spot(date_key, prev_pool_df=prev_pool)
    if derived is not None and not derived.empty:
        derived = self._sanitize_limit_up_pool(derived)
        if not derived.empty:
            self._limit_up_pool_cache[date_key] = derived
            save_limit_up_pool(date_key, derived)
            if self._log:
                self._log(f"涨停池 {date_key} spot 兜底 {len(derived)} 只（连板数推断自昨日 pool），已保存")
            return derived
except Exception as exc:
    if self._log:
        self._log(f"涨停池 {date_key} spot 兜底失败: {exc}")

empty = pd.DataFrame()
self._limit_up_pool_cache[date_key] = empty
if self._log:
    self._log(f"涨停池 {date_key} 所有源均失败，返回空")
return empty
```

注意：`_previous_trading_day` 需要确认 import 路径，按现有 stock_data.py 已 import 的方式调（如 `from src.utils.trade_calendar import _previous_trading_day` 或顶部已有 `_previous_trading_day`）。

- [ ] **Step 2: 改造 get_previous_limit_up_pool 网络分支**

定位约 1474-1488 行，类似改造（昨日 pool 派生时**不需要再递推连板数**，因为昨日 pool 的 prev_date 是前天，对今日预测意义不大；可以**直接派生**不传 prev_pool，所有连板数=1，或简单跳过递推）：

```python
em_ok = not _eastmoney_circuit_breaker_open()
if em_ok:
    try:
        df = _retry_ak_call(ak.stock_zt_pool_previous_em, date=date_key)
        if df is not None and not df.empty:
            self._prev_limit_up_pool_cache[date_key] = df
            save_limit_up_pool(date_key, df, pool_type="previous")
            return df
    except Exception as exc:
        if self._log:
            self._log(f"昨日涨停池 {date_key} 东财失败: {exc}，尝试 spot 兜底")
else:
    if self._log:
        self._log(f"昨日涨停池 {date_key} 东财熔断中，尝试 spot 兜底")

try:
    # 昨日涨停池的 spot 兜底：用 date_key 当日 spot（如果可拉到），prev_pool 用 date_key-1 的 pool
    from src.utils.trade_calendar import _previous_trading_day
    prev_date = _previous_trading_day(date_key) or ""
    prev_pool = load_limit_up_pool(prev_date) if prev_date else None
    derived = self._derive_limit_up_pool_from_spot(date_key, prev_pool_df=prev_pool)
    if derived is not None and not derived.empty:
        self._prev_limit_up_pool_cache[date_key] = derived
        save_limit_up_pool(date_key, derived, pool_type="previous")
        if self._log:
            self._log(f"昨日涨停池 {date_key} spot 兜底 {len(derived)} 只，已保存")
        return derived
except Exception as exc:
    if self._log:
        self._log(f"昨日涨停池 {date_key} spot 兜底失败: {exc}")

empty = pd.DataFrame()
self._prev_limit_up_pool_cache[date_key] = empty
return empty
```

- [ ] **Step 3: 跑 pytest 确保未回归**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: ~298 passed（与 Commit 1 后相同）

- [ ] **Step 4: import 检查**

Run: `.venv\Scripts\python -c "from stock_data import EODData; e = EODData.__new__(EODData); e._log = print; print(hasattr(e, '_fetch_spot_with_fallback'), hasattr(e, '_derive_limit_up_pool_from_spot'))"`
Expected: `True True`

- [ ] **Step 5: GUI 启动**

```powershell
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 6
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```
Expected: `OK`

- [ ] **Step 6: Commit 2**

```powershell
git -C D:\code\python\gupiao add stock_data.py
git -C D:\code\python\gupiao commit -m @'
新功能：涨停池 / 昨日涨停池 接入 spot 兜底

东财涨停池失败/熔断时，自动调 _derive_limit_up_pool_from_spot 用全市场
spot（含新浪兜底）派生涨停股，并用昨日 SQLite pool 递推连板数，结果
入 SQLite 缓存。

让 predict 流程在东财熔断期间仍能拿到非空涨停池，避免"今日 0 只"误导
+ 晋级率被错误算成 0%。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## Task 3 — 终验

- [ ] **Step 1:** `git log --oneline -5` 看 2 个新 commit
- [ ] **Step 2:** 最终 `pytest -q --tb=no` 应 ~298 passed
- [ ] **Step 3:** 报告给用户：2 个 commit + pytest + GUI 通过，等用户确认 push

---

## 自检表

| 检查 | 状态 |
|---|---|
| TDD：先红再绿 | ✅ |
| spec 覆盖：helper 2 个 + 单测 8 个 + 接入 get_limit_up_pool + get_previous_limit_up_pool | ✅ |
| 阈值表（主板 10 / 创业板 20 / 北交所 30）覆盖 | ✅ |
| 连板数递推：昨日 pool 在/不在两路径 | ✅ |
| 派生 DataFrame 列名兼容 | ✅ |
| 不动 _eastmoney_circuit_breaker_open / 现有缓存层 | ✅ |
| 每 commit 后跑 pytest / GUI smoke | ✅ |
| commit message 中文 + Co-Authored-By | ✅ |
