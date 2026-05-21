# Phase 2: 从 intraday 派生 首次封板时间 + 炸板次数 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended).

**Goal:** spot 兜底派生涨停池时，对每只涨停股调 intraday 拉 1min 数据，精确计算"首次封板时间"和"炸板次数"，修复 cont 评分在退化模式下"未炸板+15"满分偏高的问题。

**Architecture:** 2 commit。Commit 1 TDD 写 2 个 helper + 单测。Commit 2 集成到 `_derive_limit_up_pool_from_spot`，对每只候选股并发拉 intraday + 调 helper 填充字段。

**Tech Stack:** Python 3.12 + pandas + akshare + 现有 ThreadPoolExecutor

**Spec:** [`docs/superpowers/specs/2026-05-20-intraday-fallback-and-derive-design.md`](../specs/2026-05-20-intraday-fallback-and-derive-design.md)

---

## Task 0 — 基线

- [ ] `git status` 干净
- [ ] pytest `298 passed`
- [ ] import OK

---

## Task 1 — TDD: 2 个 helper + 单测（Commit 1）

**Files:**
- Create: `D:\code\python\gupiao\tests\test_intraday_derive.py`
- Modify: `D:\code\python\gupiao\stock_data.py`（在 StockDataFetcher 类内或类前新增 2 个 helper）

- [ ] **Step 1: 写单测 `tests/test_intraday_derive.py`**

```python
"""测试 intraday 派生 首次封板时间 + 炸板次数 helper。"""
from __future__ import annotations

import pandas as pd
import pytest

from stock_data import _derive_seal_time_from_intraday, _count_intraday_breaks


def _make_intraday(rows):
    """rows: [(time_str, close_price), ...]"""
    return pd.DataFrame({
        "time": pd.to_datetime([r[0] for r in rows]),
        "close": [r[1] for r in rows],
    })


class TestDeriveSealTime:
    def test_empty_returns_none(self):
        assert _derive_seal_time_from_intraday(pd.DataFrame(), 11.0) is None

    def test_no_seal_returns_none(self):
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.0),
            ("2026-05-20 09:31:00", 10.5),
            ("2026-05-20 09:32:00", 10.8),
        ])
        assert _derive_seal_time_from_intraday(df, 11.0) is None

    def test_seal_at_open(self):
        df = _make_intraday([
            ("2026-05-20 09:30:00", 11.0),  # 秒板
            ("2026-05-20 09:31:00", 11.0),
            ("2026-05-20 09:32:00", 11.0),
        ])
        result = _derive_seal_time_from_intraday(df, 11.0)
        assert result is not None
        assert result.startswith("09:30")

    def test_seal_mid_morning(self):
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.5),
            ("2026-05-20 10:15:00", 10.8),
            ("2026-05-20 10:30:00", 11.0),
            ("2026-05-20 10:31:00", 11.0),
        ])
        result = _derive_seal_time_from_intraday(df, 11.0)
        assert result is not None
        assert result.startswith("10:30")

    def test_seal_with_tolerance(self):
        # 价格略低于涨停价但在容差内（0.1%）
        # 涨停价 11.00, 容差 0.1% → 接受 10.989 及以上
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.5),
            ("2026-05-20 09:35:00", 10.99),  # 在 0.1% 容差内
        ])
        result = _derive_seal_time_from_intraday(df, 11.0, tolerance_pct=0.1)
        assert result is not None
        assert result.startswith("09:35")

    def test_seal_outside_tolerance(self):
        # 价格 10.95（差 4.5%），不算封板
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.5),
            ("2026-05-20 09:35:00", 10.95),
        ])
        assert _derive_seal_time_from_intraday(df, 11.0, tolerance_pct=0.1) is None


class TestCountIntradayBreaks:
    def test_empty_returns_zero(self):
        assert _count_intraday_breaks(pd.DataFrame(), 11.0) == 0

    def test_no_seal_returns_zero(self):
        # 全天没封板
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.0),
            ("2026-05-20 11:00:00", 10.5),
            ("2026-05-20 14:00:00", 10.8),
        ])
        assert _count_intraday_breaks(df, 11.0) == 0

    def test_seal_no_break(self):
        # 封板后一直保持
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.5),
            ("2026-05-20 10:00:00", 11.0),  # 封板
            ("2026-05-20 10:30:00", 11.0),
            ("2026-05-20 14:00:00", 11.0),
        ])
        assert _count_intraday_breaks(df, 11.0) == 0

    def test_one_break(self):
        # 封板 → 跌破 → 再封 = 1 次炸板
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.5),
            ("2026-05-20 10:00:00", 11.0),  # 封板
            ("2026-05-20 10:30:00", 10.8),  # 跌破
            ("2026-05-20 11:00:00", 11.0),  # 再封
        ])
        assert _count_intraday_breaks(df, 11.0) == 1

    def test_multiple_breaks(self):
        # 多次封板炸板
        df = _make_intraday([
            ("2026-05-20 09:30:00", 11.0),  # 封板 #1
            ("2026-05-20 09:45:00", 10.8),  # 跌破
            ("2026-05-20 10:00:00", 11.0),  # 封板 #2 (1 次炸板)
            ("2026-05-20 10:30:00", 10.7),  # 跌破
            ("2026-05-20 11:00:00", 11.0),  # 封板 #3 (2 次炸板)
        ])
        assert _count_intraday_breaks(df, 11.0) == 2
```

- [ ] **Step 2: 跑测试看 FAIL**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest tests/test_intraday_derive.py -v --tb=short 2>&1 | Select-Object -Last 15`
Expected: `ImportError: cannot import name '_derive_seal_time_from_intraday'`

- [ ] **Step 3: 在 stock_data.py 模块级实现 2 个 helper**

定位 stock_data.py 顶部 import 段之后、`class StockDataFetcher` 之前的位置（可用 grep `^class StockDataFetcher` 定位）。在 class 之前插入：

```python
def _derive_seal_time_from_intraday(
    intraday_df: "pd.DataFrame",
    limit_up_price: float,
    tolerance_pct: float = 0.1,
) -> Optional[str]:
    """从 1min 分时找首次封板时间。

    定义：close ≥ limit_up_price × (1 - tolerance_pct/100) 即视为封板。
    返回 "HH:MM:SS" 字符串，无封板返回 None。
    """
    if intraday_df is None or intraday_df.empty or "close" not in intraday_df.columns:
        return None
    if "time" not in intraday_df.columns:
        return None
    if not limit_up_price or limit_up_price <= 0:
        return None
    threshold = float(limit_up_price) * (1 - float(tolerance_pct) / 100)
    for _, row in intraday_df.iterrows():
        try:
            close = float(row.get("close"))
        except (TypeError, ValueError):
            continue
        if close >= threshold:
            t = row.get("time")
            if pd.isna(t):
                continue
            # 兼容 datetime / Timestamp / str
            if hasattr(t, "strftime"):
                return t.strftime("%H:%M:%S")
            return str(t)[-8:]  # 取末 8 字符 "HH:MM:SS"
    return None


def _count_intraday_breaks(
    intraday_df: "pd.DataFrame",
    limit_up_price: float,
    tolerance_pct: float = 0.1,
) -> int:
    """数封板后又跌破涨停价的次数（炸板次数）。

    定义：进入"封板状态"（close ≥ threshold）后又出现 close < threshold → 1 次炸板。
    """
    if intraday_df is None or intraday_df.empty or "close" not in intraday_df.columns:
        return 0
    if not limit_up_price or limit_up_price <= 0:
        return 0
    threshold = float(limit_up_price) * (1 - float(tolerance_pct) / 100)
    breaks = 0
    state_sealed = False
    for _, row in intraday_df.iterrows():
        try:
            close = float(row.get("close"))
        except (TypeError, ValueError):
            continue
        if state_sealed:
            if close < threshold:
                breaks += 1
                state_sealed = False
        else:
            if close >= threshold:
                state_sealed = True
    return breaks
```

注意：
- `Optional[str]` 需要 typing 已 import（stock_data.py 顶部应该已经有）
- helper 是模块级函数（不挂 class），方便单测直接 import
- 同步在文件顶部不需要新增 import

- [ ] **Step 4: 跑测试看 PASS**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest tests/test_intraday_derive.py -v --tb=short 2>&1 | Select-Object -Last 20`
Expected: 11 case 全过

- [ ] **Step 5: 跑全量 pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: 298 + 11 = ~309 passed

- [ ] **Step 6: Commit 1**

```powershell
git -C D:\code\python\gupiao add tests/test_intraday_derive.py stock_data.py
git -C D:\code\python\gupiao commit -m @'
新增：intraday 派生 首次封板时间 + 炸板次数 helper + 单测

新增 stock_data._derive_seal_time_from_intraday（首根 close ≥ 涨停价的 bar）
和 _count_intraday_breaks（封板→跌破→再封 的次数）。

11 个单测：空 DataFrame / 无封板 / 秒板 / 中午封板 / 容差边界 / 多次炸板。

本 commit 仅新增 helper，不接入 _derive_limit_up_pool_from_spot。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## Task 2 — 集成到 _derive_limit_up_pool_from_spot（Commit 2）

**Files:**
- Modify: `D:\code\python\gupiao\stock_data.py`（修改 `_derive_limit_up_pool_from_spot` 方法）

- [ ] **Step 1: 定位 _derive_limit_up_pool_from_spot**

Run: `grep -n "def _derive_limit_up_pool_from_spot" stock_data.py`

约 1450 行附近（具体看实施前的实际位置）。

- [ ] **Step 2: 修改派生循环，对每只涨停股拉 intraday 计算 seal_time + breaks**

在现有派生循环（生成 out list 的循环）中，对每个 `row` 多做：

```python
# 派生 首次封板时间 + 炸板次数
seal_time = ""
breaks_count = 0
try:
    limit_up_price = float(row.get("最新价") or 0)
    if limit_up_price > 0:
        intraday_df = self.get_intraday_data(
            code,
            day_offset=0,
            target_trade_date=trade_date,
            include_meta=False,
        )
        if intraday_df is not None and not intraday_df.empty:
            seal_time = _derive_seal_time_from_intraday(
                intraday_df, limit_up_price, tolerance_pct=0.1,
            ) or ""
            breaks_count = _count_intraday_breaks(
                intraday_df, limit_up_price, tolerance_pct=0.1,
            )
except Exception as exc:
    if self._log:
        self._log(f"涨停池 {trade_date} {code} intraday 派生失败: {exc}")
```

把现有 `"首次封板时间": ""` 改为 `"首次封板时间": seal_time`，`"炸板次数": 0` 改为 `"炸板次数": breaks_count`。

- [ ] **Step 3: 并发优化（可选，性能不可接受时再做）**

如果实测预测启动慢于 60s（50 只候选 × intraday 拉取），改为 ThreadPoolExecutor 并发：

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def _fetch_one(rec_row):
    code_local = str(rec_row.get("代码", "")).strip().zfill(6)
    try:
        df = self.get_intraday_data(code_local, target_trade_date=trade_date, include_meta=False)
        if df is None or df.empty:
            return code_local, "", 0
        lup = float(rec_row.get("最新价") or 0)
        seal = _derive_seal_time_from_intraday(df, lup, 0.1) or ""
        brks = _count_intraday_breaks(df, lup, 0.1)
        return code_local, seal, brks
    except Exception:
        return code_local, "", 0

with ThreadPoolExecutor(max_workers=6) as ex:
    futures = {ex.submit(_fetch_one, r): r for r in rows}
    seal_map = {}
    for fut in as_completed(futures):
        code_x, seal_x, brks_x = fut.result()
        seal_map[code_x] = (seal_x, brks_x)
```

然后在主循环里用 `seal_map.get(code, ("", 0))` 取结果。

**先不做并发优化**，串行即可，等用户反馈慢再升级。

- [ ] **Step 4: 跑 pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: ~309 passed 不下降

- [ ] **Step 5: import 检查**

Run: `.venv\Scripts\python -c "from stock_data import _derive_seal_time_from_intraday, _count_intraday_breaks; print('OK')"`

- [ ] **Step 6: GUI 启动**

```powershell
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 6
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```

- [ ] **Step 7: Commit 2**

```powershell
git -C D:\code\python\gupiao add stock_data.py
git -C D:\code\python\gupiao commit -m @'
新功能：spot 兜底涨停池补全 首次封板时间 + 炸板次数

_derive_limit_up_pool_from_spot 派生时，对每只涨停股调 get_intraday_data
拉 1min 分时（含 sina 兜底），用 _derive_seal_time_from_intraday +
_count_intraday_breaks 算精确字段，替代之前的留空 / 默认 0。

修复 cont 评分在 spot 兜底模式下"未炸板 +15"满分偏高问题。

未做并发优化（串行拉），如实测过慢再升级 ThreadPoolExecutor。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## Task 3 — 终验

- [ ] git log 看 2 commit
- [ ] pytest ~309 passed
- [ ] 报告等用户确认 push

## 自检表

| 检查 | 状态 |
|---|---|
| TDD 红→绿 | ✅ |
| spec 覆盖：2 helper + 集成 spot 派生 | ✅ |
| 容差 0.1% 在 helper 内可配置 | ✅ |
| 不动 _eastmoney_circuit_breaker_open / intraday fallback 已存在 | ✅ |
| 不动 _derive_limit_up_pool_from_spot 的连板数推断逻辑 | ✅ |
| 每个 commit 后跑 pytest | ✅ |
