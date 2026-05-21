# Phase 3: predict tab 数据源指示标签 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended).

**Goal:** 在 predict tab 顶部加一个数据源指示标签，根据本次预测使用的涨停池数据源切换显示（东财 / 本地缓存 / spot 兜底 ⚠️ / 无数据 ❌），让用户一眼看出 cont 评分是不是在退化模式下产生的。

**Architecture:** 2 commit。Commit 1 在 StockDataFetcher 添加 source 跟踪字段 + 公开 getter + 单测。Commit 2 在 predict tab UI 加 label + 预测完成后查询 source 更新 label。

**Tech Stack:** Python 3.12 + Tkinter

**Spec:** [`docs/superpowers/specs/2026-05-20-intraday-fallback-and-derive-design.md`](../specs/2026-05-20-intraday-fallback-and-derive-design.md)

---

## Task 0 — 基线

- [ ] `git status` 干净
- [ ] pytest `312 passed`

---

## Task 1 — StockDataFetcher source 跟踪 + 单测（Commit 1）

**Files:**
- Modify: `D:\code\python\gupiao\stock_data.py`（init + get_limit_up_pool 各分支 + get_previous_limit_up_pool 各分支 + 新增 get_pool_source 方法）
- Create: `D:\code\python\gupiao\tests\test_pool_source_tracking.py`

- [ ] **Step 1: 写单测 `tests/test_pool_source_tracking.py`**

```python
"""测试 StockDataFetcher._last_pool_source 跟踪 + get_pool_source 公开接口。"""
from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import patch

from stock_data import StockDataFetcher


def _build_fetcher():
    instance = StockDataFetcher.__new__(StockDataFetcher)
    instance._log = lambda msg: None
    instance._limit_up_pool_cache = {}
    instance._prev_limit_up_pool_cache = {}
    instance._last_pool_source = {}
    return instance


class TestPoolSourceTracking:
    def test_get_pool_source_unknown_by_default(self):
        f = _build_fetcher()
        assert f.get_pool_source("20260520") == "unknown"
        assert f.get_pool_source("20260520", previous=True) == "unknown"

    def test_memory_cache_hit_source(self, monkeypatch):
        f = _build_fetcher()
        f._limit_up_pool_cache["20260520"] = pd.DataFrame([{"代码": "600000"}])
        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: "20260520")
        f.get_limit_up_pool("20260520")
        assert f.get_pool_source("20260520") == "cache_memory"

    def test_db_cache_hit_source(self, monkeypatch):
        f = _build_fetcher()
        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: "20260520")
        monkeypatch.setattr(
            "stock_store.load_limit_up_pool",
            lambda *args, **kwargs: pd.DataFrame([{"代码": "600000", "连板数": 1, "最新价": 11.0, "涨跌幅": 10.0}]),
        )
        monkeypatch.setattr("stock_store.save_limit_up_pool", lambda *args, **kwargs: None)
        f.get_limit_up_pool("20260520")
        assert f.get_pool_source("20260520") == "cache_db"

    def test_eastmoney_source(self, monkeypatch):
        f = _build_fetcher()
        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: "20260520")
        monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: False)
        monkeypatch.setattr("stock_store.load_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr("stock_store.save_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            "stock_data._retry_ak_call",
            lambda _fn, date=None: pd.DataFrame([{"代码": "600000", "连板数": 1, "最新价": 11.0, "涨跌幅": 10.0}]),
        )
        f.get_limit_up_pool("20260520")
        assert f.get_pool_source("20260520") == "eastmoney"

    def test_spot_fallback_source(self, monkeypatch):
        f = _build_fetcher()
        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: "20260520")
        monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: True)  # 熔断
        monkeypatch.setattr("stock_store.load_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr("stock_store.save_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr(f, "_recent_trade_dates", lambda d, n: ["20260519", "20260520"])
        # mock 派生返回非空
        monkeypatch.setattr(
            f, "_derive_limit_up_pool_from_spot",
            lambda *args, **kwargs: pd.DataFrame([{"代码": "600000", "连板数": 1, "最新价": 11.0, "涨跌幅": 10.0}]),
        )
        f.get_limit_up_pool("20260520")
        assert f.get_pool_source("20260520") == "spot_fallback"

    def test_empty_source(self, monkeypatch):
        f = _build_fetcher()
        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: "20260520")
        monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: False)
        monkeypatch.setattr("stock_store.load_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            "stock_data._retry_ak_call",
            lambda _fn, date=None: pd.DataFrame(),  # 东财返空
        )
        df = f.get_limit_up_pool("20260520")
        assert df.empty
        assert f.get_pool_source("20260520") == "empty"
```

- [ ] **Step 2: 跑测试看 FAIL**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest tests/test_pool_source_tracking.py -v --tb=short 2>&1 | Select-Object -Last 20`
Expected: 部分或全部 FAIL（AttributeError 或行为不符合）

- [ ] **Step 3: 在 stock_data.py StockDataFetcher.__init__ 加字段**

定位 `__init__`（约 525 行）。在已有 `self._prev_limit_up_pool_cache` 后加：

```python
# 跟踪每个日期涨停池数据来源："cache_memory" / "cache_db" / "eastmoney" / "spot_fallback" / "empty"
self._last_pool_source: Dict[str, str] = {}
self._last_prev_pool_source: Dict[str, str] = {}
```

- [ ] **Step 4: 在 get_limit_up_pool 各分支末尾记录 source**

定位 `get_limit_up_pool`，在每个返回前加 source 记录：

| 返回点 | source 值 |
|---|---|
| 内存缓存命中且非空 | `"cache_memory"` |
| SQLite 缓存命中 | `"cache_db"` |
| 东财非异常非空 | `"eastmoney"` |
| 东财非异常返空 | `"empty"` |
| spot 兜底成功 | `"spot_fallback"` |
| 所有源失败 | `"empty"` |

例如东财成功分支前加：
```python
self._last_pool_source[date_key] = "eastmoney"
return df
```

同样改造 `get_previous_limit_up_pool`，写入 `self._last_prev_pool_source`。

- [ ] **Step 5: 新增 get_pool_source 公开方法**

在 `get_previous_limit_up_pool` 之后加：

```python
def get_pool_source(self, date_key: str, *, previous: bool = False) -> str:
    """返回最近一次 get_limit_up_pool / get_previous_limit_up_pool 对该日期的数据来源。

    取值：
    - "cache_memory" — 内存缓存命中
    - "cache_db" — SQLite 缓存命中
    - "eastmoney" — 东财在线
    - "spot_fallback" — spot 兜底派生
    - "empty" — 所有源失败 / 东财返空
    - "unknown" — 未查询过此日期
    """
    key = str(date_key or "").strip()
    if not key:
        return "unknown"
    source_map = self._last_prev_pool_source if previous else self._last_pool_source
    return source_map.get(key, "unknown")
```

- [ ] **Step 6: 跑测试看 PASS**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest tests/test_pool_source_tracking.py -v --tb=short 2>&1 | Select-Object -Last 20`
Expected: 6 case 全过

- [ ] **Step 7: 全量 pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: 312 + 6 = 318 passed

- [ ] **Step 8: Commit 1**

```powershell
git -C D:\code\python\gupiao add tests/test_pool_source_tracking.py stock_data.py
git -C D:\code\python\gupiao commit -m @'
新增：StockDataFetcher 涨停池数据源跟踪 + get_pool_source 公开接口

加 self._last_pool_source / _last_prev_pool_source 字典跟踪每个日期最近一次
取涨停池的数据来源（cache_memory / cache_db / eastmoney / spot_fallback / empty），
供 GUI 显示数据源指示标签使用。

6 个单测覆盖 6 种来源场景。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## Task 2 — predict tab 数据源标签 UI（Commit 2）

**Files:**
- Modify: `D:\code\python\gupiao\src\gui\app.py`（predict tab 顶部 sent_bar 区域加 label + _apply_predict_result 末尾刷新）

- [ ] **Step 1: 定位 sent_bar 构建位置**

Run: `grep -n "sent_bar = ttk.Frame(predict_frame" src/gui/app.py`

约 1307 行。

- [ ] **Step 2: 在 sent_bar 内右侧加数据源 label（详情/刷新按钮左边）**

定位约 1322 行 `self._sentiment_summary_label.pack(side=tk.LEFT, fill=tk.X, expand=True)`，在它之后但在 `ttk.Button(sent_bar, text="详情", ...)` 之前插入：

```python
# 数据源指示标签（在预测完成后由 _refresh_data_source_label 更新）
self._predict_data_source_label = ttk.Label(
    sent_bar, text="", foreground="#888",
)
self._predict_data_source_label.pack(side=tk.RIGHT, padx=(8, 4))
```

注意：放在 RIGHT 侧，紧贴"详情"按钮左边。

- [ ] **Step 3: 加 _refresh_data_source_label 方法**

在 predict 相关方法附近（如 `_apply_predict_result` 之前），新增：

```python
def _refresh_data_source_label(self, trade_date: str) -> None:
    """根据涨停池数据源更新顶部指示标签。

    调用时机：每次 _apply_predict_result 后。
    """
    if not hasattr(self, "_predict_data_source_label"):
        return
    try:
        source = self.stock_filter.fetcher.get_pool_source(trade_date)
    except Exception:
        source = "unknown"
    label = self._predict_data_source_label
    source_text = {
        "eastmoney": ("数据: 东财", "#888"),
        "cache_memory": ("数据: 本地缓存", "#888"),
        "cache_db": ("数据: 本地缓存", "#888"),
        "spot_fallback": ("数据: spot 兜底 ⚠️", "#d08000"),  # 橙色
        "empty": ("数据: 无 ❌", "#c62828"),  # 红色
        "unknown": ("", "#888"),
    }
    text, fg = source_text.get(source, ("", "#888"))
    try:
        label.configure(text=text, foreground=fg)
    except Exception:
        pass
```

- [ ] **Step 4: 在 _apply_predict_result 末尾调用 refresh**

定位 `_apply_predict_result`（约 2965 行）。找到方法末尾（status_var.set 之后），追加：

```python
# 更新数据源指示标签
predict_date = result.get("today_date") or result.get("trade_date") or ""
self._refresh_data_source_label(predict_date)
```

注意：`result` 是 predict_limit_up_candidates 返回的 dict，包含 today_date 字段。如果字段名不同，用 grep 找。

- [ ] **Step 5: pytest 不下降**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: 318 passed

- [ ] **Step 6: import / GUI 启动**

```powershell
.venv\Scripts\python -c "from src.gui.app import StockMonitorApp; print('OK')"
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 6
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```

- [ ] **Step 7: Commit 2**

```powershell
git -C D:\code\python\gupiao add src/gui/app.py
git -C D:\code\python\gupiao commit -m @'
新功能：predict tab 顶部加数据源指示标签

在 sentiment bar 右侧加一个 ttk.Label，预测完成后查 fetcher.get_pool_source(date)
更新文案 + 颜色：
- 东财在线 / 本地缓存：灰色"数据: XXX"
- spot 兜底：橙色"数据: spot 兜底 ⚠️"
- 无数据：红色"数据: 无 ❌"

让用户一眼看出 cont 评分是否在退化模式下产生。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## Task 3 — 终验

- [ ] git log 看 2 commit
- [ ] pytest 318 passed
- [ ] 报告等用户确认 push

## 自检表

| 检查 | 状态 |
|---|---|
| TDD 红→绿 | ✅ |
| 6 种数据来源全覆盖 | ✅ |
| UI 颜色编码：灰/橙/红 | ✅ |
| 不动现有 sent_bar 其他元素 | ✅ |
| 每个 commit 后跑 pytest | ✅ |
