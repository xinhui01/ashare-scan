# Phase 2: 抽 IntradayTab 到 src/gui/tabs/intraday.py 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended).

**Goal:** 把 `src/gui/app.py` 中分时 tab 相关的 ~20 个方法 + ~10 widget 变量 + ~10 state 变量集体迁移到新文件 `src/gui/tabs/intraday.py`，建立 `IntradayTab` 类（持有 app 引用），主类 StockMonitorApp 改为实例化 IntradayTab。

**Architecture:** 1 commit。先 grep + Read 现有 intraday 相关代码全集 → 创建 IntradayTab 类（参考 LogTab 模板）→ 更新主类 setup_notebook 用 `self.intraday = IntradayTab(self, self.notebook)` → 把所有 `self.intraday_xxx` / `self._intraday_xxx` 改写为 `self.app.intraday.xxx`（在 IntradayTab 内部直接 `self.xxx`）+ app.py 内剩余的跨 tab 引用改 `self.intraday.xxx`。

**Tech Stack:** Python 3.12 + Tkinter + matplotlib

**Spec:** [`docs/superpowers/specs/2026-05-20-stock-gui-modularization-design.md`](../specs/2026-05-20-stock-gui-modularization-design.md)

**模板参考：** [`src/gui/tabs/log.py`](../../../src/gui/tabs/log.py)（Phase 1 已建立的 LogTab 模板）

---

## Task 0 — 基线

- [ ] **Step 1: git status 干净**

Run: `git -C D:\code\python\gupiao status`
Expected: `nothing to commit, working tree clean`

- [ ] **Step 2: pytest baseline**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `318 passed`（或当前最新基线）

- [ ] **Step 3: 记录基线行数**

Run: `(Get-Content D:\code\python\gupiao\src\gui\app.py | Measure-Object -Line).Lines`
Expected: ~7000 行

---

## Task 1 — 摸清 intraday tab 全部依赖

**Files:** 只读

- [ ] **Step 1: 列出所有 intraday 相关 def**

Run:
```powershell
grep -n "def.*intraday\|def setup_intraday\|def navigate_intraday\|def open_intraday" D:\code\python\gupiao\src\gui\app.py
```

应找到约 20 个方法（参考 spec 调研段）。

- [ ] **Step 2: 列出所有 self.intraday_* / self._intraday_* 引用**

Run:
```powershell
grep -n "self\.intraday_\|self\._intraday_" D:\code\python\gupiao\src\gui\app.py
```

预期：
- `self.intraday_tab` / `intraday_fig` / `intraday_price_ax` / `intraday_volume_ax` / `intraday_dist_ax` / `intraday_canvas`
- `self.intraday_title_var` / `intraday_day_var`
- `self.intraday_prev_btn` / `intraday_next_btn`
- `self._intraday_request_code` / `_intraday_loading_code` / `_intraday_request_offset` / `_intraday_loading_offset` / `_intraday_request_target_date` / `_intraday_loading_target_date`
- `self._intraday_day_offset` / `_intraday_available_dates` / `_intraday_selected_date`
- `self._intraday_payload_cache`
- **注意**：`self.intraday_source_var` 是全局数据源偏好（line 371），**留在 App 上**，不迁移

- [ ] **Step 3: 列出跨 tab 调用**

Run:
```powershell
grep -n "open_intraday_view\|self\.intraday_tab" D:\code\python\gupiao\src\gui\app.py
```

定位 detail tab / predict tab 哪里调用 `open_intraday_view` 跳转到分时。这些都改为 `self.intraday.open_view(...)`。

---

## Task 2 — 创建 src/gui/tabs/intraday.py

**Files:**
- Create: `D:\code\python\gupiao\src\gui\tabs\intraday.py`

- [ ] **Step 1: 创建空骨架**

写入：

```python
"""分时 Tab：matplotlib 1min K 线 + 成交量 + 价格分布。

包含：
- ttk.Frame 容器（self.frame）
- matplotlib Figure / 三个 Axes（self.fig / price_ax / volume_ax / dist_ax）
- 导航按钮（前一天/后一天）
- 标题 / 交易日 StringVar
- 加载/渲染各方法（_load / _apply / _draw_* / _resolve_*）
- 状态字段（request_code / loading_code / day_offset / available_dates / payload_cache）

跨 tab 引用通过 self.app.xxx 访问（如 self.app.notebook / status_var / _ui / _post_to_ui / stock_filter）。
全局数据源偏好 self.app.intraday_source_var 保留在 App 上。
"""
from __future__ import annotations

import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk

import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter

from src.utils.cancel_token import CancelToken

if TYPE_CHECKING:
    from src.gui.app import StockMonitorApp


class IntradayTab:
    """分时 tab：1min K 线 + 成交量 + 价格分布矩阵。"""

    _INTRADAY_CACHE_MAX = 50  # （从 app.py 移入，原 self._INTRADAY_CACHE_MAX）

    def __init__(self, app: "StockMonitorApp", notebook: ttk.Notebook) -> None:
        self.app = app
        # 状态字段（原 self._intraday_xxx）
        self.request_code: str = ""
        self.loading_code: str = ""
        self.request_offset: int = 0
        self.loading_offset: int = 0
        self.request_target_date: str = ""
        self.loading_target_date: str = ""
        self.day_offset: int = 0
        self.available_dates: List[str] = []
        self.selected_date: str = ""
        self.payload_cache: "OrderedDict[Tuple[str, int, str], Tuple[float, Dict[str, Any], bool]]" = OrderedDict()
        self._build(notebook)

    def _build(self, notebook: ttk.Notebook) -> None:
        """构建 widget。从原 setup_intraday_tab 整体迁移。"""
        # TODO: 这里写完整的 widget 构建（参考 app.py 原 setup_intraday_tab）
        pass
```

- [ ] **Step 2: 从 app.py 整体迁移 setup_intraday_tab 内容到 _build**

定位 `src/gui/app.py:1206-1245` 的 `setup_intraday_tab` 方法。整体复制其方法体到 `IntradayTab._build`，将：
- `self.intraday_tab` → `self.frame`
- `self.intraday_fig` → `self.fig`
- `self.intraday_price_ax` → `self.price_ax`
- `self.intraday_volume_ax` → `self.volume_ax`
- `self.intraday_dist_ax` → `self.dist_ax`
- `self.intraday_canvas` → `self.canvas`
- `self.intraday_title_var` → `self.title_var`
- `self.intraday_day_var` → `self.day_var`
- `self.intraday_prev_btn` → `self.prev_btn`
- `self.intraday_next_btn` → `self.next_btn`
- `self.navigate_intraday_day(-1)` → `self.navigate_day(-1)`
- `self.navigate_intraday_day(1)` → `self.navigate_day(1)`
- `self._draw_intraday_loading(...)` → `self._draw_loading(...)`

并在 `self.frame = ttk.Frame(notebook, padding="5")` 之后加 `notebook.add(self.frame, text="分时")` 替换原 `self.notebook.add(...)`。

- [ ] **Step 3: 把 ~17 个 intraday 业务方法迁移到 IntradayTab 类**

从 app.py 把以下方法整体复制到 IntradayTab 类，**去掉 _intraday / intraday 前缀**（变成清晰的 tab 内方法名）：

| 旧方法 | 新方法（IntradayTab 内） |
|---|---|
| `open_intraday_view` | `open_view` |
| `open_intraday_view_with_offset` | `open_view_with_offset` |
| `navigate_intraday_day` | `navigate_day` |
| `_refresh_intraday_nav_buttons` | `_refresh_nav_buttons` |
| `_load_intraday` | `_load` |
| `_apply_intraday_if_current` | `_apply_if_current` |
| `_finish_intraday_status` | `_finish_status` |
| `_draw_intraday_loading` | `_draw_loading` |
| `_draw_intraday_error` | `_draw_error` |
| `_resolve_intraday_base_price` | `_resolve_base_price` |
| `_resolve_intraday_average_price` | `_resolve_average_price` |
| `_normalize_intraday_auction_snapshot` | `_normalize_auction_snapshot` |
| `_build_intraday_tick_positions` | `_build_tick_positions` |
| `_draw_intraday_price_panel` | `_draw_price_panel` |
| `_draw_intraday_volume_panel` | `_draw_volume_panel` |
| `_draw_intraday_distribution_panel` | `_draw_distribution_panel` |
| `_draw_intraday_chart` | `_draw_chart` |

在方法体内：
- `self.intraday_xxx` → `self.xxx`
- `self._intraday_xxx` → `self.xxx`（已去前缀）
- `self.intraday_payload_cache` → `self.payload_cache`
- `self._INTRADAY_CACHE_MAX` → `self._INTRADAY_CACHE_MAX`（已挪类常量）
- 跨 tab 引用改 `self.app.xxx`：
  - `self.notebook` → `self.app.notebook`
  - `self.status_var` → `self.app.status_var`
  - `self.stock_filter` → `self.app.stock_filter`
  - `self._ui` → `self.app._ui`
  - `self._post_to_ui` → `self.app._post_to_ui`
  - `self._log` / `self._log_async` → `self.app._log` / `self.app._log_async`
  - `self.intraday_source_var` → `self.app.intraday_source_var`（这个留在 App）
  - `self._current_detail_code` → `self.app._current_detail_code`（属 detail tab，但 P3 才迁；这里先用 app 引用）

---

## Task 3 — 更新 src/gui/app.py

**Files:**
- Modify: `D:\code\python\gupiao\src\gui\app.py`

- [ ] **Step 1: 顶部加 import**

在 `from src.gui.tabs.log import LogTab` 后加：

```python
from src.gui.tabs.intraday import IntradayTab
```

- [ ] **Step 2: 删除主类 __init__ 中的 intraday 状态字段**

定位约 87-95 行 + 136 行的：
```python
self._intraday_request_code = ""
self._intraday_loading_code = ""
self._intraday_request_offset = 0
self._intraday_request_target_date = ""
self._intraday_loading_offset = 0
self._intraday_loading_target_date = ""
self._intraday_day_offset = 0
self._intraday_available_dates: List[str] = []
self._intraday_selected_date = ""
# ...
self._intraday_payload_cache: "OrderedDict[...]" = OrderedDict()
```

**删除所有上述行**（这些状态全迁到 IntradayTab）。

- [ ] **Step 3: setup_notebook 内替换 setup_intraday_tab 调用**

定位约 514 行 `self.setup_intraday_tab()`，改为：

```python
self.intraday = IntradayTab(self, self.notebook)
```

- [ ] **Step 4: 删除主类内 setup_intraday_tab 整方法**

定位约 1206-1245 行 `def setup_intraday_tab(self):`，整体删除。

- [ ] **Step 5: 删除主类内 17 个 intraday 业务方法**

逐个 grep 定位并删除（约 6313-7300 行段）：
- `def open_intraday_view`
- `def navigate_intraday_day`
- `def _refresh_intraday_nav_buttons`
- `def open_intraday_view_with_offset`
- `def _load_intraday`
- `def _apply_intraday_if_current`
- `def _finish_intraday_status`
- `def _draw_intraday_loading`
- `def _draw_intraday_error`
- `def _resolve_intraday_base_price`
- `def _resolve_intraday_average_price`
- `def _normalize_intraday_auction_snapshot`
- `def _build_intraday_tick_positions`
- `def _draw_intraday_price_panel`
- `def _draw_intraday_volume_panel`
- `def _draw_intraday_distribution_panel`
- `def _draw_intraday_chart`

- [ ] **Step 6: 更新 tab 注册表**

定位约 560 行：
```python
("intraday", self.intraday_tab, "分时", False),
```
改为：
```python
("intraday", self.intraday.frame, "分时", False),
```

- [ ] **Step 7: 更新 _on_notebook_tab_changed**

定位约 536-540 行：
```python
elif current is getattr(self, "intraday_tab", None):
    self._set_top_header_for_code(
        getattr(self, "_intraday_request_code", "")
        or getattr(self, "_current_detail_code", "")
        or ""
    )
```
改为：
```python
elif current is getattr(getattr(self, "intraday", None), "frame", None):
    self._set_top_header_for_code(
        getattr(self.intraday, "request_code", "")
        or getattr(self, "_current_detail_code", "")
        or ""
    )
```

- [ ] **Step 8: 更新 detail tab / predict tab 中调用 open_intraday_view 的位置**

Run:
```powershell
grep -n "open_intraday_view\|self\.intraday_tab\|self\.intraday_payload_cache" src/gui/app.py
```

把所有 `self.open_intraday_view(code)` 改成 `self.intraday.open_view(code)`，`self.open_intraday_view_with_offset(...)` 改 `self.intraday.open_view_with_offset(...)`，`self.intraday_tab` 改 `self.intraday.frame`，`self.intraday_payload_cache` 改 `self.intraday.payload_cache`。

- [ ] **Step 9: 验证残留 0 命中**

Run:
```powershell
grep -rn "self\.intraday_tab\|self\.intraday_fig\|self\.intraday_canvas\|self\.intraday_price_ax\|self\.intraday_volume_ax\|self\.intraday_dist_ax\|self\.intraday_title_var\|self\.intraday_day_var\|self\.intraday_prev_btn\|self\.intraday_next_btn\|self\._intraday_request_code\|self\._intraday_loading_code\|self\._intraday_day_offset\|self\._intraday_available_dates\|self\._intraday_selected_date\|self\._intraday_payload_cache\|self\._intraday_request_offset\|self\._intraday_loading_offset\|self\._intraday_request_target_date\|self\._intraday_loading_target_date" src/gui/ stock_gui.py
```

预期：**0 命中**（除了 IntradayTab 内部用 `self.xxx`）。

`self.intraday_source_var` 仍允许保留（这是 App 的全局数据源偏好，不属于 tab）。

---

## Task 4 — 验证 + commit

- [ ] **Step 1: 静态 import**

Run: `.venv\Scripts\python -c "from stock_gui import StockMonitorApp; from src.gui.tabs.intraday import IntradayTab; print('OK')"`
Expected: `OK`

- [ ] **Step 2: pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: 318 passed 不下降

- [ ] **Step 3: GUI 启动**

```powershell
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 8
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```
Expected: `OK`（启动 8 秒不崩）

- [ ] **Step 4: 行数对比**

Run: `(Get-Content D:\code\python\gupiao\src\gui\app.py | Measure-Object -Line).Lines`
Expected: ~6400 行（从 ~7000 减少 ~600）

Run: `(Get-Content D:\code\python\gupiao\src\gui\tabs\intraday.py | Measure-Object -Line).Lines`
Expected: ~600 行

- [ ] **Step 5: Commit**

```powershell
git -C D:\code\python\gupiao add src/gui/app.py src/gui/tabs/intraday.py
git -C D:\code\python\gupiao commit -m @'
重构（P2）：抽 IntradayTab 类到 src/gui/tabs/intraday.py

按 stock_gui.py 模块化拆分 spec 的 Phase 2，把分时 tab 相关的 17 个方法 +
10 个 widget 变量 + 10 个 state 字段集体迁移到新文件 src/gui/tabs/intraday.py
的 IntradayTab 类（持有 app 引用模板）。

主类 StockMonitorApp:
- __init__ 删除 _intraday_* 状态字段
- setup_notebook 改为 self.intraday = IntradayTab(self, self.notebook)
- setup_intraday_tab + 17 个 intraday 业务方法整体删除
- tab 注册表 / _on_notebook_tab_changed / detail-predict 跨 tab 调用全部改为 self.intraday.xxx

行为零变化：分时 tab 视觉/交互/数据流完全不变，pytest 全绿。
app.py 削减 ~600 行（7000 → 6400），符合 spec 减少 god class 体量的目标。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## 自检表

| 检查 | 状态 |
|---|---|
| spec 模板符合 LogTab 模式（持有 app 引用 + _build 构造） | ✅ |
| `self.frame` 作为 tab 容器统一命名（P1 reviewer 建议） | ✅ |
| 17 个方法去掉 intraday 前缀 | ✅ |
| 状态字段去掉 _intraday 前缀 | ✅ |
| `intraday_source_var` 留在 App（全局数据源偏好，非 tab 私有） | ✅ |
| 跨 tab 引用 `self.app.xxx` 显式 | ✅ |
| pytest 不下降 / GUI 启动正常 | ✅ |
| commit message 中文 + Co-Authored-By | ✅ |
