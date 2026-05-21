# Phase 3: 抽 DetailTab 到 src/gui/tabs/detail.py 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended).

**Goal:** 把 `src/gui/app.py` 中股票详情 tab 相关的 ~35 个方法 + ~15 widget 变量 + ~20 state 字段集体迁移到新文件 `src/gui/tabs/detail.py`，建立 `DetailTab` 类。

**Architecture:** 1 commit，与 P2 拆 IntradayTab 同套模式。

**Tech Stack:** Python 3.12 + Tkinter + matplotlib

**Spec:** [`docs/superpowers/specs/2026-05-20-stock-gui-modularization-design.md`](../specs/2026-05-20-stock-gui-modularization-design.md)

**模板参考：**
- [`src/gui/tabs/log.py`](../../../src/gui/tabs/log.py) — LogTab（最简模板）
- [`src/gui/tabs/intraday.py`](../../../src/gui/tabs/intraday.py) — IntradayTab（P2 已建立的大 tab 模板）

---

## Task 0 — 基线

- [ ] `git status` 干净
- [ ] pytest `318 passed`
- [ ] app.py 当前 `~6127 行`

---

## Task 1 — 摸清 detail tab 依赖

只读 grep：

```powershell
grep -n "def setup_detail_tab\|def show_stock_detail\|def _load_detail\|def _apply_detail\|def _show_detail\|def _update_detail_ui\|def _refresh_detail\|def toggle_detail\|def _bind_detail\|def _move_detail\|def _resolve_detail\|def _apply_detail_chart\|def _maybe_load_more_detail\|def _load_more_detail\|def _apply_more_detail\|def _detail_scroll\|def on_detail_chart\|def _cancel_scheduled_detail\|def _schedule_show_stock_detail\|def _trigger_detail_revalidate\|def _finish_detail_status\|def _draw_detail\|def _reset_detail_chart_axes\|def _show_empty_detail_chart\|def _prepare_detail_chart_dataset\|def _build_detail_flow_series\|def _apply_quick_detail_if_current" src/gui/app.py
```

预期约 35 个方法（一处例外：`_show_sentiment_detail` 是 sentiment 不是 detail tab，**不迁**）。

State / widget grep：

```powershell
grep -n "self\.detail_\|self\._detail_\|self\._current_detail_code\|self\._DETAIL_CACHE_MAX\|self\._detail_payload_cache\|self\._detail_last_revalidate_ts" src/gui/app.py
```

预期：
- Widget：`detail_tab_frame` / `detail_info_header` / `detail_summary_toggle_btn` / `detail_summary_status_var` / `detail_labels` / `detail_label_caption_vars` / `detail_chart_header` / `detail_chart_toggle_btn` / `detail_chart_status_var` / `detail_flow_toggle_btn` / `detail_flow_status_var` / `detail_chart_window_var` / `detail_chart_window_scale` / `detail_chart_window_label_var` / `detail_chart_placeholder` / `info_frame` / `detail_watch_btn`/`detail_watch_note_btn` 已删 (Phase 1 watchlist 清理)
- 也含 matplotlib chart 相关 widget（fig / canvas / axes）—— 通过 grep `self.chart` 单独定位
- State：`_detail_request_code` / `_detail_loading_code` / `_detail_after_id` / `_current_detail_code` / `_detail_chart_dates` / `_detail_chart_window_size` / `_detail_chart_window_start` / `_detail_chart_history` / `_detail_chart_analysis` / `_detail_chart_scroll_bound` / `_detail_chart_slider_updating` / `_detail_chart_dragging` / `_detail_chart_drag_moved` / `_detail_chart_drag_start_x` / `_detail_chart_drag_start_window` / `_detail_chart_click_target_date` / `_detail_chart_loading_more` / `_detail_chart_loaded_days` / `_detail_summary_expanded` / `_detail_chart_expanded` / `_detail_flow_expanded` / `_detail_payload_cache` / `_detail_last_revalidate_ts`

跨 tab 调用（必须找到全部）：

```powershell
grep -n "self\.show_stock_detail\|self\.notebook\.select(self\.detail_tab_frame\|self\.detail_tab_frame\|self\._schedule_show_stock_detail" src/gui/app.py
```

预期：result tab 双击 / predict tab 双击 / status_bar 跳转 都调用 `show_stock_detail` 或 `notebook.select(self.detail_tab_frame)`。

---

## Task 2 — 创建 src/gui/tabs/detail.py

**Files:**
- Create: `D:\code\python\gupiao\src\gui\tabs\detail.py`

- [ ] **Step 1: 写空骨架（参照 IntradayTab）**

```python
"""股票详情 Tab：历史摘要 + K 线图 + 大单净额图。

包含：
- ttk.Frame 容器（self.frame）
- 历史摘要折叠区（labels / label_caption_vars）
- matplotlib Figure / 3 个 axes（price / volume / flow）
- 滑块控制 K 线窗口
- K 线点击/拖拽事件（触发分时 tab 跳转）
- 数据加载/缓存

跨 tab 引用走 self.app.xxx：
- self.app.notebook
- self.app.status_var
- self.app.stock_filter
- self.app._ui / _post_to_ui
- self.app._log_async
- self.app._set_top_header_for_code / _clear_top_header
- self.app.intraday.open_view_with_offset（K 线点击跳分时）
- self.app.history_source_var（全局数据源偏好）
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


class DetailTab:
    """股票详情 tab：K 线 + 摘要 + 大单流。"""

    _DETAIL_CACHE_MAX = 50  # 从 app.py 移入

    def __init__(self, app: "StockMonitorApp", notebook: ttk.Notebook) -> None:
        self.app = app
        # 状态字段（原 self._detail_* 和 self._current_detail_code）
        self.request_code: str = ""
        self.loading_code: str = ""
        self.after_id = None
        self.current_code: str = ""  # 原 _current_detail_code
        self.chart_dates: List[str] = []
        self.chart_window_size: int = 60
        self.chart_window_start: int = 0
        self.chart_history = None
        self.chart_analysis: Dict[str, Any] = {}
        self.chart_scroll_bound: bool = False
        self.chart_slider_updating: bool = False
        self.chart_dragging: bool = False
        self.chart_drag_moved: bool = False
        self.chart_drag_start_x: float = 0.0
        self.chart_drag_start_window: int = 0
        self.chart_click_target_date: str = ""
        self.chart_loading_more: bool = False
        self.chart_loaded_days: int = 0
        self.summary_expanded: bool = False
        self.chart_expanded: bool = True
        self.flow_expanded: bool = False
        self.payload_cache: "OrderedDict[str, Tuple[float, Dict[str, Any]]]" = OrderedDict()
        self.last_revalidate_ts: Dict[str, float] = {}
        self._build(notebook)

    def _build(self, notebook: ttk.Notebook) -> None:
        """构建 widget。原 setup_detail_tab 内容整体迁移到这里。"""
        pass  # 完整实现见 Step 2
```

- [ ] **Step 2: 整体迁移 setup_detail_tab → _build**

定位 `src/gui/app.py:1074` 的 `setup_detail_tab`。整体复制方法体到 `DetailTab._build`，替换：

| app.py 原 | DetailTab 内 |
|---|---|
| `self.detail_tab_frame` | `self.frame` |
| `self.detail_info_header` | `self.info_header` |
| `self.detail_summary_toggle_btn` | `self.summary_toggle_btn` |
| `self.detail_summary_status_var` | `self.summary_status_var` |
| `self.detail_labels` | `self.labels` |
| `self.detail_label_caption_vars` | `self.label_caption_vars` |
| `self.info_frame` | `self.info_frame`（在 detail tab 内独占，可保留同名） |
| `self.detail_chart_header` | `self.chart_header` |
| `self.detail_chart_toggle_btn` | `self.chart_toggle_btn` |
| `self.detail_chart_status_var` | `self.chart_status_var` |
| `self.detail_flow_toggle_btn` | `self.flow_toggle_btn` |
| `self.detail_flow_status_var` | `self.flow_status_var` |
| `self.detail_chart_window_var` | `self.chart_window_var` |
| `self.detail_chart_window_scale` | `self.chart_window_scale` |
| `self.detail_chart_window_label_var` | `self.chart_window_label_var` |
| `self.detail_chart_placeholder` | `self.chart_placeholder` |
| 其它 chart 相关 widget（fig / canvas / 3 axes / chart_frame） | 保持原名（在 detail tab 内独占）或加前缀避免冲突 |

并在开头加 `notebook.add(self.frame, text="股票详情")` 替换原 `self.notebook.add(...)`。

`self.toggle_detail_summary_section` 等 command 改为 `self.toggle_summary_section`（前缀去掉）。

- [ ] **Step 3: 整体迁移 ~35 个 detail 业务方法**

把以下方法整体复制到 `DetailTab` 类（**去掉 detail 前缀**）：

| 旧方法 | 新方法（DetailTab 内） |
|---|---|
| `show_stock_detail` | `show` |
| `_load_detail` | `_load` |
| `_apply_quick_detail_if_current` | `_apply_quick_if_current` |
| `_apply_detail_if_current` | `_apply_if_current` |
| `_finish_detail_status` | `_finish_status` |
| `_show_detail_loading` | `_show_loading` |
| `_show_detail_error` | `_show_error` |
| `_update_detail_ui` | `_update_ui` |
| `_refresh_detail_metric_labels` | `_refresh_metric_labels` |
| `_cancel_scheduled_detail` | `_cancel_scheduled` |
| `_schedule_show_stock_detail` | `_schedule_show` |
| `_trigger_detail_revalidate` | `_trigger_revalidate` |
| `_reset_detail_chart_axes` | `_reset_chart_axes` |
| `_show_empty_detail_chart` | `_show_empty_chart` |
| `_prepare_detail_chart_dataset` | `_prepare_chart_dataset` |
| `_draw_detail_price_panel` | `_draw_price_panel` |
| `_build_detail_flow_series` | `_build_flow_series` |
| `_draw_detail_volume_panel` | `_draw_volume_panel` |
| `_draw_detail_flow_panel` | `_draw_flow_panel` |
| `_resolve_detail_chart_window` | `_resolve_chart_window` |
| `_apply_detail_chart_window` | `_apply_chart_window` |
| `toggle_detail_summary_section` | `toggle_summary_section` |
| `toggle_detail_chart_section` | `toggle_chart_section` |
| `_apply_detail_section_visibility` | `_apply_section_visibility` |
| `toggle_detail_flow_section` | `toggle_flow_section` |
| `on_detail_chart_window_changed` | `on_chart_window_changed` |
| `_bind_detail_chart_scroll` | `_bind_chart_scroll` |
| `_move_detail_chart_window` | `_move_chart_window` |
| `_maybe_load_more_detail_history` | `_maybe_load_more_history` |
| `_load_more_detail_history` | `_load_more_history` |
| `_apply_more_detail_history_if_current` | `_apply_more_history_if_current` |
| `_detail_scroll_delta` | `_scroll_delta` |
| `on_detail_chart_scroll` | `on_chart_scroll` |
| `on_detail_chart_mousewheel` | `on_chart_mousewheel` |
| `on_detail_chart_click` | `on_chart_click` |
| `on_detail_chart_drag_motion` | `on_chart_drag_motion` |
| `on_detail_chart_drag_release` | `on_chart_drag_release` |

方法内部：
- `self.detail_xxx` → `self.xxx`（已去前缀）
- `self._detail_xxx` → `self.xxx`（已去前缀）
- `self._current_detail_code` → `self.current_code`
- `self._detail_payload_cache` → `self.payload_cache`
- `self._DETAIL_CACHE_MAX` → `self._DETAIL_CACHE_MAX`（类常量保留）
- `self._detail_last_revalidate_ts` → `self.last_revalidate_ts`
- `self._detail_after_id` → `self.after_id`

跨 tab 引用改 `self.app.xxx`：
- `self.notebook` → `self.app.notebook`
- `self.status_var` → `self.app.status_var`
- `self.stock_filter` → `self.app.stock_filter`
- `self._ui` → `self.app._ui`
- `self._post_to_ui` → `self.app._post_to_ui`
- `self._log` / `self._log_async` → `self.app._log` / `self.app._log_async`
- `self._set_top_header_for_code` → `self.app._set_top_header_for_code`
- `self._clear_top_header` → `self.app._clear_top_header`
- `self.history_source_var` → `self.app.history_source_var`
- `self.root` → `self.app.root`
- `self.intraday.open_view_with_offset(...)` → `self.app.intraday.open_view_with_offset(...)`（K 线点击跳分时）

---

## Task 3 — 更新 src/gui/app.py

- [ ] **Step 1: import**

在 `from src.gui.tabs.intraday import IntradayTab` 后加 `from src.gui.tabs.detail import DetailTab`。

- [ ] **Step 2: 删 __init__ 中的 detail 状态字段（plan Task 1 列的 ~20 个）**

包括 `self._current_detail_code = ""` 和所有 `self._detail_xxx`。

- [ ] **Step 3: setup_notebook 改实例化**

`self.setup_detail_tab()` → `self.detail = DetailTab(self, self.notebook)`

注意 setup_notebook 顺序：detail 必须在 intraday 之前装配？看现有顺序确认。**实际上不强制顺序**，因为 cross-tab 引用是 `self.app.intraday.xxx`，运行时延迟求值。但如果 detail 的 `_build` 内立即调用 `self.app.intraday.xxx`，则 intraday 必须先装配。一般 detail tab 不在 build 期间跳 intraday，所以顺序无所谓。

- [ ] **Step 4: 删 setup_detail_tab 整方法**

- [ ] **Step 5: 删 35 个 detail 业务方法**

逐个 grep 定位 + 删除。

- [ ] **Step 6: 更新 tab 注册表**

`("detail", self.detail_tab_frame, "股票详情", False)` → `("detail", self.detail.frame, "股票详情", False)`

- [ ] **Step 7: 更新 _on_notebook_tab_changed**

如果引用 `self.detail_tab_frame`，改为 `self.detail.frame`。

- [ ] **Step 8: 更新所有跨 tab 调用**

```powershell
grep -n "self\.show_stock_detail\|self\.notebook\.select(self\.detail_tab_frame\|self\.detail_tab_frame\|self\.detail_labels\|self\.detail_label_caption_vars\|self\._current_detail_code\|self\._detail_payload_cache\|self\._schedule_show_stock_detail\|self\._cancel_scheduled_detail" src/gui/app.py
```

把所有 `self.show_stock_detail(...)` 改 `self.detail.show(...)`，`self.notebook.select(self.detail_tab_frame)` 改 `self.notebook.select(self.detail.frame)`，`self._current_detail_code` 改 `self.detail.current_code`，`self._schedule_show_stock_detail(...)` 改 `self.detail._schedule_show(...)`，`self._cancel_scheduled_detail()` 改 `self.detail._cancel_scheduled()`。

**特殊处理 `_current_detail_code`**：这个状态有时在 IntradayTab 也读（plan P2 提到 `self.app._current_detail_code`）。**改为 `self.app.detail.current_code`**，需要在 P3 完成时同步修改 IntradayTab 内的引用。

```powershell
grep -n "self\.app\._current_detail_code\|self\.app\.detail" src/gui/tabs/intraday.py
```

把 IntradayTab 内 `self.app._current_detail_code` 改为 `self.app.detail.current_code`。

- [ ] **Step 9: 残留验证**

```powershell
grep -rn "self\.detail_tab_frame\|self\.detail_labels\|self\.detail_label_caption_vars\|self\._current_detail_code\|self\._detail_payload_cache\|self\._detail_request_code\|self\._detail_loading_code\|self\._detail_after_id\|self\._detail_chart_dates\|self\._detail_chart_window_size\|self\._detail_chart_window_start\|self\._detail_chart_history\|self\._detail_chart_analysis\|self\._detail_chart_scroll_bound\|self\._detail_chart_slider_updating\|self\._detail_chart_dragging\|self\._detail_chart_drag_moved\|self\._detail_chart_drag_start_x\|self\._detail_chart_drag_start_window\|self\._detail_chart_click_target_date\|self\._detail_chart_loading_more\|self\._detail_chart_loaded_days\|self\._detail_summary_expanded\|self\._detail_chart_expanded\|self\._detail_flow_expanded\|self\._detail_last_revalidate_ts" src/gui/ stock_gui.py
```

预期：**0 命中**（除 DetailTab 内的 self.xxx 形式）。

---

## Task 4 — 验证 + commit

- [ ] **Step 1: import**

`.venv\Scripts\python -c "from stock_gui import StockMonitorApp; from src.gui.tabs.detail import DetailTab; print('OK')"`

- [ ] **Step 2: pytest 318 passed 不下降**

- [ ] **Step 3: GUI 启动 8s**

```powershell
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 8
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```

- [ ] **Step 4: 行数对比**

`(Get-Content D:\code\python\gupiao\src\gui\app.py | Measure-Object -Line).Lines` Expected ~5000（从 6127 减 ~1100）

`(Get-Content D:\code\python\gupiao\src\gui\tabs\detail.py | Measure-Object -Line).Lines` Expected ~1100

- [ ] **Step 5: Commit**

```powershell
git -C D:\code\python\gupiao add src/gui/app.py src/gui/tabs/detail.py src/gui/tabs/intraday.py
git -C D:\code\python\gupiao commit -m @'
重构（P3）：抽 DetailTab 类到 src/gui/tabs/detail.py

按 stock_gui.py 模块化拆分 spec 的 Phase 3，把股票详情 tab 相关的 ~35 个方法
+ ~15 widget 变量 + ~20 状态字段集体迁移到 DetailTab 类（持有 app 引用模板）。

主类 StockMonitorApp:
- __init__ 删除 _detail_* / _current_detail_code 状态字段
- setup_notebook 改为 self.detail = DetailTab(self, self.notebook)
- setup_detail_tab + 35 个 detail 业务方法整体删除
- tab 注册表 / 跨 tab show_stock_detail 调用 / _current_detail_code 引用全部改为 self.detail.xxx

IntradayTab 内的 self.app._current_detail_code 同步改为 self.app.detail.current_code。

行为零变化：股票详情 tab 视觉/交互/K 线点击跳分时全不变。app.py 削减 ~1100 行。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## 自检表

| 检查 | 状态 |
|---|---|
| 模板匹配 LogTab / IntradayTab | ✅ |
| `self.frame` 作 tab 容器统一命名 | ✅ |
| 35 个方法去 detail 前缀 | ✅ |
| 状态字段去 _detail_ 前缀 | ✅ |
| `_current_detail_code` 迁到 DetailTab.current_code，同步改 IntradayTab 引用 | ✅ |
| 跨 tab 调用全部改 `self.detail.xxx` / `self.app.detail.xxx` | ✅ |
| pytest 不下降 / GUI 启动正常 | ✅ |
