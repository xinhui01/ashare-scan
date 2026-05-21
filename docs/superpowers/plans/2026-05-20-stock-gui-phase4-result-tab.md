# Phase 4: 抽 ResultTab 到 src/gui/tabs/result.py 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** 把 `src/gui/app.py` 中扫描结果 tab 相关的 ~40 方法 + 状态 + widget 集体迁移到 `src/gui/tabs/result.py` 的 `ResultTab` 类。

**Architecture:** 1 commit，沿用 P2/P3 模板。

**Spec:** [`docs/superpowers/specs/2026-05-20-stock-gui-modularization-design.md`](../specs/2026-05-20-stock-gui-modularization-design.md)

**模板参考：**
- `src/gui/tabs/intraday.py`（P2）
- `src/gui/tabs/detail.py`（P3）

---

## Task 0 — 基线

- [ ] `git status` 干净
- [ ] pytest `318 passed`
- [ ] app.py `~5791 行`

---

## Task 1 — 摸清 result tab 依赖

```powershell
grep -n "def setup_result_tab\|def _visible_result_columns\|def _save_result_column_layout\|def _load_result_column_layout\|def reset_result_columns\|def apply_result_display_columns\|def _get_result_display_columns_and_headings\|def _format_result_row_values\|def _build_result_image_pages\|def _get_selected_result_identity\|def _lookup_result_by_code\|def _build_scan_only_actions_row\|def _build_scan_only_params_row\|def _build_scan_only_flow_row\|def _parse_optional_price_limit\|def _build_filter_settings\|def _build_scan_request\|def _filter_results_by_selected_boards\|def _filter_results_by_price_range\|def _apply_result_filters\|def _filter_results_by_quick_filters\|def on_quick_filter_apply\|def clear_all_result_filters\|def on_board_filter_changed\|def on_price_filter_changed\|def clear_price_filter\|def _save_last_results\|def _load_last_results\|def start_scan\|def stop_scan\|def scan_stocks\|def scan_finished\|def _sort_results\|def on_result_heading_click\|def update_result_table\|def on_stock_select\|def on_stock_double_click\|def _refresh_result_table_if_ready\|def export_results\|def export_results_image\|def copy_selected_stock_code_name\|def _schedule_quick_filter" src/gui/app.py
```

预期约 40 方法。

```powershell
grep -n "self\.result_\|self\.all_scan_results\|self\.filtered_stocks\|self\.scan_thread\|self\.scanning\|self\.default_result_display_columns\|self\._result_columns_map\|self\._after_quick_filter_id" src/gui/app.py
```

State/widget。

跨 tab：

```powershell
grep -n "self\.start_scan\|self\.stop_scan\|self\.scan_stocks\|self\._lookup_result_by_code\|self\._get_selected_result_identity\|self\.all_scan_results\|self\.filtered_stocks\|self\.result_tree\|self\.result_tab\|self\.update_result_table\|self\.export_results\|self\.on_stock_double_click\|self\.copy_selected" src/gui/app.py
```

---

## Task 2 — 创建 src/gui/tabs/result.py

**Files:**
- Create: `D:\code\python\gupiao\src\gui\tabs\result.py`

按 IntradayTab 模板写 `ResultTab` 类：

```python
"""扫描结果 Tab：股票扫描 + 结果表 + 过滤/排序/导出。

包含：
- ttk.Frame 容器（self.frame）
- 扫描专属面板（_build_scan_only_actions_row / _params_row / _flow_row）
- 结果表 Treeview（self.tree）
- 快速过滤栏（self._build_quick_filter_row → 在 _build 内调）
- 列管理（columns / headings / column_vars / column_order）

状态：
- self.all_scan_results / self.filtered_stocks（原 self.all_scan_results / filtered_stocks）
- self.scan_thread / self.scanning / self.scan_cancel_token（scanner 状态）

跨 tab 引用走 self.app.xxx：
- self.app.notebook
- self.app.status_var
- self.app.stock_filter
- self.app._ui / _post_to_ui / _log_async
- self.app.detail.show(...)（双击跳详情）
- self.app.min_price_var / max_price_var / search_var / min_score_var 等（全局快速过滤 var，留 App）
- self.app.board_filter_var / selected_boards 等（全局板块过滤）
"""
from __future__ import annotations
# imports ...

class ResultTab:
    def __init__(self, app, notebook):
        self.app = app
        self.all_scan_results: List[Dict[str, Any]] = []
        self.filtered_stocks: List[Dict[str, Any]] = []
        self.scan_thread = None
        self.scanning = False
        # ... 其余 state
        self._build(notebook)

    def _build(self, notebook):
        # 完整搬 setup_result_tab 内容
        pass
```

### 命名映射

| 旧 | 新 |
|---|---|
| `self.result_tab` | `self.frame` |
| `self.result_tree` | `self.tree` |
| `self.result_columns` | `self.columns` |
| `self.result_headings` | `self.headings` |
| `self.result_column_vars` | `self.column_vars` |
| `self.result_column_order` | `self.column_order` |
| `self._result_columns_map` | `self.columns_map` |
| `self.default_result_display_columns` | `self.default_display_columns` |
| `self.all_scan_results` | `self.all_results`（前缀去 scan，结果列表） |
| `self.filtered_stocks` | `self.filtered_stocks` |

### 方法映射

| 旧 | 新 |
|---|---|
| `setup_result_tab` | `_build`（内化） |
| `_visible_result_columns` | `_visible_columns` |
| `_save_result_column_layout` | `_save_column_layout` |
| `_load_result_column_layout` | `_load_column_layout` |
| `reset_result_columns` | `reset_columns` |
| `apply_result_display_columns` | `apply_display_columns` |
| `_get_result_display_columns_and_headings` | `_get_display_columns_and_headings` |
| `_format_result_row_values` | `_format_row_values` |
| `_build_result_image_pages` | `_build_image_pages` |
| `_get_selected_result_identity` | `_get_selected_identity` |
| `_lookup_result_by_code` | `lookup_by_code` **(改 public，被 detail tab 等可能引用)** |
| `_build_scan_only_actions_row` | `_build_scan_actions_row` |
| `_build_scan_only_params_row` | `_build_scan_params_row` |
| `_build_scan_only_flow_row` | `_build_scan_flow_row` |
| `_parse_optional_price_limit` | `_parse_optional_price_limit`（保留） |
| `_build_filter_settings` | `_build_filter_settings`（保留） |
| `_build_scan_request` | `_build_scan_request`（保留） |
| `_filter_results_by_selected_boards` | `_filter_by_selected_boards` |
| `_filter_results_by_price_range` | `_filter_by_price_range` |
| `_apply_result_filters` | `_apply_filters` |
| `_filter_results_by_quick_filters` | `_filter_by_quick_filters` |
| `on_quick_filter_apply` | `on_quick_filter_apply` |
| `clear_all_result_filters` | `clear_all_filters` |
| `on_board_filter_changed` | `on_board_filter_changed` |
| `on_price_filter_changed` | `on_price_filter_changed` |
| `clear_price_filter` | `clear_price_filter` |
| `_save_last_results` | `_save_last_results` |
| `_load_last_results` | `_load_last_results` |
| `start_scan` | `start_scan` |
| `stop_scan` | `stop_scan` |
| `scan_stocks` | `scan_stocks` |
| `scan_finished` | `scan_finished` |
| `_sort_results` | `_sort_results` |
| `on_result_heading_click` | `on_heading_click` |
| `update_result_table` | `update_table` |
| `on_stock_select` | `on_stock_select` |
| `on_stock_double_click` | `on_stock_double_click` |
| `_refresh_result_table_if_ready` | `_refresh_table_if_ready` |
| `export_results` | `export_results` |
| `export_results_image` | `export_results_image` |
| `copy_selected_stock_code_name` | `copy_selected_code_name` |
| `_schedule_quick_filter` | `_schedule_quick_filter` |

方法内：
- `self.result_xxx` → `self.xxx`
- `self.all_scan_results` → `self.all_results`
- `self.filtered_stocks` → `self.filtered_stocks`
- 跨 tab：
  - `self.notebook` → `self.app.notebook`
  - `self.status_var` → `self.app.status_var`
  - `self.stock_filter` → `self.app.stock_filter`
  - `self._ui` / `self._post_to_ui` → `self.app._ui` / `self.app._post_to_ui`
  - `self._log` / `self._log_async` → `self.app._log` / `self.app._log_async`
  - `self.show_stock_detail(...)` → `self.app.detail.show(...)`
  - `self.notebook.select(self.detail_tab_frame)` 这种已在 P3 改了
  - **价格/快速过滤 var** 留在 App：`self.min_price_var` → `self.app.min_price_var`，搜索/评分/5日/放量/连板 var 等同样
  - **板块过滤 var**：`self.selected_boards` 等 → `self.app.selected_boards`
  - 触发 predict tab 刷新：`self._refresh_predict_display_if_ready()` → `self.app._refresh_predict_display_if_ready()`（这个方法在主类，P5 才迁）

---

## Task 3 — 更新 src/gui/app.py

- [ ] import `from src.gui.tabs.result import ResultTab`
- [ ] 删 `__init__` 中所有 result/scan 相关字段
- [ ] setup_notebook 改 `self.result = ResultTab(self, self.notebook)`
- [ ] 删 setup_result_tab 整方法
- [ ] 删 40 个 result 业务方法
- [ ] tab 注册表 `self.result_tab` → `self.result.frame`
- [ ] _on_notebook_tab_changed 引用更新
- [ ] 文件菜单 "导出结果 CSV / 图片" command 更新：
  - `self.export_results` → `self.result.export_results`
  - `self.export_results_image` → `self.result.export_results_image`
  - `self.copy_selected_stock_code_name` → `self.result.copy_selected_code_name`
- [ ] 所有跨 tab 调用：
  - `self.all_scan_results` → `self.result.all_results`
  - `self.filtered_stocks` → `self.result.filtered_stocks`
  - `self.result_tree` → `self.result.tree`
  - `self._lookup_result_by_code(...)` → `self.result.lookup_by_code(...)`
  - 任何 `self.scan_xxx` / `self.start_scan()` / `self.stop_scan()` 改 `self.result.xxx`
- [ ] DetailTab 内如有 `self.app._lookup_result_by_code` → `self.app.result.lookup_by_code`（grep 验证）

### grep 残留验证

```powershell
grep -rn "self\.result_tab\|self\.result_tree\|self\.result_columns\|self\.result_headings\|self\.result_column_vars\|self\.result_column_order\|self\.all_scan_results\|self\.filtered_stocks\|self\._lookup_result_by_code\|self\._get_selected_result_identity\|self\.start_scan\|self\.stop_scan\|self\.scan_stocks\|self\.scan_thread\|self\.scanning\|self\.update_result_table\|self\.export_results" src/gui/ stock_gui.py
```

预期：**0 命中**（除 ResultTab 内 self.xxx）

---

## Task 4 — 验证 + commit

- [ ] import OK
- [ ] pytest 318 不下降
- [ ] GUI 启动 8s
- [ ] 行数：app.py ~5791 → ~4500，tabs/result.py ~1300
- [ ] Commit

```powershell
git -C D:\code\python\gupiao add src/gui/app.py src/gui/tabs/result.py src/gui/tabs/detail.py src/gui/tabs/intraday.py
git -C D:\code\python\gupiao commit -m @'
重构（P4）：抽 ResultTab 类到 src/gui/tabs/result.py

按 stock_gui.py 模块化拆分 spec 的 Phase 4，把扫描结果 tab 相关的 ~40 方法
+ ~10 widget + 扫描状态字段集体迁移到 ResultTab 类。

主类 StockMonitorApp:
- __init__ 删除 scan/result 相关字段（all_scan_results / filtered_stocks /
  scan_thread / result_tree / result_columns 等）
- setup_notebook 改为 self.result = ResultTab(self, self.notebook)
- setup_result_tab + 40 业务方法整体删除
- tab 注册表 / 文件菜单 command / 跨 tab 调用 全部改为 self.result.xxx

DetailTab 内 _lookup_result_by_code 引用同步改为 self.app.result.lookup_by_code。

留在 App 的全局 state：min_price_var / max_price_var / 快速过滤 var /
板块过滤 var 等（多 tab 共用偏好）。

行为零变化，pytest 318 全绿，app.py 削减 ~1300 行。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## 自检表

| 检查 | 状态 |
|---|---|
| 模板匹配 IntradayTab/DetailTab | ✅ |
| `self.frame` 作 tab 容器统一命名 | ✅ |
| 40 个方法去 result 前缀（不必要的） | ✅ |
| 全局 var（min_price_var/search_var 等）留 App | ✅ |
| 扫描状态（thread/scanning）迁到 ResultTab | ✅ |
| _lookup_result_by_code 改 public lookup_by_code（被跨 tab 用） | ✅ |
| DetailTab 内引用同步更新 | ✅ |
