# 删除 compare / watchlist Tab 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 从代码层彻底删除"涨停对比"（compare）与"自选池"（watchlist）两个默认隐藏 tab，及它们散布在 detail/result tab、菜单、过滤器、store、tests 中的连带代码；保留数据库表与共用底层模块。

**Architecture:** 分两个独立 phase，每个 phase 一个 commit。Phase 1 先删 watchlist（涉及跨 tab 入口多，先动），Phase 2 删 compare（独立性强）。删除策略：先删测试用例，再删生产代码，再 grep 验证无 orphan 引用，最后跑 `pytest -q` 与 GUI 冒烟。每个 phase 通过后再进入下一个。

**Tech Stack:** Python 3.12 + Tkinter（桌面 GUI）+ SQLite（本地存储）+ pytest。

**Spec:** [`docs/superpowers/specs/2026-05-20-delete-compare-watchlist-tabs-design.md`](../specs/2026-05-20-delete-compare-watchlist-tabs-design.md)

---

## Phase 0 — 起跑准备

### Task 0: 验证起跑状态干净

**Files:** 无修改，只做检查

- [ ] **Step 1: 确认工作区干净**

Run: `git -C D:\code\python\gupiao status`
Expected: `nothing to commit, working tree clean`（或仅有 .gitignore 已提交后的状态）

- [ ] **Step 2: 跑一次完整测试，记录 baseline 通过数**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全部通过，记录类似 "N passed in X.XXs"（后续每个 phase 删完代码后，剩余通过数应该减去本 phase 删除的测试 case 数）

- [ ] **Step 3: 启动 GUI 做基线冒烟**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python main.py`
Expected: GUI 正常启动；视图菜单里能看到"扫描结果"/"涨停对比"/"自选池"三项；切到涨停对比 / 自选池 tab 能渲染。手动关闭程序。

---

## Phase 1 — 删除 watchlist 功能

### Task 1.1: 删除 watchlist 相关测试用例

**Files:**
- Modify: `tests/test_stock_store.py`
- Modify: `tests/test_db_admin_service.py`
- Modify: `tests/test_result_columns.py`
- Modify: `tests/test_result_filters.py`

> 先删测试，是为了避免删完生产代码后跑 pytest 一片红。删完测试 → 跑 pytest 全绿 → 再删生产代码。

- [ ] **Step 1: 定位 test_stock_store.py 中的 watchlist case**

Run: `grep -n "watchlist\|watch_" tests/test_stock_store.py`

预期看到 2 个测试函数 `test_save_load_delete_watchlist`、`test_watchlist_upsert`，以及它们使用的 helper（如有）。

- [ ] **Step 2: 删除上面 2 个测试函数**

打开 `tests/test_stock_store.py`，删掉这 2 个 `def test_save_load_delete_watchlist(...)` 和 `def test_watchlist_upsert(...)` 函数（包括 docstring、装饰器、辅助调用）。如有专属 helper（仅这两个 test 用）一并删。其他用例（universe / history / scan_snapshots / fund_flow / config 等）一律保留。

- [ ] **Step 3: 验证文件能 import**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -c "import tests.test_stock_store"`
Expected: 无报错。

- [ ] **Step 4: 删除 test_db_admin_service.py 中的 watchlist CSV case**

Run: `grep -n "watchlist\|csv" tests/test_db_admin_service.py`

找到对 `export_watchlist_csv` / `import_watchlist_csv` 的测试函数，删除整个函数体。其他（备份/恢复/cleanup）保留。

- [ ] **Step 5: 删除 test_result_columns.py 中的 watch 列断言**

Run: `grep -n "watch\|self_select" tests/test_result_columns.py`

找到测试 watch 列存在性、`_extract_watch_flag`、或 default visible 列含"自选"等断言。如果这些断言混在某个大测试里，把它们逐行剥离；如果是独立测试函数，整体删除。

- [ ] **Step 6: 删除 test_result_filters.py 中的 only_in_watchlist case**

Run: `grep -n "watchlist\|only_in" tests/test_result_filters.py`

找到对 `only_in_watchlist` 的测试函数，整体删除。

- [ ] **Step 7: 跑 pytest 确认仍全绿**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全部通过。通过数应该比 baseline 少（少掉的就是删的 watchlist case 数）。

> ⚠️ 如果有任何 FAIL：可能是删测试时不小心带走了别的 case，或者删得不完整有 NameError。**不要继续往下做**，回头修。

### Task 1.2: 删除 watchlist 跨 tab 入口（菜单 + 按钮 + 过滤）

**Files:**
- Modify: `stock_gui.py`

> 顺序：从外到内。先把外部的 UI 入口（菜单项、按钮、过滤复选框）删掉，确保后面删 `add_selected_result_to_watchlist` 等方法时没有遗留调用方。

- [ ] **Step 1: 删除文件菜单的"导出/导入自选股 CSV"项**

Run: `grep -n "_export_watchlist_csv\|_import_watchlist_csv\|导出自选股\|导入自选股" stock_gui.py`

定位 `setup_menu` 中的 2 行：
```python
file_menu.add_command(label="导出自选股 CSV", command=self._export_watchlist_csv)
file_menu.add_command(label="导入自选股 CSV", command=self._import_watchlist_csv)
```
连同上一行（如果是 `file_menu.add_separator()` 仅为这两项服务）一并删除。

- [ ] **Step 2: 删除 result tab 上的"加入自选/移除自选"按钮**

Run: `grep -n "加入自选\|移除自选\|add_selected_result_to_watchlist\|remove_selected_result_from_watchlist" stock_gui.py`

定位 `setup_result_tab` 中 `action_frame` 下的 2 行：
```python
ttk.Button(action_frame, text="加入自选", command=self.add_selected_result_to_watchlist).pack(side=tk.LEFT, padx=8)
ttk.Button(action_frame, text="移除自选", command=self.remove_selected_result_from_watchlist).pack(side=tk.LEFT)
```
删除这 2 行。

- [ ] **Step 3: 删除 detail tab 上的"加入自选"和"编辑备注"按钮**

Run: `grep -n "detail_watch_btn\|detail_watch_note_btn\|toggle_current_detail_watchlist\|edit_current_detail_watch_note" stock_gui.py`

定位 `setup_detail_tab` 中的代码块：
```python
self.detail_watch_btn = ttk.Button(
    info_header, text="加入自选",
    command=self.toggle_current_detail_watchlist,
)
self.detail_watch_btn.pack(side=tk.LEFT, padx=(8, 0))
self.detail_watch_note_btn = ttk.Button(
    info_header, text="编辑备注",
    command=self.edit_current_detail_watch_note,
)
self.detail_watch_note_btn.pack(side=tk.LEFT, padx=(8, 0))
```
删除整段（约 12 行）。

- [ ] **Step 4: 删除 detail tab 的 "watch_status" 详情字段**

Run: `grep -n "watch_status\|自选状态" stock_gui.py`

定位 `items = [...]` 列表中的 `("watch_status", "自选状态"),` 这一行，删除它。

- [ ] **Step 5: 删除 result tab 的"只显示自选"快速过滤复选框**

Run: `grep -n "only_watchlist_var\|只显示" stock_gui.py`

定位（约 488-496）：
```python
self.only_watchlist_var = tk.BooleanVar(value=False)
ttk.Checkbutton(
    row6, text="自选", variable=self.only_watchlist_var,
    command=self.on_quick_filter_apply,
).pack(side=tk.LEFT, padx=4)
```
删除这 5-6 行。

> 注意上一行 `ttk.Label(row6, text="只显示:").pack(...)` 是否还有别的复选框跟随。如果"只显示"这一行只剩 Label，把 Label 一起删；如果还有"涨停"等其它复选框跟随，保留 Label。

- [ ] **Step 6: 删除 update_result_table / 快速过滤逻辑中对 only_watchlist_var 的引用**

Run: `grep -n "only_watchlist_var" stock_gui.py`

应该还能找到 2-3 处引用（apply 过滤、保存配置等）。逐处删除对应逻辑分支。

- [ ] **Step 7: 删除快速过滤里调用 `only_in_watchlist` 的逻辑**

Run: `grep -n "only_in_watchlist" stock_gui.py`

找到调用点（约 5658-5678）并删除相应 if 分支。

- [ ] **Step 8: 跑 pytest 确认仍全绿（应该全绿，但可能有 NameError 等问题）**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全部通过。若有 NameError，说明上面 7 步还有漏删的引用。

### Task 1.3: 删除 watchlist tab 和 18 个 watchlist 方法

**Files:**
- Modify: `stock_gui.py`

- [ ] **Step 1: 删除 watchlist tab 的注册条目和启动钩子**

Run: `grep -n "setup_watchlist_tab\|self\.watchlist_tab\|self\._watch_tree\|self\.watchlist_summary_var\|_load_watchlist_items" stock_gui.py`

定位 3 处：
1. 约 542 行：`self.setup_watchlist_tab()`
2. 约 199 行：`self._load_watchlist_items()`（在 `__init__` 末尾）
3. 约 590 行：tab 注册表 `("watchlist", self.watchlist_tab, "自选池", True),`

删除这 3 处。

- [ ] **Step 2: 删除 `setup_watchlist_tab` 整个方法**

Run: `grep -n "def setup_watchlist_tab" stock_gui.py`

定位（约 5147 行），删除整个方法体（直到下一个 `def ...:` 之前）。范围约 5147-5183。

- [ ] **Step 3: 逐个删除 18 个 watchlist 方法**

对下列每一个，先 grep 定位再删除整个 `def ...` 方法体：

| 方法 | grep 命令 |
|---|---|
| `_load_watchlist_items` | `grep -n "def _load_watchlist_items" stock_gui.py` |
| `_build_watchlist_item_payload` | `grep -n "def _build_watchlist_item_payload" stock_gui.py` |
| `refresh_watchlist_view` | `grep -n "def refresh_watchlist_view" stock_gui.py` |
| `on_watchlist_select` | `grep -n "def on_watchlist_select" stock_gui.py` |
| `on_watchlist_double_click` | `grep -n "def on_watchlist_double_click" stock_gui.py` |
| `add_selected_result_to_watchlist` | `grep -n "def add_selected_result_to_watchlist" stock_gui.py` |
| `remove_selected_result_from_watchlist` | `grep -n "def remove_selected_result_from_watchlist" stock_gui.py` |
| `add_current_detail_to_watchlist` | `grep -n "def add_current_detail_to_watchlist" stock_gui.py` |
| `toggle_current_detail_watchlist` | `grep -n "def toggle_current_detail_watchlist" stock_gui.py` |
| `edit_current_detail_watch_note` | `grep -n "def edit_current_detail_watch_note" stock_gui.py` |
| `_add_code_to_watchlist` | `grep -n "def _add_code_to_watchlist" stock_gui.py` |
| `_remove_code_from_watchlist` | `grep -n "def _remove_code_from_watchlist" stock_gui.py` |
| `_edit_watchlist_item` | `grep -n "def _edit_watchlist_item" stock_gui.py` |
| `edit_selected_watchlist_item` | `grep -n "def edit_selected_watchlist_item" stock_gui.py` |
| `remove_selected_watchlist_item` | `grep -n "def remove_selected_watchlist_item" stock_gui.py` |
| `_update_detail_watch_state` | `grep -n "def _update_detail_watch_state" stock_gui.py` |
| `_sync_watchlist_with_scan_results` | `grep -n "def _sync_watchlist_with_scan_results" stock_gui.py` |
| `_export_watchlist_csv` | `grep -n "def _export_watchlist_csv" stock_gui.py` |
| `_import_watchlist_csv` | `grep -n "def _import_watchlist_csv" stock_gui.py` |

每个方法删完后 Read 一次该位置确认没有遗留 docstring 或孤儿 helper。

- [ ] **Step 4: 删除其他地方对这些方法和变量的调用**

Run: `grep -n "_update_detail_watch_state\|_sync_watchlist_with_scan_results\|refresh_watchlist_view\|watchlist_summary_var\|self\.watchlist_tab\|self\._watch_tree" stock_gui.py`

每一个仍存在的引用位置（除了刚才已经删掉的 def 之外），逐个看上下文：
- 如果是函数调用，删除该调用行
- 如果是属性赋值（如 `self._watch_tree = ...`），删除整段
- 如果是某个保留方法里出现的引用（比如某个 dispatch 表），删除对应条目

- [ ] **Step 5: 跑 pytest 确认仍全绿**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全部通过。

### Task 1.4: 删除 result_columns / result_filters 的 watchlist 部分

**Files:**
- Modify: `src/gui/result_columns.py`
- Modify: `src/gui/result_filters.py`

- [ ] **Step 1: 删除 result_columns.py 的 watch 列定义和提取函数**

Run: `grep -n "_extract_watch_flag\|watchlist\|extract=_extract_watch_flag" src/gui/result_columns.py`

定位：
1. 约 123 行 `def _extract_watch_flag(...)` 函数（约 5 行）
2. 约 238-240 行的 watch 列条目（`ColumnDef(...)` 或类似结构）

把这两块一并删除。还要看顶部是否有 `watch` 相关的 import 或常量（如 `WATCH_COL_ID = "..."`），有就一并删。

- [ ] **Step 2: 修改 docstring 中"watchlist_items"的注释（如有）**

Run: `grep -n "watchlist_items" src/gui/result_columns.py`

如果仅是 docstring 里的示例文字（如行 19），可保留或顺手把示例换掉。

- [ ] **Step 3: 删除 result_filters.py 的 only_in_watchlist 函数**

Run: `grep -n "def only_in_watchlist\|watchlist_codes" src/gui/result_filters.py`

定位（约 85 行），删除整个 `def only_in_watchlist(...)` 函数（包括 docstring，约 10 行）。

- [ ] **Step 4: 删除 stock_gui.py 中对 only_in_watchlist 的 import**

Run: `grep -n "only_in_watchlist" stock_gui.py`

若顶部 `from src.gui.result_filters import (...)` 列表里有 `only_in_watchlist`，删掉该符号。

- [ ] **Step 5: 跑 pytest 确认全绿**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全部通过。

### Task 1.5: 删除 store / db_admin_service 中的 watchlist 函数

**Files:**
- Modify: `stock_store.py`
- Modify: `src/services/db_admin_service.py`

- [ ] **Step 1: 删除 `stock_store.py` 中 4 个 watchlist 读写函数**

Run: `grep -n "def save_watchlist_item\|def load_watchlist\|def load_watchlist_item\|def delete_watchlist_item" stock_store.py`

定位约 369 / 414 / 447 / 481 行。逐个删除整个 `def ...` 函数体。

- [ ] **Step 2: 验证保留物**

Run: `grep -n "CREATE TABLE IF NOT EXISTS watchlist\|'watchlist'" stock_store.py`

必须仍能找到：
1. `CREATE TABLE IF NOT EXISTS watchlist (...)`（DDL，约 234-245 行）—— **保留**
2. 备份表清单中的 `"watchlist"` 字符串条目 —— **保留**

如果误删了上述任一，恢复。

- [ ] **Step 3: 删除 `src/services/db_admin_service.py` 的 2 个 CSV 函数**

Run: `grep -n "def export_watchlist_csv\|def import_watchlist_csv" src/services/db_admin_service.py`

定位约 153 / 214 行。删除整个 `def ...` 函数体。如果文件顶部有专门为这两函数的 import（如 `csv` 已被其他函数用就保留），按需精修。

- [ ] **Step 4: 删除 `db_admin_service.py` 顶部对 `save_watchlist_item` / `load_watchlist` 的导入**

Run: `grep -n "save_watchlist_item\|load_watchlist" src/services/db_admin_service.py`

把这两个符号从 `from stock_store import (...)` 列表里删除（保留其它符号）。

- [ ] **Step 5: 跑 pytest 确认全绿**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全部通过。

### Task 1.6: 终极 grep 验证 + GUI 冒烟 + 提交 Phase 1

**Files:** 无修改，只做验证

- [ ] **Step 1: grep 验证 stock_gui.py / src/ 内无 watchlist 业务代码残留**

Run: `grep -rn "watchlist\|_watch_tree\|only_in_watchlist\|_add_code_to_watchlist\|only_watchlist_var" stock_gui.py src/`

Expected: 残留仅限：
- `stock_store.py` 中 `CREATE TABLE IF NOT EXISTS watchlist (...)` DDL
- `stock_store.py` 中备份表清单的 `"watchlist"` 字符串
- 注释/docstring 中无意义的历史描述（如果有）

任何 `def`、变量赋值、`self.xxx.watchlist` 之类都不应残留。

- [ ] **Step 2: 静态语法/import 检查**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -c "import stock_gui; import stock_store; import src.services.db_admin_service; import src.gui.result_columns; import src.gui.result_filters; print('OK')"`

Expected: 输出 `OK`，无 ImportError / NameError。

- [ ] **Step 3: 跑完整测试**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全绿。

- [ ] **Step 4: GUI 冒烟测试**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python main.py`

手动验证：
- [ ] GUI 启动 5 秒内无 traceback
- [ ] 文件菜单：无"导出/导入自选股 CSV"
- [ ] 视图菜单：无"自选池"项
- [ ] 扫描结果 tab：无"加入自选/移除自选"按钮，无"只显示自选"复选框
- [ ] detail tab：无"加入自选"/"编辑备注"按钮，详情字段无"自选状态"行
- [ ] 涨停预测、详情、分时、扫描结果、涨停对比 5 个 tab 都能切换且无报错（compare tab 此时还未删，应正常）
- [ ] 关闭程序

- [ ] **Step 5: 提交 Phase 1**

```powershell
git -C D:\code\python\gupiao add -u
git -C D:\code\python\gupiao status
```

确认 staged 文件清单合理（应包含 stock_gui.py、stock_store.py、src/services/db_admin_service.py、src/gui/result_columns.py、src/gui/result_filters.py、tests/test_*.py 共 7-8 个文件，且无意外文件）。

然后：
```powershell
git -C D:\code\python\gupiao commit -m @'
清理：删除自选池功能（保留 DB 表）

按 docs/superpowers/specs/2026-05-20-delete-compare-watchlist-tabs-design.md
完成 Phase 1：移除 watchlist tab、18 个 watchlist 方法、跨 tab 入口
（detail "加入自选" 按钮、result "加入自选/移除自选" 按钮、菜单 CSV 项、
"只显示自选" 过滤复选框）、store 读写函数、CSV 导入导出服务、
result_columns 的 watch 列、result_filters.only_in_watchlist 谓词、
相关测试。

保留：watchlist 表 DDL 与备份清单条目（保护历史数据）；
predict tab 等其余功能不受影响。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

Expected: 提交成功，`git log --oneline -1` 显示新 commit。

---

## Phase 2 — 删除 compare 功能

### Task 2.1: 删除 compare tab 注册和启动钩子

**Files:**
- Modify: `stock_gui.py`

- [ ] **Step 1: 删除启动钩子和恢复调用**

Run: `grep -n "setup_limit_up_compare_tab\|_load_last_limit_up_compare\|self\.compare_tab" stock_gui.py`

定位 2 处：
1. 约 197 行：`self._load_last_limit_up_compare()`
2. 约 541 行：`self.setup_limit_up_compare_tab()`

删除这 2 行。

- [ ] **Step 2: 删除 tab 注册表条目**

Run: `grep -n "\"compare\", self.compare_tab\|涨停对比" stock_gui.py`

定位 `_tab_registry` 列表中的 `("compare", self.compare_tab, "涨停对比", True),` 行，删除。

- [ ] **Step 3: 跑 pytest 确认仍能绿（理论上还会绿，因为 def 还在）**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全部通过。

### Task 2.2: 删除 setup_limit_up_compare_tab 及其内部所有 `_zt_*` widget 引用

**Files:**
- Modify: `stock_gui.py`

- [ ] **Step 1: 删除 setup_limit_up_compare_tab 整个方法**

Run: `grep -n "def setup_limit_up_compare_tab" stock_gui.py`

定位（约 1544 行），删除整个方法体（含 docstring，直到下一个 `def`）。范围约 1544-1698。

- [ ] **Step 2: 删除 14 个 compare 业务方法**

对下列每个方法，先 grep 定位再删除整个 `def ...` 方法体：

| 方法 | grep 命令 |
|---|---|
| `_start_limit_up_compare` | `grep -n "def _start_limit_up_compare" stock_gui.py` |
| `_load_limit_up_compare` | `grep -n "def _load_limit_up_compare" stock_gui.py` |
| `_zt_fill_today_and_prev` | `grep -n "def _zt_fill_today_and_prev" stock_gui.py` |
| `_estimate_yesterday` | `grep -n "def _estimate_yesterday" stock_gui.py` |
| `_zt_show_error` | `grep -n "def _zt_show_error" stock_gui.py` |
| `_zt_filter_records` | `grep -n "def _zt_filter_records" stock_gui.py` |
| `_refresh_zt_compare_display` | `grep -n "def _refresh_zt_compare_display" stock_gui.py` |
| `_apply_limit_up_compare` | `grep -n "def _apply_limit_up_compare" stock_gui.py` |
| `_refresh_compare_history_dates` | `grep -n "def _refresh_compare_history_dates" stock_gui.py` |
| `_on_compare_history_selected` | `grep -n "def _on_compare_history_selected" stock_gui.py` |
| `_refresh_selected_compare_date` | `grep -n "def _refresh_selected_compare_date" stock_gui.py` |
| `_save_limit_up_compare_snapshot` | `grep -n "def _save_limit_up_compare_snapshot" stock_gui.py` |
| `_load_last_limit_up_compare` | `grep -n "def _load_last_limit_up_compare" stock_gui.py` |
| `_infer_board_from_code` | `grep -n "def _infer_board_from_code" stock_gui.py` |

- [ ] **Step 3: 删除 zt tree 选择回调**

Run: `grep -n "def on_zt_stock_select\|def on_zt_stock_double_click" stock_gui.py`

定位约 6270 / 6280 行，删除这 2 个方法。

- [ ] **Step 4: 跑 pytest 确认全绿**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全部通过。

### Task 2.3: 删除 compare tab 专属 AI 题材聚类（保留底层 LLM 模块）

**Files:**
- Modify: `stock_gui.py`

- [ ] **Step 1: 删除 3 个 compare 专属的聚类入口方法**

Run: `grep -n "def _start_ai_theme_clustering\|def _load_ai_theme_clustering\|def _apply_ai_theme_clustering" stock_gui.py`

定位约 922-1040 行的 3 个方法，逐个删除整个 `def` 方法体。

- [ ] **Step 2: 验证保留物**

Run: `grep -n "def _open_nim_key_dialog\|NIM Key" stock_gui.py`

应仍存在：
- `def _open_nim_key_dialog` 方法 —— **保留**
- predict tab action_bar 上的 `text="NIM Key"` 按钮（约 2102）—— **保留**

如不存在，恢复。

- [ ] **Step 3: 检查 `from llm_theme_clustering import ...` 是否还需要**

Run: `grep -n "llm_theme_clustering\|cluster_themes\|llm_load_cached_themes" stock_gui.py`

如果除了顶部 import 外没有其它引用，把 `from llm_theme_clustering import (...)` 整段删掉。  
如果还有引用（例如某处仍调用 `llm_load_cached_themes`），保留对应符号、删除其余符号。

- [ ] **Step 4: 检查 `from llm_client import ...` 中各符号是否还需要**

Run: `grep -n "llm_has_api_key\|llm_save_api_key\|_resolve_api_key\|LlmConfigError\|LlmRequestError" stock_gui.py`

- `llm_has_api_key` / `llm_save_api_key`：被 `_open_nim_key_dialog` 用，**保留**
- `LlmConfigError` / `LlmRequestError`：被 daily brief 错误处理用（约 3474 行），**保留**
- `_resolve_api_key`（约 899 行）：检查它是否还在用，如果仅服务于已删除的 `_start_ai_theme_clustering`，删掉对应 import 和调用点

按实际 grep 结果增删 import。

- [ ] **Step 5: 跑 pytest 确认全绿**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全部通过。

### Task 2.4: 删除 stock_store.py 中 3 个 compare 函数

**Files:**
- Modify: `stock_store.py`

- [ ] **Step 1: 删除 3 个 limit_up_compare 读写函数**

Run: `grep -n "def save_limit_up_compare_record\|def load_limit_up_compare_by_date\|def list_limit_up_compare_dates" stock_store.py`

定位约 1245 / 1292 / 1319 行。逐个删除整个 `def ...` 函数体。

- [ ] **Step 2: 验证保留物**

Run: `grep -n "CREATE TABLE IF NOT EXISTS limit_up_compares\|'limit_up_compares'" stock_store.py`

必须仍能找到：
- `CREATE TABLE IF NOT EXISTS limit_up_compares (...)` DDL（约 258 行）—— **保留**
- 备份表清单中的 `"limit_up_compares"` 字符串 —— **保留**

如果误删，恢复。

- [ ] **Step 3: 跑 pytest 确认全绿**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全部通过。

### Task 2.5: 终极 grep 验证 + GUI 冒烟 + 提交 Phase 2

**Files:** 无修改

- [ ] **Step 1: grep 验证无 compare 业务代码残留**

Run: `grep -rn "compare_tab\|_zt_\|_start_ai_theme_clustering\|setup_limit_up_compare_tab" stock_gui.py src/`

Expected: 无残留。

Run: `grep -rn "limit_up_compare" stock_gui.py stock_store.py src/`

Expected: 残留仅限：
- `stock_store.py` 的 `CREATE TABLE IF NOT EXISTS limit_up_compares` DDL
- `stock_store.py` 备份表清单中 `"limit_up_compares"` 字符串
- `stock_filter.py` 中 `compare_limit_up_pools` / `_build_compare_market_context` / `_score_continuation_by_compare` —— **predict 共用，保留**

- [ ] **Step 2: 静态 import 检查**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -c "import stock_gui; import stock_store; import stock_filter; import llm_client; import llm_theme_clustering; print('OK')"`

Expected: 输出 `OK`。

- [ ] **Step 3: 跑完整测试**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全绿。

- [ ] **Step 4: GUI 冒烟**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python main.py`

手动验证：
- [ ] GUI 启动 5 秒内无 traceback
- [ ] 视图菜单：无"涨停对比"，无"自选池"，仅剩"扫描结果"可切换
- [ ] 涨停预测 tab：默认显示，做一次预测（点"预测"或"基准日期 今天"按钮）
- [ ] 涨停预测 tab：action bar 上的 "NIM Key"、"AI 博弈短报"、"批量回测" 按钮都在；点 "AI 博弈短报" 弹出窗口（不一定调用 NIM，能弹出即可）
- [ ] 双击涨停预测候选 → 跳详情 tab 正常
- [ ] 切到分时 tab 正常
- [ ] 切到扫描结果 tab（视图菜单打开）正常
- [ ] 关闭程序

- [ ] **Step 5: 提交 Phase 2**

```powershell
git -C D:\code\python\gupiao add -u
git -C D:\code\python\gupiao status
```

确认 staged 文件清单合理（stock_gui.py + stock_store.py，可能加 import 调整）。

```powershell
git -C D:\code\python\gupiao commit -m @'
清理：删除涨停对比 tab（保留 DB 表）

按 docs/superpowers/specs/2026-05-20-delete-compare-watchlist-tabs-design.md
完成 Phase 2：移除 compare tab、14 个 compare 方法、AI 题材聚类入口
（_start_ai_theme_clustering 等 3 个 compare 专属方法）、compare action bar
按钮、_zt_* 回调、store 读写函数。

保留：limit_up_compares 表 DDL 与备份清单条目；
_open_nim_key_dialog 方法与 NIM Key 按钮（predict tab 仍用）；
llm_client.py / llm_theme_clustering.py（daily_brief / concept_hype 共用）；
stock_filter.py 中 predict 共用的 limit-up 工具函数。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

Expected: 提交成功。

---

## 完工后检查

### Task 3: 整体收尾

- [ ] **Step 1: 看最近 2 个 commit**

Run: `git -C D:\code\python\gupiao log --oneline -3`

Expected: 看到 Phase 2 commit、Phase 1 commit、之前的 spec commit。

- [ ] **Step 2: 终极全面 grep**

Run: `grep -rn "watchlist\|limit_up_compare\|_zt_\|_watch_tree\|only_in_watchlist\|setup_watchlist_tab\|setup_limit_up_compare_tab" stock_gui.py stock_store.py stock_filter.py src/ tests/`

Expected: 仅剩两张表的 DDL 字符串 / 备份表名常量 / `stock_filter.py` 中 predict 共用的 `compare_limit_up_pools` 等工具。其他任何位置都不应有引用。

- [ ] **Step 3: 最后一次完整 pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q`
Expected: 全绿。

- [ ] **Step 4: 告诉用户：完工**

向用户报告：两个 phase 都已提交，pytest 全绿，GUI 冒烟通过。提示后续可以继续 brainstorm "拆 stock_gui.py" 等下一个子工程。

---

## 自检表（在你点"开始执行"之前过一遍）

| 检查 | 状态 |
|---|---|
| spec 覆盖：watchlist 删除清单 7 类（tab、18 方法、跨 tab 入口、过滤、columns/filters、store/services、tests）→ Phase 1 任务 1.1-1.6 覆盖 | ✅ |
| spec 覆盖：compare 删除清单 6 类（tab、14 方法、AI 聚类入口、_zt_ 回调、store、imports）→ Phase 2 任务 2.1-2.5 覆盖 | ✅ |
| spec 覆盖：保留物清单 8 项 → 在 1.5/2.3/2.4 步骤中显式 grep 验证 | ✅ |
| 无占位符 / TBD / "类似 Task N" | ✅ |
| 每个 grep 都给了完整命令 | ✅ |
| 每个删除步骤后都跑 pytest | ✅ |
| 每个 phase 结尾都有 grep 验证 + GUI 冒烟 + 提交 | ✅ |
| commit message 中文 + 与项目历史 commit 风格一致 | ✅ |
