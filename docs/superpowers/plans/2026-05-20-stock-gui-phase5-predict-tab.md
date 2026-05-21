# Phase 5: 抽 PredictTab 到 src/gui/tabs/predict.py 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** 把 `src/gui/app.py` 中涨停预测 tab 相关的 ~35 方法 + ~50 widget 变量 + ~25 state 字段全部迁移到 `src/gui/tabs/predict.py` 的 `PredictTab` 类。这是 5-phase 拆分中最大的一个 tab（占 app.py 约 50%）。

**Architecture:** 1 commit。所有 predict 相关代码搬到单文件 `tabs/predict.py`（估算 ~2500 行）。如果后续发现文件太大难维护，再单独拆 `predict_compare_window.py` / `predict_strategy_window.py` / `predict_backtest_dialog.py` 子模块，不在本 phase 范围。

**Spec:** [`docs/superpowers/specs/2026-05-20-stock-gui-modularization-design.md`](../specs/2026-05-20-stock-gui-modularization-design.md)

**模板参考：** P2 IntradayTab / P3 DetailTab / P4 ResultTab

---

## Task 0 — 基线

- [ ] `git status` 干净
- [ ] pytest `318 passed`
- [ ] app.py 当前行数（grep `(Get-Content D:\code\python\gupiao\src\gui\app.py | Measure-Object -Line).Lines`）

---

## Task 1 — 摸清依赖

```powershell
# 所有 predict 相关方法
grep -n "def.*predict\|def setup_predict_tab\|def _matches_predict\|def _filter_predict\|def _render_predict\|def _sort_predict\|def _start_predict\|def _load_predict\|def _apply_predict\|def _refresh_predict\|def _reset_predict\|def _open_predict\|def _build_predict\|def _on_predict\|def _get_predict\|def _run_predict\|def _find_best_bucket_for_category\|def _refresh_data_source_label\|def _predict_show_error\|def _predict_row_tag\|def _predict_sort_value\|def _predict_bucket_priority_for\|def _load_last_limit_up_prediction\|def _refresh_selected_predict_date" src/gui/app.py
```

预期约 35 方法（含 backtest dialog / compare window / strategy window 等弹窗 helper）。

```powershell
# State / widget 字段
grep -n "self\.predict_\|self\._predict_\|self\.predict_tab\|self\._sentiment_\|self\._daily_brief" src/gui/app.py
```

注意：sentiment（_sentiment_）和 daily_brief 是 predict tab 的子组件，也要一并迁移（在 sent_bar 区域）。

```powershell
# 跨 tab 调用
grep -n "self\._refresh_predict_display_if_ready\|self\._refresh_predict_accuracy_async\|self\.notebook\.select(self\.predict_tab" src/gui/app.py
```

---

## Task 2 — 创建 src/gui/tabs/predict.py

**Files:**
- Create: `D:\code\python\gupiao\src\gui\tabs\predict.py`

按 ResultTab 模板写 `PredictTab` 类。结构：

```python
"""涨停预测 Tab：5 sub-tab 候选 + 预测/对比/策略/回测/AI 短报。

这是项目最复杂的 tab，包含：
- 涨停预测 5 sub-tab notebook（保留涨停/二波接力/首板涨停/反包承接/趋势涨停）
- 顶部 action_bar（开始预测/历史日期/命中对比/策略分析/批量回测/AI 博弈短报/NIM Key）
- 市场情绪条（sent_bar：评分/建议/详情/刷新）
- 数据源指示标签
- 筛选栏（filter_bar）
- 多个子窗口（命中对比/策略分析/批量回测/AI 短报）

跨 tab 引用走 self.app.xxx：
- self.app.notebook
- self.app.status_var / top_header_var
- self.app.stock_filter
- self.app._ui / _post_to_ui / _log_async
- self.app.detail.show(...)（双击跳详情）
- self.app.min_price_var / max_price_var / selected_boards（全局过滤 var）
- self.app.history_source_var（数据源偏好）
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from datetime import datetime
from tkinter import scrolledtext, simpledialog, messagebox
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk

import pandas as pd

from src.utils.cancel_token import CancelToken
from src.services import prediction_accuracy_service

if TYPE_CHECKING:
    from src.gui.app import StockMonitorApp


class PredictTab:
    def __init__(self, app: "StockMonitorApp", notebook: ttk.Notebook) -> None:
        self.app = app
        # state 字段全部前缀去 predict
        self.lists: Dict[str, List[Dict[str, Any]]] = {}
        self.results_map: Dict = {}
        self.bucket_rates_cache: Dict = {}
        self.best_buckets: Dict[str, Optional[Tuple[int, int]]] = {}
        # ... 完整列表见 plan Task 1 grep 结果
        self.cont_sort_column: str = ""
        self.cont_sort_reverse: bool = False
        # ... 其他
        self._build(notebook)

    def _build(self, notebook):
        # 整体搬 setup_predict_tab
        pass
```

### 命名映射

| 旧 | 新 |
|---|---|
| `self.predict_tab` | `self.frame` |
| `self._predict_table_nb` | `self.table_nb` |
| `self._predict_cont_tree` | `self.cont_tree` |
| `self._predict_first_tree` | `self.first_tree` |
| `self._predict_fresh_tree` | `self.fresh_tree` |
| `self._predict_wrap_tree` | `self.wrap_tree` |
| `self._predict_trend_tree` | `self.trend_tree` |
| `self._predict_status_label` | `self.status_label` |
| `self._predict_date_var` | `self.date_var` |
| `self._predict_lookback_var` | `self.lookback_var` |
| `self._predict_history_var` | `self.history_var` |
| `self._predict_history_combo` | `self.history_combo` |
| `self._predict_summary_text` | `self.summary_text` |
| `self._predict_stat_labels` | `self.stat_labels` |
| `self._predict_best_bucket_labels` | `self.best_bucket_labels` |
| `self._predict_subcategory_best_labels` | `self.subcategory_best_labels` |
| `self._predict_subcategory_stat_labels` | `self.subcategory_stat_labels` |
| `self._predict_filter_min_score` | `self.filter_min_score` |
| `self._predict_filter_keyword` | `self.filter_keyword` |
| `self._predict_filter_industry` | `self.filter_industry` |
| `self._predict_filter_lhb_only` | `self.filter_lhb_only` |
| `self._predict_filter_northbound_only` | `self.filter_northbound_only` |
| `self._predict_filter_theme_only` | `self.filter_theme_only` |
| `self._predict_filter_count_label` | `self.filter_count_label` |
| `self._predict_filter_industry_combo` | `self.filter_industry_combo` |
| `self._predict_data_source_label` | `self.data_source_label` |
| `self._sentiment_score_label` | `self.sentiment_score_label` |
| `self._sentiment_advice_label` | `self.sentiment_advice_label` |
| `self._sentiment_summary_label` | `self.sentiment_summary_label` |
| `self._sentiment_result` | `self.sentiment_result` |
| `self._sentiment_thread` | `self.sentiment_thread` |
| `self._predict_lists` | `self.lists` |
| `self._predict_results_map` | `self.results_map` |
| `self._predict_bucket_rates_cache` | `self.bucket_rates_cache` |
| `self._predict_best_buckets` | `self.best_buckets` |
| `self._predict_thread` | `self.thread` |
| `self._predict_result` | `self.result` |
| `self._predict_cont_sort_column` / `_reverse` | `self.cont_sort_column` / `_reverse` |
| `self._predict_first_sort_column` / `_reverse` | `self.first_sort_column` / `_reverse` |
| `self._predict_fresh_sort_column` / `_reverse` | `self.fresh_sort_column` / `_reverse` |
| `self._predict_wrap_sort_column` / `_reverse` | 同上 |
| `self._predict_trend_sort_column` / `_reverse` | 同上 |
| `self._predict_sort_mode_var` | `self.sort_mode_var` |

### 方法映射（35 方法）

按 plan Task 1 grep 结果列表，全部迁移到 PredictTab 类，去掉 predict/_predict_ 前缀。

| 旧 | 新 |
|---|---|
| `setup_predict_tab` | `_build`（内化） |
| `_load_last_limit_up_prediction` | `_load_last_prediction` |
| `_refresh_predict_history_dates` | `_refresh_history_dates` |
| `_on_predict_history_selected` | `_on_history_selected` |
| `_refresh_selected_predict_date` | `_refresh_selected_date` |
| `_refresh_predict_display_if_ready` | `_refresh_display_if_ready` |
| `_predict_row_tag` | `_row_tag` |
| `_predict_sort_value` | `_sort_value`（@staticmethod） |
| `_sort_predict_records` | `_sort_records` |
| `_predict_bucket_priority_for` | `_bucket_priority_for` |
| `_get_predict_bucket_rates` | `_get_bucket_rates` |
| `_on_predict_heading_click` | `_on_heading_click` |
| `_start_predict` | `start` |
| `_load_predict` | `_load` |
| `_predict_show_error` | `_show_error` |
| `_open_predict_backtest_dialog` | `_open_backtest_dialog` |
| `_apply_predict_result` | `_apply_result` |
| `_start_predict_prewarm` | `_start_prewarm` |
| `_run_predict_prewarm` | `_run_prewarm` |
| `_refresh_predict_industry_options` | `_refresh_industry_options` |
| `_reset_predict_filters` | `_reset_filters` |
| `_on_predict_filter_changed` | `_on_filter_changed` |
| `_on_predict_sort_mode_changed` | `_on_sort_mode_changed` |
| `_matches_predict_filters` | `_matches_filters` |
| `_filter_predict_records` | `_filter_records` |
| `_render_predict_trees` | `_render_trees` |
| `_on_predict_stock_select` | `_on_stock_select` |
| `_on_predict_stock_double_click` | `_on_stock_double_click` |
| `_refresh_predict_accuracy_async` | `_refresh_accuracy_async` |
| `_apply_predict_accuracy` | `_apply_accuracy` |
| `_refresh_predict_best_bucket_labels` | `_refresh_best_bucket_labels` |
| `_refresh_predict_subcategory_best_buckets` | `_refresh_subcategory_best_buckets` |
| `_find_best_bucket_for_category` | `_find_best_bucket_for_category` |
| `_open_predict_compare_window` | `open_compare_window` |
| `_build_predict_compare_window` | `_build_compare_window` |
| `_open_predict_strategy_window` | `open_strategy_window` |
| `_open_nim_key_dialog` | `open_nim_key_dialog`（已 public） |
| `_open_daily_brief_window` | `open_daily_brief_window` |
| `_refresh_sentiment_async` | `_refresh_sentiment_async` |
| `_apply_sentiment_result` | `_apply_sentiment_result` |
| `_show_sentiment_detail` | `_show_sentiment_detail` |
| `_refresh_data_source_label` | `_refresh_data_source_label` |

注意：sentiment 相关方法属于 predict tab 顶部 sent_bar 的功能，全部迁入 PredictTab。

方法内部：
- 所有 `self._predict_xxx` → `self.xxx`
- 所有 `self._sentiment_xxx` → `self.sentiment_xxx`
- 跨 tab：
  - `self.notebook` → `self.app.notebook`
  - `self.status_var` → `self.app.status_var`
  - `self.stock_filter` → `self.app.stock_filter`
  - `self._ui` → `self.app._ui`
  - `self._post_to_ui` → `self.app._post_to_ui`
  - `self._log_async` → `self.app._log_async`
  - `self._set_top_header_for_code` → `self.app._set_top_header_for_code`
  - `self.notebook.select(self.detail.frame)` → `self.app.notebook.select(self.app.detail.frame)`
  - `self.detail.show(...)` → `self.app.detail.show(...)`
  - `self.detail.payload_cache` → `self.app.detail.payload_cache`
  - `self.intraday.payload_cache` → `self.app.intraday.payload_cache`
  - `self.min_price_var` → `self.app.min_price_var`
  - `self.max_price_var` → `self.app.max_price_var`
  - `self.selected_boards` → `self.app.selected_boards`
  - `self.result.filtered_stocks` 等 → `self.app.result.filtered_stocks`
  - `self.root` → `self.app.root`

---

## Task 3 — 更新 app.py

- [ ] import `from src.gui.tabs.predict import PredictTab`
- [ ] 删 __init__ 中所有 _predict_xxx / _sentiment_xxx 状态字段
- [ ] setup_notebook 改 `self.predict = PredictTab(self, self.notebook)`
- [ ] 删 setup_predict_tab + 35 业务方法
- [ ] tab 注册表 `self.predict_tab` → `self.predict.frame`
- [ ] 文件菜单 / action_bar command 引用更新（如有）
- [ ] 跨 tab 引用（detail/intraday/result 内部对 `_refresh_predict_display_if_ready` 等的调用）：
  - `self.app._refresh_predict_display_if_ready` → `self.app.predict._refresh_display_if_ready`
  - `self.app._refresh_predict_accuracy_async` → `self.app.predict._refresh_accuracy_async`

### grep 残留

```powershell
grep -rn "self\.predict_tab\|self\._predict_\|self\._sentiment_\|self\._daily_brief\|self\.setup_predict_tab\|self\._start_predict\|self\._load_predict\|self\._apply_predict\|self\._render_predict\|self\._refresh_predict\|self\._open_predict\|self\._on_predict\|self\._matches_predict\|self\._filter_predict\|self\._sort_predict_records\|self\._predict_row_tag\|self\._predict_sort_value\|self\._predict_bucket_priority\|self\._get_predict_bucket_rates\|self\._reset_predict_filters\|self\._build_predict_compare_window\|self\._start_predict_prewarm\|self\._run_predict_prewarm\|self\._refresh_predict_industry_options\|self\._refresh_predict_accuracy_async\|self\._refresh_predict_best_bucket_labels\|self\._refresh_predict_subcategory_best_buckets\|self\._find_best_bucket_for_category\|self\._refresh_data_source_label" src/gui/ stock_gui.py
```

预期：**0 命中**（除 PredictTab 内 self.xxx）

---

## Task 4 — 验证 + commit

- [ ] import OK
- [ ] pytest 318 不下降
- [ ] GUI 8s 启动
- [ ] 行数：app.py ~4362 → ~1500，tabs/predict.py ~2800
- [ ] Commit

```powershell
git -C D:\code\python\gupiao add src/gui/app.py src/gui/tabs/predict.py src/gui/tabs/intraday.py src/gui/tabs/detail.py src/gui/tabs/result.py
git -C D:\code\python\gupiao commit -m @'
重构（P5）：抽 PredictTab 类到 src/gui/tabs/predict.py

按 stock_gui.py 模块化拆分 spec 的 Phase 5（最后一个 tab，也是最大的）。
把涨停预测 tab 相关的 ~35 方法 + ~50 widget + ~25 state 字段集体迁移到
PredictTab 类。含 sentiment 条、5 sub-tab 候选表、命中对比/策略分析/批量
回测/AI 博弈短报弹窗、数据源指示标签等所有功能。

主类 StockMonitorApp:
- __init__ 删除所有 _predict_* / _sentiment_* 字段
- setup_notebook 改为 self.predict = PredictTab(self, self.notebook)
- setup_predict_tab + 35 个业务方法整体删除
- 跨 tab 调用（detail/intraday/result）引用同步更新为 self.app.predict.xxx

行为零变化，pytest 318 全绿，app.py 削减 ~2800 行（4362 → ~1500，主类完成
"thin orchestrator" 转型）。

至此 stock_gui.py 模块化 5-phase 拆分全部完成，主类只剩装配 + 全局共享状态
+ 全局菜单/信号转发，符合 spec 验收：每个 tabs/*.py < 1500 行 / app.py <
1000 行（接近）。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

---

## 自检表

| 检查 | 状态 |
|---|---|
| 模板匹配 P2/P3/P4 | ✅ |
| `self.frame` 作 tab 容器 | ✅ |
| 35 方法去 predict/_predict 前缀 | ✅ |
| sentiment 子组件一并迁入 PredictTab | ✅ |
| 命中对比/策略分析/批量回测 弹窗作为 PredictTab 方法保留（不强制拆子文件） | ✅ |
| 全局 var（min_price/max_price/selected_boards 等）留 App | ✅ |
| 跨 tab 调用全部更新 | ✅ |
