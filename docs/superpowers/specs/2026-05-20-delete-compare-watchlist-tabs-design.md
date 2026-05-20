# 删除「涨停对比」与「自选池」Tab 设计文档

- 创建日期：2026-05-20
- 状态：待实施
- 范围：仅删除两个默认隐藏 tab 及其连带代码；不影响其他 tab、不动数据库表结构

## 背景

`stock_gui.py` 当前注册了 7 个 tab，其中 3 个默认隐藏（result / compare / watchlist），需通过"视图"菜单显式开启。用户确认 **compare（涨停对比）** 与 **watchlist（自选池）** 两个功能已停用，需要从代码层完整下线，仅保留 result tab。

清理目的：
- 减少 `stock_gui.py`（397 KB 单体）的死代码面
- 移除散布在 detail / result tab 中的跨 tab 入口（按钮、菜单、过滤），避免误点
- 移除 `stock_store.py` / `db_admin_service.py` / `src/gui/` 中只服务于这两个功能的函数和测试

非目的：
- 不重构 `stock_gui.py` 的整体结构（留待后续子工程）
- 不 DROP 数据库表，也不改动 CREATE TABLE DDL（保护历史数据 + 避免迁移风险）
- 不删除 `stock_filter.py` 中被 predict tab 共用的"涨停"相关逻辑
- 不删除 `llm_client.py`、`llm_theme_clustering.py`（被 predict 的 daily_brief / concept_hype 链路使用）

## 设计原则

1. **保护数据库**：现有 SQLite 文件中的 `watchlist` / `limit_up_compares` 表及数据原样保留，`CREATE TABLE` DDL 与备份清单条目不动，仅删除上层读写函数。
2. **分两阶段提交**：Phase 1（watchlist）和 Phase 2（compare）各自一个 commit，可独立验证、出问题时易于 `git bisect`。
3. **跨 tab 入口要清理彻底**：watchlist 的"加入自选"按钮散布在 detail / result tab，必须一并删除，避免出现"按钮点了什么都没发生"的死代码。
4. **被共用的底层保留**：`_open_nim_key_dialog` 方法、`llm_*` 模块、`stock_filter.py` 里被 predict 使用的 limit-up 工具函数全部保留。

## 实施方案：方案 B（两阶段删除）

### Phase 1 — 删除 watchlist 功能

| 文件 | 删除项 | 备注 |
|---|---|---|
| `stock_gui.py` | `setup_watchlist_tab()`（约 5147-5183）+ widget 引用 `self.watchlist_tab` / `self._watch_tree` / `self.watchlist_summary_var` | tab 构建入口 |
| `stock_gui.py` | 18 个 watchlist 方法：`_load_watchlist_items` / `_build_watchlist_item_payload` / `refresh_watchlist_view` / `on_watchlist_select` / `on_watchlist_double_click` / `add_selected_result_to_watchlist` / `remove_selected_result_from_watchlist` / `add_current_detail_to_watchlist` / `toggle_current_detail_watchlist` / `edit_current_detail_watch_note` / `_add_code_to_watchlist` / `_remove_code_from_watchlist` / `_edit_watchlist_item` / `edit_selected_watchlist_item` / `remove_selected_watchlist_item` / `_update_detail_watch_state` / `_sync_watchlist_with_scan_results` / `_export_watchlist_csv` / `_import_watchlist_csv` | 完整移除 |
| `stock_gui.py` | detail tab "加入自选" 按钮 + "编辑备注" 按钮（约 1386-1397）和 `watch_status` 详情行（约 1414） | 跨 tab 入口清理 |
| `stock_gui.py` | result tab "加入自选 / 移除自选" 按钮（约 698-699） | 跨 tab 入口清理 |
| `stock_gui.py` | 菜单 "导出/导入自选股 CSV"（约 272-273） | 跨 tab 入口清理 |
| `stock_gui.py` | `only_watchlist_var` 复选框（约 488-496）+ 对应过滤逻辑（约 5658-5678） | 快速过滤入口清理 |
| `stock_gui.py` | tab 注册表中 `("watchlist", ...)` 条目（约 590）；启动钩子 `setup_watchlist_tab()` 调用（约 542）；`_load_watchlist_items()` 初始化调用 | 注册与启动 |
| `stock_gui.py` | `from ... import` 段中所有 watchlist 相关符号 | 收尾 |
| `src/gui/result_columns.py` | `_extract_watch_flag()`（约 123）+ watch 列定义（约 238-240） | 表列定义 |
| `src/gui/result_filters.py` | `only_in_watchlist()`（约 85） | 过滤谓词 |
| `src/services/db_admin_service.py` | `export_watchlist_csv()`（约 153）+ `import_watchlist_csv()`（约 214） | CSV 导入导出服务 |
| `stock_store.py` | `save_watchlist_item` / `load_watchlist` / `load_watchlist_item` / `delete_watchlist_item`（约 369 / 414 / 447 / 481） | 读写函数 |
| `stock_store.py` | **保留：** `CREATE TABLE watchlist` DDL（约 234-245）、`_TABLES_FOR_BACKUP` 中 `watchlist` 条目 | DB schema 不动 |
| `tests/test_stock_store.py` | `test_save_load_delete_watchlist` / `test_watchlist_upsert`（约 140 / 155） | 删测试 |
| `tests/test_db_admin_service.py` | watchlist CSV 相关 test case | 删测试 |
| `tests/test_result_columns.py` | watch 列相关 case | 删测试 |
| `tests/test_result_filters.py` | `only_in_watchlist` 相关 case | 删测试 |

**Phase 1 验收：**
1. GUI 启动 5 秒内无错误日志
2. 扫描结果表无"自选"列、无"只显示自选"过滤复选框
3. detail tab 无"加入自选"/"编辑备注"按钮，无"自选状态"详情行
4. 菜单"文件"下无"导出/导入自选股 CSV"项
5. 视图菜单不再有"自选池"
6. `pytest -q` 全绿
7. `grep -rn "watchlist\|_watch_tree\|only_in_watchlist\|_add_code_to_watchlist" src/ stock_*.py tests/` 仅剩 DDL 字符串 / 备份表名常量

### Phase 2 — 删除 compare 功能

| 文件 | 删除项 | 备注 |
|---|---|---|
| `stock_gui.py` | `setup_limit_up_compare_tab()`（约 1544-1698）+ widget `self.compare_tab` 及所有 `_zt_*` 成员变量 | tab 构建入口 |
| `stock_gui.py` | 14 个 compare 方法：`_start_limit_up_compare` / `_load_limit_up_compare` / `_zt_fill_today_and_prev` / `_estimate_yesterday` / `_zt_show_error` / `_zt_filter_records` / `_refresh_zt_compare_display` / `_apply_limit_up_compare` / `_refresh_compare_history_dates` / `_on_compare_history_selected` / `_refresh_selected_compare_date` / `_save_limit_up_compare_snapshot` / `_load_last_limit_up_compare` / `_infer_board_from_code` | 完整移除 |
| `stock_gui.py` | compare tab 专属 AI 聚类入口：`_start_ai_theme_clustering` / `_load_ai_theme_clustering` / `_apply_ai_theme_clustering`（约 922-1040） | AI 聚类按钮专用 |
| `stock_gui.py` | compare action bar 上的"设置 NIM Key"按钮（约 1580） | 同名按钮在 predict tab 仍保留 |
| `stock_gui.py` | `on_zt_stock_select` / `on_zt_stock_double_click`（约 6270 / 6280） | tree 行回调 |
| `stock_gui.py` | tab 注册条目 `("compare", ...)`（约 589）；启动钩子 `setup_limit_up_compare_tab()`（约 541）；`_load_last_limit_up_compare()` 启动恢复 | 注册与启动 |
| `stock_gui.py` | `from llm_theme_clustering import ...`（约 51）—— 若 stock_gui 中其它处未再使用则删除；若仍被引用则保留 | import 收尾 |
| `stock_store.py` | `save_limit_up_compare_record` / `load_limit_up_compare_by_date` / `list_limit_up_compare_dates`（约 1245 / 1292 / 1319） | 读写函数 |
| `stock_store.py` | **保留：** `CREATE TABLE limit_up_compares` DDL（约 258）、备份表清单条目 | DB schema 不动 |
| `stock_filter.py` | **不删任何东西** —— `classify_limit_up_pool` / `_build_compare_market_context` / `_score_continuation_by_compare` / `compare_limit_up_pools` 等均被 predict tab 共用 | 共享逻辑保留 |
| `tests/` | 目前 tests/ 下未发现 compare tab 专用测试文件 | 无需动 |

**Phase 2 验收：**
1. GUI 启动后视图菜单不再有"涨停对比"
2. predict tab 各功能正常：涨停预测、AI 博弈短报、批量回测、概念炒作子 tab、NIM Key 按钮
3. detail / intraday 跳转链路正常
4. `pytest -q` 全绿
5. `grep -rn "compare_tab\|_zt_\|limit_up_compare\|_start_ai_theme_clustering" src/ stock_*.py` 仅剩 DDL 字符串 / 备份表名常量 / `stock_filter.py` 中 predict 共用的工具函数

## 必须保留清单（防误删）

| 项 | 原因 |
|---|---|
| `llm_client.py` 全部 | `daily_brief_service.py`（AI 博弈短报）和 predict 的多处仍在使用 |
| `llm_theme_clustering.py` 中至少 `load_cached_themes` | `src/services/concept_hype_service.py`（predict "概念炒作"子 tab）在用；`stock_filter.py:2003` 也调用 |
| `_open_nim_key_dialog` 方法 | predict tab `action_bar` 上的 "NIM Key" 按钮（约 2102）仍绑定它 |
| `llm_has_api_key` / `llm_save_api_key` import | `_open_nim_key_dialog` 内部使用 |
| `stock_filter.py` 中 `classify_limit_up_pool` / `_build_compare_market_context` / `_score_continuation_by_compare` / `compare_limit_up_pools` / `compare_limit_up_pools_window` | predict tab 共用 |
| `watchlist` / `limit_up_compares` 两张表的 `CREATE TABLE` DDL | 保护已有数据，避免迁移成本 |
| `_TABLES_FOR_BACKUP` 中上述两张表的条目 | 数据库备份/恢复链路仍要正确处理这两张表 |
| `_lookup_result_by_code` 方法 | 与 watchlist 无关；detail tab 用它在扫描结果里查 board，本次不动 |

## 数据流变化

- 删除前：result tab 双击 → detail tab，detail tab 显示"自选状态" / 提供"加入自选"按钮 → 写 `watchlist` 表；result tab 提供"只显示自选"过滤；菜单可 CSV 导入导出 watchlist
- 删除后：result tab 双击 → detail tab（保留），detail tab 不再有自选相关 UI；watchlist 表数据仍在 SQLite 中（只读不写）；菜单无自选 CSV 入口
- 删除前：视图菜单切换 → compare tab 显示 → 调用 `compare_limit_up_pools` 等 fetcher 接口 → 渲染今日/昨日涨停形态分类
- 删除后：视图菜单无该项；`compare_limit_up_pools` 等接口仍可被 predict 调用（path 不变）

## 错误处理

- 启动期发现旧版 app_config 中残留 `limit_up_compare_snapshot` / watchlist 相关偏好键：可忽略（`load_app_config` 默认值兜底）
- DB 备份/恢复仍会处理 `watchlist` 和 `limit_up_compares` 表，由于 schema 未动，备份恢复链路无需改造
- `from llm_theme_clustering import ...` 在 stock_gui 顶部如果完整移除后 stock_gui 不再引用该模块即可；若 stock_gui 中其它处仍引用则按需保留对应导入

## 测试策略

- Phase 1 完成后立刻 `pytest -q`，确保被删 watchlist 测试外的全部用例通过
- Phase 2 完成后再次 `pytest -q`
- 每个 phase 完成后手动运行 GUI 做冒烟：
  - 启动 → 无报错
  - 视图菜单符合预期
  - 涨停预测、详情、分时、扫描结果四个 tab 切换均可用
  - 涨停预测做一次预测、双击候选跳详情、再切分时
  - 文件菜单的"导出结果 CSV / PNG"仍可用（这是 result tab 功能，不在删除范围）

## 分支与提交

- 不开 feature branch，直接在 `main` 提交（与项目现有惯例一致）
- Phase 1 commit message：`清理：删除自选池功能（保留 DB 表）`
- Phase 2 commit message：`清理：删除涨停对比 tab（保留 DB 表）`
- 每个 commit 前必须本地跑通 `pytest -q`

## 后续工程（不在本 spec 范围）

本次清理仅是"企业级化"路线图的第 1 步。后续独立子工程（每个单独 brainstorm → spec → 实施）：
- 拆分 `stock_gui.py`（397 KB）到 `src/gui/` 子模块
- 拆分 `stock_filter.py`（215 KB）到 `src/services/scoring/` 子模块
- 完成 `stock_data.py` / `stock_store.py` → src 子模块的最后迁移
- 工程化基建（类型注解 / lint / CI / 统一异常 / 配置管理）
