# stock_gui.py 模块化拆分设计

- 创建日期：2026-05-20
- 状态：待实施
- 范围：把 6630 行 / 348 KB 的单体 `stock_gui.py` 拆成按 tab 组织的 `src/gui/tabs/*.py` + thin orchestrator `src/gui/app.py`
- 5 个 phase 串行执行，按 tab 复杂度由易到难

## 背景

经过 watchlist + compare 删除清理后，`stock_gui.py` 仍是 6630 行 / 229 方法的 god class。5 个 tab（log / intraday / detail / result / predict）+ 控制面板 + 全局菜单 + 后台 worker 全堆在一起，最重的 predict tab 单独占 48%。这是项目"企业级化"路线图剩余的最大结构性债务。

清理是上一轮的产出；本轮聚焦**结构性模块化**。

## 目标

1. **每个 tab 一个文件**：`src/gui/tabs/log.py` / `intraday.py` / `detail.py` / `result.py` / `predict.py`，每个文件 ≤ 1500 行
2. **主类瘦身**：`stock_gui.py` 最终 ≤ 1000 行，且只剩"装配 + 共享状态 + 全局信号"职责
3. **责任清晰**：Tab 类 own 自己的 widget 和 tab 私有状态；跨 tab 引用走显式 `self.app.xxx`
4. **可独立读懂**：拆完后，看 `tabs/intraday.py` 不用跳到主文件就能理解分时 tab 的全部行为
5. **零行为回归**：5 个 tab 的视觉/交互/数据流完全不变；pytest 全程 274 passed

非目标：
- 不重设计任何 UI / UX（视觉零变化）
- 不引入 mypy / 类型注解全覆盖（type hint 按既有风格补，不强求 strict）
- 不抽 ViewModel / 观察者模式（已评估为对 Tk 桌面工具过度设计）
- 不动 src/services / src/sources / src/network / src/utils 等已模块化的目录
- 不新增单元测试（行为不变，靠现有 274 个测试守底；新测试留作后续工程）
- 不动数据库 schema / migration

## 设计原则

1. **Tab 类约定**统一为：
   ```python
   class XxxTab:
       def __init__(self, app: "StockGui", notebook: ttk.Notebook) -> None:
           self.app = app
           self._build(notebook)
       
       def _build(self, notebook: ttk.Notebook) -> None:
           """构建 widget。挂自己的 instance attrs。"""
           ...
       
       # 业务方法直接挂在 tab 类上；跨 tab 引用走 self.app.xxx
   ```
   主类构造期间装配各 tab：`self.predict = PredictTab(self, self.notebook)`

2. **状态迁移规则**：
   - **tab 私有 widget**（仅 1 个 tab 用）→ 迁到 tab 类：`self.intraday_fig` → `self.intraday.fig`
   - **tab 私有数据**（仅 1 个 tab 用）→ 迁到 tab 类：`self._intraday_request_code` → `self.intraday.request_code`
   - **全局共享 var**（被多 tab 读写或被菜单读写）→ 留在 App 上：`min_price_var` / `max_price_var` / `status_var` / `top_header_var` / `notebook` / `_ui`（UIDispatcher）/ `stock_filter`
   - **跨 tab 共享方法** → 留在 App 上：`_post_to_ui` / `_lookup_result_by_code` / `_infer_board_from_code` / `_clear_top_header` / `_set_top_header_for_code`

3. **方法命名**：
   - tab 类内部方法去掉 tab 前缀：`navigate_intraday_day` → `IntradayTab.navigate_day`
   - 但**公开给主类调用的方法**（如 `open_intraday_view`）保留语义化全名：`IntradayTab.open_view(code: str)`
   - 私有方法保留 `_xxx` 前缀

4. **向后兼容**：
   - `stock_gui.py` 退化成 facade：`from src.gui.app import StockGui`
   - `main.py` 不动（仍 `from stock_gui import StockGui`）
   - 外部脚本 / 测试如有引用 `app.intraday_fig` 等老属性 → grep 验证；如发现外部依赖，加属性别名（`@property def intraday_fig(self): return self.intraday.fig`）兜底

5. **不开 feature branch**，直接在 main 提交。

## 目录结构（目标）

```
src/gui/
├── log_drainer.py        (existing, 不动)
├── ui_dispatch.py        (existing, 不动)
├── tree_enhancer.py      (existing, 不动)
├── result_columns.py     (existing, 不动)
├── result_filters.py     (existing, 不动)
├── app.py                ← 新：StockGui 主类
└── tabs/
    ├── __init__.py
    ├── log.py            ← Phase 1：~30 行
    ├── intraday.py       ← Phase 2：~600 行
    ├── detail.py         ← Phase 3：~400 行
    ├── result.py         ← Phase 4：~800 行
    └── predict.py        ← Phase 5：主壳 + 可能再 split 成
                              predict_render.py / predict_worker.py /
                              predict_compare_window.py（行数过大再决定）

stock_gui.py              ← facade：from src.gui.app import StockGui
main.py                   ← 不动
```

## 5 个 phase 概览

每个 phase = 一个 commit + 独立 spec compliance review + 代码质量 review。

| Phase | 改造 | 估算 | 风险 |
|---|---|---|---|
| **P1** | log tab + 模式落地（建 app.py 主类骨架 + tabs/log.py + facade 转发） | 模板建立，~150 行新增 + ~30 行删除 | 极低 |
| **P2** | intraday tab（含 matplotlib Figure / 分时缓存 / 导航按钮 / K 线交互入口） | ~600 行迁移 | 中 |
| **P3** | detail tab（含 K 线 + 跨 tab 触发 intraday） | ~400 行迁移 | 中 |
| **P4** | result tab（含价格过滤共用 var 留 App / 快速过滤栏 / 结果列定义） | ~800 行迁移 | 中高 |
| **P5** | predict tab（5 个 sub-tab notebook + worker 线程链 + AI 博弈短报 + 命中对比窗口 + 策略分析窗口 + 批量回测） | ~3000 行迁移，可能再 split | 高 |

### Phase 间依赖

- P1 必须先做：建立 `src/gui/app.py` 主类骨架 + `tabs/__init__.py` + facade 模式。后续 phase 直接复用模板。
- P2-P5 之间**无硬依赖**，但建议按 P2 → P3 → P4 → P5 顺序：
  - P3 (detail) 依赖 P2 (intraday) 的 `open_view(code)` 接口稳定
  - P4 (result) 依赖 P3 (detail) 的双击跳详情接口稳定
  - P5 (predict) 依赖 P3/P4 的"跳详情/跳分时"接口稳定

## 数据流变化（示例）

**改造前**（intraday tab 相关）：
```python
# stock_gui.py 同一类内
self.intraday_fig = Figure(...)
self.intraday_canvas = FigureCanvasTkAgg(self.intraday_fig, ...)

def open_intraday_view(self, stock_code: str):
    self.intraday_request_code = stock_code
    self._load_intraday(...)

def _load_intraday(self, stock_code, day_offset, target_date, cancel_token):
    # 大量 self.intraday_xxx 引用
```

**改造后**：
```python
# src/gui/tabs/intraday.py
class IntradayTab:
    def __init__(self, app: "StockGui", notebook: ttk.Notebook) -> None:
        self.app = app
        self._build(notebook)
    
    def _build(self, notebook):
        self.frame = ttk.Frame(notebook, padding="5")
        notebook.add(self.frame, text="分时")
        self.fig = Figure(...)
        self.canvas = FigureCanvasTkAgg(self.fig, ...)
        # ...
    
    def open_view(self, stock_code: str):
        self.request_code = stock_code
        self._load(...)
    
    def _load(self, stock_code, day_offset, target_date, cancel_token):
        # self.fig / self.canvas / self.request_code 等
        # 跨 tab：self.app.status_var.set(...) / self.app._post_to_ui(...)

# src/gui/app.py
class StockGui:
    def __init__(self):
        # 全局共享 state
        self.min_price_var = tk.StringVar(value="")
        self.status_var = tk.StringVar(value="就绪")
        # ...
        self._setup_notebook()
        self.intraday = IntradayTab(self, self.notebook)
        # detail tab 之后 K 线点击会调 self.intraday.open_view(code)
```

## 错误处理

- 迁移过程中如果发现某方法**被外部脚本/测试引用**（grep `app.intraday_fig` 等），加 `@property` 别名兜底；不删原属性引用，但内部实现走 tab 类
- 如果迁移中发现某 helper 实际被多 tab 共用（spec 漏判定为单 tab 私有）→ 升级为 App 共享方法，在 spec/plan 现场记录
- 老 `stock_gui.py` 中保留**所有原导出符号的 facade 转发**，保证 `from stock_gui import XxxxYyy` 老调用方不会断

## 测试策略

- 每个 phase 完成后跑 `pytest -q`，必须 274 passed 不下降
- 每个 phase 完成后做 import check：`python -c "import stock_gui; print('OK')"` + 关键模块
- 每个 phase 完成后跑 GUI 静态启动 5-6 秒，无 traceback
- 不新增测试（行为不变，靠现有测试守底）

## 验收标准

1. **文件大小**：每个 `tabs/*.py` ≤ 1500 行；`app.py` ≤ 1000 行；`stock_gui.py` ≤ 100 行（facade）
2. **pytest** 全 phase 后保持 274 passed
3. **GUI 启动**正常，5 个 tab 都能切换、显示无异常
4. **核心交互回归测试**（人工 + 静态）：
   - 涨停预测：点"开始预测" → 表格填充 → 双击候选 → 详情 tab 显示数据
   - 详情 tab：K 线点击 → 分时 tab 显示对应日期分时
   - 分时 tab：前一天 / 后一天导航
   - 扫描结果：开始扫描 → 表格填充 → 应用价格过滤生效
   - 全局菜单：备份数据库 / 恢复数据库 / 清理过期数据 不报错
5. **依赖正确性**：grep `from stock_gui import` 全代码库，确认所有引用方仍能 import 成功
6. **代码组织**：grep `^class IntradayTab` `^class DetailTab` 等，确认 5 个 tab 类都已在 `src/gui/tabs/*.py` 中存在

## 不在范围

- 不改 UI / UX 视觉
- 不动 src/services / src/sources / src/network / src/utils
- 不引入 mypy / ruff / black / pre-commit hook（留作后续工程）
- 不抽 ViewModel / 状态管理库（Tk 不 reactive，过度设计）
- 不新增单元测试（避免在结构变更与新测试中夹带太多变量）
- 不动数据库 / 配置 / 启动方式

## 后续工程（不在本 spec）

- 工程化基建：mypy + ruff + black + pre-commit + GitHub Actions CI
- 业务逻辑从 GUI 抽出来做单元测试覆盖（特别是 predict 评分链路）
- 拆 `stock_filter.py`（215 KB）成 `src/services/scoring/` 子模块
- 完成 `stock_data.py` / `stock_store.py` 收尾迁移
- 文档：README + usage.md 更新模块图
