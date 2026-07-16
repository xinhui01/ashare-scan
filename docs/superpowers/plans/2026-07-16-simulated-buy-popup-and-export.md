# 模拟买入弹窗与当日导出 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 主模拟买入页仅展示当前预测日，同时用两个弹窗承载全部历史和累计收益率，并让 Excel 导出包含当前日模拟买入。

**Architecture:** `PredictTab` 负责按当前结果日期筛选流水、管理两个只读弹窗，并向导出服务注入当日流水。Excel 服务只把调用方提供的记录写成新工作表；现有流水存储和收益曲线服务保持不变。

**Tech Stack:** Python 3、Tkinter/ttk、Matplotlib、SQLite、openpyxl、pytest

---

### Task 1: Excel 导出当前日模拟买入

**Files:**
- Modify: `src/services/prediction_excel_export_service.py`
- Modify: `src/gui/tabs/predict.py`
- Test: `tests/test_prediction_excel_export_service.py`
- Test: `tests/test_predict_trend_tab.py`

- [ ] **Step 1: 写失败测试**

```python
def test_export_prediction_to_excel_writes_simulated_buy_sheet(tmp_path):
    out = tmp_path / "prediction.xlsx"
    trades = [{"prediction_date": "20260716", "trade_date": "", "code": "600992",
               "name": "贵绳股份", "category_label": "反包", "score": 79,
               "buy_price": None, "sell_price": None, "profit_pct": None,
               "trade_status": "pending", "reasons": "测试"}]
    export_prediction_to_excel({"trade_date": "20260716"}, out, simulated_buy_trades=trades)
    ws = load_workbook(out)["模拟买入"]
    assert ws["A2"].value == "20260716"
    assert ws["C2"].value == "600992"
    assert ws["J2"].value == "等待交易"
```

- [ ] **Step 2: 运行测试确认因新参数缺失而失败**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_prediction_excel_export_service.py::test_export_prediction_to_excel_writes_simulated_buy_sheet -v`

Expected: FAIL，`export_prediction_to_excel` 不接受 `simulated_buy_trades`。

- [ ] **Step 3: 实现工作表写入与 GUI 注入**

新增 `_write_simulated_buy_sheet(wb, trades)`，固定写入以下字段：

```python
SIMULATED_BUY_COLUMNS = [
    ("prediction_date", "预测日"), ("trade_date", "交易日"),
    ("code", "代码"), ("name", "名称"), ("category_label", "来源"),
    ("score", "预测分"), ("buy_price", "买入价"), ("sell_price", "卖出价"),
    ("profit_pct", "单笔盈亏%"), ("_status_text", "状态"),
    ("reasons", "入选依据"),
]
```

扩展导出函数为：

```python
def export_prediction_to_excel(prediction, path, *, simulated_buy_trades=None):
    trades = list(simulated_buy_trades or [])
    # 原有工作表写入保持原顺序
    _write_simulated_buy_sheet(wb, trades)
```

GUI 调用前执行：

```python
all_trades = stock_store.load_simulated_buy_trades()
current_trades = [row for row in all_trades if row.get("prediction_date") == trade_date]
export_prediction_to_excel(payload, path, simulated_buy_trades=current_trades)
```

- [ ] **Step 4: 运行 Excel 与 GUI 接线测试**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_prediction_excel_export_service.py tests/test_predict_trend_tab.py -q`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add src/services/prediction_excel_export_service.py src/gui/tabs/predict.py tests/test_prediction_excel_export_service.py tests/test_predict_trend_tab.py
git commit -m "feat: export current simulated buys to excel"
```

### Task 2: 主页面恢复当前日模式

**Files:**
- Modify: `src/gui/tabs/predict.py`
- Test: `tests/test_predict_trend_tab.py`

- [ ] **Step 1: 写失败测试**

```python
def test_simulated_buy_main_view_filters_current_prediction_date():
    source = inspect.getsource(PredictTab._current_simulated_buy_trades)
    render = inspect.getsource(PredictTab._render_simulated_buy_history)
    assert 'row.get("prediction_date") == prediction_date' in source
    assert "_current_simulated_buy_trades" in render

def test_simulated_buy_tab_has_history_and_return_buttons():
    source = inspect.getsource(PredictTab._build)
    assert 'text="历史模拟买入"' in source
    assert 'text="收益率"' in source
```

- [ ] **Step 2: 运行测试确认缺少筛选方法和按钮**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_predict_trend_tab.py -v`

Expected: FAIL 于新增断言。

- [ ] **Step 3: 调整主页面**

移除主页面内嵌账户累计收益 Figure，把摘要和两个按钮放入顶部工具栏；下方保持当前日表格与右侧分时图。实现：

```python
def _current_simulated_buy_trades(self):
    prediction_date = str((self.result or {}).get("trade_date") or "").strip()
    return [row for row in stock_store.load_simulated_buy_trades()
            if str(row.get("prediction_date") or "").strip() == prediction_date]
```

`_render_simulated_buy_history` 只渲染上述结果，摘要改为“模拟买入: N只 / 等待或当日统计”，不再绘制全历史账户图。

- [ ] **Step 4: 运行 GUI 测试**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_predict_trend_tab.py -q`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add src/gui/tabs/predict.py tests/test_predict_trend_tab.py
git commit -m "feat: keep simulated buy main view current"
```

### Task 3: 历史与收益率弹窗

**Files:**
- Modify: `src/gui/tabs/predict.py`
- Test: `tests/test_predict_trend_tab.py`

- [ ] **Step 1: 写失败测试**

```python
def test_simulated_buy_popups_use_all_history():
    history = inspect.getsource(PredictTab.open_simulated_buy_history)
    returns = inspect.getsource(PredictTab.open_simulated_buy_returns)
    assert "load_simulated_buy_trades" in history
    assert "load_simulated_buy_trades" in returns
    assert "build_account_curve" in returns
```

- [ ] **Step 2: 运行测试确认弹窗方法缺失**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_predict_trend_tab.py::test_simulated_buy_popups_use_all_history -v`

Expected: FAIL，两个方法不存在。

- [ ] **Step 3: 实现可复用弹窗**

`open_simulated_buy_history` 创建或前置 `Toplevel`，用独立 Treeview 按倒序展示 `load_simulated_buy_trades()` 全部记录。`open_simulated_buy_returns` 创建或前置另一个 `Toplevel`，用 `build_account_curve(all_trades)` 绘制全部历史累计收益率，并展示有效数、胜率、累计收益和单笔平均。

两个窗口都设置 `WM_DELETE_WINDOW` 回调，销毁窗口并把相应引用设为 `None`。空数据时分别显示空表和“暂无已完成交易”。

- [ ] **Step 4: 运行聚焦与完整测试**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_prediction_excel_export_service.py tests/test_predict_trend_tab.py tests/test_simulated_buy_service.py -q`

Expected: PASS。

Run: `.\.venv\Scripts\python.exe -m pytest -q`

Expected: 全部测试通过。

- [ ] **Step 5: 静态检查并提交**

Run: `.\.venv\Scripts\python.exe -m py_compile src/gui/tabs/predict.py src/services/prediction_excel_export_service.py`

Expected: 无输出且退出码 0。

```bash
git add src/gui/tabs/predict.py tests/test_predict_trend_tab.py
git commit -m "feat: add simulated buy history and return popups"
```
