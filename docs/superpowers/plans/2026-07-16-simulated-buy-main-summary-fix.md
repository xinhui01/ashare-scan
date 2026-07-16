# 模拟买入主页面统计与布局修复 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 恢复主页面历史累计统计，删除右侧分时收益区域并让当日表格铺满宽度。

**Architecture:** `PredictTab` 主页面摘要同时使用当前日筛选流水和全部流水；历史累计收益复用 `build_account_curve`。删除只为主页面分时服务的组件、状态和方法，历史与收益率弹窗保持不变。

**Tech Stack:** Python 3、Tkinter/ttk、pytest

---

### Task 1: 恢复双行统计并删除主页面分时区域

**Files:**
- Modify: `src/gui/tabs/predict.py`
- Modify: `tests/test_predict_trend_tab.py`

- [ ] **Step 1: 写失败测试**

```python
def test_simulated_buy_main_summary_includes_history_metrics():
    source = inspect.getsource(PredictTab._render_simulated_buy_history)
    assert "load_simulated_buy_trades" in source
    assert "build_account_curve" in source
    assert "历史累计" in source

def test_simulated_buy_main_removes_intraday_panel():
    source = inspect.getsource(PredictTab._build)
    assert "买入当日分时收益" not in source
    assert "simulated_intraday_canvas" not in source
    assert "simulated_lower.add(history_frame" not in source
```

- [ ] **Step 2: 运行测试确认失败**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_predict_trend_tab.py -v`

Expected: FAIL，摘要只使用当前日流水，构建方法仍包含分时面板。

- [ ] **Step 3: 实现最小修复**

主页面使用单个 `LabelFrame` 直接 `pack(fill=tk.BOTH, expand=True)`，删除分时 Figure/Canvas 和 Treeview 选择绑定。摘要方法分别取得：

```python
current_trades = self._current_simulated_buy_trades()
all_trades = stock_store.load_simulated_buy_trades()
curve = build_account_curve(all_trades)
```

第一行格式为 `模拟买入: 2只 · 有效0只 · 等待交易2只`，第二行格式为 `历史累计: 124笔 · 有效118笔 · 胜率25.4% (30/118) · 账户累计+86.5% · 单笔平均+1.2% · 等待交易4笔`。删除 `_show_simulated_intraday_message`、`_on_simulated_trade_select` 和 `_apply_simulated_intraday`，以及只被这些方法使用的请求状态字段和 `build_intraday_return_curve` 导入。

- [ ] **Step 4: 运行聚焦和完整测试**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_predict_trend_tab.py tests/test_simulated_buy_service.py tests/test_prediction_excel_export_service.py -q`

Expected: PASS。

Run: `.\.venv\Scripts\python.exe -m pytest -q`

Expected: 全部测试通过。

- [ ] **Step 5: 编译、提交和推送**

Run: `.\.venv\Scripts\python.exe -m py_compile src/gui/tabs/predict.py`

Expected: 无输出且退出码 0。

```bash
git add src/gui/tabs/predict.py tests/test_predict_trend_tab.py
git commit -m "fix: restore simulated buy history summary"
git push origin main
```
