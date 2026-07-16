# 模拟买入历史与收益曲线 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为模拟买入建立不可变历史流水，并在现有 Tkinter 页面展示历史列表、等权复利账户曲线和选中股票当日分时收益曲线。

**Architecture:** SQLite 流水表保存每天实际入选快照与 T+1 交易结果；`simulated_buy_service` 提供历史同步、状态回填和纯收益计算；`PredictTab` 只负责异步加载和 Matplotlib/Tk 展示。历史分时按选择懒加载，不批量抓取。

**Tech Stack:** Python 3、SQLite、pandas、Tkinter/ttk、Matplotlib、pytest/unittest

---

## 文件结构

- Modify: `stock_store.py` — 新表、流水幂等写入/回填/查询 API。
- Modify: `src/services/simulated_buy_service.py` — 历史同步、交易状态、复利曲线和分时收益纯函数。
- Modify: `src/gui/tabs/predict.py` — 新布局、历史表、账户图、异步分时加载和新预测快照写入。
- Modify: `tests/test_stock_store.py` — 流水持久化和唯一约束测试。
- Modify: `tests/test_simulated_buy_service.py` — 同步、回填、等权复利和分时换算测试。
- Modify: `tests/test_predict_trend_tab.py` — GUI 结构与异步防串线接线测试。

### Task 1: 模拟交易流水存储

**Files:**
- Modify: `stock_store.py`
- Test: `tests/test_stock_store.py`

- [ ] **Step 1: 写失败的存储测试**

在 `tests/test_stock_store.py` 增加 `TestSimulatedBuyTrades`，验证幂等插入、倒序查询与结果回填：

```python
class TestSimulatedBuyTrades(StockStoreTestCase):
    def test_save_load_and_update_simulated_buy_trade(self):
        from stock_store import (
            load_simulated_buy_trades,
            save_simulated_buy_trades,
            update_simulated_buy_trade_result,
        )
        row = {
            "prediction_date": "20260701", "code": "000001", "name": "甲",
            "category": "first", "category_label": "二波接力", "score": 88,
            "buy_status": "可买", "reasons": "测试",
        }
        self.assertEqual(save_simulated_buy_trades([row]), 1)
        self.assertEqual(save_simulated_buy_trades([row]), 0)
        self.assertTrue(update_simulated_buy_trade_result(
            "20260701", "000001", trade_date="20260702",
            buy_price=10.0, sell_price=10.5, profit_pct=5.0,
            is_buyable=True, trade_status="completed", unavailable_reason="",
        ))
        rows = load_simulated_buy_trades()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["trade_date"], "20260702")
        self.assertEqual(rows[0]["profit_pct"], 5.0)
        self.assertEqual(rows[0]["trade_status"], "completed")
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_stock_store.py::TestSimulatedBuyTrades -v`

Expected: FAIL，缺少三个流水 API。

- [ ] **Step 3: 增加表结构与 API**

在 `_init_schema` 的 SQL 中新增 `simulated_buy_trades`，主键为 `(prediction_date, code)`；字段使用设计文档中的稳定命名。实现：

```python
def save_simulated_buy_trades(records: List[Dict[str, Any]]) -> int:
    """INSERT OR IGNORE immutable pick snapshots; return inserted count."""

def update_simulated_buy_trade_result(
    prediction_date: str, code: str, *, trade_date: str,
    buy_price: Optional[float], sell_price: Optional[float],
    profit_pct: Optional[float], is_buyable: bool,
    trade_status: str, unavailable_reason: str,
) -> bool:
    """Update only mutable T+1 evaluation fields."""

def load_simulated_buy_trades(*, descending: bool = True) -> List[Dict[str, Any]]:
    """Return plain dictionaries ordered by prediction date and code."""
```

插入必须使用 `INSERT OR IGNORE`，不得用 upsert 覆盖历史选股快照；回填 API 只更新交易日、价格、收益、可买状态、原因和更新时间。

- [ ] **Step 4: 运行存储测试**

Run: `pytest tests/test_stock_store.py::TestSimulatedBuyTrades -v`

Expected: PASS。

- [ ] **Step 5: 提交存储层**

```bash
git add stock_store.py tests/test_stock_store.py
git commit -m "feat: persist simulated buy trade ledger"
```

### Task 2: 流水同步、复利曲线与分时换算

**Files:**
- Modify: `src/services/simulated_buy_service.py`
- Test: `tests/test_simulated_buy_service.py`

- [ ] **Step 1: 写失败的服务测试**

增加以下测试，覆盖快照生成、状态映射、日等权复利和分时基准：

```python
def test_build_trade_snapshot_and_apply_completed_result():
    pick = {"trade_date": "20260701", "code": "000001", "category": "first", "score": 88}
    snapshot = build_trade_snapshot(pick)
    updated = apply_accuracy_result(snapshot, {
        "verify_date": "20260702", "t1_open": 10.0, "t1_close": 10.5,
        "t1_open_close_pct": 5.0, "hit_buyable": 1,
    })
    assert updated["prediction_date"] == "20260701"
    assert updated["trade_status"] == "completed"
    assert updated["profit_pct"] == 5.0


def test_build_account_curve_equal_weights_and_compounds():
    rows = [
        {"trade_date": "20260702", "trade_status": "completed", "is_buyable": 1, "profit_pct": 10.0},
        {"trade_date": "20260702", "trade_status": "completed", "is_buyable": 1, "profit_pct": -2.0},
        {"trade_date": "20260703", "trade_status": "completed", "is_buyable": 1, "profit_pct": 5.0},
        {"trade_date": "20260703", "trade_status": "unbuyable", "is_buyable": 0, "profit_pct": 99.0},
    ]
    curve = build_account_curve(rows)
    assert curve[0]["daily_return_pct"] == pytest.approx(4.0)
    assert curve[0]["cumulative_return_pct"] == pytest.approx(4.0)
    assert curve[1]["cumulative_return_pct"] == pytest.approx(9.2)


def test_build_intraday_return_curve_uses_open_buy_price():
    df = pd.DataFrame({"time": ["09:30", "15:00"], "price": [10.0, 10.5]})
    points = build_intraday_return_curve(df, buy_price=10.0)
    assert points["returns_pct"] == pytest.approx([0.0, 5.0])
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_simulated_buy_service.py -v`

Expected: FAIL，缺少新服务函数。

- [ ] **Step 3: 实现纯函数和历史同步入口**

实现下列公开接口：

```python
def build_trade_snapshot(pick: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "prediction_date": str(pick.get("trade_date") or "").strip(),
        "trade_date": "", "code": str(pick.get("code") or "").zfill(6),
        "name": str(pick.get("name") or ""),
        "industry": str(pick.get("industry") or ""),
        "theme": str(pick.get("theme") or ""),
        "category": str(pick.get("category") or ""),
        "category_label": str(pick.get("category_label") or ""),
        "score": _score_value(pick), "buy_status": str(pick.get("buy_status") or ""),
        "reasons": str(pick.get("reasons") or ""),
        "buy_price": None, "sell_price": None, "profit_pct": None,
        "is_buyable": 0, "trade_status": "pending", "unavailable_reason": "",
    }


def apply_accuracy_result(trade, result):
    row = dict(trade)
    if not result:
        return row
    row["trade_date"] = str(result.get("verify_date") or "")
    row["buy_price"] = result.get("t1_open")
    row["sell_price"] = result.get("t1_close")
    row["profit_pct"] = result.get("t1_open_close_pct")
    row["is_buyable"] = int(bool(result.get("hit_buyable")))
    if result.get("t1_one_word"):
        row.update(trade_status="one_word", unavailable_reason="一字板不可买")
    elif result.get("t1_suspended"):
        row.update(trade_status="suspended", unavailable_reason="停牌")
    elif row["buy_price"] is None or row["sell_price"] is None:
        row.update(trade_status="missing_price", unavailable_reason="开盘价或收盘价缺失")
    elif not row["is_buyable"]:
        row.update(trade_status="unbuyable", unavailable_reason="不可买")
    else:
        row.update(trade_status="completed", unavailable_reason="")
    return row


def build_account_curve(trades):
    daily = {}
    for row in trades:
        if row.get("trade_status") != "completed" or not int(row.get("is_buyable") or 0):
            continue
        if not row.get("trade_date") or row.get("profit_pct") is None:
            continue
        daily.setdefault(str(row["trade_date"]), []).append(float(row["profit_pct"]))
    equity = 1.0
    curve = []
    for trade_date in sorted(daily):
        daily_pct = sum(daily[trade_date]) / len(daily[trade_date])
        equity *= 1.0 + daily_pct / 100.0
        curve.append({"trade_date": trade_date, "daily_return_pct": daily_pct,
                      "equity": equity, "cumulative_return_pct": (equity - 1.0) * 100.0})
    return curve


def build_intraday_return_curve(intraday_df, *, buy_price):
    if intraday_df is None or intraday_df.empty or not buy_price:
        return {"times": [], "returns_pct": []}
    time_col = "time" if "time" in intraday_df.columns else "datetime"
    price_col = "price" if "price" in intraday_df.columns else "close"
    frame = intraday_df[[time_col, price_col]].copy()
    frame[price_col] = pd.to_numeric(frame[price_col], errors="coerce")
    frame = frame.dropna(subset=[price_col]).sort_values(time_col)
    return {"times": frame[time_col].astype(str).tolist(),
            "returns_pct": ((frame[price_col] / float(buy_price) - 1.0) * 100.0).tolist()}


def sync_simulated_buy_history(
    prediction_results: Iterable[Mapping[str, Any]],
    results_maps_by_date: Mapping[str, Mapping[Tuple[str, str], Mapping[str, Any]]],
    *, save_trades_fn, update_result_fn, limit: int = 2,
) -> Dict[str, int]:
    inserted = updated = 0
    for prediction in prediction_results:
        picks = build_simulated_buy_picks(prediction, limit=limit)
        snapshots = [build_trade_snapshot(pick) for pick in picks]
        inserted += int(save_trades_fn(snapshots) or 0)
        result_map = results_maps_by_date.get(str(prediction.get("trade_date") or ""), {})
        for snapshot in snapshots:
            result = result_map.get((snapshot["code"], snapshot["category"]))
            if not result:
                continue
            evaluated = apply_accuracy_result(snapshot, result)
            updated += int(bool(update_result_fn(
                snapshot["prediction_date"], snapshot["code"],
                trade_date=evaluated["trade_date"], buy_price=evaluated["buy_price"],
                sell_price=evaluated["sell_price"], profit_pct=evaluated["profit_pct"],
                is_buyable=bool(evaluated["is_buyable"]),
                trade_status=evaluated["trade_status"],
                unavailable_reason=evaluated["unavailable_reason"],
            )))
    return {"inserted": inserted, "updated": updated}
```

`apply_accuracy_result` 的状态规则必须明确：无结果为 `pending`；`t1_one_word` 为 `one_word`；`t1_suspended` 为 `suspended`；价格缺失为 `missing_price`；可买且开收盘齐全为 `completed`。`build_account_curve` 只接受 `completed + is_buyable + profit_pct 非空` 的记录，按交易日求均值后复利。

分时函数兼容现有数据中的 `time/datetime` 时间列和 `price/close` 价格列；清除无效数值并保持时间顺序。

- [ ] **Step 4: 运行服务测试**

Run: `pytest tests/test_simulated_buy_service.py -v`

Expected: PASS。

- [ ] **Step 5: 提交服务层**

```bash
git add src/services/simulated_buy_service.py tests/test_simulated_buy_service.py
git commit -m "feat: calculate simulated trade equity curves"
```

### Task 3: 历史补建与新预测快照接线

**Files:**
- Modify: `src/gui/tabs/predict.py`
- Test: `tests/test_predict_trend_tab.py`

- [ ] **Step 1: 写失败的接线测试**

```python
def test_prediction_tab_syncs_trade_ledger_and_saves_new_picks():
    init_src = inspect.getsource(PredictTab._start_simulated_buy_history_sync)
    apply_src = inspect.getsource(PredictTab._apply_result)
    accuracy_src = inspect.getsource(PredictTab._apply_accuracy)
    assert "sync_simulated_buy_history" in init_src
    assert "save_simulated_buy_trades" in apply_src
    assert "_start_simulated_buy_history_sync" in accuracy_src
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_predict_trend_tab.py::test_prediction_tab_syncs_trade_ledger_and_saves_new_picks -v`

Expected: FAIL，缺少后台同步方法。

- [ ] **Step 3: 实现后台同步与快照保存**

在 `PredictTab` 初始化运行态加入同步线程引用和请求序号。实现 `_start_simulated_buy_history_sync()`：后台加载全部预测、逐日准确率结果并调用 `sync_simulated_buy_history`，最后通过 `self.app._post_to_ui(self._render_simulated_buy_history)` 刷新。

在 `_apply_result` 生成 `self.simulated_buy_picks` 后，立即执行：

```python
snapshots = [build_trade_snapshot(pick) for pick in self.simulated_buy_picks]
stock_store.save_simulated_buy_trades(snapshots)
```

在 `_apply_accuracy` 完成时再次触发后台同步，使等待记录获得 T+1 结果。后台异常记录日志，不阻塞现有预测结果展示。

- [ ] **Step 4: 运行接线与现有模拟买入测试**

Run: `pytest tests/test_predict_trend_tab.py tests/test_simulated_buy_service.py -v`

Expected: PASS。

- [ ] **Step 5: 提交同步接线**

```bash
git add src/gui/tabs/predict.py tests/test_predict_trend_tab.py
git commit -m "feat: backfill simulated buy history"
```

### Task 4: 历史表和账户累计收益图

**Files:**
- Modify: `src/gui/tabs/predict.py`
- Test: `tests/test_predict_trend_tab.py`

- [ ] **Step 1: 写失败的 GUI 结构测试**

```python
def test_simulated_buy_tab_has_history_and_account_curve():
    build_src = inspect.getsource(PredictTab._build)
    render_src = inspect.getsource(PredictTab._render_simulated_buy_history)
    assert "simulated_account_canvas" in build_src
    assert '"buy_price"' in build_src
    assert '"sell_price"' in build_src
    assert "build_account_curve" in render_src
    assert "load_simulated_buy_trades" in render_src
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_predict_trend_tab.py::test_simulated_buy_tab_has_history_and_account_curve -v`

Expected: FAIL，旧页只有当前两只股票的表格。

- [ ] **Step 3: 构建三段式页面并渲染历史**

将模拟买入 sub-tab 改成垂直 `ttk.PanedWindow`：上方账户图，下方再用水平 `ttk.PanedWindow` 放历史表和个股图。历史表列为：

```python
sim_cols = (
    "prediction_date", "trade_date", "code", "name", "category",
    "score", "buy_price", "sell_price", "profit", "status", "reasons",
)
```

创建持久化的 `Figure`、Axes 和 `FigureCanvasTkAgg`。`_render_simulated_buy_history()` 从存储层读取历史，倒序填表，同时调用 `build_account_curve` 绘制按日期升序的累计收益折线和 0% 基准线。没有有效交易时清空坐标轴并显示“暂无已完成交易”。

摘要统一展示总记录、完成、有效、胜率、账户累计收益和单笔平均收益；胜率继续调用现有 `_is_hit` 口径，不用正收益代替命中。

- [ ] **Step 4: 运行 GUI 接线测试**

Run: `pytest tests/test_predict_trend_tab.py -v`

Expected: PASS。

- [ ] **Step 5: 提交历史页面**

```bash
git add src/gui/tabs/predict.py tests/test_predict_trend_tab.py
git commit -m "feat: show simulated buy history and equity chart"
```

### Task 5: 选中股票的当日分时收益图

**Files:**
- Modify: `src/gui/tabs/predict.py`
- Test: `tests/test_predict_trend_tab.py`

- [ ] **Step 1: 写失败的异步选择测试**

```python
def test_simulated_history_selection_loads_target_intraday_safely():
    select_src = inspect.getsource(PredictTab._on_simulated_trade_select)
    apply_src = inspect.getsource(PredictTab._apply_simulated_intraday)
    assert "target_trade_date=trade_date" in select_src
    assert "simulated_intraday_request_id" in select_src
    assert "request_id != self.simulated_intraday_request_id" in apply_src
    assert "build_intraday_return_curve" in apply_src
```

- [ ] **Step 2: 运行测试确认失败**

Run: `pytest tests/test_predict_trend_tab.py::test_simulated_history_selection_loads_target_intraday_safely -v`

Expected: FAIL，缺少选择处理器。

- [ ] **Step 3: 实现懒加载和绘图**

历史表绑定 `<<TreeviewSelect>>` 到 `_on_simulated_trade_select`。从行 iid 映射取得 `code/trade_date/buy_price`，递增 `simulated_intraday_request_id`，后台调用：

```python
payload = self.app.stock_filter.get_stock_intraday(
    code, day_offset=0, target_trade_date=trade_date,
)
```

回到 UI 线程调用 `_apply_simulated_intraday(request_id, trade, payload)`；如果请求序号或当前选中键不一致立即返回。绘制相对开盘价收益线、0% 基准线、首点“买入”和末点“卖出”。等待交易、缺价格、空 DataFrame 或异常时在图内显示明确提示。

- [ ] **Step 4: 运行 GUI 与分时测试**

Run: `pytest tests/test_predict_trend_tab.py tests/test_simulated_buy_service.py -v`

Expected: PASS。

- [ ] **Step 5: 提交分时图**

```bash
git add src/gui/tabs/predict.py tests/test_predict_trend_tab.py
git commit -m "feat: plot simulated trade intraday returns"
```

### Task 6: 回归验证与文档一致性

**Files:**
- Modify only if verification exposes a defect.

- [ ] **Step 1: 运行聚焦测试**

Run: `pytest tests/test_stock_store.py tests/test_simulated_buy_service.py tests/test_predict_trend_tab.py -v`

Expected: PASS。

- [ ] **Step 2: 运行完整测试套件**

Run: `pytest -q`

Expected: 全部测试通过；若存在与本功能无关的既有失败，记录具体测试名和失败输出，不宣称全绿。

- [ ] **Step 3: 做静态与差异检查**

Run: `python -m py_compile stock_store.py src/services/simulated_buy_service.py src/gui/tabs/predict.py`

Expected: 无输出，退出码 0。

Run: `git diff --check`

Expected: 无输出，退出码 0。

- [ ] **Step 4: 检查验收点**

确认：旧预测幂等补建、新预测固定快照、T+1 回填、每日等权复利、历史倒序列表、账户图、按需分时图、不可买和缺数据降级均有对应实现与测试。

- [ ] **Step 5: 提交验证阶段产生的必要修复**

如果 Step 1–4 没有产生修复，不创建空提交；如有修复：

```bash
git add stock_store.py src/services/simulated_buy_service.py src/gui/tabs/predict.py tests/test_stock_store.py tests/test_simulated_buy_service.py tests/test_predict_trend_tab.py
git commit -m "fix: harden simulated buy history curves"
```
