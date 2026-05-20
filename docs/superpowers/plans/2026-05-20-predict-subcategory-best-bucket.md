# 保留涨停子类别最优分数段显示 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在涨停预测 → 保留涨停候选 sub-tab 顶部，为 5 个连板数子类别（1进2/2进3/3进4/4进5/5进6+）各自显示历史最优分数段，并用颜色编码标记表现强弱。

**Architecture:** 0 后端改动 —— 完全复用 `prediction_accuracy_service.get_score_bucket_rates(category="cont_1to2")` 等已有 API（已支持子类别作为 category 字符串过滤）。前端把 cont sub-tab 顶部子类别带状栏（横向 5 列）重排为 5 行 × 4 列的 grid 表格，新增"最优分数段"列；worker 线程多 5 次 DB 查询取数。

**Tech Stack:** Python 3.12 + Tkinter (ttk.Frame + grid 布局) + SQLite 后端。

**Spec:** [`docs/superpowers/specs/2026-05-20-predict-subcategory-best-bucket-design.md`](../specs/2026-05-20-predict-subcategory-best-bucket-design.md)

---

## Task 0 — 起跑基线

### Step 1: 工作树干净

Run: `git -C D:\code\python\gupiao status`
Expected: `nothing to commit, working tree clean`

### Step 2: pytest baseline

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `274 passed, 20 subtests passed`

### Step 3: 静态 import baseline

Run: `.venv\Scripts\python -c "import stock_gui; print('OK')"`
Expected: `OK`

---

## Task 1 — Worker 追加 5 个子类别 bucket_rates 查询

**Files:**
- Modify: `stock_gui.py` (`_refresh_predict_accuracy_async` 中的 worker 循环)

- [ ] **Step 1: 定位 worker 循环**

Run: `grep -n "_refresh_predict_accuracy_async\|bucket_rates_by_cat" stock_gui.py`

应找到约 3644 行的 `def _refresh_predict_accuracy_async` 和约 3673 行的 `for cat in ("cont", "first", "fresh", "wrap", "trend"):` 循环。

- [ ] **Step 2: 把循环里的类别元组扩展为 10 个**

打开 stock_gui.py，找到 `_refresh_predict_accuracy_async` 里的循环。修改前：

```python
for cat in ("cont", "first", "fresh", "wrap", "trend"):
    try:
        bucket_rates_by_cat[cat] = prediction_accuracy_service.get_score_bucket_rates(
            category=cat, lookback_dates=20, min_samples=5,
        )
    except Exception:
        bucket_rates_by_cat[cat] = {}
```

改为：

```python
for cat in (
    "cont", "first", "fresh", "wrap", "trend",
    "cont_1to2", "cont_2to3", "cont_3to4", "cont_4to5", "cont_5plus",
):
    try:
        bucket_rates_by_cat[cat] = prediction_accuracy_service.get_score_bucket_rates(
            category=cat, lookback_dates=20, min_samples=5,
        )
    except Exception:
        bucket_rates_by_cat[cat] = {}
```

- [ ] **Step 3: 验证 import 还能跑**

Run: `.venv\Scripts\python -c "import stock_gui; print('OK')"`
Expected: `OK`

- [ ] **Step 4: 跑 pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `274 passed`

---

## Task 2 — cont sub-tab 顶部布局重排（横向条 → grid 表格）

**Files:**
- Modify: `stock_gui.py` (`setup_predict_tab` 内 cont sub-tab 子类别区域，约 1462-1469 行)

- [ ] **Step 1: 定位当前子类别条**

Run: `grep -n "cont_sub_frame\|cont_1to2.*cont_2to3\|sub_keys.*=" stock_gui.py`

应找到约 1462-1469 行：

```python
# 1进2 / 2进3 / 3进4 / 4进5 / 5进6+ 子类别命中率（独立统计，不影响主类别）
cont_sub_frame = ttk.Frame(cont_tab)
cont_sub_frame.pack(side=tk.TOP, fill=tk.X)
for sub_key in ("cont_1to2", "cont_2to3", "cont_3to4", "cont_4to5", "cont_5plus"):
    sub_lbl = ttk.Label(cont_sub_frame, text="-", foreground="#666",
                        anchor=tk.W, padding=(12, 1))
    sub_lbl.pack(side=tk.LEFT, fill=tk.X, expand=True)
    self._predict_stat_labels[sub_key] = sub_lbl
```

- [ ] **Step 2: 替换为 grid 5×4 布局**

把上面整段（含注释）替换为：

```python
# 1进2 / 2进3 / 3进4 / 4进5 / 5进6+ 子类别命中率（独立统计，不影响主类别）
# 5 行 × 4 列 grid：子类别名 / 昨日 / 近20d / 最优分数段
cont_sub_frame = ttk.Frame(cont_tab)
cont_sub_frame.pack(side=tk.TOP, fill=tk.X, padx=4, pady=(2, 4))
# 4 列均匀拉伸
for col_idx in range(4):
    cont_sub_frame.columnconfigure(col_idx, weight=1, uniform="cont_sub")

# 表头
header_font = ("", 9)  # 比默认小一点
ttk.Label(cont_sub_frame, text="", font=header_font).grid(
    row=0, column=0, sticky="w", padx=(8, 0))
ttk.Label(cont_sub_frame, text="昨日", foreground="#888",
          font=header_font, anchor=tk.W).grid(row=0, column=1, sticky="w")
ttk.Label(cont_sub_frame, text="近20d", foreground="#888",
          font=header_font, anchor=tk.W).grid(row=0, column=2, sticky="w")
ttk.Label(cont_sub_frame, text="最优分数段", foreground="#888",
          font=header_font, anchor=tk.W).grid(row=0, column=3, sticky="w")

# 子类别名 → 显示文案
_SUB_DISPLAY = {
    "cont_1to2": "1进2", "cont_2to3": "2进3", "cont_3to4": "3进4",
    "cont_4to5": "4进5", "cont_5plus": "5进6+",
}
# 用于刷新最优段 Label 的字典；Label 创建后 configure(text=..., foreground=...)
self._predict_subcategory_best_labels: Dict[str, ttk.Label] = {}
# 拆开"昨日"和"近20d"两列，原来的 self._predict_stat_labels[sub_key] 只放一个
# Label 不够，现在改放 (yest_label, recent_label) 元组
self._predict_subcategory_stat_labels: Dict[str, Tuple[ttk.Label, ttk.Label]] = {}

for row_idx, sub_key in enumerate(
    ("cont_1to2", "cont_2to3", "cont_3to4", "cont_4to5", "cont_5plus"), start=1,
):
    name = _SUB_DISPLAY[sub_key]
    ttk.Label(cont_sub_frame, text=name, foreground="#444",
              anchor=tk.W, padding=(8, 1)).grid(
        row=row_idx, column=0, sticky="w")
    yest_lbl = ttk.Label(cont_sub_frame, text="-", foreground="#444",
                         anchor=tk.W, padding=(0, 1))
    yest_lbl.grid(row=row_idx, column=1, sticky="w")
    recent_lbl = ttk.Label(cont_sub_frame, text="-", foreground="#444",
                           anchor=tk.W, padding=(0, 1))
    recent_lbl.grid(row=row_idx, column=2, sticky="w")
    best_lbl = ttk.Label(cont_sub_frame, text="-", foreground="#888",
                         anchor=tk.W, padding=(0, 1))
    best_lbl.grid(row=row_idx, column=3, sticky="w")
    self._predict_subcategory_stat_labels[sub_key] = (yest_lbl, recent_lbl)
    self._predict_subcategory_best_labels[sub_key] = best_lbl
    # 兼容旧 _predict_stat_labels：让 sub_key 指向 recent_lbl（"近20d"列），
    # 这样 _apply_predict_accuracy 现有 sub_key 分支的 fallback 仍能工作
    self._predict_stat_labels[sub_key] = recent_lbl
```

- [ ] **Step 3: 验证 import + grid 渲染**

Run: `.venv\Scripts\python -c "import stock_gui; print('OK')"`
Expected: `OK`

GUI 静态启动测试：
```powershell
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 5
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```
Expected: `OK`

- [ ] **Step 4: 跑 pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `274 passed`

---

## Task 3 — `_apply_predict_accuracy` 中子类别渲染拆分

**Files:**
- Modify: `stock_gui.py` (`_apply_predict_accuracy` 子类别分支，约 3738-3746 行)

- [ ] **Step 1: 定位当前子类别渲染分支**

Run: `grep -n "if cat in sub_keys" stock_gui.py`

应找到约 3738 行：

```python
if cat in sub_keys:
    # 子类别紧凑格式：1进2 · 昨 60.0% (3/5) | 近20d 42.1% (8/19)
    if buyable <= 0:
        txt = f"{name}: 昨{y_str} | 近-"
    else:
        txt = (
            f"{name}: 昨{y_str} | "
            f"近{dates}d {rate:.1f}% ({hit}/{buyable})"
        )
```

- [ ] **Step 2: 把字符串拼接改成写到两个独立 Label**

找到 `_apply_predict_accuracy` 里的 `for cat, lbl in labels.items():` 循环。注意：现在 `labels` 还会迭代到 sub_key（因为 Step 2 仍把 sub_key 指向 recent_lbl），所以我们要把"sub_key 走单独 grid 渲染、不走 lbl 路径"分离出来。

把 `if cat in sub_keys:` 分支整段替换为：

```python
if cat in sub_keys:
    # 子类别走 grid 表格，由独立 (yest_lbl, recent_lbl) 渲染
    yest_lbl, recent_lbl = self._predict_subcategory_stat_labels.get(
        cat, (None, None)
    )
    if yest_lbl is not None:
        try:
            yest_lbl.configure(text=y_str)
        except Exception:
            pass
    if recent_lbl is not None:
        try:
            if buyable <= 0:
                recent_lbl.configure(text="-")
            else:
                recent_lbl.configure(text=f"{rate:.1f}% ({hit}/{buyable})")
        except Exception:
            pass
    # 继续循环；不要走到 lbl.configure 的旧路径
    continue
elif buyable <= 0:
    txt = (
        f"{name} · 昨日命中率 {y_str} · 历史近{dates}日: "
        f"-（暂无回填数据）"
    )
else:
    txt = (
        f"{name} · 昨日命中率 {y_str} · "
        f"近{dates}日 {rate:.1f}% ({hit}/{buyable})  "
        f"平均次日涨幅 {avg_pct:+.2f}%"
    )
```

注意：原代码 `if cat in sub_keys: ... elif buyable <= 0: ... else: ...` 是一个 if/elif/else 三分支。改造后 `if cat in sub_keys` 内 `continue`，剩下 `elif/else` 走主类别分支。逻辑上等价于"子类别走 continue 跳过 lbl.configure"。

- [ ] **Step 3: 验证 import + GUI 启动**

Run: `.venv\Scripts\python -c "import stock_gui; print('OK')"`
Expected: `OK`

GUI 启动测试：见 Task 2 Step 3。
Expected: `OK`

- [ ] **Step 4: 跑 pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `274 passed`

---

## Task 4 — 新增 `_refresh_predict_subcategory_best_buckets` + 颜色编码

**Files:**
- Modify: `stock_gui.py`

- [ ] **Step 1: 定位 `_refresh_predict_best_bucket_labels`**

Run: `grep -n "def _refresh_predict_best_bucket_labels" stock_gui.py`

约 3781 行。读它的实现（约 3781-3830）了解现有"找最优 eligible 桶"的逻辑。

- [ ] **Step 2: 提炼出 `_find_best_bucket_for_category` helper**

如果 `_refresh_predict_best_bucket_labels` 里"找最优桶"逻辑还没有提炼成独立函数，可以新增一个 helper（或直接复用 inline 逻辑）。

在 `_refresh_predict_best_bucket_labels` **之后**添加：

```python
def _find_best_bucket_for_category(
    self, cat: str,
) -> Optional[Tuple[Tuple[int, int], Dict[str, Any]]]:
    """从 self._predict_bucket_rates_cache 取 cat 的所有 bucket rates，
    返回 eligible=True 中 rate 最大的桶，None 表示无 eligible 桶。
    同 rate 时取分数段更高的（高分往往更稳）。
    """
    rates = self._predict_bucket_rates_cache.get(cat) or {}
    best: Optional[Tuple[Tuple[int, int], Dict[str, Any]]] = None
    for (lo, hi), info in rates.items():
        if not info.get("eligible"):
            continue
        if best is None:
            best = ((lo, hi), info)
            continue
        b_rate = best[1].get("rate", 0)
        cur_rate = info.get("rate", 0)
        if cur_rate > b_rate or (cur_rate == b_rate and lo > best[0][0]):
            best = ((lo, hi), info)
    return best

def _refresh_predict_subcategory_best_buckets(self) -> None:
    """刷新 5 个 cont 子类别（cont_1to2/.../cont_5plus）顶部的"最优分数段"Label。
    使用 self._predict_bucket_rates_cache 里 worker 预取的 rates。
    按 rate 着色：≥40 绿，25-40 黄，<25 红，无数据/不足灰。
    """
    labels = getattr(self, "_predict_subcategory_best_labels", {}) or {}
    if not labels:
        return
    for cat, lbl in labels.items():
        best = self._find_best_bucket_for_category(cat)
        if best is None:
            try:
                lbl.configure(text="-（样本不足）", foreground="#888")
            except Exception:
                pass
            continue
        (lo, hi), info = best
        rate = float(info.get("rate") or 0.0)
        hit = int(info.get("hit") or 0)
        buyable = int(info.get("buyable") or 0)
        if rate >= 40.0:
            fg = "#1b5e20"  # 绿
        elif rate >= 25.0:
            fg = "#9c7a00"  # 黄
        else:
            fg = "#c62828"  # 红
        txt = f"{lo}-{hi}: {rate:.0f}% ({hit}/{buyable})"
        try:
            lbl.configure(text=txt, foreground=fg)
        except Exception:
            pass
```

- [ ] **Step 3: 在 `_apply_predict_accuracy` 末尾追加调用**

Run: `grep -n "_refresh_predict_best_bucket_labels()" stock_gui.py`

约 3764 行：

```python
# 计算每个主类别的"历史最优分数段"并刷新黄色提示标签
self._refresh_predict_best_bucket_labels()
```

紧跟其后追加：

```python
# 计算 5 个 cont 子类别各自的"最优分数段"并刷新（含颜色编码）
self._refresh_predict_subcategory_best_buckets()
```

- [ ] **Step 4: 验证 + pytest + GUI 启动**

Run: `.venv\Scripts\python -c "import stock_gui; print('OK')"`
Expected: `OK`

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `274 passed`

GUI 启动：
```powershell
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 6
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```
Expected: `OK`

---

## Task 5 — grep 验证 + 提交 + push

- [ ] **Step 1: grep 验证 5 个子类别"最优段"Label 真的被创建**

Run: `grep -n "_predict_subcategory_best_labels\|_predict_subcategory_stat_labels\|_refresh_predict_subcategory_best_buckets\|_find_best_bucket_for_category" stock_gui.py`

应有 5-8 处命中（属性赋值 + 调用点 + 方法定义）。

- [ ] **Step 2: grep 确认旧的"横向 5 列"代码已清干净**

Run: `grep -n "for sub_key in (\"cont_1to2\"" stock_gui.py`

应仅剩 1 处（Task 2 替换后的 `for row_idx, sub_key in enumerate(...)`，不再有旧的 `for sub_key in ...).pack(side=tk.LEFT,...)` 模式）。

- [ ] **Step 3: 终极 pytest**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `274 passed`

- [ ] **Step 4: git diff 检查**

Run: `git -C D:\code\python\gupiao diff --stat`

预期：仅 `stock_gui.py` 改动；估算 ~170 行（+150 / -20 上下）。

- [ ] **Step 5: commit**

```powershell
git -C D:\code\python\gupiao add stock_gui.py
git -C D:\code\python\gupiao commit -m @'
新功能：保留涨停 5 个子类别新增最优分数段显示

涨停预测 → 保留涨停 sub-tab 顶部新增 5 行 × 4 列 grid 表格，
为 1进2/2进3/3进4/4进5/5进6+ 各自展示昨日命中率、近20日命中率、
历史最优分数段（含 (h/b) 分子分母），最优段按命中率上色：
≥40% 绿、25-40% 黄、<25% 红、样本不足灰。

0 后端改动：复用 prediction_accuracy_service.get_score_bucket_rates
对子类别字符串作为 category 过滤的能力，worker 线程多 5 次 DB 查询。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

- [ ] **Step 6: push**

向用户确认后再 push：

> Phase 已完成，可以 push 吗？

得到确认后：
```powershell
git -C D:\code\python\gupiao push origin main
```

---

## 自检表

| 检查 | 状态 |
|---|---|
| spec 覆盖：每个文件 + 验收 + 错误处理 → 任务 1-5 都覆盖到 | ✅ |
| 颜色阈值 40/25 落实到 Task 4 `_refresh_predict_subcategory_best_buckets` | ✅ |
| 同 rate 取高分段 tiebreaker 落实到 `_find_best_bucket_for_category` | ✅ |
| 样本不足 fallback "-（样本不足）" 灰色 落实 | ✅ |
| 每个 task 后都跑 pytest | ✅ |
| GUI 静态启动测试 每个 task 后跑 | ✅（Task 2/3/4） |
| 无占位符 / TBD / "类似 Task N" | ✅ |
| commit message 风格与项目历史一致（中文 + Co-Authored-By） | ✅ |
