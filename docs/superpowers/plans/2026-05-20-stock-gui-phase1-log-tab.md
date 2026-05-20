# Phase 1: 建立 app.py + facade + 抽 log tab 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建立 stock_gui.py 模块化拆分的脚手架：把 `StockMonitorApp` 主类从 `stock_gui.py` 移到 `src/gui/app.py`，stock_gui.py 退化成 facade；同时抽出最简单的 log tab 到 `src/gui/tabs/log.py` 作为后续 phase 的 Tab 类模板。

**Architecture:** 2 个 commit。Commit 1: 纯文件移动（class move across files），stock_gui.py 改成 `from src.gui.app import StockMonitorApp` facade。Commit 2: 抽 LogTab 类到 `src/gui/tabs/log.py`，主类改成实例化 LogTab，原 `setup_log_tab` / `self.log_text` / `self.log_tab` 改写。

**Tech Stack:** Python 3.12 + Tkinter

**Spec:** [`docs/superpowers/specs/2026-05-20-stock-gui-modularization-design.md`](../specs/2026-05-20-stock-gui-modularization-design.md)

**注意（spec 用词修正）：** spec 里把主类写成 `StockGui`，但代码里实际名字是 **`StockMonitorApp`**（见 `stock_gui.py:69`）。实施时使用 `StockMonitorApp`，不要重命名。

---

## Task 0 — 起跑基线

- [ ] **Step 1: 工作树干净**

Run: `git -C D:\code\python\gupiao status`
Expected: `nothing to commit, working tree clean`

- [ ] **Step 2: pytest baseline**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `274 passed, 20 subtests passed`

- [ ] **Step 3: 静态 import baseline**

Run: `.venv\Scripts\python -c "import stock_gui; from stock_gui import StockMonitorApp; print('OK')"`
Expected: `OK`

- [ ] **Step 4: 记录基线行数**

Run: `(Get-Content D:\code\python\gupiao\stock_gui.py | Measure-Object -Line).Lines`
Expected: ~6630 lines

---

## Task 1 — 把 StockMonitorApp 主类从 stock_gui.py 移到 src/gui/app.py

**Files:**
- Create: `D:\code\python\gupiao\src\gui\app.py`
- Modify: `D:\code\python\gupiao\stock_gui.py`（退化为 facade）

> 这一步是**纯结构移动**，行为不应有任何变化。pytest / GUI / import 全程必须保持现状。

- [ ] **Step 1: 创建 src/gui/app.py，把 stock_gui.py 整体内容搬过来**

操作：
1. 读 `stock_gui.py` 完整内容（用 Read，注意 6630 行可能要分批，但 Write 是一次性写）
2. 创建新文件 `src/gui/app.py`，内容**完全复制** stock_gui.py（保留所有 import、所有 module-level 常量、整个 `StockMonitorApp` 类）

注意：
- module-level 的 `plt.rcParams[...]` 等副作用代码也要搬过来
- 不要在 app.py 里改任何 `from xxx import` 路径（保持原样）
- 不要修改类的任何方法

- [ ] **Step 2: 改写 stock_gui.py 成 facade**

把 `stock_gui.py` 完整内容替换为：

```python
"""facade：StockMonitorApp 主类已迁移到 src.gui.app；本文件仅保留向后兼容入口。

外部老调用方（如 main.py、ad-hoc 脚本）通常这样用：
    from stock_gui import StockMonitorApp
本文件转发到 src.gui.app，让旧调用方无需修改。
"""
from src.gui.app import StockMonitorApp

__all__ = ["StockMonitorApp"]
```

- [ ] **Step 3: 静态 import 验证**

Run: `cd /d D:\code\python\gupiao && .venv\Scripts\python -c "from stock_gui import StockMonitorApp; from src.gui.app import StockMonitorApp as A2; assert StockMonitorApp is A2; print('facade OK')"`
Expected: `facade OK`

- [ ] **Step 4: pytest**

Run: `.venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `274 passed`

- [ ] **Step 5: GUI 静态启动**

```powershell
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 6
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```
Expected: `OK`（exit code 1 from Stop-Process 是正常副作用）

- [ ] **Step 6: 提交 Commit 1**

```powershell
git -C D:\code\python\gupiao add stock_gui.py src/gui/app.py
git -C D:\code\python\gupiao commit -m @'
重构（P1.1）：StockMonitorApp 主类移到 src/gui/app.py，stock_gui.py 退化为 facade

按 docs/superpowers/specs/2026-05-20-stock-gui-modularization-design.md 第 1 阶段第 1 步，
把整个 StockMonitorApp 类（+ 所有 module-level imports / plt.rcParams 配置）
从 stock_gui.py 整体搬到 src/gui/app.py。stock_gui.py 改成 1 行 facade：
from src.gui.app import StockMonitorApp。

行为完全不变：pytest 274 passed、GUI 启动测试通过、main.py 不需要任何改动。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

Expected: 提交成功，diff stat 显示 stock_gui.py 删除大量行 + src/gui/app.py 新增等量行。

---

## Task 2 — 抽 LogTab 到 src/gui/tabs/log.py

**Files:**
- Create: `D:\code\python\gupiao\src\gui\tabs\__init__.py`
- Create: `D:\code\python\gupiao\src\gui\tabs\log.py`
- Modify: `D:\code\python\gupiao\src\gui\app.py`

> Log tab 是最简单的 tab（仅 7 行 widget 代码 + 2 处外部读写）。作为 Tab 类模板的首次落地。

- [ ] **Step 1: 创建 src/gui/tabs/__init__.py**

写入空文件（或仅一个模块 docstring）：

```python
"""按 tab 组织的 GUI 模块。

每个 tab 类持有 app 引用（self.app），own 自己的 widget 与 tab 私有状态；
跨 tab 引用走显式 self.app.xxx。详见 docs/superpowers/specs/2026-05-20-stock-gui-modularization-design.md。
"""
```

- [ ] **Step 2: 创建 src/gui/tabs/log.py 含 LogTab 类**

写入：

```python
"""运行日志 Tab：最简单的 tab，作为模块化模式的模板。

提供给 LogDrainer 写日志用的 self.text 引用。
"""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk, scrolledtext
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.gui.app import StockMonitorApp


class LogTab:
    """运行日志 tab。仅一个 ScrolledText，由 LogDrainer 异步写入。"""

    def __init__(self, app: "StockMonitorApp", notebook: ttk.Notebook) -> None:
        self.app = app
        self._build(notebook)

    def _build(self, notebook: ttk.Notebook) -> None:
        self.frame = ttk.Frame(notebook, padding="5")
        notebook.add(self.frame, text="运行日志")
        self.text = scrolledtext.ScrolledText(self.frame, height=30, width=100)
        self.text.pack(fill=tk.BOTH, expand=True)
```

- [ ] **Step 3: 修改 src/gui/app.py 使用 LogTab**

在 app.py 中做 3 处改动：

**改动 a — 顶部新增 import**

在 `from src.gui.ui_dispatch import UIDispatcher` 后面加：
```python
from src.gui.tabs.log import LogTab
```

**改动 b — 删除 setup_log_tab 方法**

Run: `grep -n "def setup_log_tab" src/gui/app.py`

找到 `def setup_log_tab(self):` 方法（约 4420 行原行号，但移到 app.py 后行号可能略变；用 grep 实时定位）。**完整删除整个方法体**（7 行）：

```python
def setup_log_tab(self):
    log_frame = ttk.Frame(self.notebook, padding="5")
    self.notebook.add(log_frame, text="运行日志")
    self.log_tab = log_frame

    self.log_text = scrolledtext.ScrolledText(log_frame, height=30, width=100)
    self.log_text.pack(fill=tk.BOTH, expand=True)
```

**改动 c — 在 setup_notebook 末尾加 LogTab 实例化**

Run: `grep -n "self.setup_log_tab\(\)" src/gui/app.py`

找到 `setup_notebook` 内调用 `self.setup_log_tab()` 的那一行（约 513 行原行号）。把它替换为：

```python
self.log = LogTab(self, self.notebook)
```

**改动 d — 更新 _tab_registry 中的 log 条目**

Run: `grep -n '"log", self.log_tab' src/gui/app.py`

找到约 561 行的：
```python
("log", self.log_tab, "运行日志", False),
```

改为：
```python
("log", self.log.frame, "运行日志", False),
```

**改动 e — 更新 log_text 引用（写日志的两处）**

Run: `grep -n "self\.log_text" src/gui/app.py`

应找到 2 处（约 4471 / 4472 行原行号）：
```python
self.log_text.insert(tk.END, line)
self.log_text.see(tk.END)
```

改为：
```python
self.log.text.insert(tk.END, line)
self.log.text.see(tk.END)
```

- [ ] **Step 4: grep 确认 log_text / log_tab 已无残留引用**

Run: `grep -rn "self\.log_text\|self\.log_tab" src/gui/app.py stock_gui.py src/`
Expected: **0 matches**（log_text / log_tab 应全部改成 log.text / log.frame）

如果有命中，回 Step 3 修。

- [ ] **Step 5: 静态 import 验证**

Run: `.venv\Scripts\python -c "from stock_gui import StockMonitorApp; from src.gui.tabs.log import LogTab; print('OK')"`
Expected: `OK`

- [ ] **Step 6: pytest**

Run: `.venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `274 passed`

- [ ] **Step 7: GUI 静态启动**

```powershell
$proc = Start-Process -FilePath ".venv\Scripts\python.exe" -ArgumentList "main.py" -PassThru -NoNewWindow -RedirectStandardError "stderr.log"
Start-Sleep -Seconds 6
if ($proc.HasExited) { Write-Output "FAIL"; Get-Content stderr.log } else { Stop-Process $proc; Write-Output "OK" }
Remove-Item stderr.log -ErrorAction SilentlyContinue
```
Expected: `OK`

- [ ] **Step 8: 提交 Commit 2**

```powershell
git -C D:\code\python\gupiao add src/gui/tabs/__init__.py src/gui/tabs/log.py src/gui/app.py
git -C D:\code\python\gupiao commit -m @'
重构（P1.2）：抽 LogTab 类到 src/gui/tabs/log.py，建立 Tab 类模板

按 docs/superpowers/specs/2026-05-20-stock-gui-modularization-design.md 第 1 阶段第 2 步，
建立 src/gui/tabs/ 子目录与 Tab 类约定模板。LogTab 持有 app 引用，own 自己的
frame/text widget。主类 StockMonitorApp 改为实例化 LogTab；原 self.log_tab /
self.log_text 引用全部改为 self.log.frame / self.log.text。

这套模板（持有 app 引用 + own widget + _build 构造 + 跨 tab 走 self.app.xxx）
是后续 P2-P5 各 tab 抽离的参考样式。

行为完全不变：pytest 274 passed、GUI 启动测试通过。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
'@
```

Expected: 提交成功。

---

## Task 3 — Phase 1 最终验证

- [ ] **Step 1: git log 看两个 commit**

Run: `git -C D:\code\python\gupiao log --oneline -3`
Expected:
```
<sha2> 重构（P1.2）：抽 LogTab 类到 src/gui/tabs/log.py，建立 Tab 类模板
<sha1> 重构（P1.1）：StockMonitorApp 主类移到 src/gui/app.py，stock_gui.py 退化为 facade
f90fd15 docs(spec): stock_gui.py 模块化拆分设计
```

- [ ] **Step 2: 文件结构验证**

Run: `Get-ChildItem D:\code\python\gupiao\src\gui -Recurse -File | Where-Object { $_.Extension -eq '.py' } | Format-Table FullName, Length -AutoSize`

预期看到：
- `src\gui\app.py` 应该是新文件（大小约等于 stock_gui.py 原大小，~340KB）
- `src\gui\tabs\__init__.py` 应该是新文件（很小）
- `src\gui\tabs\log.py` 应该是新文件（约 30 行）

- [ ] **Step 3: stock_gui.py 体积**

Run: `(Get-Content D:\code\python\gupiao\stock_gui.py | Measure-Object -Line).Lines`
Expected: < 20（应退化为 facade，仅几行）

- [ ] **Step 4: 最终 pytest**

Run: `.venv\Scripts\python -m pytest -q --tb=no 2>&1 | Select-Object -Last 3`
Expected: `274 passed`

- [ ] **Step 5: 全模块 import 检查**

Run: `.venv\Scripts\python -c "import stock_gui; from stock_gui import StockMonitorApp; from src.gui.app import StockMonitorApp as A2; from src.gui.tabs.log import LogTab; assert StockMonitorApp is A2; print('all OK')"`
Expected: `all OK`

- [ ] **Step 6: 报告给用户**

向用户报告：
- Phase 1 已完成（2 个 commit）
- stock_gui.py 行数 6630 → ~10（仅 facade）
- src/gui/app.py 行数 ~6630 → ~6630（主类全搬过来，少了 log tab 7 行）
- src/gui/tabs/log.py 新增 ~30 行
- pytest 274 passed、GUI 启动 OK
- 等用户确认是否 push

---

## 自检表

| 检查 | 状态 |
|---|---|
| spec 覆盖：P1 = log tab + app.py + facade 转发 → Task 1+2 覆盖 | ✅ |
| 使用 StockMonitorApp（实际类名）而非 spec 的 StockGui 占位名 | ✅ 已明确说明 |
| 跨模块依赖 grep 验证 main.py 唯一外部引用 → 兼容性方案设计正确（facade re-export） | ✅ |
| 无占位符 / TBD | ✅ |
| 每个 commit 后跑 pytest + import + GUI smoke | ✅ |
| commit message 中文 + 与项目历史风格一致 | ✅ |
| 不开 feature branch，直接 main | ✅ |
| log_text / log_tab 改名清单完整：定义 + _tab_registry + 写日志的 2 处 = 共 4-5 处 | ✅ |
