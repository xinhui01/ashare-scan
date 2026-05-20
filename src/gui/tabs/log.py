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
