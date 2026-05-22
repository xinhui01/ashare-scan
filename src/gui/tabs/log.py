"""运行日志 Tab：最简单的 tab，作为模块化模式的模板。

提供给 LogDrainer 写日志用的 self.text 引用。
顶部工具栏支持「只显示警告/错误」过滤 + 清空；文本区按级别着色（err 红 / warn 橙）。
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
        self.show_only_errors_var = tk.BooleanVar(value=False)
        self._build(notebook)

    def _build(self, notebook: ttk.Notebook) -> None:
        self.frame = ttk.Frame(notebook, padding="5")
        notebook.add(self.frame, text="运行日志")

        toolbar = ttk.Frame(self.frame)
        toolbar.pack(fill=tk.X, pady=(0, 5))
        ttk.Checkbutton(
            toolbar,
            text="只显示警告/错误",
            variable=self.show_only_errors_var,
            command=self._on_filter_toggle,
        ).pack(side=tk.LEFT)
        ttk.Button(toolbar, text="清空", command=self._clear_log).pack(side=tk.RIGHT)

        self.text = scrolledtext.ScrolledText(self.frame, height=30, width=100)
        self.text.pack(fill=tk.BOTH, expand=True)
        # 错误/警告颜色标签
        self.text.tag_configure("err", foreground="#cc0000")
        self.text.tag_configure("warn", foreground="#cc6a00")

    def _on_filter_toggle(self) -> None:
        self.app._rerender_log()

    def _clear_log(self) -> None:
        self.text.delete("1.0", tk.END)
        if hasattr(self.app, "_log_buffer"):
            self.app._log_buffer.clear()
