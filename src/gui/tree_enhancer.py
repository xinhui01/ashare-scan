"""Treeview UX 增强：悬停 tooltip + 表头双击自动撑宽列宽。

ttk.Treeview 单元格不支持文字换行（每行高度固定，tkinter 设计限制）。
对长内容场景，这里提供两个非侵入式增强：

1. 鼠标悬停在被截断单元格上 → 浮窗 tooltip 显示完整内容
2. 双击列标题 → 该列自动撑宽到 max(列名, 所有可见行) 的内容宽度（上限 600px）

挂载方式：调用 `attach_enhancers_recursively(root_widget)` 一次性扫描子树，
对所有 `ttk.Treeview` 实例幂等挂载（已挂载的不会重复）。
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk, font as tkfont
from typing import Optional, Tuple


class TreeviewEnhancer:
    """对单个 ttk.Treeview 挂上"截断单元格悬停 + 表头双击自适应"两个 UX 增强。"""

    MAX_AUTO_WIDTH = 600  # 自动撑宽时的列宽上限（像素），防止把整张表拉到屏幕外
    TOOLTIP_BG = "#ffffe0"

    def __init__(self, tree: ttk.Treeview):
        self.tree = tree
        self._tip: Optional[tk.Toplevel] = None
        self._tip_label: Optional[tk.Label] = None
        self._last_cell: Tuple[Optional[str], Optional[str]] = (None, None)

        # 字体测量：复用 Treeview 当前样式字体，宽度估算才精准
        try:
            style = ttk.Style(tree)
            font_name = style.lookup("Treeview", "font") or "TkDefaultFont"
            if font_name in tkfont.names():
                self._font = tkfont.nametofont(font_name)
            else:
                self._font = tkfont.Font(font=font_name)
        except Exception:
            self._font = tkfont.nametofont("TkDefaultFont")

        tree.bind("<Motion>", self._on_motion, add="+")
        tree.bind("<Leave>", self._on_leave, add="+")
        tree.bind("<Double-Button-1>", self._on_double_click, add="+")

    # ---------- 悬停 tooltip ----------
    def _on_motion(self, event) -> None:
        region = self.tree.identify_region(event.x, event.y)
        if region != "cell":
            self._hide_tip()
            self._last_cell = (None, None)
            return
        row = self.tree.identify_row(event.y)
        col = self.tree.identify_column(event.x)
        if not row or not col:
            self._hide_tip()
            return
        if (row, col) == self._last_cell and self._tip is not None:
            # 还在同一格，仅更新位置避免 tooltip 挡鼠标
            try:
                self._tip.geometry(f"+{event.x_root + 16}+{event.y_root + 14}")
            except Exception:
                pass
            return
        self._last_cell = (row, col)

        text = self._read_cell_text(row, col)
        if not text:
            self._hide_tip()
            return

        col_name = self._col_name_from_identifier(col)
        if col_name is None:
            self._hide_tip()
            return
        col_width = 0
        try:
            col_width = int(self.tree.column(col_name, "width") or 0)
        except Exception:
            pass
        text_width = self._font.measure(text)
        # 留 16px 余量给单元格 padding；只有真正被截断才弹 tooltip
        if text_width + 16 <= col_width:
            self._hide_tip()
            return

        self._show_tip(event.x_root, event.y_root, text)

    def _on_leave(self, _event) -> None:
        self._hide_tip()
        self._last_cell = (None, None)

    def _show_tip(self, x_root: int, y_root: int, text: str) -> None:
        if self._tip is None or self._tip_label is None:
            tip = tk.Toplevel(self.tree)
            tip.wm_overrideredirect(True)
            try:
                tip.attributes("-topmost", True)
            except Exception:
                pass
            label = tk.Label(
                tip, text=text, justify="left",
                background=self.TOOLTIP_BG, relief="solid", borderwidth=1,
                font=self._font, wraplength=520, padx=6, pady=3,
            )
            label.pack()
            self._tip = tip
            self._tip_label = label
        else:
            self._tip_label.config(text=text)
        try:
            self._tip.geometry(f"+{x_root + 16}+{y_root + 14}")
            self._tip.deiconify()
        except Exception:
            pass

    def _hide_tip(self) -> None:
        if self._tip is not None:
            try:
                self._tip.withdraw()
            except Exception:
                pass

    # ---------- 表头双击自适应 ----------
    def _on_double_click(self, event):
        # 仅在表头(heading)区域响应；单元格双击留给原有 detail/intraday 打开逻辑
        region = self.tree.identify_region(event.x, event.y)
        if region != "heading":
            return None
        col = self.tree.identify_column(event.x)
        col_name = self._col_name_from_identifier(col) if col else None
        if not col_name:
            return None

        # max(列标题, 所有可见行) 中最宽的内容
        try:
            heading_text = str(self.tree.heading(col_name, "text") or "")
        except Exception:
            heading_text = ""
        max_w = self._font.measure(heading_text) + 24
        for iid in self.tree.get_children(""):
            try:
                val = str(self.tree.set(iid, col_name) or "")
            except Exception:
                continue
            if not val:
                continue
            w = self._font.measure(val) + 24
            if w > max_w:
                max_w = w
                if max_w >= self.MAX_AUTO_WIDTH:
                    max_w = self.MAX_AUTO_WIDTH
                    break
        try:
            self.tree.column(col_name, width=int(max_w))
        except Exception:
            pass
        return "break"

    # ---------- 工具 ----------
    def _col_name_from_identifier(self, col_id: str) -> Optional[str]:
        try:
            idx = int(str(col_id).lstrip("#")) - 1
        except (TypeError, ValueError):
            return None
        cols = self.tree.cget("columns")
        if not cols or idx < 0 or idx >= len(cols):
            return None
        return cols[idx]

    def _read_cell_text(self, row: str, col: str) -> str:
        col_name = self._col_name_from_identifier(col)
        if col_name is None:
            return ""
        try:
            return str(self.tree.set(row, col_name) or "")
        except Exception:
            return ""


def attach_enhancer(tree: ttk.Treeview) -> TreeviewEnhancer:
    """对单个 Treeview 挂上 UX 增强。返回 enhancer 实例。"""
    return TreeviewEnhancer(tree)


def attach_enhancers_recursively(widget) -> int:
    """递归遍历 widget 子树，对所有未挂载过的 Treeview 加上 UX 增强。

    用 `_tv_enhancer_attached` 标记避免重复挂载——后续在弹窗场景重复调用也安全。
    返回本次新增挂载的 Treeview 数量。
    """
    count = 0
    try:
        if isinstance(widget, ttk.Treeview):
            if not getattr(widget, "_tv_enhancer_attached", False):
                attach_enhancer(widget)
                setattr(widget, "_tv_enhancer_attached", True)
                count += 1
    except Exception:
        pass
    try:
        children = list(widget.winfo_children())
    except Exception:
        children = []
    for child in children:
        count += attach_enhancers_recursively(child)
    return count
