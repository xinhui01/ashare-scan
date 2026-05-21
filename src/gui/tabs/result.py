"""扫描结果 Tab：股票扫描 + 结果表 + 过滤/排序/导出。

包含：
- ttk.Frame 容器（self.frame）
- 扫描专属面板（_build_scan_actions_row / _build_scan_params_row / _build_scan_flow_row）
- 结果表 Treeview（self.tree）
- 客户端快速过滤栏（_build_quick_filter_row，在 _build 内调）
- 列定义/排序/列管理（columns / headings / column_vars / column_order）

状态：
- self.all_results / self.filtered_stocks
- self.scan_thread / self.scan_cancel_token（扫描线程及取消令牌）
- 排序字段 self.sort_column / self.sort_reverse 仍留在 App（被多 tab 共用）

跨 tab 引用走 self.app.xxx：
- self.app.notebook / status_var / stock_filter / progress_var / progress_text_var
- self.app._ui / _post_to_ui / _log_async / _log
- self.app.detail.show(...) / detail._schedule_show / detail._cancel_scheduled
- 价格/快速过滤 var / 板块过滤 var 留 App（多 tab 共用偏好）
- self.app.is_scanning / is_updating_cache（被 update_history_cache 等共用）
- self.app._selected_boards / _infer_board_from_code / _refresh_predict_display_if_ready
"""
from __future__ import annotations

import csv
import json
import threading
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import tkinter as tk
from tkinter import ttk, messagebox, filedialog

import matplotlib.pyplot as plt
from matplotlib.figure import Figure

from scan_models import FilterSettings, ScanRequest
from src.gui import result_filters
from src.gui.result_columns import (
    RESULT_COLUMNS,
    all_column_ids,
    columns_by_id,
    default_visible_ids,
    desc_by_default_ids,
)
from src.utils.cancel_token import CancelToken
from stock_filter import StockFilter
from stock_store import (
    load_latest_scan_snapshot,
    load_scan_snapshot,
    save_app_config,
    save_scan_snapshot,
)

if TYPE_CHECKING:
    from src.gui.app import StockMonitorApp


class ResultTab:
    """扫描结果 tab：扫描 + 结果表 + 过滤/排序/导出/列管理。"""

    def __init__(self, app: "StockMonitorApp", notebook: ttk.Notebook) -> None:
        self.app = app
        # 状态字段（原 self.all_scan_results / filtered_stocks / scan/cache 线程及 token）
        self.all_results: List[Dict[str, Any]] = []
        self.filtered_stocks: List[Dict[str, Any]] = []
        self.scan_thread: Optional[threading.Thread] = None
        self.scan_cancel_token: Optional[CancelToken] = None
        # 结果表列定义
        self.columns: tuple[str, ...] = ()
        self.headings: Dict[str, tuple[str, int]] = {}
        self.column_vars: Dict[str, tk.BooleanVar] = {}
        self.column_order: List[str] = []
        self.default_display_columns: tuple[str, ...] = ()
        self.columns_map: Dict[str, Any] = {}
        self._build(notebook)

    def _build(self, notebook: ttk.Notebook) -> None:
        """构建 widget。从原 setup_result_tab 整体迁移。"""
        result_frame = ttk.Frame(notebook, padding="5")
        notebook.add(result_frame, text="扫描结果")
        self.frame = result_frame

        # ---- 扫描专属面板（从顶部控制面板迁过来）----
        # 扫描动作（开始扫描按钮）
        self._build_scan_actions_row(result_frame)
        # 扫描参数（连续天数/MA周期/近N日涨停/放量观察天数/启用放量倍数/放量倍数阈值 + 备注）
        self._build_scan_params_row(result_frame)
        # 扫描流程开关（仅显示近N日有涨停 / 忽略本地结果快照）
        self._build_scan_flow_row(result_frame)

        action_frame = ttk.Frame(result_frame)
        action_frame.pack(fill=tk.X, pady=(0, 6))
        ttk.Button(action_frame, text="导出结果图片", command=self.export_results_image).pack(side=tk.LEFT)
        ttk.Button(action_frame, text="复制代码", command=self.copy_selected_code_name).pack(side=tk.LEFT, padx=8)
        ttk.Label(
            action_frame,
            text="导出图片固定仅包含代码和名称两列，按 Ctrl+C 可复制选中股票代码。",
        ).pack(side=tk.RIGHT)

        # 客户端快速过滤栏（搜索/评分/5日涨幅/放量/连板 + 只显示涨停/承接强势/...）
        # 仅对扫描结果表生效，因此放在本 tab 内部。
        # 全局过滤 var（min_score / search / only_xxx 等）仍由 App 拥有以便多 tab 共用，
        # 控件构建/事件绑定走 App 的 helper（command 已改为指向 self.result.xxx）
        self.app._build_control_quick_filter_row(result_frame)

        # 列定义全部来自 src/gui/result_columns.py 的注册表
        self.columns_map = columns_by_id()
        self.columns = all_column_ids()
        self.headings = {
            col.id: (col.label, col.width) for col in RESULT_COLUMNS
        }
        default_visible_columns = default_visible_ids()

        tree_container = ttk.Frame(result_frame)
        tree_container.pack(fill=tk.BOTH, expand=True)
        tree_container.grid_rowconfigure(0, weight=1)
        tree_container.grid_columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(tree_container, columns=self.columns, show="headings", height=20)

        self.default_display_columns = default_visible_columns
        self.column_order = list(self.columns)
        self.column_vars = {
            col: tk.BooleanVar(value=(col in default_visible_columns))
            for col in self.columns
        }
        for col_def in RESULT_COLUMNS:
            self.tree.heading(
                col_def.id,
                text=col_def.label,
                command=lambda c=col_def.id: self.on_heading_click(c),
            )
            anchor = tk.W if col_def.anchor == "w" else tk.CENTER
            self.tree.column(col_def.id, width=col_def.width, anchor=anchor)
        self.tree.configure(displaycolumns=default_visible_columns)

        scrollbar = ttk.Scrollbar(tree_container, orient=tk.VERTICAL, command=self.tree.yview)
        xscrollbar = ttk.Scrollbar(tree_container, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.configure(xscrollcommand=xscrollbar.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        xscrollbar.grid(row=1, column=0, sticky="ew")

        self.tree.bind("<<TreeviewSelect>>", self.on_stock_select)
        self.tree.bind("<Double-1>", self.on_stock_double_click)
        self.tree.bind("<Control-c>", self.copy_selected_code_name)
        self.tree.bind("<Control-C>", self.copy_selected_code_name)

    # ======================== 扫描专属面板（顶部行）========================

    def _build_scan_actions_row(self, parent) -> None:
        """扫描专属动作：'开始扫描' 按钮，渲染到扫描结果 tab 内部。"""
        row = ttk.Frame(parent)
        row.pack(fill=tk.X, pady=4)
        self.app.scan_btn = ttk.Button(row, text="开始扫描", command=self.start_scan)
        self.app.scan_btn.pack(side=tk.LEFT, padx=5)

    def _build_scan_params_row(self, parent) -> None:
        """扫描专属参数：仅 _build 调用，渲染到扫描结果 tab 内部。"""
        row = ttk.Frame(parent)
        row.pack(fill=tk.X, pady=4)

        ttk.Label(row, text="连续天数:").pack(side=tk.LEFT, padx=5)
        ttk.Entry(row, textvariable=self.app.trend_days_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Label(row, text="MA周期:").pack(side=tk.LEFT, padx=5)
        ttk.Entry(row, textvariable=self.app.ma_period_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Label(row, text="近N日涨停:").pack(side=tk.LEFT, padx=5)
        ttk.Entry(row, textvariable=self.app.limit_up_lookback_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Label(row, text="放量观察天数:").pack(side=tk.LEFT, padx=5)
        ttk.Entry(row, textvariable=self.app.volume_lookback_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Checkbutton(
            row, text="启用放量倍数", variable=self.app.volume_expand_enabled_var,
        ).pack(side=tk.LEFT, padx=8)

        ttk.Label(row, text="放量倍数阈值:").pack(side=tk.LEFT, padx=5)
        ttk.Entry(row, textvariable=self.app.volume_expand_factor_var, width=6).pack(side=tk.LEFT, padx=5)

        note = ttk.Frame(parent)
        note.pack(fill=tk.X, pady=2)
        ttk.Label(
            note,
            text="备注：放量倍数=最近N天成交量最大值/最小值，勾选“启用放量倍数”后才参与筛选。",
            foreground="#666",
        ).pack(side=tk.LEFT, padx=5)

    def _build_scan_flow_row(self, parent) -> None:
        """扫描专属流程开关，仅 _build 调用。"""
        row = ttk.Frame(parent)
        row.pack(fill=tk.X, pady=4)
        ttk.Checkbutton(
            row, text="仅显示近N日内有涨停",
            variable=self.app.require_limit_up_var,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Checkbutton(
            row, text="忽略本地结果快照",
            variable=self.app.ignore_result_snapshot_var,
        ).pack(side=tk.LEFT, padx=18)

    # ======================== 列管理 ========================

    def _visible_columns(self) -> tuple[str, ...]:
        ordered_columns = self.column_order or list(self.columns)
        visible = tuple(
            col
            for col in ordered_columns
            if self.column_vars.get(col) and self.column_vars[col].get()
        )
        if visible:
            return visible
        return ("code", "name", "latest_close")

    def _save_column_layout(self) -> None:
        payload = {
            "order": list(self.column_order or self.columns),
            "visible": list(self._visible_columns()),
        }
        save_app_config("result_column_layout", payload)

    def _load_column_layout(self) -> None:
        from stock_store import load_app_config
        if not self.columns:
            return
        payload = load_app_config("result_column_layout")
        if not isinstance(payload, dict):
            return

        saved_order = payload.get("order") or []
        normalized_order = [col for col in saved_order if col in self.columns]
        for col in self.columns:
            if col not in normalized_order:
                normalized_order.append(col)
        if normalized_order:
            self.column_order = normalized_order

        saved_visible = set(payload.get("visible") or [])
        if saved_visible:
            for col, var in self.column_vars.items():
                var.set(col in saved_visible)

    def reset_columns(self) -> None:
        self.column_order = list(self.columns)
        visible = set(self.default_display_columns)
        for col, var in self.column_vars.items():
            var.set(col in visible)
        self.apply_display_columns()

    def apply_display_columns(self, save: bool = True) -> None:
        if not hasattr(self, "tree"):
            return
        self.tree.configure(displaycolumns=self._visible_columns())
        if save:
            self._save_column_layout()

    def _get_display_columns_and_headings(self) -> List[tuple[str, str]]:
        return [
            (col, self.headings.get(col, (col, 100))[0])
            for col in self._visible_columns()
        ]

    def _format_row_values(self, result: Dict[str, Any]) -> Dict[str, str]:
        context: Dict[str, Any] = {}
        return {
            col.id: col.format_cell(result, context)
            for col in RESULT_COLUMNS
        }

    def _build_image_pages(self, rows: List[List[str]], page_size: int = 40) -> List[List[List[str]]]:
        if not rows:
            return []
        return [rows[index : index + page_size] for index in range(0, len(rows), page_size)]

    def _get_selected_identity(self) -> Optional[tuple[str, str]]:
        selection = self.tree.selection()
        if not selection:
            return None
        item = self.tree.item(selection[0])
        values = item.get("values") or []
        if len(values) < 2:
            return None
        stock_code = str(values[0]).strip().zfill(6)
        stock_name = str(values[1]).strip()
        if not stock_code or not stock_name:
            return None
        return stock_code, stock_name

    def lookup_by_code(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """根据股票代码在结果列表中查找。被 DetailTab 等跨 tab 调用。"""
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return None
        for result in self.all_results:
            if str(result.get("code", "") or "").strip().zfill(6) == code:
                return result
        for result in self.filtered_stocks:
            if str(result.get("code", "") or "").strip().zfill(6) == code:
                return result
        return None

    # ======================== 扫描请求构建（参数解析）========================

    def _parse_optional_price_limit(self, raw_value: str, field_name: str) -> Optional[float]:
        text = str(raw_value).strip()
        if not text:
            return None
        try:
            value = float(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} 必须是数字") from exc
        if value < 0:
            raise ValueError(f"{field_name} 不能小于 0")
        return value

    def _build_filter_settings(self) -> FilterSettings:
        return FilterSettings(
            trend_days=self.app._parse_int_value(self.app.trend_days_var.get(), "连续天数", minimum=1, maximum=120),
            ma_period=self.app._parse_int_value(self.app.ma_period_var.get(), "MA周期", minimum=1, maximum=250),
            limit_up_lookback_days=self.app._parse_int_value(
                self.app.limit_up_lookback_var.get(),
                "近N日涨停",
                minimum=1,
                maximum=60,
            ),
            volume_lookback_days=self.app._parse_int_value(
                self.app.volume_lookback_var.get(),
                "放量观察天数",
                minimum=1,
                maximum=60,
            ),
            volume_expand_enabled=bool(self.app.volume_expand_enabled_var.get()),
            volume_expand_factor=self.app._parse_float_value(
                self.app.volume_expand_factor_var.get(),
                "放量倍数阈值",
                minimum=1.0,
                maximum=50.0,
            ),
            require_limit_up_within_days=bool(self.app.require_limit_up_var.get()),
            strong_ft_enabled=bool(self.app.strong_ft_enabled_var.get()),
            strong_ft_max_pullback_pct=self.app._parse_float_value(
                self.app.strong_ft_max_pullback_pct_var.get(),
                "承接强势-最大回撤%",
                minimum=0.0,
                maximum=20.0,
            ),
            strong_ft_max_volume_ratio=self.app._parse_float_value(
                self.app.strong_ft_max_volume_ratio_var.get(),
                "承接强势-次日量能上限",
                minimum=0.0,
                maximum=2.0,
            ),
            strong_ft_min_hold_days=self.app._parse_int_value(
                self.app.strong_ft_min_hold_days_var.get(),
                "承接强势-至少站稳天数",
                minimum=0,
                maximum=30,
                allow_zero=True,
            ),
        )

    def _build_scan_request(self) -> Optional[ScanRequest]:
        settings = self.app._apply_filter_settings_from_ui(show_error=True)
        if settings is None:
            return None
        try:
            max_stocks = self.app._parse_int_value(
                self.app.scan_count_var.get(),
                "扫描数量",
                minimum=1,
                maximum=10000,
                allow_zero=True,
            )
            scan_workers = self.app._parse_int_value(
                self.app.scan_workers_var.get(),
                "并发线程",
                minimum=1,
                maximum=16,
            )
        except ValueError as exc:
            messagebox.showerror("错误", str(exc))
            return None
        return ScanRequest(
            filter_settings=settings,
            max_stocks=max_stocks,
            scan_workers=scan_workers,
            history_source=str(self.app.history_source_var.get() or "auto").strip().lower() or "auto",
            allowed_boards=tuple(self.app._selected_boards()),
            refresh_universe=bool(self.app.refresh_universe_var.get()),
            ignore_result_snapshot=bool(self.app.ignore_result_snapshot_var.get()),
        )

    # ======================== 过滤 ========================

    def _filter_by_selected_boards(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        allowed = {str(board).strip() for board in self.app._selected_boards() if str(board).strip()}
        if not allowed:
            return list(results)
        filtered: List[Dict[str, Any]] = []
        for item in results:
            data = item.get("data", {}) or {}
            code = str(item.get("code", "")).strip().zfill(6)
            board = str(data.get("board") or "").strip()
            if not board:
                if code.startswith(("300", "301")):
                    board = "创业板"
                elif code.startswith("688"):
                    board = "科创板"
                elif code.startswith(("000", "001", "002", "003")):
                    board = "深交所主板"
                elif code.startswith(("5", "6", "9")):
                    board = "上交所主板"
                else:
                    board = str(data.get("exchange") or "").strip()
            if board in allowed:
                filtered.append(item)
        return filtered

    def _get_latest_close_value(self, item: Dict[str, Any]) -> Optional[float]:
        data = item.get("data", {}) or {}
        analysis = data.get("analysis") or {}
        value = analysis.get("latest_close")
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _filter_by_price_range(
        self,
        results: List[Dict[str, Any]],
        raise_error: bool = False,
    ) -> List[Dict[str, Any]]:
        try:
            min_price = self._parse_optional_price_limit(self.app.min_price_var.get(), "最低价")
            max_price = self._parse_optional_price_limit(self.app.max_price_var.get(), "最高价")
            if min_price is not None and max_price is not None and min_price > max_price:
                raise ValueError("最低价不能大于最高价")
        except ValueError:
            if raise_error:
                raise
            return list(results)
        if min_price is None and max_price is None:
            return list(results)

        filtered: List[Dict[str, Any]] = []
        for item in results:
            latest_close = self._get_latest_close_value(item)
            if latest_close is None:
                continue
            if min_price is not None and latest_close < min_price:
                continue
            if max_price is not None and latest_close > max_price:
                continue
            filtered.append(item)
        return filtered

    def _apply_filters(self, results: List[Dict[str, Any]], raise_price_error: bool = False) -> List[Dict[str, Any]]:
        filtered = self._filter_by_selected_boards(results)
        filtered = self._filter_by_price_range(filtered, raise_error=raise_price_error)
        filtered = self._filter_by_quick_filters(filtered)
        return filtered

    def _filter_by_quick_filters(
        self,
        results: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """用"快速过滤"行的条件再筛一遍。

        数值解析失败时忽略该条件（当作未填），同时通过 `status_var` 给用户
        一个可见反馈——之前完全静默，用户填错数字会误以为自己的条件生效了。
        """
        parse_errors: List[str] = []

        def _parse_float(name: str, raw_var) -> Optional[float]:
            try:
                return self.app._parse_optional_float(raw_var.get(), name)
            except (ValueError, AttributeError) as exc:
                if str(exc):
                    parse_errors.append(str(exc))
                return None

        def _parse_int(name: str, raw_var) -> Optional[int]:
            try:
                return self.app._parse_optional_int(raw_var.get(), name)
            except (ValueError, AttributeError) as exc:
                if str(exc):
                    parse_errors.append(str(exc))
                return None

        min_score = _parse_float("评分", self.app.min_score_var)
        min_five_day = _parse_float("5日涨幅", self.app.min_five_day_var)
        min_volume_ratio = _parse_float("放量倍数", self.app.min_volume_ratio_var)
        min_streak = _parse_int("连板数", self.app.min_streak_var)
        if parse_errors:
            # 只在 status_var 里提示,不弹窗打断实时输入
            try:
                self.app.status_var.set(
                    "快速过滤有非法输入(已忽略): " + "；".join(parse_errors[:3])
                )
            except (AttributeError, tk.TclError):
                pass

        def _get_bool_var(name: str) -> bool:
            var = getattr(self.app, name, None)
            try:
                return bool(var.get()) if var is not None else False
            except tk.TclError:
                return False

        needle = getattr(self.app, "search_var", None)
        needle_str = needle.get() if needle is not None else ""

        only_lu = _get_bool_var("only_limit_up_var")
        only_broken = _get_bool_var("only_broken_limit_up_var")
        only_vol = _get_bool_var("only_volume_expand_var")
        only_strong = _get_bool_var("only_strong_ft_var")

        output: List[Dict[str, Any]] = []
        for item in results:
            if not result_filters.matches_search(item, needle_str):
                continue
            if not result_filters.at_least_score(item, min_score):
                continue
            if not result_filters.at_least_five_day_return(item, min_five_day):
                continue
            if not result_filters.at_least_volume_ratio(item, min_volume_ratio):
                continue
            if not result_filters.at_least_limit_up_streak(item, min_streak):
                continue
            if not result_filters.only_limit_up(item, only_lu):
                continue
            if not result_filters.only_broken_limit_up(item, only_broken):
                continue
            if not result_filters.only_volume_expand(item, only_vol):
                continue
            if not result_filters.only_strong_followthrough(item, only_strong):
                continue
            output.append(item)
        return output

    def _schedule_quick_filter(self) -> None:
        """搜索框 debounce：输入时不立刻过滤，等 250ms 再统一刷新。"""
        existing = getattr(self, "_quick_filter_after_id", None)
        if existing is not None:
            try:
                self.app.root.after_cancel(existing)
            except tk.TclError:
                pass
            self._quick_filter_after_id = None
        self._quick_filter_after_id = self.app._safe_after(250, self._quick_filter_tick)

    def _quick_filter_tick(self) -> None:
        self._quick_filter_after_id = None
        if not self.app.is_scanning:
            self.on_quick_filter_apply()

    def on_quick_filter_apply(self) -> None:
        """用户点"应用"或勾选复选框时触发。"""
        if self.app.is_scanning:
            return
        source = self.all_results or self.filtered_stocks
        if not source:
            return
        try:
            self.update_table(source, announce=False, persist=False)
            self.app.status_var.set(f"已应用快速过滤，当前显示 {len(self.filtered_stocks)} 只")
        except ValueError as exc:
            messagebox.showerror("错误", str(exc))

    def clear_all_filters(self) -> None:
        """一键清空所有结果表过滤（板块、价格、快速过滤）。"""
        for board_var in self.app.board_filter_vars.values():
            board_var.set(True)
        self.app.min_price_var.set("")
        self.app.max_price_var.set("")
        self.app.search_var.set("")
        self.app.min_score_var.set("")
        self.app.min_five_day_var.set("")
        self.app.min_volume_ratio_var.set("")
        self.app.min_streak_var.set("")
        self.app.only_limit_up_var.set(False)
        self.app.only_broken_limit_up_var.set(False)
        self.app.only_volume_expand_var.set(False)
        self.app.only_strong_ft_var.set(False)
        self.on_quick_filter_apply()

    def on_board_filter_changed(self):
        self.app._save_board_filter_layout()
        if self.app.is_scanning:
            return
        source = self.all_results or self.filtered_stocks
        if source:
            try:
                self.update_table(source, announce=False, persist=False)
                self.app.status_var.set(f"已按筛选条件更新，当前显示 {len(self.filtered_stocks)} 只")
            except ValueError as exc:
                messagebox.showerror("错误", str(exc))
        else:
            self.app.status_var.set("已保存显示板块筛选设置")
        # 同步刷新涨停预测
        self.app._refresh_predict_display_if_ready()

    def on_price_filter_changed(self, event=None):
        if self.app.is_scanning:
            return "break" if event is not None else None
        source = self.all_results or self.filtered_stocks
        if source:
            try:
                self._apply_filters(source, raise_price_error=True)
                self.update_table(source, announce=False, persist=False)
                self.app.status_var.set(f"已按价格过滤，当前显示 {len(self.filtered_stocks)} 只")
            except ValueError as exc:
                messagebox.showerror("错误", str(exc))
                return "break" if event is not None else None
        # 同步刷新涨停预测
        self.app._refresh_predict_display_if_ready()
        return "break" if event is not None else None

    def clear_price_filter(self):
        self.app.min_price_var.set("")
        self.app.max_price_var.set("")
        self.on_price_filter_changed()

    # ======================== 快照保存/加载 ========================

    def _save_last_results(
        self,
        results: List[Dict[str, Any]],
        complete: bool = True,
        request: Optional[ScanRequest] = None,
    ) -> None:
        payload_results = []
        for result in results:
            data = result.get("data", {}) or {}
            analysis = data.get("analysis") or {}
            payload_results.append(
                {
                    "code": result.get("code", ""),
                    "name": result.get("name", ""),
                    "passed": bool(result.get("passed")),
                    "reasons": result.get("reasons", []),
                    "data": {
                        "board": data.get("board", ""),
                        "exchange": data.get("exchange", ""),
                        "analysis": analysis,
                    },
                }
            )
        signature_request = request or self.app._active_scan_request
        if signature_request is None:
            current_settings = self.app.stock_filter.get_settings()
            signature_request = ScanRequest(
                filter_settings=current_settings,
                max_stocks=int(self.app._current_scan_max_stocks),
                allowed_boards=tuple(self.app._current_scan_allowed_boards),
            )
        payload = {
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scan_date": datetime.now().strftime("%Y-%m-%d"),
            "complete": bool(complete),
            "row_count": len(payload_results),
            "signature": self.app._scan_signature(signature_request),
            "results": payload_results,
        }
        save_scan_snapshot(json.dumps(payload["signature"], ensure_ascii=False, sort_keys=True), payload)

    def _load_last_results(self) -> None:
        payload = load_latest_scan_snapshot()
        if not payload:
            return
        results = payload.get("results", []) or []
        if not results:
            return
        self.all_results = list(results)
        self.filtered_stocks = list(results)
        self.update_table(results, announce=False, persist=False)
        self.app.status_var.set("已从本地结果恢复")

    # ======================== 扫描启停 + 主流程 ========================

    def start_scan(self):
        if self.app.is_scanning or self.app.is_updating_cache:
            return
        request = self._build_scan_request()
        if request is None:
            return

        self.app._active_scan_request = request
        self.app._current_scan_allowed_boards = list(request.allowed_boards)
        self.app._current_scan_max_stocks = int(request.max_stocks)

        if self.app._can_use_snapshot(request):
            self.app._log("命中本地结果快照，直接恢复上次扫描结果。")
            signature = json.dumps(self.app._scan_signature(request), ensure_ascii=False, sort_keys=True)
            payload = load_scan_snapshot(signature)
            if payload:
                self.all_results = payload.get("results", []) or []
                self.update_table(self.all_results, announce=False, persist=False)
                self.app.status_var.set("已从本地结果恢复")
                self.app.progress_var.set(100)
                self.scan_finished("已从本地结果恢复")
                return

        self.app.is_scanning = True
        self.app.scan_btn.config(state=tk.DISABLED)
        self.app.stop_btn.config(state=tk.NORMAL)
        self.all_results.clear()
        self.filtered_stocks.clear()
        for item in self.tree.get_children():
            self.tree.delete(item)

        self.app._open_run_log()
        self.app._log(
            f"开始扫描：最近{request.filter_settings.trend_days}日收盘 > MA{request.filter_settings.ma_period}，"
            f"近{request.filter_settings.limit_up_lookback_days}日内涨停过滤={'开' if request.filter_settings.require_limit_up_within_days else '关'}。"
        )
        self.app._log(self.app._history_cache_summary_text())
        self.app._log(f"本轮历史数据源：{request.history_source}")
        if request.max_stocks <= 0:
            self.app._log("本次为全量扫描，建议优先在收盘后执行，并尽量复用本地结果快照。")
        if request.scan_workers >= 4:
            self.app._log(
                f"你当前设置了 {request.scan_workers} 个扫描线程；程序会自动做并发保护，但较大的线程数仍可能增加外部接口压力。"
            )
        if request.refresh_universe:
            self.app._log("已开启“重新拉取股票池”，本轮会刷新股票池缓存，整体耗时会更长。")
        if request.ignore_result_snapshot:
            self.app._log("已开启“忽略本地结果快照”，本轮不会直接复用上次扫描结果。")
        self.app._log("扫描阶段只拉历史日线，不拉实时、资金流或内外盘。")
        self.app.status_var.set("正在扫描...")
        self.app.progress_var.set(0)
        self.app._set_progress_text(0, 0)

        token = CancelToken()
        self.scan_cancel_token = token
        # 与 App 共享同名字段（_on_restore_database 等通过它访问）
        self.app._scan_cancel_token = token
        self.app._register_cancel_token(token)
        self.scan_thread = threading.Thread(
            target=self.scan_stocks, args=(request, token), daemon=True
        )
        self.app._scan_thread = self.scan_thread
        self.scan_thread.start()

    def stop_scan(self):
        # 布尔标记保留做兼容；CancelToken 才是真正可传播的停止信号
        self.app.is_scanning = False
        self.app.is_updating_cache = False
        for token in (self.app._scan_cancel_token, self.app._cache_cancel_token):
            if token is not None:
                token.cancel("user_stop")
        self.app.status_var.set("正在停止...")
        self.app._log("已请求停止，正在等待当前任务结束。")

    def scan_stocks(self, request: ScanRequest, cancel_token: Optional[CancelToken] = None):
        import time as _time
        token = cancel_token or CancelToken()
        try:
            scan_filter = StockFilter()
            scan_filter.apply_settings(request.filter_settings)
            scan_filter.set_log_callback(self.app._log_async)
            self.app._log_async(
                f"扫描参数：数量={'全量' if request.max_stocks <= 0 else request.max_stocks}，并发线程={request.scan_workers}，历史源={request.history_source}"
            )

            scan_t0 = _time.time()

            def progress_callback(current, total, code, name):
                if token.is_cancelled() or not self.app.is_scanning:
                    raise StopIteration
                progress = (current / total) * 100 if total else 0
                elapsed = _time.time() - scan_t0
                speed = current / elapsed if elapsed > 0 else 0
                eta_sec = (total - current) / speed if speed > 0 else 0
                if eta_sec >= 60:
                    eta_text = f"{int(eta_sec // 60)}分{int(eta_sec % 60)}秒"
                else:
                    eta_text = f"{int(eta_sec)}秒"
                status_text = f"扫描中 {current}/{total} ({progress:.0f}%) | {speed:.1f}只/秒 | 剩余 {eta_text}"
                self.app._post_to_ui(lambda: self.app.progress_var.set(progress))
                self.app._post_to_ui(lambda c=current, t=total: self.app._set_progress_text(c, t))
                self.app._post_to_ui(lambda s=status_text: self.app.status_var.set(s))

            results = scan_filter.scan_all_stocks(
                max_stocks=request.max_stocks,
                progress_callback=progress_callback,
                max_workers=request.scan_workers,
                history_source=request.history_source,
                local_history_only=True,
                cancel_token=token,
                should_stop=lambda: not self.app.is_scanning,
                refresh_universe=request.refresh_universe,
                allowed_boards=list(request.allowed_boards),
            )
            if token.is_cancelled() or not self.app.is_scanning:
                self.app._post_to_ui(lambda: self.app._log("扫描已停止。"))
                self.app._post_to_ui(lambda: self.scan_finished("扫描已停止"))
                return
            self.all_results = results
            self.app._post_to_ui(lambda res=results, req=request: self.update_table(res, request=req))
            self.app._post_to_ui(lambda count=len(results): self.scan_finished(f"扫描完成，命中 {count} 只。"))
        except StopIteration:
            self.app._post_to_ui(lambda: self.app._log("扫描已停止。"))
            self.app._post_to_ui(lambda: self.scan_finished("扫描已停止"))
        except Exception as e:
            error_text = str(e)
            self.app._post_to_ui(lambda: self.app._log(f"扫描出错: {error_text}"))
            self.app._post_to_ui(lambda: self.scan_finished(f"扫描失败: {error_text}"))
            self.app._post_to_ui(lambda et=error_text: self.app._show_network_error_alert(et))
        finally:
            self.app._unregister_cancel_token(token)

    def scan_finished(self, status_text: str = "扫描完成"):
        self.app._set_progressbar_indeterminate(False)
        self.app.scan_btn.config(state=tk.NORMAL)
        self.app.update_cache_btn.config(state=tk.NORMAL)
        self.app.stop_btn.config(state=tk.DISABLED)
        self.app.status_var.set(status_text)
        if self.app.progress_var.get() >= 100:
            progress_total_text = self.app.progress_text_var.get().strip()
            self.app.progress_text_var.set(progress_total_text)
        elif not self.app.is_scanning and not self.app.is_updating_cache:
            self.app.progress_text_var.set("")
        self.app.is_scanning = False
        self.app.is_updating_cache = False
        self.scan_thread = None
        self.app._scan_thread = None
        self.app._cache_thread = None
        self.app._active_scan_request = None
        self.app.refresh_universe_var.set(False)
        self.app._close_run_log()

    # ======================== 排序 + 表格刷新 ========================

    def _sort_value_for_column(self, item: Dict[str, Any], column: str):
        col_def = self.columns_map.get(column)
        context: Dict[str, Any] = {}
        if col_def is not None:
            return col_def.sort_key(item, context)
        # 未知列名：退回到 latest_change_pct，保持原先的兜底语义
        analysis = (item.get("data", {}) or {}).get("analysis") or {}
        latest_change_pct = analysis.get("latest_change_pct")
        return float(latest_change_pct) if latest_change_pct is not None else float("-inf")

    def _sort_results(
        self,
        results: List[Dict[str, Any]],
        column: Optional[str] = None,
        reverse: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        sort_column = column or self.app.sort_column
        sort_reverse = self.app.sort_reverse if reverse is None else bool(reverse)
        secondary_columns = [
            "score",
            "limit_up_streak",
            "volume_break_limit_up",
            "volume_expand_ratio",
            "limit_up",
            "five_day_return",
            "latest_change_pct",
        ]
        if sort_column in secondary_columns:
            secondary_columns = [c for c in secondary_columns if c != sort_column]
        return sorted(
            results,
            key=lambda item: tuple(
                [self._sort_value_for_column(item, sort_column)]
                + [self._sort_value_for_column(item, c) for c in secondary_columns]
                + [str(item.get("code", ""))]
            ),
            reverse=sort_reverse,
        )

    def on_heading_click(self, column: str):
        if column == self.app.sort_column:
            self.app.sort_reverse = not self.app.sort_reverse
        else:
            self.app.sort_column = column
            self.app.sort_reverse = column in desc_by_default_ids()
        if self.filtered_stocks:
            self.update_table(self.filtered_stocks, announce=False, persist=False)

    def update_table(
        self,
        results: List[Dict[str, Any]],
        announce: bool = True,
        persist: bool = True,
        request: Optional[ScanRequest] = None,
    ):
        for item in self.tree.get_children():
            self.tree.delete(item)

        results = self._apply_filters(results)
        results = self._sort_results(results)

        for result in results:
            row_values = self._format_row_values(result)
            values = tuple(row_values.get(col, "-") for col in self.columns)
            self.tree.insert("", tk.END, values=values)

        self.filtered_stocks = results
        if persist:
            self._save_last_results(results, complete=True, request=request)
        if announce:
            self.app._log(f"扫描完成，命中 {len(results)} 只。")

    # ======================== 选中/双击/复制 ========================

    def on_stock_select(self, event):
        try:
            selection = self.tree.selection()
            if not selection:
                return
            item = self.tree.item(selection[0])
            values = item.get("values") or []
            if not values:
                return
            self.app.detail._schedule_show(values[0])
        except Exception as e:
            self.app._log(f"选择股票详情失败: {e}")

    def on_stock_double_click(self, event):
        selection = self.tree.selection()
        if selection:
            item = self.tree.item(selection[0])
            stock_code = item["values"][0]
            self.app.detail._cancel_scheduled()
            self.app.detail.show(stock_code, force_refresh=True)
            self.app.notebook.select(self.app.detail.frame)

    def _refresh_table_if_ready(self) -> None:
        if hasattr(self, "tree") and self.filtered_stocks:
            self.update_table(self.filtered_stocks, announce=False, persist=False)

    # ======================== 导出 + 复制 ========================

    def export_results(self):
        if not self.filtered_stocks:
            messagebox.showwarning("警告", "没有可导出的结果")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV文件", "*.csv"), ("所有文件", "*.*")],
        )

        if not file_path:
            return

        try:
            with open(file_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(["代码", "名称", "板块", "最新日期", "最新收盘", "MA", "5日涨幅", "放量倍数", "放量", "涨停", "最近收盘", "结论"])
                for result in self.filtered_stocks:
                    data = result.get("data", {}) or {}
                    analysis = data.get("analysis") or {}
                    recent = analysis.get("recent_closes") or []
                    writer.writerow(
                        [
                            result.get("code", ""),
                            result.get("name", ""),
                            data.get("board", data.get("exchange", "")),
                            analysis.get("latest_date", ""),
                            "" if analysis.get("latest_close") is None else f"{analysis['latest_close']:.2f}",
                            "" if analysis.get("latest_ma") is None else f"{analysis['latest_ma']:.2f}",
                            "" if analysis.get("five_day_return") is None else f"{analysis['five_day_return']:.2f}%",
                            "" if analysis.get("volume_expand_ratio") is None else f"{analysis['volume_expand_ratio']:.2f}x",
                            "是" if analysis.get("volume_expand") else "否",
                            "是" if analysis.get("limit_up") else "否",
                            ", ".join("" if v is None else f"{v:.2f}" for v in recent),
                            analysis.get("summary", ""),
                        ]
                    )
            messagebox.showinfo("成功", f"结果已导出到 {file_path}")
            self.app._log(f"结果已导出到 {file_path}")
        except Exception as e:
            messagebox.showerror("错误", f"导出失败: {e}")

    def export_results_image(self):
        if not self.filtered_stocks:
            messagebox.showwarning("警告", "没有可导出的结果")
            return

        export_columns: List[tuple[str, str]] = [("code", "代码"), ("name", "名称")]

        file_path = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG图片", "*.png"), ("所有文件", "*.*")],
        )

        if not file_path:
            return

        rows = []
        for result in self.filtered_stocks:
            row_values = self._format_row_values(result)
            rows.append([str(row_values.get(col, "-")) for col, _ in export_columns])

        if not rows:
            messagebox.showwarning("警告", "没有可导出的结果")
            return

        output_path = Path(file_path)
        headings = [heading for _, heading in export_columns]
        column_widths = [max(self.headings.get(col, ("", 100))[1], 80) for col, _ in export_columns]
        total_width = sum(column_widths) or 1
        normalized_widths = [width / total_width for width in column_widths]
        exported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            figure_width = min(max(total_width / 85, 7.2), 14.0)
            figure_height = max(4.8, 1.6 + len(rows) * 0.34)

            fig = Figure(figsize=(figure_width, figure_height), dpi=180)
            fig.patch.set_facecolor("white")
            ax = fig.add_subplot(111)
            ax.axis("off")

            fig.text(0.01, 0.985, "扫描结果导出", ha="left", va="top", fontsize=16, fontweight="bold")
            fig.text(
                0.01,
                0.957,
                f"导出时间：{exported_at}    结果数量：{len(rows)}    显示列：{len(export_columns)}",
                ha="left",
                va="top",
                fontsize=9.5,
                color="#4b5563",
            )

            table = ax.table(
                cellText=rows,
                colLabels=headings,
                colLoc="center",
                cellLoc="center",
                colWidths=normalized_widths,
                loc="upper left",
                bbox=[0, 0, 1, 0.92],
            )
            table.auto_set_font_size(False)
            table.set_fontsize(9.2)

            left_aligned_columns = {"name"}
            for (row_index, col_index), cell in table.get_celld().items():
                cell.set_edgecolor("#d7deea")
                cell.set_linewidth(0.6)
                cell.get_text().set_wrap(True)
                if row_index == 0:
                    cell.set_facecolor("#eaf2ff")
                    cell.set_text_props(weight="bold", color="#111827")
                    cell.set_height(0.042)
                else:
                    cell.set_facecolor("#ffffff" if row_index % 2 else "#f8fafc")
                    cell.set_height(0.037)
                    if export_columns[col_index][0] in left_aligned_columns:
                        cell.set_text_props(ha="left")

            fig.savefig(output_path, bbox_inches="tight", facecolor=fig.get_facecolor())
            plt.close(fig)
            messagebox.showinfo("成功", f"结果图片已导出到 {output_path}")
            self.app._log(f"结果图片已导出到 {output_path}")
        except Exception as e:
            messagebox.showerror("错误", f"导出图片失败: {e}")

    def copy_selected_code_name(self, event=None):
        selection = self._get_selected_identity()
        if selection is None:
            messagebox.showwarning("提示", "请先在结果表中选中一只股票")
            return "break" if event is not None else None

        stock_code, _ = selection
        payload = stock_code

        try:
            self.app.root.clipboard_clear()
            self.app.root.clipboard_append(payload)
            self.app.root.update_idletasks()
            self.app.status_var.set(f"已复制: {payload}")
            self.app._log(f"已复制股票代码: {payload}")
        except tk.TclError as e:
            messagebox.showerror("错误", f"复制失败: {e}")

        return "break" if event is not None else None
