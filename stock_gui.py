from __future__ import annotations

import csv
import json
import threading
import time
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import matplotlib
matplotlib.use("TkAgg")

import matplotlib.pyplot as plt
import pandas as pd
import tkinter as tk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter
from tkinter import ttk, messagebox, scrolledtext, filedialog, simpledialog

from scan_models import FilterSettings, ScanRequest
from data_source_models import DATA_SOURCE_OPTIONS
from src.gui.log_drainer import LogDrainer
from src.gui.result_columns import (
    RESULT_COLUMNS,
    all_column_ids,
    columns_by_id,
    default_visible_ids,
    desc_by_default_ids,
)
from src.gui import result_filters
from src.gui.tree_enhancer import attach_enhancers_recursively as _attach_tree_enhancers
from src.gui.ui_dispatch import UIDispatcher
from src.utils.cancel_token import CancelToken, CancelTokenRegistry
from src.utils.trade_calendar import _get_trade_calendar, _is_trading_day, _previous_trading_day
from src.services import prediction_accuracy_service
from src.services import concept_hype_service
from src.services import market_sentiment_service
import stock_store
from stock_filter import StockFilter
from stock_data import clear_history_data, clear_universe_data
from llm_client import (
    DEFAULT_MODEL as LLM_DEFAULT_MODEL,
    LlmConfigError,
    LlmRequestError,
    has_api_key as llm_has_api_key,
    save_api_key as llm_save_api_key,
)
from llm_theme_clustering import (
    cluster_themes as llm_cluster_themes,
    load_cached_themes as llm_load_cached_themes,
)
from stock_store import (
    backup_database,
    cleanup_all,
    ensure_store_ready,
    export_watchlist_csv,
    import_watchlist_csv,
    list_backups,
    list_limit_up_compare_dates,
    list_limit_up_prediction_dates,
    load_app_config,
    load_last_limit_up_prediction,
    load_latest_scan_snapshot,
    load_limit_up_compare_by_date,
    load_limit_up_prediction_by_date,
    load_scan_snapshot,
    load_watchlist,
    load_watchlist_item,
    reset_all_connections,
    restore_database,
    save_app_config,
    save_limit_up_compare_record,
    save_scan_snapshot,
    save_watchlist_item,
    delete_watchlist_item,
)

plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False


class StockMonitorApp:
    def __init__(self, root: tk.Tk):
        ensure_store_ready()
        self.root = root
        self.root.title("A股筛选")
        self.root.minsize(1280, 820)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self._set_initial_window_geometry()

        self.stock_filter = StockFilter()
        self.stock_filter.set_log_callback(self._log_async)
        self.all_scan_results: List[Dict[str, Any]] = []
        self.filtered_stocks: List[Dict[str, Any]] = []
        self._detail_request_code = ""
        self._detail_loading_code = ""
        self._detail_after_id = None
        self._current_detail_code = ""
        self._intraday_request_code = ""
        self._intraday_loading_code = ""
        self._intraday_request_offset = 0
        self._intraday_request_target_date = ""
        self._intraday_loading_offset = 0
        self._intraday_loading_target_date = ""
        self._intraday_day_offset = 0
        self._intraday_available_dates: List[str] = []
        self._intraday_selected_date = ""
        self._detail_chart_dates: List[str] = []
        self._detail_chart_window_size = 60
        self._detail_chart_window_start = 0
        self._detail_chart_history = None
        self._detail_chart_analysis: Dict[str, Any] = {}
        self._detail_chart_scroll_bound = False
        self._detail_chart_slider_updating = False
        self._detail_chart_dragging = False
        self._detail_chart_drag_moved = False
        self._detail_chart_drag_start_x = 0.0
        self._detail_chart_drag_start_window = 0
        self._detail_chart_click_target_date = ""
        self._detail_chart_loading_more = False
        self._detail_chart_loaded_days = 0
        self._detail_summary_expanded = False
        self._detail_chart_expanded = True
        self._detail_flow_expanded = False
        self.sort_column = "score"
        self.sort_reverse = True
        self._predict_cont_sort_column = "score"
        self._predict_cont_sort_reverse = True
        self._predict_first_sort_column = "score"
        self._predict_first_sort_reverse = True
        self._predict_fresh_sort_column = "score"
        self._predict_fresh_sort_reverse = True
        self._predict_wrap_sort_column = "score"
        self._predict_wrap_sort_reverse = True
        self._predict_trend_sort_column = "score"
        self._predict_trend_sort_reverse = True
        # 按历史命中段排序时，缓存每个类别的 {(lo, hi): {rate, eligible, ...}}
        # 在 _apply_predict_result / _refresh_predict_accuracy_async 后清空，懒加载
        self._predict_bucket_rates_cache: Dict[str, Dict[Tuple[int, int], Dict[str, Any]]] = {}
        # 每个主类别的"历史最优分数段" (lo, hi) —— eligible 桶里命中率最高的
        # 在 _apply_predict_accuracy 里刷新；用于 tab header 提示 + 行高亮
        self._predict_best_buckets: Dict[str, Optional[Tuple[int, int]]] = {}
        self._predict_best_bucket_labels: Dict[str, Any] = {}
        # 详情/分时 payload 的 GUI 层 LRU 缓存：用户反复点击同一只股票时秒开
        # 详情 TTL 120s（兼顾盘中实时性）；分时仅"实时请求"用 30s TTL，
        # 指定历史日（day_offset<0 或 target_trade_date 非空）则数据不变长缓存
        self._detail_payload_cache: "OrderedDict[str, Tuple[float, Dict[str, Any]]]" = OrderedDict()
        self._intraday_payload_cache: "OrderedDict[Tuple[str, int, str], Tuple[float, Dict[str, Any], bool]]" = OrderedDict()
        self._DETAIL_CACHE_MAX = 20
        self._INTRADAY_CACHE_MAX = 30
        self._DETAIL_CACHE_TTL_SEC = 120.0
        self._INTRADAY_LIVE_TTL_SEC = 30.0
        # stale-while-revalidate：缓存命中后总在后台再拉一次最新数据（盘后/盘中都修），
        # 同一只票 N 秒内只会触发一次，避免高频点击雪崩。
        self._DETAIL_REVALIDATE_THROTTLE_SEC = 5.0
        self._detail_last_revalidate_ts: Dict[str, float] = {}
        self._top_header_name_by_code: Dict[str, str] = {}
        self.is_scanning = False
        self.is_updating_cache = False
        self._scan_cancel_token: Optional[CancelToken] = None
        self._cache_cancel_token: Optional[CancelToken] = None
        # 所有后台任务（扫描、缓存、详情、分时、涨停对比、涨停预测）的取消令牌
        # 统一登记在 registry 中，on_close / restore_database 触发 broadcast_cancel
        self._cancel_registry = CancelTokenRegistry()
        self._scan_thread: Optional[threading.Thread] = None
        self._cache_thread: Optional[threading.Thread] = None
        self._active_scan_request: Optional[ScanRequest] = None
        self._run_log_file: Optional[Path] = None
        self._current_scan_allowed_boards: List[str] = []
        self._current_scan_max_stocks: int = 0
        self.watchlist_items: Dict[str, Dict[str, Any]] = {}
        self.result_columns: tuple[str, ...] = ()
        self.result_headings: Dict[str, tuple[str, int]] = {}
        self.result_column_vars: Dict[str, tk.BooleanVar] = {}
        self.result_column_order: List[str] = []
        self.default_result_display_columns: tuple[str, ...] = ()
        # GUI 设置统一存储在 SQLite app_config 表中（key: result_column_layout / board_filter_layout / app_settings / limit_up_compare_snapshot）
        self._main_thread_id = threading.get_ident()
        self._ui = UIDispatcher(self.root)
        self._log_drainer = LogDrainer(
            dispatcher=self._ui,
            main_thread_id=self._main_thread_id,
            sink=self._log,
            poll_interval_ms=100,
        )

        self.setup_ui()
        self._load_app_settings()
        self._apply_source_preferences()
        self._load_result_column_layout()
        self._load_board_filter_layout()
        self.apply_result_display_columns(save=False)
        self._load_last_results()
        self._load_last_limit_up_compare()
        self._load_last_limit_up_prediction()
        self._load_watchlist_items()
        self._log_drainer.start()

    def _set_initial_window_geometry(self) -> None:
        default_width = 1440
        default_height = 900

        try:
            screen_width = max(self.root.winfo_screenwidth(), self.root.minsize()[0])
            screen_height = max(self.root.winfo_screenheight(), self.root.minsize()[1])
        except tk.TclError:
            self.root.geometry(f"{default_width}x{default_height}")
            return

        width = min(default_width, screen_width - 120)
        height = min(default_height, screen_height - 120)
        width = max(width, 1280)
        height = max(height, 820)
        x = max((screen_width - width) // 2, 0)
        y = max((screen_height - height) // 2, 0)
        self.root.geometry(f"{width}x{height}+{x}+{y}")

    def setup_ui(self):
        self.setup_menu()

        main_frame = ttk.Frame(self.root, padding="5")
        main_frame.pack(fill=tk.BOTH, expand=True)

        self._setup_top_header(main_frame)
        self.setup_control_panel(main_frame)
        self.setup_notebook(main_frame)
        self.setup_status_bar()

    def _setup_top_header(self, parent) -> None:
        """窗口最顶部一行，与系统标题栏 X 按钮大致同高度，用于在详情/分时页显示当前股票名称。"""
        header = ttk.Frame(parent)
        header.pack(side=tk.TOP, fill=tk.X)
        self.top_header_var = tk.StringVar(value="")
        # 右对齐 → 视觉上贴近窗口右上角的系统 X
        self.top_header_label = ttk.Label(
            header,
            textvariable=self.top_header_var,
            anchor=tk.E,
            font=("Microsoft YaHei", 11, "bold"),
            foreground="#1a4f8a",
        )
        self.top_header_label.pack(side=tk.RIGHT, padx=(0, 8))

    def _set_top_header_for_code(self, code: str, name: str = "") -> None:
        code = str(code or "").strip().zfill(6)
        if not code:
            self.top_header_var.set("")
            return
        name = (name or self._top_header_name_by_code.get(code, "")).strip()
        if name:
            self._top_header_name_by_code[code] = name
            self.top_header_var.set(f"{code}  {name}")
        else:
            self.top_header_var.set(code)

    def _clear_top_header(self) -> None:
        self.top_header_var.set("")

    def setup_menu(self):
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="文件", menu=file_menu)
        file_menu.add_command(label="导出结果 CSV", command=self.export_results)
        file_menu.add_command(label="导出结果图片", command=self.export_results_image)
        file_menu.add_command(label="复制代码", command=self.copy_selected_stock_code_name, accelerator="Ctrl+C")
        file_menu.add_separator()
        file_menu.add_command(label="导出自选股 CSV", command=self._export_watchlist_csv)
        file_menu.add_command(label="导入自选股 CSV", command=self._import_watchlist_csv)
        file_menu.add_separator()
        file_menu.add_command(label="退出", command=self.on_close)

        setting_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="设置", menu=setting_menu)
        setting_menu.add_command(label="扫描参数", command=self.show_settings)
        setting_menu.add_command(label="清空股票池", command=self.on_clear_universe_data)
        setting_menu.add_command(label="清空历史数据", command=self.on_clear_history_data)
        setting_menu.add_separator()
        setting_menu.add_command(label="清理过期数据", command=self._on_cleanup_data)
        setting_menu.add_command(label="备份数据库", command=self._on_backup_database)
        setting_menu.add_command(label="恢复数据库", command=self._on_restore_database)

        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="帮助", menu=help_menu)
        help_menu.add_command(label="关于", command=self.show_about)

    def _build_control_scan_params_row(self, control_frame) -> None:
        row1 = ttk.Frame(control_frame)
        row1.pack(fill=tk.X, pady=5)
        ttk.Label(row1, text="扫描数量(0=全量):").pack(side=tk.LEFT, padx=5)
        self.scan_count_var = tk.StringVar(value="0")
        ttk.Entry(row1, textvariable=self.scan_count_var, width=8).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="并发线程:").pack(side=tk.LEFT, padx=5)
        self.scan_workers_var = tk.StringVar(value="3")
        ttk.Entry(row1, textvariable=self.scan_workers_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="连续天数:").pack(side=tk.LEFT, padx=5)
        self.trend_days_var = tk.StringVar(value="5")
        ttk.Entry(row1, textvariable=self.trend_days_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="MA周期:").pack(side=tk.LEFT, padx=5)
        self.ma_period_var = tk.StringVar(value="5")
        ttk.Entry(row1, textvariable=self.ma_period_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="近N日涨停:").pack(side=tk.LEFT, padx=5)
        self.limit_up_lookback_var = tk.StringVar(value="5")
        ttk.Entry(row1, textvariable=self.limit_up_lookback_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="放量观察天数:").pack(side=tk.LEFT, padx=5)
        self.volume_lookback_var = tk.StringVar(value="5")
        ttk.Entry(row1, textvariable=self.volume_lookback_var, width=6).pack(side=tk.LEFT, padx=5)

        self.volume_expand_enabled_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            row1,
            text="启用放量倍数",
            variable=self.volume_expand_enabled_var,
        ).pack(side=tk.LEFT, padx=8)

        ttk.Label(row1, text="放量倍数阈值:").pack(side=tk.LEFT, padx=5)
        self.volume_expand_factor_var = tk.StringVar(value="2.0")
        ttk.Entry(row1, textvariable=self.volume_expand_factor_var, width=6).pack(side=tk.LEFT, padx=5)

        # 承接强势形态相关的变量在这里先声明，实体控件放在"扫描参数"弹窗里（show_settings）。
        self.strong_ft_enabled_var = tk.BooleanVar(value=False)
        self.strong_ft_max_pullback_pct_var = tk.StringVar(value="3.0")
        self.strong_ft_max_volume_ratio_var = tk.StringVar(value="0.7")
        self.strong_ft_min_hold_days_var = tk.StringVar(value="1")

        row1_note = ttk.Frame(control_frame)
        row1_note.pack(fill=tk.X, pady=2)
        ttk.Label(
            row1_note,
            text="备注：放量倍数=最近N天成交量最大值/最小值，勾选“启用放量倍数”后才参与筛选。",
        ).pack(side=tk.LEFT, padx=5)

        self.refresh_universe_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row1,
            text="重新拉取股票池",
            variable=self.refresh_universe_var,
        ).pack(side=tk.LEFT, padx=15)

    def _build_control_actions_row(self, control_frame) -> None:
        row2 = ttk.Frame(control_frame)
        row2.pack(fill=tk.X, pady=5)
        self.scan_btn = ttk.Button(row2, text="开始扫描", command=self.start_scan)
        self.scan_btn.pack(side=tk.LEFT, padx=5)

        self.update_cache_btn = ttk.Button(row2, text="更新历史缓存", command=self.start_history_cache_update)
        self.update_cache_btn.pack(side=tk.LEFT, padx=5)

        self.stop_btn = ttk.Button(row2, text="停止", command=self.stop_scan, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=5)

        ttk.Label(row2, text="股票代码:").pack(side=tk.LEFT, padx=5)
        self.stock_code_var = tk.StringVar()
        ttk.Entry(row2, textvariable=self.stock_code_var, width=10).pack(side=tk.LEFT, padx=5)
        ttk.Button(row2, text="查询股票", command=self.query_single_stock).pack(side=tk.LEFT, padx=15)
        ttk.Label(row2, text="历史源:").pack(side=tk.LEFT, padx=(8, 4))
        self.history_source_var = tk.StringVar(value="auto")
        self.history_source_combo = ttk.Combobox(
            row2,
            textvariable=self.history_source_var,
            width=12,
            state="readonly",
            values=DATA_SOURCE_OPTIONS["history"],
        )
        self.history_source_combo.pack(side=tk.LEFT, padx=4)
        self.history_source_combo.bind("<<ComboboxSelected>>", self.on_history_source_changed)
        ttk.Button(row2, text="列表列设置", command=self.show_column_picker).pack(side=tk.LEFT, padx=8)

        self.intraday_source_var = tk.StringVar(value="auto")
        self.fund_flow_source_var = tk.StringVar(value="auto")
        self.limit_up_reason_source_var = tk.StringVar(value="auto")

    def _build_control_board_filter_row(self, control_frame) -> None:
        row3 = ttk.Frame(control_frame)
        row3.pack(fill=tk.X, pady=5)
        ttk.Label(row3, text="显示板块:").pack(side=tk.LEFT, padx=5)
        self.board_filter_vars = {
            "上交所主板": tk.BooleanVar(value=True),
            "深交所主板": tk.BooleanVar(value=True),
            "创业板": tk.BooleanVar(value=True),
            "科创板": tk.BooleanVar(value=True),
        }
        for label in ("上交所主板", "深交所主板", "创业板", "科创板"):
            ttk.Checkbutton(
                row3,
                text=label,
                variable=self.board_filter_vars[label],
                command=self.on_board_filter_changed,
            ).pack(side=tk.LEFT, padx=8)

        self.require_limit_up_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row3,
            text="仅显示近N日内有涨停",
            variable=self.require_limit_up_var,
        ).pack(side=tk.LEFT, padx=18)

        self.ignore_result_snapshot_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row3,
            text="忽略本地结果快照",
            variable=self.ignore_result_snapshot_var,
        ).pack(side=tk.LEFT, padx=18)

    def _build_control_price_filter_row(self, control_frame) -> None:
        row4 = ttk.Frame(control_frame)
        row4.pack(fill=tk.X, pady=5)
        ttk.Label(row4, text="价格过滤(最新收盘):").pack(side=tk.LEFT, padx=5)
        ttk.Label(row4, text="最低").pack(side=tk.LEFT, padx=(8, 2))
        self.min_price_var = tk.StringVar(value="")
        min_price_entry = ttk.Entry(row4, textvariable=self.min_price_var, width=8)
        min_price_entry.pack(side=tk.LEFT, padx=(0, 8))
        ttk.Label(row4, text="最高").pack(side=tk.LEFT, padx=(0, 2))
        self.max_price_var = tk.StringVar(value="")
        max_price_entry = ttk.Entry(row4, textvariable=self.max_price_var, width=8)
        max_price_entry.pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(row4, text="应用价格过滤", command=self.on_price_filter_changed).pack(side=tk.LEFT, padx=4)
        ttk.Button(row4, text="清空价格过滤", command=self.clear_price_filter).pack(side=tk.LEFT, padx=4)
        min_price_entry.bind("<Return>", self.on_price_filter_changed)
        max_price_entry.bind("<Return>", self.on_price_filter_changed)

    def _build_control_quick_filter_row(self, control_frame) -> None:
        """结果表客户端快速过滤：对扫描出来的几百条再细筛。"""
        row5 = ttk.Frame(control_frame)
        row5.pack(fill=tk.X, pady=5)

        ttk.Label(row5, text="搜索:").pack(side=tk.LEFT, padx=(5, 2))
        self.search_var = tk.StringVar(value="")
        search_entry = ttk.Entry(row5, textvariable=self.search_var, width=14)
        search_entry.pack(side=tk.LEFT, padx=(0, 10))
        # 输入时即时过滤（debounce 交给 _schedule_quick_filter 的 after）
        self.search_var.trace_add("write", lambda *_: self._schedule_quick_filter())

        ttk.Label(row5, text="评分≥").pack(side=tk.LEFT, padx=(0, 2))
        self.min_score_var = tk.StringVar(value="")
        ttk.Entry(row5, textvariable=self.min_score_var, width=5).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Label(row5, text="5日涨幅≥").pack(side=tk.LEFT, padx=(0, 2))
        self.min_five_day_var = tk.StringVar(value="")
        ttk.Entry(row5, textvariable=self.min_five_day_var, width=5).pack(side=tk.LEFT, padx=(0, 2))
        ttk.Label(row5, text="%").pack(side=tk.LEFT, padx=(0, 10))

        ttk.Label(row5, text="放量≥").pack(side=tk.LEFT, padx=(0, 2))
        self.min_volume_ratio_var = tk.StringVar(value="")
        ttk.Entry(row5, textvariable=self.min_volume_ratio_var, width=5).pack(side=tk.LEFT, padx=(0, 2))
        ttk.Label(row5, text="倍").pack(side=tk.LEFT, padx=(0, 10))

        ttk.Label(row5, text="连板≥").pack(side=tk.LEFT, padx=(0, 2))
        self.min_streak_var = tk.StringVar(value="")
        ttk.Entry(row5, textvariable=self.min_streak_var, width=4).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Button(row5, text="应用", command=self.on_quick_filter_apply).pack(side=tk.LEFT, padx=4)
        ttk.Button(row5, text="清空全部", command=self.clear_all_result_filters).pack(side=tk.LEFT, padx=4)

        row6 = ttk.Frame(control_frame)
        row6.pack(fill=tk.X, pady=2)
        ttk.Label(row6, text="只显示:").pack(side=tk.LEFT, padx=5)

        self.only_watchlist_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row6, text="自选", variable=self.only_watchlist_var,
            command=self.on_quick_filter_apply,
        ).pack(side=tk.LEFT, padx=4)

        self.only_limit_up_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row6, text="涨停", variable=self.only_limit_up_var,
            command=self.on_quick_filter_apply,
        ).pack(side=tk.LEFT, padx=4)

        self.only_broken_limit_up_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row6, text="断板", variable=self.only_broken_limit_up_var,
            command=self.on_quick_filter_apply,
        ).pack(side=tk.LEFT, padx=4)

        self.only_volume_expand_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row6, text="放量", variable=self.only_volume_expand_var,
            command=self.on_quick_filter_apply,
        ).pack(side=tk.LEFT, padx=4)

        self.only_strong_ft_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row6, text="承接强势", variable=self.only_strong_ft_var,
            command=self.on_quick_filter_apply,
        ).pack(side=tk.LEFT, padx=4)

        search_entry.bind("<Return>", lambda e: self.on_quick_filter_apply())

    def setup_control_panel(self, parent):
        control_frame = ttk.LabelFrame(parent, text="控制面板", padding="10")
        control_frame.pack(fill=tk.X, pady=5)
        self._build_control_scan_params_row(control_frame)
        self._build_control_actions_row(control_frame)
        self._build_control_board_filter_row(control_frame)
        self._build_control_price_filter_row(control_frame)
        # 快速过滤栏（搜索/评分≥/5日涨幅≥/放量≥/连板≥/只显示...）只对扫描结果有意义，
        # 移到 setup_result_tab 内部，避免在涨停预测/详情等 tab 顶部干扰视线

    def setup_notebook(self, parent):
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=5)

        self.setup_predict_tab()
        self.setup_detail_tab()
        self.setup_intraday_tab()
        self.setup_result_tab()
        self.setup_limit_up_compare_tab()
        self.setup_watchlist_tab()
        self.setup_log_tab()

        # 构建 tab 注册表 + 应用用户配置的可见性（默认只显示预测/详情/分时）
        self._init_tab_visibility()

        # 给所有 Treeview 挂上"截断单元格悬停 tooltip + 表头双击自适应列宽"增强
        try:
            _attach_tree_enhancers(self.root)
        except Exception:
            pass

        self.notebook.bind("<<NotebookTabChanged>>", self._on_notebook_tab_changed)

    def _on_notebook_tab_changed(self, _event=None) -> None:
        try:
            current = self.notebook.nametowidget(self.notebook.select())
        except Exception:
            return
        if current is getattr(self, "detail_tab_frame", None):
            self._set_top_header_for_code(getattr(self, "_current_detail_code", "") or "")
        elif current is getattr(self, "intraday_tab", None):
            self._set_top_header_for_code(
                getattr(self, "_intraday_request_code", "")
                or getattr(self, "_current_detail_code", "")
                or ""
            )
        else:
            self._clear_top_header()

    # ============== Tab 可见性管理 ==============
    _TAB_VISIBILITY_CONFIG_KEY = "visible_tabs"
    _DEFAULT_VISIBLE_TABS = ("predict", "detail", "intraday", "log")

    def _init_tab_visibility(self) -> None:
        """构建 tab 注册表并按用户配置隐藏不需要的 tab。

        默认只显示 涨停预测 / 股票详情 / 分时 / 运行日志；其他通过"视图"菜单随时切换。
        预测/详情/分时/运行日志是核心工作流，作为 always-on 不让用户隐藏掉
        （否则预测候选双击会跳到不存在的 tab；日志关键报错也看不到）。
        """
        # (key, widget, text, can_hide) —— 顺序与 setup_notebook 中 setup_*_tab 调用顺序一致
        self._tab_registry: List[Tuple[str, Any, str, bool]] = [
            ("predict", self.predict_tab, "涨停预测", False),
            ("detail", self.detail_tab_frame, "股票详情", False),
            ("intraday", self.intraday_tab, "分时", False),
            ("result", self.result_tab, "扫描结果", True),
            ("compare", self.compare_tab, "涨停对比", True),
            ("watchlist", self.watchlist_tab, "自选池", True),
            ("log", self.log_tab, "运行日志", False),
        ]

        # 加载用户偏好（首次运行用默认）
        try:
            saved = stock_store.load_app_config(
                self._TAB_VISIBILITY_CONFIG_KEY, default=None,
            )
        except Exception:
            saved = None
        if isinstance(saved, list):
            visible_set = set(str(x) for x in saved)
        else:
            visible_set = set(self._DEFAULT_VISIBLE_TABS)
            # 把默认值写入，让后续 load 拿到一致结果
            try:
                stock_store.save_app_config(
                    self._TAB_VISIBILITY_CONFIG_KEY, sorted(visible_set),
                )
            except Exception:
                pass

        # always-on tab 强制可见
        for key, _w, _t, can_hide in self._tab_registry:
            if not can_hide:
                visible_set.add(key)

        self._visible_tab_set = visible_set

        # 隐藏初始不在可见集合里的 tab
        for key, widget, _text, _can_hide in self._tab_registry:
            if key not in visible_set:
                try:
                    self.notebook.hide(widget)
                except Exception:
                    pass

        # 视图菜单
        self._build_view_menu()

    def _build_view_menu(self) -> None:
        """在已有 menubar 上追加"视图"菜单，每个可隐藏 tab 一个 checkbox。"""
        menubar = self.root.nametowidget(self.root["menu"]) if self.root["menu"] else None
        if menubar is None:
            return
        view_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="视图", menu=view_menu)
        self._tab_visible_vars: Dict[str, tk.BooleanVar] = {}
        for key, _widget, text, can_hide in self._tab_registry:
            if not can_hide:
                continue  # always-on 不进菜单
            var = tk.BooleanVar(value=(key in self._visible_tab_set))
            self._tab_visible_vars[key] = var
            view_menu.add_checkbutton(
                label=f"显示「{text}」",
                variable=var,
                command=lambda k=key: self._toggle_tab_visibility(k),
            )

    def _toggle_tab_visibility(self, key: str) -> None:
        entry = next(
            ((w, t, ch) for k, w, t, ch in self._tab_registry if k == key),
            None,
        )
        if not entry:
            return
        widget, _text, can_hide = entry
        if not can_hide:
            return
        want_show = bool(self._tab_visible_vars[key].get())
        try:
            if want_show:
                # 用 state='normal' 取消隐藏，tab 位置自动保留在注册表原序
                # （hide 不移除 tab 节点，只把 state 设成 hidden；用 insert 反而不能取消隐藏）
                self.notebook.tab(widget, state="normal")
                self._visible_tab_set.add(key)
            else:
                self.notebook.hide(widget)
                self._visible_tab_set.discard(key)
        except Exception:
            return
        try:
            stock_store.save_app_config(
                self._TAB_VISIBILITY_CONFIG_KEY, sorted(self._visible_tab_set),
            )
        except Exception:
            pass

    def setup_result_tab(self):
        result_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(result_frame, text="扫描结果")
        self.result_tab = result_frame

        action_frame = ttk.Frame(result_frame)
        action_frame.pack(fill=tk.X, pady=(0, 6))
        ttk.Button(action_frame, text="导出结果图片", command=self.export_results_image).pack(side=tk.LEFT)
        ttk.Button(action_frame, text="复制代码", command=self.copy_selected_stock_code_name).pack(side=tk.LEFT, padx=8)
        ttk.Button(action_frame, text="加入自选", command=self.add_selected_result_to_watchlist).pack(side=tk.LEFT, padx=8)
        ttk.Button(action_frame, text="移除自选", command=self.remove_selected_result_from_watchlist).pack(side=tk.LEFT)
        ttk.Label(
            action_frame,
            text="导出图片固定仅包含代码和名称两列，按 Ctrl+C 可复制选中股票代码。",
        ).pack(side=tk.RIGHT)

        # 客户端快速过滤栏（搜索/评分/5日涨幅/放量/连板 + 只显示自选/涨停/...）
        # 仅对扫描结果表生效，因此放在本 tab 内部
        self._build_control_quick_filter_row(result_frame)

        # 列定义全部来自 src/gui/result_columns.py 的注册表
        self._result_columns_map = columns_by_id()
        self.result_columns = all_column_ids()
        self.result_headings = {
            col.id: (col.label, col.width) for col in RESULT_COLUMNS
        }
        default_visible_columns = default_visible_ids()

        tree_container = ttk.Frame(result_frame)
        tree_container.pack(fill=tk.BOTH, expand=True)
        tree_container.grid_rowconfigure(0, weight=1)
        tree_container.grid_columnconfigure(0, weight=1)

        self.result_tree = ttk.Treeview(tree_container, columns=self.result_columns, show="headings", height=20)

        self.default_result_display_columns = default_visible_columns
        self.result_column_order = list(self.result_columns)
        self.result_column_vars = {
            col: tk.BooleanVar(value=(col in default_visible_columns))
            for col in self.result_columns
        }
        for col_def in RESULT_COLUMNS:
            self.result_tree.heading(
                col_def.id,
                text=col_def.label,
                command=lambda c=col_def.id: self.on_result_heading_click(c),
            )
            anchor = tk.W if col_def.anchor == "w" else tk.CENTER
            self.result_tree.column(col_def.id, width=col_def.width, anchor=anchor)
        self.result_tree.configure(displaycolumns=default_visible_columns)

        scrollbar = ttk.Scrollbar(tree_container, orient=tk.VERTICAL, command=self.result_tree.yview)
        xscrollbar = ttk.Scrollbar(tree_container, orient=tk.HORIZONTAL, command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=scrollbar.set)
        self.result_tree.configure(xscrollcommand=xscrollbar.set)

        self.result_tree.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        xscrollbar.grid(row=1, column=0, sticky="ew")

        self.result_tree.bind("<<TreeviewSelect>>", self.on_stock_select)
        self.result_tree.bind("<Double-1>", self.on_stock_double_click)
        self.result_tree.bind("<Control-c>", self.copy_selected_stock_code_name)
        self.result_tree.bind("<Control-C>", self.copy_selected_stock_code_name)

    def _visible_result_columns(self) -> tuple[str, ...]:
        ordered_columns = self.result_column_order or list(self.result_columns)
        visible = tuple(
            col
            for col in ordered_columns
            if self.result_column_vars.get(col) and self.result_column_vars[col].get()
        )
        if visible:
            return visible
        return ("code", "name", "latest_close")

    def _save_board_filter_layout(self) -> None:
        payload = {
            "selected": [board for board, var in self.board_filter_vars.items() if var.get()],
        }
        save_app_config("board_filter_layout", payload)

    def _load_board_filter_layout(self) -> None:
        if not self.board_filter_vars:
            return
        payload = load_app_config("board_filter_layout")
        if not isinstance(payload, dict):
            return
        saved_selected = {
            str(board).strip()
            for board in (payload.get("selected") or [])
            if str(board).strip() in self.board_filter_vars
        }
        if not saved_selected:
            return
        for board, var in self.board_filter_vars.items():
            var.set(board in saved_selected)

    def _save_app_settings(self) -> None:
        payload = {
            "history_source": str(self.history_source_var.get() or "auto").strip().lower() or "auto",
            "intraday_source": str(self.intraday_source_var.get() or "auto").strip().lower() or "auto",
            "fund_flow_source": str(self.fund_flow_source_var.get() or "auto").strip().lower() or "auto",
            "limit_up_reason_source": str(self.limit_up_reason_source_var.get() or "auto").strip().lower() or "auto",
        }
        save_app_config("app_settings", payload)

    def _load_app_settings(self) -> None:
        payload = load_app_config("app_settings")
        if not isinstance(payload, dict):
            return
        source = str(payload.get("history_source") or "auto").strip().lower() or "auto"
        if source in DATA_SOURCE_OPTIONS["history"]:
            self.history_source_var.set(source)
        intraday_source = str(payload.get("intraday_source") or "auto").strip().lower() or "auto"
        if intraday_source == "legacy":
            intraday_source = "sina"
        if intraday_source in DATA_SOURCE_OPTIONS["intraday"]:
            self.intraday_source_var.set(intraday_source)
        fund_flow_source = str(payload.get("fund_flow_source") or "auto").strip().lower() or "auto"
        if fund_flow_source in DATA_SOURCE_OPTIONS["fund_flow"]:
            self.fund_flow_source_var.set(fund_flow_source)
        limit_up_reason_source = str(payload.get("limit_up_reason_source") or "auto").strip().lower() or "auto"
        if limit_up_reason_source in DATA_SOURCE_OPTIONS["limit_up_reason"]:
            self.limit_up_reason_source_var.set(limit_up_reason_source)

    def _apply_source_preferences(self) -> None:
        self.stock_filter.set_history_source_preference(self.history_source_var.get())
        self.stock_filter.set_intraday_source_preference(self.intraday_source_var.get())
        self.stock_filter.set_fund_flow_source_preference(self.fund_flow_source_var.get())
        self.stock_filter.set_limit_up_reason_source_preference(self.limit_up_reason_source_var.get())

    def _save_limit_up_compare_snapshot(self, result: Dict[str, Any]) -> None:
        payload = {
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "result": result,
        }
        save_app_config("limit_up_compare_snapshot", payload)
        try:
            save_limit_up_compare_record(result)
        except Exception:
            pass

    def _load_last_limit_up_compare(self) -> None:
        payload = load_app_config("limit_up_compare_snapshot")
        self._refresh_compare_history_dates()
        if not isinstance(payload, dict):
            return
        result = payload.get("result")
        if not isinstance(result, dict):
            return
        today_date = str(result.get("today_date") or "").strip()
        yesterday_date = str(result.get("yesterday_date") or "").strip()
        compare_days = int(result.get("compare_days", 2) or 2)
        if today_date:
            self._zt_today_var.set(today_date)
            if hasattr(self, "_zt_history_var"):
                self._zt_history_var.set(today_date)
        if yesterday_date:
            self._zt_yesterday_var.set(yesterday_date)
        self._zt_compare_days_var.set(str(max(2, compare_days)))
        self._apply_limit_up_compare(result, persist=False, status_message="已从本地恢复涨停对比")

    def _refresh_compare_history_dates(self, select: Optional[str] = None) -> None:
        """刷新涨停对比的历史日期下拉框；可选地选中指定日期。"""
        if not hasattr(self, "_zt_history_combo"):
            return
        try:
            dates = list_limit_up_compare_dates()
        except Exception:
            dates = []
        self._zt_history_combo["values"] = dates
        if select and select in dates:
            self._zt_history_var.set(select)
        elif not self._zt_history_var.get() and dates:
            self._zt_history_var.set(dates[0])

    def _on_compare_history_selected(self, _event=None) -> None:
        today_date = (self._zt_history_var.get() or "").strip()
        if not today_date:
            return
        result = load_limit_up_compare_by_date(today_date)
        if not isinstance(result, dict):
            self._zt_status_label.config(text=f"无 {today_date} 的历史对比")
            return
        yesterday_date = str(result.get("yesterday_date") or "").strip()
        compare_days = int(result.get("compare_days", 2) or 2)
        self._zt_today_var.set(today_date)
        if yesterday_date:
            self._zt_yesterday_var.set(yesterday_date)
        self._zt_compare_days_var.set(str(max(2, compare_days)))
        self._apply_limit_up_compare(
            result, persist=False, status_message=f"已加载 {today_date} 的涨停对比历史",
        )

    def _refresh_selected_compare_date(self) -> None:
        """重新拉取下拉中选中的历史日期对比数据，覆盖原记录。"""
        today_date = (self._zt_history_var.get() or "").strip()
        if not today_date:
            today_date = (self._zt_today_var.get() or "").strip()
        if not today_date:
            self._zt_status_label.config(text="请先选择要刷新的日期")
            return
        self._zt_today_var.set(today_date)
        # 让 _start_limit_up_compare 自行根据 compare_days 决定是否使用 yesterday
        self._start_limit_up_compare()

    # ============== AI 题材聚类（NVIDIA NIM）==============
    def _open_nim_key_dialog(self) -> None:
        """简易对话框：输入并保存 NVIDIA NIM API Key。"""
        from llm_client import _resolve_api_key

        try:
            current = _resolve_api_key()
            current_hint = f"当前已配置（末 4 位 ****{current[-4:]}）"
        except LlmConfigError:
            current_hint = "尚未配置"

        new_key = simpledialog.askstring(
            "NVIDIA NIM API Key",
            f"在 build.nvidia.com 获取免费 API Key（前缀 nvapi-）。\n{current_hint}\n\n"
            "粘贴 API Key（留空取消）：",
            parent=self.root,
            show="*",
        )
        if not new_key or not str(new_key).strip():
            return
        try:
            llm_save_api_key(str(new_key).strip())
            messagebox.showinfo("已保存", "NIM API Key 已保存到本地配置。", parent=self.root)
        except Exception as e:
            messagebox.showerror("保存失败", f"无法保存 API Key: {e}", parent=self.root)

    def _start_ai_theme_clustering(self) -> None:
        """从当前涨停对比结果里提取今日涨停股，调用 LLM 聚类题材。"""
        if not llm_has_api_key():
            messagebox.showwarning(
                "未配置 API Key",
                "请先点击「设置 NIM Key」配置 NVIDIA NIM API Key。",
                parent=self.root,
            )
            return
        if self._zt_compare_result is None:
            messagebox.showinfo(
                "无数据",
                "请先点击「获取涨停对比」加载今日涨停股，再做 AI 题材聚类。",
                parent=self.root,
            )
            return
        today_date = str(self._zt_compare_result.get("today_date") or "").strip()
        records = self._zt_compare_result.get("today_classified") or []
        if not today_date or not records:
            messagebox.showinfo("无数据", "今日涨停股清单为空。", parent=self.root)
            return

        self._zt_status_label.config(text=f"AI 题材聚类中（{today_date}，{len(records)} 只）...")
        self.status_var.set("AI 题材聚类中...")

        self._start_background_job(
            self._load_ai_theme_clustering,
            name="ai-theme-cluster",
            args=(today_date, list(records)),
        )

    def _load_ai_theme_clustering(
        self,
        today_date: str,
        records: List[Dict[str, Any]],
        cancel_token: CancelToken,
    ) -> None:
        try:
            # 先看缓存
            cached = llm_load_cached_themes(today_date)
            if cached is not None:
                if cancel_token.is_cancelled():
                    return
                self._post_to_ui(
                    lambda r=cached: self._apply_ai_theme_clustering(r, from_cache=True)
                )
                return

            # 拉每只票的入选理由
            stocks: List[Dict[str, Any]] = []
            fetcher = self.stock_filter.fetcher
            for rec in records:
                if cancel_token.is_cancelled():
                    return
                code = str(rec.get("code") or "").strip().zfill(6)
                if not code:
                    continue
                try:
                    reason = fetcher.get_limit_up_reason(code, today_date)
                except Exception:
                    reason = ""
                stocks.append({
                    "code": code,
                    "name": rec.get("name", ""),
                    "industry": rec.get("industry", ""),
                    "reason": reason,
                    "consecutive_boards": rec.get("consecutive_boards") or 1,
                })
            if cancel_token.is_cancelled():
                return

            result = llm_cluster_themes(
                stocks,
                trade_date=today_date,
                use_cache=False,
            )
            if cancel_token.is_cancelled():
                return
            self._post_to_ui(lambda r=result: self._apply_ai_theme_clustering(r))
        except LlmConfigError as e:
            err = str(e)
            self._post_to_ui(
                lambda: self._zt_status_label.config(text=f"AI 聚类失败：{err}")
            )
        except LlmRequestError as e:
            err = str(e)
            self._post_to_ui(
                lambda: self._zt_status_label.config(text=f"AI 聚类失败：{err[:80]}")
            )
        except Exception as e:
            err = str(e)
            self._post_to_ui(
                lambda: self._zt_status_label.config(text=f"AI 聚类异常：{err[:80]}")
            )

    def _apply_ai_theme_clustering(
        self,
        result: Dict[str, Any],
        *,
        from_cache: bool = False,
    ) -> None:
        themes = result.get("themes") or []
        market_summary = str(result.get("market_summary") or "").strip()
        model = str(result.get("model") or LLM_DEFAULT_MODEL)
        td = str(result.get("trade_date") or "").strip()
        suffix = "（缓存）" if from_cache else ""

        # 构造代码 → 名称映射，便于在题材里展示名字
        code_to_name: Dict[str, str] = {}
        if self._zt_compare_result is not None:
            for rec in (self._zt_compare_result.get("today_classified") or []):
                code = str(rec.get("code") or "").strip().zfill(6)
                if code:
                    code_to_name[code] = str(rec.get("name") or "")

        self._zt_summary_text.config(state=tk.NORMAL)
        self._zt_summary_text.insert(tk.END, "\n" + "=" * 36 + "\n")
        self._zt_summary_text.insert(tk.END, f"  🤖 AI 题材聚类 {td}{suffix}\n")
        self._zt_summary_text.insert(tk.END, f"  模型：{model}\n")
        self._zt_summary_text.insert(tk.END, "=" * 36 + "\n")
        if market_summary:
            self._zt_summary_text.insert(tk.END, f"\n概览：{market_summary}\n")
        if not themes:
            self._zt_summary_text.insert(tk.END, "\n（未识别出有效题材）\n")
        for t in themes:
            name = str(t.get("name") or "?")
            codes = list(t.get("codes") or [])
            core = str(t.get("core_concept") or "").strip()
            leaders = list(t.get("leaders") or [])
            self._zt_summary_text.insert(tk.END, f"\n· {name}（{len(codes)}只）")
            if core:
                self._zt_summary_text.insert(tk.END, f" — {core}")
            self._zt_summary_text.insert(tk.END, "\n")
            for c in codes[:12]:
                tag = " 🔥" if c in leaders else ""
                self._zt_summary_text.insert(
                    tk.END, f"   {c} {code_to_name.get(c, '')}{tag}\n"
                )
            if len(codes) > 12:
                self._zt_summary_text.insert(tk.END, f"   …还有 {len(codes) - 12} 只\n")
        self._zt_summary_text.config(state=tk.DISABLED)
        self._zt_summary_text.see(tk.END)
        self._zt_status_label.config(
            text=f"AI 题材聚类完成（{len(themes)} 个题材）"
        )
        self.status_var.set(f"AI 题材聚类完成: {len(themes)} 题材")

    def _load_last_limit_up_prediction(self) -> None:
        payload = load_last_limit_up_prediction()
        self._refresh_predict_history_dates()
        if not isinstance(payload, dict):
            return
        trade_date = str(payload.get("trade_date") or "").strip()
        if trade_date:
            self._predict_date_var.set(trade_date)
            if hasattr(self, "_predict_history_var"):
                self._predict_history_var.set(trade_date)
        self._apply_predict_result(payload)

    def _refresh_predict_history_dates(self, select: Optional[str] = None) -> None:
        """刷新历史记录下拉框；可选地选中指定日期。"""
        if not hasattr(self, "_predict_history_combo"):
            return
        try:
            dates = list_limit_up_prediction_dates()
        except Exception:
            dates = []
        self._predict_history_combo["values"] = dates
        if select and select in dates:
            self._predict_history_var.set(select)
        elif not self._predict_history_var.get() and dates:
            self._predict_history_var.set(dates[0])

    def _on_predict_history_selected(self, _event=None) -> None:
        trade_date = (self._predict_history_var.get() or "").strip()
        if not trade_date:
            return
        payload = load_limit_up_prediction_by_date(trade_date)
        if not isinstance(payload, dict):
            self._predict_status_label.config(text=f"无 {trade_date} 的历史预测")
            return
        self._predict_date_var.set(trade_date)
        self._apply_predict_result(payload)
        self.status_var.set(f"已加载 {trade_date} 的涨停预测历史")

    def _refresh_selected_predict_date(self) -> None:
        """重新预测下拉中选中的历史日期，并覆盖原记录。"""
        trade_date = (self._predict_history_var.get() or "").strip()
        if not trade_date:
            trade_date = (self._predict_date_var.get() or "").strip()
        if not trade_date:
            self._predict_status_label.config(text="请先选择要刷新的日期")
            return
        self._predict_date_var.set(trade_date)
        self._start_predict()

    def _save_result_column_layout(self) -> None:
        payload = {
            "order": list(self.result_column_order or self.result_columns),
            "visible": list(self._visible_result_columns()),
        }
        save_app_config("result_column_layout", payload)

    def _load_result_column_layout(self) -> None:
        if not self.result_columns:
            return
        payload = load_app_config("result_column_layout")
        if not isinstance(payload, dict):
            return

        saved_order = payload.get("order") or []
        normalized_order = [col for col in saved_order if col in self.result_columns]
        for col in self.result_columns:
            if col not in normalized_order:
                normalized_order.append(col)
        if normalized_order:
            self.result_column_order = normalized_order

        saved_visible = set(payload.get("visible") or [])
        if saved_visible:
            for col, var in self.result_column_vars.items():
                var.set(col in saved_visible)

    def reset_result_columns(self) -> None:
        self.result_column_order = list(self.result_columns)
        visible = set(self.default_result_display_columns)
        for col, var in self.result_column_vars.items():
            var.set(col in visible)
        self.apply_result_display_columns()

    def apply_result_display_columns(self, save: bool = True) -> None:
        if not hasattr(self, "result_tree"):
            return
        self.result_tree.configure(displaycolumns=self._visible_result_columns())
        if save:
            self._save_result_column_layout()

    def _get_result_display_columns_and_headings(self) -> List[tuple[str, str]]:
        return [
            (col, self.result_headings.get(col, (col, 100))[0])
            for col in self._visible_result_columns()
        ]

    def _format_result_row_values(self, result: Dict[str, Any]) -> Dict[str, str]:
        context = {"watchlist_items": self.watchlist_items}
        return {
            col.id: col.format_cell(result, context)
            for col in RESULT_COLUMNS
        }

    def _build_result_image_pages(self, rows: List[List[str]], page_size: int = 40) -> List[List[List[str]]]:
        if not rows:
            return []
        return [rows[index : index + page_size] for index in range(0, len(rows), page_size)]

    def _get_selected_result_identity(self) -> Optional[tuple[str, str]]:
        selection = self.result_tree.selection()
        if not selection:
            return None
        item = self.result_tree.item(selection[0])
        values = item.get("values") or []
        if len(values) < 2:
            return None
        stock_code = str(values[0]).strip().zfill(6)
        stock_name = str(values[1]).strip()
        if not stock_code or not stock_name:
            return None
        return stock_code, stock_name

    def _load_watchlist_items(self) -> None:
        items = load_watchlist()
        self.watchlist_items = {
            str(item.get("code", "") or "").strip().zfill(6): dict(item)
            for item in items
            if str(item.get("code", "") or "").strip()
        }
        if hasattr(self, "_watch_tree"):
            self.refresh_watchlist_view()
        if self.filtered_stocks:
            self.update_result_table(self.filtered_stocks, announce=False, persist=False)

    def _lookup_result_by_code(self, stock_code: str) -> Optional[Dict[str, Any]]:
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return None
        for result in self.all_scan_results:
            if str(result.get("code", "") or "").strip().zfill(6) == code:
                return result
        for result in self.filtered_stocks:
            if str(result.get("code", "") or "").strip().zfill(6) == code:
                return result
        return None

    def _build_watchlist_item_payload(
        self,
        stock_code: str,
        stock_name: str = "",
        status: str = "",
        note: Optional[str] = None,
        detail: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        code = str(stock_code or "").strip().zfill(6)
        existing = self.watchlist_items.get(code, {})
        result = self._lookup_result_by_code(code)
        data = result.get("data", {}) if result else {}
        analysis = data.get("analysis") if data else {}
        board = data.get("board", "") if data else ""

        if detail:
            analysis = detail.get("analysis") or analysis or {}
            board = detail.get("board", "") or board
            stock_name = stock_name or str(detail.get("name", "") or "")
        elif result:
            stock_name = stock_name or str(result.get("name", "") or "")

        payload = {
            "code": code,
            "name": stock_name or existing.get("name", ""),
            "status": status or existing.get("status", "") or "观察",
            "note": existing.get("note", "") if note is None else note,
            "board": board or existing.get("board", ""),
            "latest_close": analysis.get("latest_close") if isinstance(analysis, dict) else existing.get("latest_close"),
            "score": analysis.get("score") if isinstance(analysis, dict) else existing.get("score"),
            "score_breakdown": (
                analysis.get("score_breakdown", "") if isinstance(analysis, dict) else existing.get("score_breakdown", "")
            ),
            "added_at": existing.get("added_at", ""),
        }
        return payload

    def show_column_picker(self) -> None:
        picker = tk.Toplevel(self.root)
        picker.title("列表列设置")
        picker.geometry("520x460")
        picker.transient(self.root)
        picker.grab_set()

        frame = ttk.Frame(picker, padding="16")
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="可调整列顺序和显示状态，设置会自动保存。").pack(anchor=tk.W, pady=(0, 10))

        body = ttk.Frame(frame)
        body.pack(fill=tk.BOTH, expand=True)

        list_frame = ttk.Frame(body)
        list_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        column_listbox = tk.Listbox(list_frame, height=14, activestyle="dotbox")
        column_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        list_scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=column_listbox.yview)
        list_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        column_listbox.configure(yscrollcommand=list_scrollbar.set)

        button_frame = ttk.Frame(body)
        button_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(12, 0))

        def refresh_column_listbox(keep_selection: Optional[int] = None) -> None:
            column_listbox.delete(0, tk.END)
            for col in self.result_column_order:
                label, _ = self.result_headings[col]
                flag = "显示" if self.result_column_vars[col].get() else "隐藏"
                column_listbox.insert(tk.END, f"[{flag}] {label}")
            if keep_selection is None and self.result_column_order:
                keep_selection = 0
            if keep_selection is not None and self.result_column_order:
                keep_selection = max(0, min(keep_selection, len(self.result_column_order) - 1))
                column_listbox.selection_clear(0, tk.END)
                column_listbox.selection_set(keep_selection)
                column_listbox.activate(keep_selection)

        def selected_index() -> Optional[int]:
            selection = column_listbox.curselection()
            if not selection:
                return None
            return int(selection[0])

        def move_selected(offset: int) -> None:
            index = selected_index()
            if index is None:
                return
            new_index = index + offset
            if new_index < 0 or new_index >= len(self.result_column_order):
                return
            self.result_column_order[index], self.result_column_order[new_index] = (
                self.result_column_order[new_index],
                self.result_column_order[index],
            )
            self.apply_result_display_columns()
            refresh_column_listbox(new_index)

        def toggle_selected() -> None:
            index = selected_index()
            if index is None:
                return
            col = self.result_column_order[index]
            self.result_column_vars[col].set(not self.result_column_vars[col].get())
            self.apply_result_display_columns()
            refresh_column_listbox(index)

        def show_all() -> None:
            for var in self.result_column_vars.values():
                var.set(True)
            self.apply_result_display_columns()
            refresh_column_listbox(selected_index())

        def show_core() -> None:
            core = {
                "code",
                "name",
                "watch",
                "score",
                "board",
                "latest_close",
                "latest_ma",
                "five_day_return",
                "limit_up_streak",
                "broken_limit_up",
                "volume_expand_ratio",
                "volume_expand",
                "volume_break_limit_up",
                "after_two_limit_up",
                "limit_up",
            }
            for col, var in self.result_column_vars.items():
                var.set(col in core)
            self.apply_result_display_columns()
            refresh_column_listbox(selected_index())

        def reset_columns() -> None:
            self.reset_result_columns()
            refresh_column_listbox(0)

        ttk.Button(button_frame, text="上移", command=lambda: move_selected(-1)).pack(fill=tk.X, pady=4)
        ttk.Button(button_frame, text="下移", command=lambda: move_selected(1)).pack(fill=tk.X, pady=4)
        ttk.Button(button_frame, text="显示/隐藏", command=toggle_selected).pack(fill=tk.X, pady=4)
        ttk.Button(button_frame, text="显示核心列", command=show_core).pack(fill=tk.X, pady=(16, 4))
        ttk.Button(button_frame, text="显示全部列", command=show_all).pack(fill=tk.X, pady=4)
        ttk.Button(button_frame, text="重置列", command=reset_columns).pack(fill=tk.X, pady=4)

        action_row = ttk.Frame(frame)
        action_row.pack(fill=tk.X, pady=(12, 0))
        ttk.Button(action_row, text="关闭", command=picker.destroy).pack(side=tk.RIGHT)

        refresh_column_listbox(0)

    def setup_detail_tab(self):
        detail_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(detail_frame, text="股票详情")
        self.detail_tab_frame = detail_frame

        info_header = ttk.Frame(detail_frame)
        info_header.pack(fill=tk.X, pady=(5, 2))
        self.detail_info_header = info_header
        self.detail_summary_toggle_btn = ttk.Button(
            info_header,
            text="展开历史摘要",
            command=self.toggle_detail_summary_section,
        )
        self.detail_summary_toggle_btn.pack(side=tk.LEFT)
        self.detail_watch_btn = ttk.Button(
            info_header,
            text="加入自选",
            command=self.toggle_current_detail_watchlist,
        )
        self.detail_watch_btn.pack(side=tk.LEFT, padx=(8, 0))
        self.detail_watch_note_btn = ttk.Button(
            info_header,
            text="编辑备注",
            command=self.edit_current_detail_watch_note,
        )
        self.detail_watch_note_btn.pack(side=tk.LEFT, padx=(8, 0))
        self.detail_summary_status_var = tk.StringVar(value="历史摘要已收起")
        ttk.Label(info_header, textvariable=self.detail_summary_status_var).pack(side=tk.LEFT, padx=10)

        self.info_frame = ttk.LabelFrame(detail_frame, text="历史摘要", padding="10")
        self.info_frame.pack(fill=tk.X, pady=5)

        self.detail_labels: Dict[str, ttk.Label] = {}
        self.detail_label_caption_vars: Dict[str, tk.StringVar] = {}
        items = [
            ("code", "股票代码"),
            ("name", "股票名称"),
            ("industry", "行业"),
            ("latest_date", "最新日期"),
            ("quote_time", "刷新时间"),
            ("latest_close", "最新收盘"),
            ("score", "综合评分"),
            ("watch_status", "自选状态"),
            ("latest_ma", f"MA{max(1, int(self.stock_filter.ma_period))}"),
            ("latest_ma10", "MA10"),
            ("latest_volume", "成交量"),
            ("latest_amount", "成交额"),
            ("five_day_return", "5日涨幅"),
            ("limit_up", "涨停"),
            ("volume_expand", "放量"),
            ("volume_expand_ratio", "放量倍数"),
            ("big_order_amount", "大单净额"),
            ("main_force_amount", "主力净额"),
            ("macd", "MACD"),
            ("kdj", "KDJ"),
            ("rsi", "RSI"),
            ("boll", "BOLL"),
            ("summary", "结论"),
        ]

        for i, (key, label) in enumerate(items):
            row = i // 3
            col = (i % 3) * 2
            caption_var = tk.StringVar(value=f"{label}:")
            self.detail_label_caption_vars[key] = caption_var
            ttk.Label(self.info_frame, textvariable=caption_var).grid(row=row, column=col, padx=5, pady=5, sticky=tk.E)
            self.detail_labels[key] = ttk.Label(self.info_frame, text="-", width=30)
            self.detail_labels[key].grid(row=row, column=col + 1, padx=5, pady=5, sticky=tk.W)

        self.info_frame.pack_forget()

        chart_header = ttk.Frame(detail_frame)
        chart_header.pack(fill=tk.X, pady=(6, 2))
        self.detail_chart_header = chart_header
        self.detail_chart_toggle_btn = ttk.Button(
            chart_header,
            text="收起历史K线",
            command=self.toggle_detail_chart_section,
        )
        self.detail_chart_toggle_btn.pack(side=tk.LEFT)
        self.detail_chart_status_var = tk.StringVar(value="历史K线已展开")
        ttk.Label(chart_header, textvariable=self.detail_chart_status_var).pack(side=tk.LEFT, padx=10)

        self.detail_flow_toggle_btn = ttk.Button(
            chart_header,
            text="展开大单净额",
            command=self.toggle_detail_flow_section,
        )
        self.detail_flow_toggle_btn.pack(side=tk.RIGHT)
        self.detail_flow_status_var = tk.StringVar(value="大单净额已收起")
        ttk.Label(chart_header, textvariable=self.detail_flow_status_var).pack(side=tk.RIGHT, padx=10)

        self.chart_frame = ttk.LabelFrame(detail_frame, text="K线图", padding="5")
        self.chart_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.chart_body = ttk.Frame(self.chart_frame)
        self.chart_body.pack(fill=tk.BOTH, expand=True)

        self.fig, (self.price_ax, self.volume_ax, self.flow_ax) = plt.subplots(
            3,
            1,
            figsize=(12.8, 10.0),
            sharex=True,
            gridspec_kw={"height_ratios": [4.8, 1.25, 1.25]},
        )
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.chart_body)
        canvas_widget = self.canvas.get_tk_widget()
        canvas_widget.pack(fill=tk.BOTH, expand=True)
        self.canvas.mpl_connect("button_press_event", self.on_detail_chart_click)
        self.canvas.mpl_connect("scroll_event", self.on_detail_chart_scroll)
        self.canvas.mpl_connect("motion_notify_event", self.on_detail_chart_drag_motion)
        self.canvas.mpl_connect("button_release_event", self.on_detail_chart_drag_release)
        self._bind_detail_chart_scroll(canvas_widget)

        slider_row = ttk.Frame(self.chart_body)
        slider_row.pack(fill=tk.X, pady=(6, 0))
        ttk.Label(slider_row, text="左右滑动").pack(side=tk.LEFT)
        self.detail_chart_window_var = tk.DoubleVar(value=0.0)
        self.detail_chart_window_scale = tk.Scale(
            slider_row,
            orient=tk.HORIZONTAL,
            from_=0,
            to=0,
            resolution=1,
            showvalue=False,
            variable=self.detail_chart_window_var,
            command=self.on_detail_chart_window_changed,
            length=480,
        )
        self.detail_chart_window_scale.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=8)
        self.detail_chart_window_label_var = tk.StringVar(value="窗口: -")
        ttk.Label(slider_row, textvariable=self.detail_chart_window_label_var).pack(side=tk.RIGHT)

        self.detail_chart_placeholder = None
        self._apply_detail_section_visibility()

    def setup_intraday_tab(self):
        intraday_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(intraday_frame, text="分时")
        self.intraday_tab = intraday_frame

        info = ttk.Frame(intraday_frame)
        info.pack(fill=tk.X, pady=(0, 6))
        self.intraday_title_var = tk.StringVar(value="分时图（点击 K 线打开）")
        ttk.Label(info, textvariable=self.intraday_title_var).pack(side=tk.LEFT)
        self.intraday_day_var = tk.StringVar(value="交易日: -")
        ttk.Label(info, textvariable=self.intraday_day_var).pack(side=tk.LEFT, padx=(12, 8))
        self.intraday_prev_btn = ttk.Button(info, text="前一天", command=lambda: self.navigate_intraday_day(-1), state=tk.DISABLED)
        self.intraday_prev_btn.pack(side=tk.RIGHT, padx=(6, 0))
        self.intraday_next_btn = ttk.Button(info, text="后一天", command=lambda: self.navigate_intraday_day(1), state=tk.DISABLED)
        self.intraday_next_btn.pack(side=tk.RIGHT)

        chart_frame = ttk.LabelFrame(intraday_frame, text="分时走势", padding="5")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.intraday_fig = Figure(figsize=(11, 6.8), dpi=100)
        gs = self.intraday_fig.add_gridspec(
            2,
            2,
            width_ratios=[4.0, 1.4],
            height_ratios=[3.0, 1.2],
            wspace=0.32,
            hspace=0.14,
        )
        self.intraday_price_ax = self.intraday_fig.add_subplot(gs[0, 0])
        self.intraday_volume_ax = self.intraday_fig.add_subplot(gs[1, 0], sharex=self.intraday_price_ax)
        self.intraday_dist_ax = self.intraday_fig.add_subplot(gs[:, 1])
        self.intraday_canvas = FigureCanvasTkAgg(self.intraday_fig, master=chart_frame)
        self.intraday_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self._draw_intraday_loading("点击详情页 K 线打开分时")

    # ================= 涨停对比 Tab =================
    def setup_limit_up_compare_tab(self):
        compare_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(compare_frame, text="涨停对比")
        self.compare_tab = compare_frame

        style = ttk.Style()
        style.configure("ZT.Treeview", rowheight=24)
        style.map(
            "ZT.Treeview",
            background=[("selected", "#2f6fd6")],
            foreground=[("selected", "#ffffff")],
        )

        # ---- 操作栏 ----
        action_bar = ttk.Frame(compare_frame)
        action_bar.pack(fill=tk.X, pady=(0, 6))
        ttk.Button(action_bar, text="获取涨停对比", command=self._start_limit_up_compare).pack(side=tk.LEFT)
        ttk.Label(action_bar, text="今日:").pack(side=tk.LEFT, padx=(12, 2))
        self._zt_today_var = tk.StringVar(value=datetime.now().strftime("%Y%m%d"))
        ttk.Entry(action_bar, textvariable=self._zt_today_var, width=10).pack(side=tk.LEFT)
        ttk.Button(
            action_bar, text="今天", width=5,
            command=self._zt_fill_today_and_prev,
        ).pack(side=tk.LEFT, padx=(2, 0))
        ttk.Label(action_bar, text="昨日:").pack(side=tk.LEFT, padx=(8, 2))
        self._zt_yesterday_var = tk.StringVar()
        ttk.Entry(action_bar, textvariable=self._zt_yesterday_var, width=10).pack(side=tk.LEFT)
        ttk.Label(action_bar, text="对比天数:").pack(side=tk.LEFT, padx=(10, 2))
        self._zt_compare_days_var = tk.StringVar(value="2")
        ttk.Entry(action_bar, textvariable=self._zt_compare_days_var, width=4).pack(side=tk.LEFT)
        ttk.Label(action_bar, text="(>2 时自动回看最近N个交易日)").pack(side=tk.LEFT, padx=4)
        ttk.Button(
            action_bar, text="AI 聚类题材",
            command=self._start_ai_theme_clustering,
        ).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(
            action_bar, text="设置 NIM Key",
            command=self._open_nim_key_dialog,
        ).pack(side=tk.LEFT, padx=(4, 0))
        self._zt_status_label = ttk.Label(action_bar, text="")
        self._zt_status_label.pack(side=tk.RIGHT, padx=8)

        # 历史记录选择 + 刷新此日期
        ttk.Button(
            action_bar, text="刷新此日期",
            command=self._refresh_selected_compare_date,
        ).pack(side=tk.RIGHT, padx=(4, 0))
        ttk.Label(action_bar, text="历史记录:").pack(side=tk.RIGHT, padx=(12, 2))
        self._zt_history_var = tk.StringVar(value="")
        self._zt_history_combo = ttk.Combobox(
            action_bar, textvariable=self._zt_history_var,
            width=12, state="readonly", values=(),
        )
        self._zt_history_combo.pack(side=tk.RIGHT)
        self._zt_history_combo.bind(
            "<<ComboboxSelected>>", self._on_compare_history_selected,
        )

        # ---- 主区域：左侧摘要 + 右侧表格 ----
        body = ttk.PanedWindow(compare_frame, orient=tk.HORIZONTAL)
        body.pack(fill=tk.BOTH, expand=True)

        # 左侧：摘要面板
        summary_frame = ttk.LabelFrame(body, text="对比摘要 + 形态分布", padding="6")
        self._zt_summary_text = scrolledtext.ScrolledText(summary_frame, width=42, height=30, wrap=tk.WORD)
        self._zt_summary_text.pack(fill=tk.BOTH, expand=True)
        self._zt_summary_text.insert(tk.END, "点击「获取涨停对比」开始分析\n\n"
            "分析每只涨停股的技术形态:\n"
            "  - 回踩MA5涨停: 前日触及五日线后反弹\n"
            "  - 超跌反弹涨停: 下跌趋势中突然涨停\n"
            "  - 趋势加速涨停: 均线多头中涨停加速\n"
            "  - 高位连板: 连续涨停\n"
            "  - 断板反包: 近期涨停被打掉后今日反包阴线\n"
            "  - 突破平台涨停: 横盘整理后突破\n"
            "  - 首板低位涨停: 低位首次涨停\n")
        self._zt_summary_text.config(state=tk.DISABLED)
        body.add(summary_frame, weight=2)

        # 右侧：表格区
        table_frame = ttk.Frame(body)
        self._zt_table_nb = ttk.Notebook(table_frame)
        self._zt_table_nb.pack(fill=tk.BOTH, expand=True)

        # 今日涨停形态分类 Tab（核心表格）
        pattern_tab = ttk.Frame(self._zt_table_nb)
        self._zt_table_nb.add(pattern_tab, text="今日涨停形态分类")
        zt_pattern_cols = ("code", "name", "industry", "pattern", "change_pct", "close",
                           "burst", "dist_ma5", "trend_10d", "pos_60d", "detail")
        self._zt_pattern_tree = ttk.Treeview(
            pattern_tab,
            columns=zt_pattern_cols,
            show="headings",
            height=22,
            style="ZT.Treeview",
        )
        for col, (heading, w) in {
            "code": ("代码", 70), "name": ("名称", 85), "industry": ("行业", 85),
            "pattern": ("技术形态", 110), "change_pct": ("涨跌幅%", 70), "close": ("最新价", 70),
            "burst": ("放量倍数", 80),
            "dist_ma5": ("距MA5%", 70), "trend_10d": ("10日涨幅%", 80),
            "pos_60d": ("60日分位%", 80), "detail": ("形态说明", 220),
        }.items():
            self._zt_pattern_tree.heading(col, text=heading)
            self._zt_pattern_tree.column(col, width=w, anchor=tk.CENTER if col != "detail" else tk.W)
        sb_p = ttk.Scrollbar(pattern_tab, orient=tk.VERTICAL, command=self._zt_pattern_tree.yview)
        self._zt_pattern_tree.configure(yscrollcommand=sb_p.set)
        sb_p.pack(side=tk.RIGHT, fill=tk.Y)
        self._zt_pattern_tree.pack(fill=tk.BOTH, expand=True)
        self._zt_pattern_tree.bind("<<TreeviewSelect>>", self.on_zt_stock_select)
        self._zt_pattern_tree.bind("<Double-1>", self.on_zt_stock_double_click)
        # 行标签色
        self._zt_pattern_tree.tag_configure("pat_ma5", background="#e8f5e9", foreground="#1f1f1f")
        self._zt_pattern_tree.tag_configure("pat_oversold", background="#fff3e0", foreground="#1f1f1f")
        self._zt_pattern_tree.tag_configure("pat_trend", background="#e3f2fd", foreground="#1f1f1f")
        self._zt_pattern_tree.tag_configure("pat_streak", background="#fce4ec", foreground="#1f1f1f")
        self._zt_pattern_tree.tag_configure("pat_breakout", background="#f3e5f5", foreground="#1f1f1f")
        self._zt_pattern_tree.tag_configure("pat_lowpos", background="#e0f7fa", foreground="#1f1f1f")
        self._zt_pattern_tree.tag_configure("pat_burst", background="#ffe9d6", foreground="#1f1f1f")
        self._zt_pattern_tree.tag_configure("pat_wrap", background="#ffcdd2", foreground="#1f1f1f")

        # 昨日首板今日表现 Tab
        yest_tab = ttk.Frame(self._zt_table_nb)
        self._zt_yest_tab = yest_tab
        self._zt_table_nb.add(yest_tab, text="昨日首板今日表现")
        zt_cols_yest = ("code", "name", "industry", "pattern", "today_chg", "close", "still_zt", "status")
        self._zt_yest_tree = ttk.Treeview(
            yest_tab,
            columns=zt_cols_yest,
            show="headings",
            height=22,
            style="ZT.Treeview",
        )
        for col, (heading, w) in {
            "code": ("代码", 70), "name": ("名称", 85), "industry": ("行业", 85),
            "pattern": ("昨日形态", 110), "today_chg": ("今日涨跌%", 80), "close": ("最新价", 70),
            "still_zt": ("继续涨停", 70), "status": ("状态", 70),
        }.items():
            self._zt_yest_tree.heading(col, text=heading)
            self._zt_yest_tree.column(col, width=w, anchor=tk.CENTER)
        sb2 = ttk.Scrollbar(yest_tab, orient=tk.VERTICAL, command=self._zt_yest_tree.yview)
        self._zt_yest_tree.configure(yscrollcommand=sb2.set)
        sb2.pack(side=tk.RIGHT, fill=tk.Y)
        self._zt_yest_tree.pack(fill=tk.BOTH, expand=True)
        self._zt_yest_tree.bind("<<TreeviewSelect>>", self.on_zt_stock_select)
        self._zt_yest_tree.bind("<Double-1>", self.on_zt_stock_double_click)

        body.add(table_frame, weight=4)

        self._zt_compare_thread = None
        self._zt_compare_result: Optional[Dict[str, Any]] = None

    _ZT_PATTERN_TAG_MAP = {
        "暴量涨停": "pat_burst",
        "回踩MA5涨停": "pat_ma5",
        "超跌反弹涨停": "pat_oversold",
        "趋势加速涨停": "pat_trend",
        "高位连板": "pat_streak",
        "断板反包": "pat_wrap",
        "突破平台涨停": "pat_breakout",
        "首板低位涨停": "pat_lowpos",
    }

    def _zt_fill_today_and_prev(self) -> None:
        """一键填好「今日 = 今天」「昨日 = 上一个交易日」。

        优先使用真实交易日历（节假日感知），日历不可用时退回到周末过滤。
        """
        today = datetime.now().date()
        cal = _get_trade_calendar()
        try:
            prev = _previous_trading_day(today, cal)
            prev_str = prev.strftime("%Y%m%d")
        except Exception:
            prev_str = self._estimate_yesterday(today.strftime("%Y%m%d"))
        self._zt_today_var.set(today.strftime("%Y%m%d"))
        self._zt_yesterday_var.set(prev_str)

    def _estimate_yesterday(self, today_str: str) -> str:
        from datetime import timedelta
        try:
            d = datetime.strptime(today_str.strip(), "%Y%m%d").date()
        except (ValueError, TypeError):
            d = datetime.now().date()
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d.strftime("%Y%m%d")

    def _start_limit_up_compare(self):
        if self._zt_compare_thread is not None and self._zt_compare_thread.is_alive():
            return
        today = self._zt_today_var.get().strip()
        if not today:
            today = datetime.now().strftime("%Y%m%d")
            self._zt_today_var.set(today)
        try:
            compare_days = max(2, min(int(self._zt_compare_days_var.get().strip() or "2"), 15))
        except ValueError:
            compare_days = 2
        self._zt_compare_days_var.set(str(compare_days))
        yesterday = self._zt_yesterday_var.get().strip()
        if compare_days <= 2 and not yesterday:
            yesterday = self._estimate_yesterday(today)
            self._zt_yesterday_var.set(yesterday)

        self._zt_summary_text.config(state=tk.NORMAL)
        self._zt_summary_text.delete("1.0", tk.END)
        if compare_days > 2:
            self._zt_summary_text.insert(tk.END, f"正在获取截至 {today} 的最近 {compare_days} 个交易日涨停对比...\n")
        else:
            self._zt_summary_text.insert(tk.END, f"正在获取 {today} vs {yesterday} 涨停数据...\n")
        self._zt_summary_text.config(state=tk.DISABLED)
        self._zt_status_label.config(text="获取涨停池...")
        self.status_var.set("正在获取涨停对比...")

        self._zt_compare_thread, _ = self._start_background_job(
            self._load_limit_up_compare,
            name="limit-up-compare",
            args=(today, yesterday, compare_days),
        )

    def _load_limit_up_compare(self, today: str, yesterday: str, compare_days: int, cancel_token: CancelToken):
        try:
            if cancel_token.is_cancelled():
                return
            # 阶段1：获取涨停池对比数据
            if compare_days > 2:
                result = self.stock_filter.fetcher.compare_limit_up_pools_window(today, compare_days)
            else:
                result = self.stock_filter.fetcher.compare_limit_up_pools(today, yesterday)
                result["compare_days"] = 2
                result["trade_dates"] = [result.get("yesterday_date", ""), result.get("today_date", "")]
                result["daily_stats"] = []
            if cancel_token.is_cancelled():
                return
            self._post_to_ui(lambda: self._zt_status_label.config(
                text=f"涨停池获取完成，正在分析今日 {len(result.get('today_first', []))} 只首板形态..."
            ))

            # 阶段2：对今日首板做技术形态分类
            today_first = result.get("today_first", [])
            def _progress(cur, tot, info):
                if cancel_token.is_cancelled():
                    raise StopIteration
                self._post_to_ui(lambda c=cur, t=tot, i=info:
                    self._zt_status_label.config(text=f"分类今日首板 {c}/{t}: {i}"))
            today_classified = self.stock_filter.classify_limit_up_pool(today_first, progress_callback=_progress)
            result["today_classified"] = today_classified
            if cancel_token.is_cancelled():
                return

            # 阶段3：对昨日首板做技术形态分类
            yesterday_first = result.get("yesterday_first", [])
            if yesterday_first:
                self._post_to_ui(lambda: self._zt_status_label.config(
                    text=f"正在分析上一交易日 {len(yesterday_first)} 只首板形态..."))
                def _progress2(cur, tot, info):
                    if cancel_token.is_cancelled():
                        raise StopIteration
                    self._post_to_ui(lambda c=cur, t=tot, i=info:
                        self._zt_status_label.config(text=f"分类上一交易日首板 {c}/{t}: {i}"))
                yesterday_classified = self.stock_filter.classify_limit_up_pool(yesterday_first, progress_callback=_progress2)
                result["yesterday_classified"] = yesterday_classified

            if cancel_token.is_cancelled():
                return
            self._post_to_ui(lambda r=result: self._apply_limit_up_compare(r))
        except StopIteration:
            self._post_to_ui(lambda: self._zt_status_label.config(text="已取消"))
        except Exception as e:
            err = str(e)
            self._post_to_ui(lambda: self._zt_show_error(f"涨停对比失败: {err}"))

    def _zt_show_error(self, msg: str):
        self._zt_summary_text.config(state=tk.NORMAL)
        self._zt_summary_text.delete("1.0", tk.END)
        self._zt_summary_text.insert(tk.END, msg)
        self._zt_summary_text.config(state=tk.DISABLED)
        self._zt_status_label.config(text="")
        self.status_var.set("涨停对比失败")

    def _infer_board_from_code(self, code: str) -> str:
        c = str(code).strip().zfill(6)
        if c.startswith(("300", "301")):
            return "创业板"
        if c.startswith("688"):
            return "科创板"
        if c.startswith(("000", "001", "002", "003")):
            return "深交所主板"
        if c.startswith(("5", "6", "9")):
            return "上交所主板"
        return ""

    def _zt_filter_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """按当前控制面板的板块和价格筛选涨停分类记录。"""
        allowed_boards = {str(b).strip() for b in self._selected_boards() if str(b).strip()}
        try:
            min_price = float(self.min_price_var.get().strip()) if self.min_price_var.get().strip() else None
        except ValueError:
            min_price = None
        try:
            max_price = float(self.max_price_var.get().strip()) if self.max_price_var.get().strip() else None
        except ValueError:
            max_price = None

        filtered: List[Dict[str, Any]] = []
        for rec in records:
            code = str(rec.get("code", "")).strip().zfill(6)
            board = self._infer_board_from_code(code)
            if allowed_boards and board not in allowed_boards:
                continue
            close = rec.get("close")
            if close is not None:
                if min_price is not None and close < min_price:
                    continue
                if max_price is not None and close > max_price:
                    continue
            filtered.append(rec)
        return filtered

    def _refresh_zt_compare_display(self):
        """用当前筛选条件重新渲染涨停对比（不重新拉数据）。"""
        if self._zt_compare_result is None:
            return
        self._apply_limit_up_compare(
            self._zt_compare_result, persist=False, status_message="涨停对比已按筛选条件更新"
        )

    def _refresh_predict_display_if_ready(self):
        """顶部价格/板块筛选变化时同步刷新涨停预测表（如已有预测结果）。"""
        if not getattr(self, "_predict_lists", None):
            return
        try:
            self._render_predict_trees()
        except Exception:
            pass

    def _apply_limit_up_compare(
        self,
        result: Dict[str, Any],
        *,
        persist: bool = True,
        status_message: str = "涨停对比完成",
    ):
        self._zt_compare_result = result
        # 应用板块 + 价格过滤
        today_classified = self._zt_filter_records(result.get("today_classified", []))
        yesterday_classified = self._zt_filter_records(result.get("yesterday_classified", []))
        compare_days = int(result.get("compare_days", 2) or 2)
        trade_dates = [d for d in result.get("trade_dates", []) if d]
        daily_stats = result.get("daily_stats", []) or []
        ref_label = "昨日" if compare_days <= 2 else "上一交易日"

        # 筛选状态提示
        total_today = len(result.get("today_classified", []))
        total_yest = len(result.get("yesterday_classified", []))
        filter_active = len(today_classified) < total_today or len(yesterday_classified) < total_yest
        filter_hint = ""
        if filter_active:
            boards_text = "/".join(self._selected_boards())
            parts = [f"板块={boards_text}"]
            min_p = self.min_price_var.get().strip()
            max_p = self.max_price_var.get().strip()
            if min_p or max_p:
                parts.append(f"价格={min_p or '*'}~{max_p or '*'}")
            filter_hint = (
                f"[筛选中: {', '.join(parts)}] "
                f"今日 {len(today_classified)}/{total_today}，{ref_label} {len(yesterday_classified)}/{total_yest}"
            )

        # ---- 统计形态分布 ----
        pattern_counts_today: Dict[str, int] = {}
        for rec in today_classified:
            p = rec.get("pattern", "其他涨停")
            pattern_counts_today[p] = pattern_counts_today.get(p, 0) + 1

        pattern_counts_yest: Dict[str, int] = {}
        for rec in yesterday_classified:
            p = rec.get("pattern", "其他涨停")
            pattern_counts_yest[p] = pattern_counts_yest.get(p, 0) + 1
        today_burst_count = sum(1 for rec in today_classified if rec.get("is_volume_burst"))
        yest_burst_count = sum(1 for rec in yesterday_classified if rec.get("is_volume_burst"))

        # ---- 填充摘要 ----
        self._zt_summary_text.config(state=tk.NORMAL)
        self._zt_summary_text.delete("1.0", tk.END)
        txt = self._zt_summary_text

        txt.insert(tk.END, result.get("summary", "") + "\n")
        if filter_hint:
            txt.insert(tk.END, f"\n{filter_hint}\n")
        if today_burst_count or yest_burst_count:
            txt.insert(tk.END, f"今日暴量涨停: {today_burst_count} 只；{ref_label}暴量涨停: {yest_burst_count} 只\n")

        if compare_days > 2 and daily_stats:
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, f"  最近 {compare_days} 个交易日逐日统计\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for item in daily_stats:
                industries_text = "、".join(f"{name}({count})" for name, count in item.get("top_industries", [])) or "-"
                txt.insert(
                    tk.END,
                    f"  {item.get('trade_date', '-')}: 涨停 {item.get('pool_count', 0):3d} 只 | "
                    f"首板 {item.get('first_count', 0):3d} 只 | TOP行业 {industries_text}\n",
                )

        txt.insert(tk.END, f"\n{'='*36}\n")
        txt.insert(tk.END, f"  今日首板形态分布 ({len(today_classified)} 只)\n")
        txt.insert(tk.END, f"{'='*36}\n")
        for p, c in sorted(pattern_counts_today.items(), key=lambda x: -x[1]):
            pct = c / max(len(today_classified), 1) * 100
            bar = "#" * int(pct / 3)
            txt.insert(tk.END, f"  {p:10s}  {c:3d} 只  {pct:5.1f}%  {bar}\n")

        if pattern_counts_yest:
            all_patterns = sorted(
                set(list(pattern_counts_today.keys()) + list(pattern_counts_yest.keys())),
                key=lambda p: (-pattern_counts_today.get(p, 0), -pattern_counts_yest.get(p, 0), p),
            )
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, f"  各涨停类型数量对比（今日 vs {ref_label}）\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for p in all_patterns:
                t = pattern_counts_today.get(p, 0)
                y = pattern_counts_yest.get(p, 0)
                delta = t - y
                sign = "+" if delta > 0 else ""
                txt.insert(tk.END, f"  {p:10s}  {y:2d} -> {t:2d}  ({sign}{delta})\n")

            txt.insert(tk.END, f"\n{'='*36}\n")
            label_date = result.get("yesterday_date", trade_dates[-2] if len(trade_dates) >= 2 else "")
            txt.insert(tk.END, f"  {ref_label}首板形态分布 ({label_date}, {len(yesterday_classified)} 只)\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for p, c in sorted(pattern_counts_yest.items(), key=lambda x: -x[1]):
                pct = c / max(len(yesterday_classified), 1) * 100
                bar = "#" * int(pct / 3)
                txt.insert(tk.END, f"  {p:10s}  {c:3d} 只  {pct:5.1f}%  {bar}\n")

        # 行业分布
        ind_today = result.get("industry_today", {})
        ind_yest = result.get("industry_yesterday", {})
        if ind_today:
            txt.insert(tk.END, "\n── 今日首板 TOP 行业 ──\n")
            for k, v in sorted(ind_today.items(), key=lambda x: -x[1])[:8]:
                txt.insert(tk.END, f"  {k}: {v} 只\n")
        if ind_yest:
            txt.insert(tk.END, f"\n── {ref_label}首板 TOP 行业 ──\n")
            for k, v in sorted(ind_yest.items(), key=lambda x: -x[1])[:8]:
                txt.insert(tk.END, f"  {k}: {v} 只\n")

        continued = result.get("continued_codes", [])
        lost = result.get("lost_codes", [])
        if continued:
            txt.insert(tk.END, f"\n── {ref_label}首板→今日继续涨停 ({len(continued)}) ──\n")
            txt.insert(tk.END, "  " + ", ".join(continued) + "\n")
        if lost:
            txt.insert(tk.END, f"\n── {ref_label}首板→今日未涨停 ({len(lost)}) ──\n")
            txt.insert(tk.END, "  " + ", ".join(lost) + "\n")
        self._zt_summary_text.config(state=tk.DISABLED)

        # ---- 填充今日涨停形态分类表格 ----
        self._zt_pattern_tree.delete(*self._zt_pattern_tree.get_children())
        for rec in sorted(today_classified, key=lambda r: r.get("pattern", "")):
            tag = self._ZT_PATTERN_TAG_MAP.get(rec.get("pattern", ""), "")
            vals = (
                rec.get("code", ""),
                rec.get("name", ""),
                rec.get("industry", ""),
                rec.get("pattern", ""),
                f"{rec['change_pct']:.2f}" if rec.get("change_pct") is not None else "-",
                f"{rec['close']:.2f}" if rec.get("close") is not None else "-",
                f"{rec['volume_burst_ratio']:.2f}x" if rec.get("volume_burst_ratio") is not None else "-",
                f"{rec['distance_ma5_pct']:+.1f}" if rec.get("distance_ma5_pct") is not None else "-",
                f"{rec['trend_10d_pct']:+.1f}" if rec.get("trend_10d_pct") is not None else "-",
                f"{rec['position_60d_pct']:.0f}" if rec.get("position_60d_pct") is not None else "-",
                rec.get("pattern_detail", ""),
            )
            self._zt_pattern_tree.insert("", tk.END, values=vals, tags=(tag,) if tag else ())

        # ---- 填充上一交易日首板今日表现表格（带形态） ----
        self._zt_table_nb.tab(self._zt_yest_tab, text=f"{ref_label}首板今日表现")
        self._zt_yest_tree.delete(*self._zt_yest_tree.get_children())
        perf_map = {p["code"]: p for p in result.get("yesterday_first_today_performance", [])}
        for rec in yesterday_classified:
            code = rec.get("code", "")
            perf = perf_map.get(code, {})
            chg = perf.get("change_pct")
            close_val = perf.get("close")
            still = perf.get("still_limit_up", False)
            if still:
                status = "晋级"
            elif chg is not None and chg > 0:
                status = "高开"
            elif chg is not None and chg < -3:
                status = "大跌"
            elif chg is not None:
                status = "平开" if chg >= -1 else "低开"
            else:
                status = "-"
            vals = (
                code,
                rec.get("name", ""),
                rec.get("industry", ""),
                rec.get("pattern", ""),
                f"{chg:.2f}" if chg is not None else "-",
                f"{close_val:.2f}" if close_val is not None else "-",
                "是" if still else "否",
                status,
            )
            self._zt_yest_tree.insert("", tk.END, values=vals)

        self._zt_status_label.config(text="")
        self.status_var.set(status_message)
        if persist:
            self._save_limit_up_compare_snapshot(result)
        # 同步刷新历史记录下拉，并选中当前结果对应的日期
        current_today = str(result.get("today_date") or "").strip()
        self._refresh_compare_history_dates(select=current_today or None)

    # ================= 涨停预测 Tab =================
    def setup_predict_tab(self):
        predict_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(predict_frame, text="涨停预测")
        self.predict_tab = predict_frame

        style = ttk.Style()
        style.configure("Predict.Treeview", rowheight=24)
        style.map(
            "Predict.Treeview",
            background=[("selected", "#2f6fd6")],
            foreground=[("selected", "#ffffff")],
        )

        # ---- 操作栏 ----
        action_bar = ttk.Frame(predict_frame)
        action_bar.pack(fill=tk.X, pady=(0, 6))
        ttk.Button(action_bar, text="开始预测", command=self._start_predict).pack(side=tk.LEFT)
        ttk.Button(
            action_bar, text="命中对比", command=self._open_predict_compare_window,
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            action_bar, text="策略分析", command=self._open_predict_strategy_window,
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            action_bar, text="批量回测", command=self._open_predict_backtest_dialog,
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            action_bar, text="AI 博弈短报", command=self._open_daily_brief_window,
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            action_bar, text="NIM Key", command=self._open_nim_key_dialog,
        ).pack(side=tk.LEFT, padx=(2, 0))
        ttk.Label(action_bar, text="基准日期:").pack(side=tk.LEFT, padx=(12, 2))
        self._predict_date_var = tk.StringVar(value=datetime.now().strftime("%Y%m%d"))
        ttk.Entry(action_bar, textvariable=self._predict_date_var, width=10).pack(side=tk.LEFT)
        ttk.Button(
            action_bar, text="今天", width=5,
            command=lambda: self._predict_date_var.set(datetime.now().strftime("%Y%m%d")),
        ).pack(side=tk.LEFT, padx=(2, 0))
        ttk.Label(action_bar, text="回溯天数:").pack(side=tk.LEFT, padx=(10, 2))
        self._predict_lookback_var = tk.StringVar(value="5")
        ttk.Entry(action_bar, textvariable=self._predict_lookback_var, width=4).pack(side=tk.LEFT)
        ttk.Label(action_bar, text="(回看N日涨停对比环境 + 识别二波接力)").pack(side=tk.LEFT, padx=6)
        self._predict_status_label = ttk.Label(action_bar, text="")
        self._predict_status_label.pack(side=tk.RIGHT, padx=8)

        # 历史记录选择：可按日期查看每天的预测数据
        ttk.Button(
            action_bar, text="刷新此日期",
            command=self._refresh_selected_predict_date,
        ).pack(side=tk.RIGHT, padx=(4, 0))
        ttk.Label(action_bar, text="历史记录:").pack(side=tk.RIGHT, padx=(12, 2))
        self._predict_history_var = tk.StringVar(value="")
        self._predict_history_combo = ttk.Combobox(
            action_bar, textvariable=self._predict_history_var,
            width=12, state="readonly", values=(),
        )
        self._predict_history_combo.pack(side=tk.RIGHT)
        self._predict_history_combo.bind(
            "<<ComboboxSelected>>", self._on_predict_history_selected,
        )

        # ---- 市场情绪条 ----
        sent_bar = ttk.Frame(predict_frame, padding=(4, 3))
        sent_bar.pack(fill=tk.X, pady=(0, 4))
        try:
            sent_bar.configure(relief=tk.GROOVE, borderwidth=1)
        except tk.TclError:
            pass
        self._sentiment_score_label = ttk.Label(
            sent_bar, text="情绪: -/100", font=("", 11, "bold"),
        )
        self._sentiment_score_label.pack(side=tk.LEFT, padx=(0, 6))
        self._sentiment_advice_label = ttk.Label(sent_bar, text="→ -")
        self._sentiment_advice_label.pack(side=tk.LEFT, padx=(0, 12))
        self._sentiment_summary_label = ttk.Label(
            sent_bar, text="点击右侧「刷新」分析市场情绪", foreground="#666",
        )
        self._sentiment_summary_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(
            sent_bar, text="详情", width=6,
            command=self._show_sentiment_detail,
        ).pack(side=tk.RIGHT)
        ttk.Button(
            sent_bar, text="刷新", width=6,
            command=self._refresh_sentiment_async,
        ).pack(side=tk.RIGHT, padx=(0, 4))
        self._sentiment_result: Optional[Dict[str, Any]] = None
        self._sentiment_thread: Optional[threading.Thread] = None
        # 启动后 1.5s 自动算一次（有 app_config 缓存时秒回）
        try:
            self.root.after(1500, self._refresh_sentiment_async)
        except Exception:
            pass

        # ---- 筛选栏 ----
        filter_bar = ttk.Frame(predict_frame)
        filter_bar.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(filter_bar, text="最低分≥").pack(side=tk.LEFT)
        self._predict_filter_min_score = tk.IntVar(value=0)
        min_score_spin = ttk.Spinbox(
            filter_bar, from_=0, to=100, increment=5, width=5,
            textvariable=self._predict_filter_min_score,
            command=self._on_predict_filter_changed,
        )
        min_score_spin.pack(side=tk.LEFT, padx=(2, 4))
        # 让"键盘输入"也触发筛选（Spinbox 默认 command 只响应上下箭头）
        min_score_spin.bind("<KeyRelease>", lambda _e: self._on_predict_filter_changed())
        min_score_spin.bind("<FocusOut>", lambda _e: self._on_predict_filter_changed())
        min_score_spin.bind("<Return>", lambda _e: self._on_predict_filter_changed())
        # 一键预设按钮
        for preset in (50, 60, 70):
            ttk.Button(
                filter_bar, text=f"≥{preset}", width=4,
                command=lambda v=preset: (
                    self._predict_filter_min_score.set(v),
                    self._on_predict_filter_changed(),
                ),
            ).pack(side=tk.LEFT, padx=1)
        ttk.Frame(filter_bar, width=8).pack(side=tk.LEFT)

        ttk.Label(filter_bar, text="关键词:").pack(side=tk.LEFT)
        self._predict_filter_keyword = tk.StringVar(value="")
        kw_entry = ttk.Entry(filter_bar, textvariable=self._predict_filter_keyword, width=14)
        kw_entry.pack(side=tk.LEFT, padx=(2, 10))
        kw_entry.bind("<KeyRelease>", lambda _e: self._on_predict_filter_changed())

        ttk.Label(filter_bar, text="行业:").pack(side=tk.LEFT)
        self._predict_filter_industry = tk.StringVar(value="全部")
        self._predict_filter_industry_combo = ttk.Combobox(
            filter_bar, textvariable=self._predict_filter_industry,
            width=14, state="readonly", values=("全部",),
        )
        self._predict_filter_industry_combo.pack(side=tk.LEFT, padx=(2, 10))
        self._predict_filter_industry_combo.bind(
            "<<ComboboxSelected>>", lambda _e: self._on_predict_filter_changed(),
        )

        self._predict_filter_lhb_only = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            filter_bar, text="仅 LHB", variable=self._predict_filter_lhb_only,
            command=self._on_predict_filter_changed,
        ).pack(side=tk.LEFT, padx=(0, 8))

        self._predict_filter_northbound_only = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            filter_bar, text="仅北向加仓", variable=self._predict_filter_northbound_only,
            command=self._on_predict_filter_changed,
        ).pack(side=tk.LEFT, padx=(0, 8))

        self._predict_filter_theme_only = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            filter_bar, text="仅命中题材", variable=self._predict_filter_theme_only,
            command=self._on_predict_filter_changed,
        ).pack(side=tk.LEFT, padx=(0, 8))

        # 按历史命中段排序：以策略分析的"分数段命中率"做主排序键，
        # 把历史高命中的分数段顶到表头（每个 tab 用各自类别的命中率）
        self._predict_sort_by_hit_bucket = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            filter_bar, text="按历史命中段排序",
            variable=self._predict_sort_by_hit_bucket,
            command=self._on_predict_sort_mode_changed,
        ).pack(side=tk.LEFT, padx=(0, 8))

        ttk.Button(
            filter_bar, text="重置筛选",
            command=self._reset_predict_filters,
        ).pack(side=tk.LEFT, padx=(4, 0))

        self._predict_filter_count_label = ttk.Label(filter_bar, text="", foreground="#666")
        self._predict_filter_count_label.pack(side=tk.RIGHT, padx=8)

        # ---- 主区域：左侧摘要 + 右侧表格 ----
        body = ttk.PanedWindow(predict_frame, orient=tk.HORIZONTAL)
        body.pack(fill=tk.BOTH, expand=True)

        # 左侧：摘要面板
        summary_frame = ttk.LabelFrame(body, text="预测摘要", padding="6")
        self._predict_summary_text = scrolledtext.ScrolledText(summary_frame, width=42, height=30, wrap=tk.WORD)
        self._predict_summary_text.pack(fill=tk.BOTH, expand=True)
        self._predict_summary_text.insert(tk.END,
            "点击「开始预测」分析明日涨停候选\n\n"
            "思路：不再做“涨停前画像”，改为直接\n"
            "使用最近N日的涨停对比数据，观察首板\n"
            "晋级率、接力强弱，再结合“涨停→次日\n"
            "回落缩量→站稳”的旧逻辑，现重点看\n"
            "“近期爆量后回落到MA5附近”的承接。\n\n"
            "预测维度:\n"
            "  1. 保留涨停: 今日涨停股次日保板概率\n"
            "     - 最近首板晋级率、炸板、封板时间\n"
            "     - 板块热度、连板高度、涨停形态\n\n"
            "  2. 二波接力: 近期涨停过 + 今日已启动，\n"
            "     重点看放量启动、收盘强势、距前涨停\n"
            "     ≤5日的接力窗口、距涨停可达性\n\n"
            "说明: 预测仅供参考，请结合盘面综合判断")
        self._predict_summary_text.config(state=tk.DISABLED)
        body.add(summary_frame, weight=2)

        # 右侧：表格区
        table_frame = ttk.Frame(body)
        self._predict_table_nb = ttk.Notebook(table_frame)
        self._predict_table_nb.pack(fill=tk.BOTH, expand=True)

        # 用于在每个 tab 顶部展示历史命中率
        self._predict_stat_labels: Dict[str, ttk.Label] = {}

        # 连板延续候选 Tab
        cont_tab = ttk.Frame(self._predict_table_nb)
        self._predict_table_nb.add(cont_tab, text="保留涨停候选")
        cont_stat = ttk.Label(cont_tab, text="历史命中率: -", foreground="#444",
                              anchor=tk.W, padding=(6, 2))
        cont_stat.pack(side=tk.TOP, fill=tk.X)
        self._predict_stat_labels["cont"] = cont_stat
        cont_best = ttk.Label(cont_tab, text="历史最优段: -",
                              foreground="#b8860b", anchor=tk.W, padding=(6, 1))
        cont_best.pack(side=tk.TOP, fill=tk.X)
        self._predict_best_bucket_labels["cont"] = cont_best

        # 1进2 / 2进3 / 3进4 / 4进5 / 5进6+ 子类别命中率（独立统计，不影响主类别）
        cont_sub_frame = ttk.Frame(cont_tab)
        cont_sub_frame.pack(side=tk.TOP, fill=tk.X)
        for sub_key in ("cont_1to2", "cont_2to3", "cont_3to4", "cont_4to5", "cont_5plus"):
            sub_lbl = ttk.Label(cont_sub_frame, text="-", foreground="#666",
                                anchor=tk.W, padding=(12, 1))
            sub_lbl.pack(side=tk.LEFT, fill=tk.X, expand=True)
            self._predict_stat_labels[sub_key] = sub_lbl
        cont_cols = ("code", "name", "industry", "boards", "change_pct", "close",
                     "seal_time", "breaks", "turnover", "score", "result", "reasons")
        self._predict_cont_tree = ttk.Treeview(
            cont_tab, columns=cont_cols, show="headings", height=22, style="Predict.Treeview",
        )
        for col, (heading, w) in {
            "code": ("代码", 70), "name": ("名称", 85), "industry": ("行业", 85),
            "boards": ("连板数", 60), "change_pct": ("涨跌幅%", 70), "close": ("收盘价", 70),
            "seal_time": ("首封时间", 80), "breaks": ("炸板", 50),
            "turnover": ("换手%", 65), "score": ("预测分", 65),
            "result": ("结果", 90),
            "reasons": ("预测依据", 300),
        }.items():
            self._predict_cont_tree.heading(
                col, text=heading,
                command=lambda c=col: self._on_predict_heading_click("cont", c),
            )
            self._predict_cont_tree.column(col, width=w, anchor=tk.CENTER if col != "reasons" else tk.W)
        sb_cont = ttk.Scrollbar(cont_tab, orient=tk.VERTICAL, command=self._predict_cont_tree.yview)
        self._predict_cont_tree.configure(yscrollcommand=sb_cont.set)
        sb_cont.pack(side=tk.RIGHT, fill=tk.Y)
        self._predict_cont_tree.pack(fill=tk.BOTH, expand=True)
        self._predict_cont_tree.bind("<<TreeviewSelect>>", self._on_predict_stock_select)
        self._predict_cont_tree.bind("<Double-1>", self._on_predict_stock_double_click)
        # 行标签色：按分数段
        self._predict_cont_tree.tag_configure("score_high", background="#c8e6c9", foreground="#1f1f1f")
        self._predict_cont_tree.tag_configure("score_mid", background="#fff9c4", foreground="#1f1f1f")
        self._predict_cont_tree.tag_configure("score_low", background="#ffecb3", foreground="#1f1f1f")
        self._predict_cont_tree.tag_configure("hit", background="#a5d6a7", foreground="#1f1f1f")
        self._predict_cont_tree.tag_configure("miss", background="#ffcdd2", foreground="#1f1f1f")
        self._predict_cont_tree.tag_configure("best_bucket", background="#ffd54f", foreground="#1f1f1f")

        # 首板候选 Tab
        first_tab = ttk.Frame(self._predict_table_nb)
        self._predict_table_nb.add(first_tab, text="二波接力候选")
        first_stat = ttk.Label(first_tab, text="历史命中率: -", foreground="#444",
                               anchor=tk.W, padding=(6, 2))
        first_stat.pack(side=tk.TOP, fill=tk.X)
        self._predict_stat_labels["first"] = first_stat
        first_best = ttk.Label(first_tab, text="历史最优段: -",
                               foreground="#b8860b", anchor=tk.W, padding=(6, 1))
        first_best.pack(side=tk.TOP, fill=tk.X)
        self._predict_best_bucket_labels["first"] = first_best
        first_cols = ("code", "name", "industry", "change_pct", "close",
                      "burst_date", "burst_ratio", "dist_ma5", "days_since_burst",
                      "score", "result", "reasons")
        self._predict_first_tree = ttk.Treeview(
            first_tab, columns=first_cols, show="headings", height=22, style="Predict.Treeview",
        )
        for col, (heading, w) in {
            "code": ("代码", 70), "name": ("名称", 85), "industry": ("行业", 85),
            "change_pct": ("今日涨幅%", 75), "close": ("收盘价", 70),
            "burst_date": ("爆量日", 90), "burst_ratio": ("爆量倍数", 70),
            "dist_ma5": ("距MA5%", 65), "days_since_burst": ("距爆量日", 65),
            "score": ("预测分", 65), "result": ("结果", 90),
            "reasons": ("预测依据", 300),
        }.items():
            self._predict_first_tree.heading(
                col, text=heading,
                command=lambda c=col: self._on_predict_heading_click("first", c),
            )
            self._predict_first_tree.column(col, width=w, anchor=tk.CENTER if col != "reasons" else tk.W)
        sb_first = ttk.Scrollbar(first_tab, orient=tk.VERTICAL, command=self._predict_first_tree.yview)
        self._predict_first_tree.configure(yscrollcommand=sb_first.set)
        sb_first.pack(side=tk.RIGHT, fill=tk.Y)
        self._predict_first_tree.pack(fill=tk.BOTH, expand=True)
        self._predict_first_tree.bind("<<TreeviewSelect>>", self._on_predict_stock_select)
        self._predict_first_tree.bind("<Double-1>", self._on_predict_stock_double_click)
        self._predict_first_tree.tag_configure("score_high", background="#c8e6c9", foreground="#1f1f1f")
        self._predict_first_tree.tag_configure("score_mid", background="#fff9c4", foreground="#1f1f1f")
        self._predict_first_tree.tag_configure("score_low", background="#ffecb3", foreground="#1f1f1f")
        self._predict_first_tree.tag_configure("hit", background="#a5d6a7", foreground="#1f1f1f")
        self._predict_first_tree.tag_configure("miss", background="#ffcdd2", foreground="#1f1f1f")
        self._predict_first_tree.tag_configure("best_bucket", background="#ffd54f", foreground="#1f1f1f")

        # 首板涨停候选 Tab（最近 N 日未涨停、今日量价启动）
        fresh_tab = ttk.Frame(self._predict_table_nb)
        self._predict_table_nb.add(fresh_tab, text="首板涨停候选")
        fresh_stat = ttk.Label(fresh_tab, text="历史命中率: -", foreground="#444",
                               anchor=tk.W, padding=(6, 2))
        fresh_stat.pack(side=tk.TOP, fill=tk.X)
        self._predict_stat_labels["fresh"] = fresh_stat
        fresh_best = ttk.Label(fresh_tab, text="历史最优段: -",
                               foreground="#b8860b", anchor=tk.W, padding=(6, 1))
        fresh_best.pack(side=tk.TOP, fill=tk.X)
        self._predict_best_bucket_labels["fresh"] = fresh_best
        fresh_cols = ("code", "name", "industry", "change_pct", "close",
                      "volume_ratio", "dist_ma5", "trend_5d", "position_60d",
                      "turnover", "score", "result", "reasons")
        self._predict_fresh_tree = ttk.Treeview(
            fresh_tab, columns=fresh_cols, show="headings", height=22, style="Predict.Treeview",
        )
        for col, (heading, w) in {
            "code": ("代码", 70), "name": ("名称", 85), "industry": ("行业", 85),
            "change_pct": ("今日涨幅%", 75), "close": ("收盘价", 70),
            "volume_ratio": ("量比", 60), "dist_ma5": ("距MA5%", 65),
            "trend_5d": ("5日涨幅%", 70), "position_60d": ("60日位置%", 75),
            "turnover": ("换手%", 65),
            "score": ("预测分", 65), "result": ("结果", 90),
            "reasons": ("预测依据", 300),
        }.items():
            self._predict_fresh_tree.heading(
                col, text=heading,
                command=lambda c=col: self._on_predict_heading_click("fresh", c),
            )
            self._predict_fresh_tree.column(col, width=w, anchor=tk.CENTER if col != "reasons" else tk.W)
        sb_fresh = ttk.Scrollbar(fresh_tab, orient=tk.VERTICAL, command=self._predict_fresh_tree.yview)
        self._predict_fresh_tree.configure(yscrollcommand=sb_fresh.set)
        sb_fresh.pack(side=tk.RIGHT, fill=tk.Y)
        self._predict_fresh_tree.pack(fill=tk.BOTH, expand=True)
        self._predict_fresh_tree.bind("<<TreeviewSelect>>", self._on_predict_stock_select)
        self._predict_fresh_tree.bind("<Double-1>", self._on_predict_stock_double_click)
        self._predict_fresh_tree.tag_configure("score_high", background="#c8e6c9", foreground="#1f1f1f")
        self._predict_fresh_tree.tag_configure("score_mid", background="#fff9c4", foreground="#1f1f1f")
        self._predict_fresh_tree.tag_configure("score_low", background="#ffecb3", foreground="#1f1f1f")
        self._predict_fresh_tree.tag_configure("hit", background="#a5d6a7", foreground="#1f1f1f")
        self._predict_fresh_tree.tag_configure("miss", background="#ffcdd2", foreground="#1f1f1f")
        self._predict_fresh_tree.tag_configure("best_bucket", background="#ffd54f", foreground="#1f1f1f")

        # 断板反包候选 Tab（近期涨停被打掉，今日逼近反包）
        wrap_tab = ttk.Frame(self._predict_table_nb)
        self._predict_table_nb.add(wrap_tab, text="反包/承接候选")
        wrap_stat = ttk.Label(wrap_tab, text="历史命中率: -", foreground="#444",
                              anchor=tk.W, padding=(6, 2))
        wrap_stat.pack(side=tk.TOP, fill=tk.X)
        self._predict_stat_labels["wrap"] = wrap_stat
        wrap_best = ttk.Label(wrap_tab, text="历史最优段: -",
                              foreground="#b8860b", anchor=tk.W, padding=(6, 1))
        wrap_best.pack(side=tk.TOP, fill=tk.X)
        self._predict_best_bucket_labels["wrap"] = wrap_best
        wrap_cols = ("code", "name", "industry", "pattern_kind", "change_pct", "close",
                     "prior_lu_date", "prior_lu_close", "wrap_gap", "days_since_lu",
                     "worst_drop", "volume_ratio", "score", "result", "reasons")
        self._predict_wrap_tree = ttk.Treeview(
            wrap_tab, columns=wrap_cols, show="headings", height=22, style="Predict.Treeview",
        )
        for col, (heading, w) in {
            "code": ("代码", 70), "name": ("名称", 85), "industry": ("行业", 85),
            "pattern_kind": ("形态", 70),
            "change_pct": ("今日涨幅%", 75), "close": ("收盘价", 70),
            "prior_lu_date": ("前涨停日", 90), "prior_lu_close": ("前涨停价", 75),
            "wrap_gap": ("反包缺口%", 80), "days_since_lu": ("距前涨停", 70),
            "worst_drop": ("最深阴线%", 80), "volume_ratio": ("量比", 60),
            "score": ("预测分", 65), "result": ("结果", 90),
            "reasons": ("预测依据", 300),
        }.items():
            self._predict_wrap_tree.heading(
                col, text=heading,
                command=lambda c=col: self._on_predict_heading_click("wrap", c),
            )
            self._predict_wrap_tree.column(col, width=w, anchor=tk.CENTER if col != "reasons" else tk.W)
        sb_wrap = ttk.Scrollbar(wrap_tab, orient=tk.VERTICAL, command=self._predict_wrap_tree.yview)
        self._predict_wrap_tree.configure(yscrollcommand=sb_wrap.set)
        sb_wrap.pack(side=tk.RIGHT, fill=tk.Y)
        self._predict_wrap_tree.pack(fill=tk.BOTH, expand=True)
        self._predict_wrap_tree.bind("<<TreeviewSelect>>", self._on_predict_stock_select)
        self._predict_wrap_tree.bind("<Double-1>", self._on_predict_stock_double_click)
        self._predict_wrap_tree.tag_configure("score_high", background="#c8e6c9", foreground="#1f1f1f")
        self._predict_wrap_tree.tag_configure("score_mid", background="#fff9c4", foreground="#1f1f1f")
        self._predict_wrap_tree.tag_configure("score_low", background="#ffecb3", foreground="#1f1f1f")
        self._predict_wrap_tree.tag_configure("hit", background="#a5d6a7", foreground="#1f1f1f")
        self._predict_wrap_tree.tag_configure("miss", background="#ffcdd2", foreground="#1f1f1f")
        self._predict_wrap_tree.tag_configure("best_bucket", background="#ffd54f", foreground="#1f1f1f")

        # 趋势涨停候选 Tab（多头排列稳健上行）
        trend_tab = ttk.Frame(self._predict_table_nb)
        self._predict_table_nb.add(trend_tab, text="趋势涨停候选")
        trend_stat = ttk.Label(trend_tab, text="历史命中率: -", foreground="#444",
                               anchor=tk.W, padding=(6, 2))
        trend_stat.pack(side=tk.TOP, fill=tk.X)
        self._predict_stat_labels["trend"] = trend_stat
        trend_best = ttk.Label(trend_tab, text="历史最优段: -",
                               foreground="#b8860b", anchor=tk.W, padding=(6, 1))
        trend_best.pack(side=tk.TOP, fill=tk.X)
        self._predict_best_bucket_labels["trend"] = trend_best
        trend_cols = ("code", "name", "industry", "change_pct", "close",
                      "ma_spread", "dist_ma5", "ma20_slope",
                      "trend_5d", "trend_10d", "position_60d",
                      "volume_ratio", "score", "result", "reasons")
        self._predict_trend_tree = ttk.Treeview(
            trend_tab, columns=trend_cols, show="headings", height=22, style="Predict.Treeview",
        )
        for col, (heading, w) in {
            "code": ("代码", 70), "name": ("名称", 85), "industry": ("行业", 85),
            "change_pct": ("今日涨幅%", 75), "close": ("收盘价", 70),
            "ma_spread": ("多头开口%", 75), "dist_ma5": ("距MA5%", 65),
            "ma20_slope": ("MA20斜率%", 80),
            "trend_5d": ("5日涨幅%", 70), "trend_10d": ("10日涨幅%", 75),
            "position_60d": ("60日位置%", 75),
            "volume_ratio": ("量比", 60),
            "score": ("预测分", 65), "result": ("结果", 90),
            "reasons": ("预测依据", 300),
        }.items():
            self._predict_trend_tree.heading(
                col, text=heading,
                command=lambda c=col: self._on_predict_heading_click("trend", c),
            )
            self._predict_trend_tree.column(col, width=w, anchor=tk.CENTER if col != "reasons" else tk.W)
        sb_trend = ttk.Scrollbar(trend_tab, orient=tk.VERTICAL, command=self._predict_trend_tree.yview)
        self._predict_trend_tree.configure(yscrollcommand=sb_trend.set)
        sb_trend.pack(side=tk.RIGHT, fill=tk.Y)
        self._predict_trend_tree.pack(fill=tk.BOTH, expand=True)
        self._predict_trend_tree.bind("<<TreeviewSelect>>", self._on_predict_stock_select)
        self._predict_trend_tree.bind("<Double-1>", self._on_predict_stock_double_click)
        self._predict_trend_tree.tag_configure("score_high", background="#c8e6c9", foreground="#1f1f1f")
        self._predict_trend_tree.tag_configure("score_mid", background="#fff9c4", foreground="#1f1f1f")
        self._predict_trend_tree.tag_configure("score_low", background="#ffecb3", foreground="#1f1f1f")
        self._predict_trend_tree.tag_configure("hit", background="#a5d6a7", foreground="#1f1f1f")
        self._predict_trend_tree.tag_configure("miss", background="#ffcdd2", foreground="#1f1f1f")
        self._predict_trend_tree.tag_configure("best_bucket", background="#ffd54f", foreground="#1f1f1f")

        # 概念炒作 Tab（按"题材"维度看：哪些概念在被炒、持续多久、主线/龙头/潜伏）
        self._setup_concept_hype_subtab(self._predict_table_nb)

        body.add(table_frame, weight=4)

        self._predict_thread: Optional[threading.Thread] = None
        self._predict_result: Optional[Dict[str, Any]] = None
        self._predict_results_map: Dict = {}
        self._predict_prewarm_thread: Optional[threading.Thread] = None
        self._predict_prewarm_token: Optional[CancelToken] = None
        self._concept_index_thread: Optional[threading.Thread] = None
        self._concept_index_token: Optional[CancelToken] = None

        # 启动时即刻把 5 个 tab 的"历史命中率"标签填上
        self._refresh_predict_accuracy_async("")

    def _score_tag(self, score: int) -> str:
        if score >= 70:
            return "score_high"
        elif score >= 50:
            return "score_mid"
        return "score_low"

    def _predict_row_tag(self, category: str, hit_tag: Optional[str], score: Any) -> str:
        """决定预测候选行的背景色 tag。

        优先级：hit/miss（已回填的次日结果）> best_bucket（历史最优分数段）> 分数段色。
        """
        if hit_tag:
            return hit_tag
        best = (getattr(self, "_predict_best_buckets", None) or {}).get(category)
        if best is not None:
            try:
                s = int(score)
                if best[0] <= s <= best[1]:
                    return "best_bucket"
            except (TypeError, ValueError):
                pass
        try:
            return self._score_tag(int(score) if score is not None else 0)
        except (TypeError, ValueError):
            return self._score_tag(0)

    @staticmethod
    def _predict_sort_value(record: Dict[str, Any], column: str):
        value_map = {
            "code": record.get("code"),
            "name": record.get("name"),
            "industry": record.get("industry"),
            "boards": record.get("consecutive_boards"),
            "change_pct": record.get("change_pct"),
            "close": record.get("close"),
            "seal_time": record.get("first_board_time"),
            "breaks": record.get("break_count"),
            "turnover": record.get("turnover"),
            "score": record.get("score"),
            "reasons": record.get("reasons"),
            "burst_date": record.get("burst_date"),
            "burst_ratio": record.get("volume_ratio"),
            "dist_ma5": record.get("dist_ma5_pct"),
            "days_since_burst": record.get("days_since_burst"),
            "volume_ratio": record.get("volume_ratio"),
            "trend_5d": record.get("trend_5d"),
            "position_60d": record.get("position_60d"),
            "prior_lu_date": record.get("prior_lu_date"),
            "prior_lu_close": record.get("prior_lu_close"),
            "wrap_gap": record.get("wrap_gap_pct"),
            "days_since_lu": record.get("days_since_lu"),
            "worst_drop": record.get("worst_drop"),
            "ma_spread": record.get("ma_spread_pct"),
            "ma20_slope": record.get("ma20_slope_pct"),
            "trend_10d": record.get("trend_10d"),
            "result": record.get("_t1_pct"),
        }
        value = value_map.get(column)
        if column in {"name", "industry", "reasons", "seal_time", "burst_date", "code", "prior_lu_date"}:
            return str(value or "")
        if value is None or value == "":
            return float("-inf")
        try:
            return float(value)
        except (TypeError, ValueError):
            return str(value)

    def _sort_predict_records(
        self,
        records: List[Dict[str, Any]],
        table_kind: str,
    ) -> List[Dict[str, Any]]:
        if table_kind == "cont":
            column = self._predict_cont_sort_column
            reverse = self._predict_cont_sort_reverse
            secondary = ["score", "boards", "change_pct", "turnover"]
        elif table_kind == "fresh":
            column = self._predict_fresh_sort_column
            reverse = self._predict_fresh_sort_reverse
            secondary = ["score", "volume_ratio", "change_pct", "turnover"]
        elif table_kind == "wrap":
            column = self._predict_wrap_sort_column
            reverse = self._predict_wrap_sort_reverse
            secondary = ["score", "wrap_gap", "change_pct", "volume_ratio"]
        elif table_kind == "trend":
            column = self._predict_trend_sort_column
            reverse = self._predict_trend_sort_reverse
            secondary = ["score", "ma_spread", "change_pct", "volume_ratio"]
        else:
            column = self._predict_first_sort_column
            reverse = self._predict_first_sort_reverse
            secondary = ["score", "burst_ratio", "dist_ma5", "change_pct"]
        if column in secondary:
            secondary = [c for c in secondary if c != column]

        # "按历史命中段排序"模式：把当前类别下"历史命中率最高的分数段"顶到最上
        # 桶内按 score 降序（再叠加原 secondary 列），保证细排仍然可控
        bucket_priority = self._predict_bucket_priority_for(table_kind)
        if bucket_priority is not None:
            return sorted(
                records,
                key=lambda rec: tuple(
                    [bucket_priority(rec)]
                    + [self._predict_sort_value(rec, "score")]
                    + [self._predict_sort_value(rec, c) for c in secondary
                       if c != "score"]
                    + [str(rec.get("code", ""))]
                ),
                reverse=True,
            )

        return sorted(
            records,
            key=lambda rec: tuple(
                [self._predict_sort_value(rec, column)]
                + [self._predict_sort_value(rec, c) for c in secondary]
                + [str(rec.get("code", ""))]
            ),
            reverse=reverse,
        )

    def _predict_bucket_priority_for(self, table_kind: str):
        """返回桶排序的 key 函数；若关闭模式或拿不到数据则返回 None。

        样本不足（< 5）的桶 priority 退化为 -1，让它们沉到分数排序之后；
        其它桶用其历史命中率（0-100），越大越靠前。
        """
        try:
            if not self._predict_sort_by_hit_bucket.get():
                return None
        except (AttributeError, tk.TclError):
            return None
        cat_key = {
            "cont": "cont", "first": "first", "fresh": "fresh",
            "wrap": "wrap", "trend": "trend",
        }.get(table_kind)
        if cat_key is None:
            return None
        rates = self._get_predict_bucket_rates(cat_key)
        if not rates:
            return None
        from src.services import prediction_accuracy_service as svc

        def _priority(rec: Dict[str, Any]) -> float:
            bucket = svc.score_to_bucket(rec.get("score"))
            info = rates.get(bucket)
            if not info or not info.get("eligible"):
                return -1.0
            return float(info.get("rate") or 0.0)

        return _priority

    def _get_predict_bucket_rates(self, category: str) -> Dict[Tuple[int, int], Dict[str, Any]]:
        """读取类别的历史分数段命中率（带缓存，lookback=20）。"""
        cache = self._predict_bucket_rates_cache
        if category in cache:
            return cache[category]
        try:
            from src.services import prediction_accuracy_service as svc
            rates = svc.get_score_bucket_rates(
                category=category, lookback_dates=20, min_samples=5,
            )
        except Exception:
            rates = {}
        cache[category] = rates
        return rates

    def _on_predict_heading_click(self, table_kind: str, column: str) -> None:
        if table_kind == "cont":
            if column == self._predict_cont_sort_column:
                self._predict_cont_sort_reverse = not self._predict_cont_sort_reverse
            else:
                self._predict_cont_sort_column = column
                self._predict_cont_sort_reverse = column in {"score", "boards", "change_pct", "close", "turnover", "breaks"}
        elif table_kind == "fresh":
            if column == self._predict_fresh_sort_column:
                self._predict_fresh_sort_reverse = not self._predict_fresh_sort_reverse
            else:
                self._predict_fresh_sort_column = column
                self._predict_fresh_sort_reverse = column in {"score", "volume_ratio", "change_pct", "close", "trend_5d", "position_60d", "turnover"}
        elif table_kind == "wrap":
            if column == self._predict_wrap_sort_column:
                self._predict_wrap_sort_reverse = not self._predict_wrap_sort_reverse
            else:
                self._predict_wrap_sort_column = column
                # 反包缺口越小越好，因此 wrap_gap / days_since_lu 默认升序
                self._predict_wrap_sort_reverse = column in {"score", "change_pct", "close", "volume_ratio", "prior_lu_close"}
        elif table_kind == "trend":
            if column == self._predict_trend_sort_column:
                self._predict_trend_sort_reverse = not self._predict_trend_sort_reverse
            else:
                self._predict_trend_sort_column = column
                self._predict_trend_sort_reverse = column in {
                    "score", "change_pct", "close", "volume_ratio", "ma_spread",
                    "ma20_slope", "trend_5d", "trend_10d", "position_60d",
                }
        else:
            if column == self._predict_first_sort_column:
                self._predict_first_sort_reverse = not self._predict_first_sort_reverse
            else:
                self._predict_first_sort_column = column
                self._predict_first_sort_reverse = column in {"score", "burst_ratio", "change_pct", "close", "dist_ma5", "days_since_burst"}

        if self._predict_result:
            self._apply_predict_result(self._predict_result)

    def _start_predict(self):
        if self._predict_thread is not None and self._predict_thread.is_alive():
            return
        trade_date = self._predict_date_var.get().strip()
        if not trade_date:
            trade_date = datetime.now().strftime("%Y%m%d")
        # 非交易日（周末/节假日）自动回退到最近一个交易日，避免拿到空涨停池
        try:
            parsed = datetime.strptime(trade_date, "%Y%m%d").date()
            cal = _get_trade_calendar()
            if not _is_trading_day(parsed, cal):
                rolled = _previous_trading_day(parsed, cal)
                trade_date = rolled.strftime("%Y%m%d")
        except ValueError:
            pass
        self._predict_date_var.set(trade_date)
        try:
            lookback = max(2, min(int(self._predict_lookback_var.get().strip() or "5"), 15))
        except ValueError:
            lookback = 5
        self._predict_lookback_var.set(str(lookback))

        self._predict_summary_text.config(state=tk.NORMAL)
        self._predict_summary_text.delete("1.0", tk.END)
        self._predict_summary_text.insert(tk.END,
            f"正在回溯最近 {lookback} 天涨停股特征，基于 {trade_date} 数据预测...\n")
        self._predict_summary_text.config(state=tk.DISABLED)
        self._predict_status_label.config(text="正在回溯涨停前兆画像...")
        self.status_var.set("正在执行涨停预测...")

        self._predict_thread, _ = self._start_background_job(
            self._load_predict,
            name="limit-up-predict",
            args=(trade_date, lookback),
        )

    def _load_predict(self, trade_date: str, lookback_days: int, cancel_token: CancelToken):
        try:
            if cancel_token.is_cancelled():
                return
            def _progress(cur, tot, info):
                if cancel_token.is_cancelled():
                    raise StopIteration
                self._post_to_ui(lambda c=cur, t=tot, i=info:
                    self._predict_status_label.config(text=f"预测分析 {c}/{t}: {i}"))

            result = self.stock_filter.predict_limit_up_candidates(
                trade_date, lookback_days=lookback_days, progress_callback=_progress,
            )
            if cancel_token.is_cancelled():
                return
            self._post_to_ui(lambda r=result: self._apply_predict_result(r))
        except StopIteration:
            self._post_to_ui(lambda: self._predict_status_label.config(text="已取消"))
        except Exception as e:
            err = str(e)
            self._post_to_ui(lambda: self._predict_show_error(f"涨停预测失败: {err}"))

    def _predict_show_error(self, msg: str):
        self._predict_summary_text.config(state=tk.NORMAL)
        self._predict_summary_text.delete("1.0", tk.END)
        self._predict_summary_text.insert(tk.END, msg)
        self._predict_summary_text.config(state=tk.DISABLED)
        self._predict_status_label.config(text="")
        self.status_var.set("涨停预测失败")

    # ============== 概念炒作 sub-tab ==============
    def _setup_concept_hype_subtab(self, parent_nb: ttk.Notebook) -> None:
        """在涨停预测的内层 notebook 里加一个'概念炒作'子 tab。

        左侧为摘要面板（主线 + 萌芽题材），右侧上下分栏：
        - 上：题材排行表（按 今日涨停 / 累计 / 持续 排序）
        - 下：选中题材的涨停成员表（双击跳详情）
        """
        hype_tab = ttk.Frame(parent_nb)
        parent_nb.add(hype_tab, text="概念炒作")
        self._concept_hype_tab = hype_tab

        # ---- 操作栏 ----
        action = ttk.Frame(hype_tab)
        action.pack(fill=tk.X, pady=(2, 4))
        ttk.Button(
            action, text="开始分析", command=self._start_concept_hype_analysis,
        ).pack(side=tk.LEFT)
        ttk.Label(action, text="基准日期:").pack(side=tk.LEFT, padx=(10, 2))
        self._concept_hype_end_date_var = tk.StringVar(
            value=datetime.now().strftime("%Y%m%d"),
        )
        ttk.Entry(
            action, textvariable=self._concept_hype_end_date_var, width=10,
        ).pack(side=tk.LEFT)
        ttk.Button(
            action, text="今日", width=5,
            command=lambda: self._concept_hype_end_date_var.set(
                datetime.now().strftime("%Y%m%d"),
            ),
        ).pack(side=tk.LEFT, padx=(2, 0))
        ttk.Button(
            action, text="同步上方", width=8,
            command=lambda: self._concept_hype_end_date_var.set(
                (self._predict_date_var.get() or "").strip().replace("-", ""),
            ),
        ).pack(side=tk.LEFT, padx=(2, 0))
        ttk.Label(action, text="回看交易日:").pack(side=tk.LEFT, padx=(10, 2))
        self._concept_hype_lookback_var = tk.StringVar(value="10")
        ttk.Spinbox(
            action, from_=3, to=30, width=4,
            textvariable=self._concept_hype_lookback_var,
        ).pack(side=tk.LEFT)
        ttk.Label(
            action, text="(按 limit_up_pool 已缓存日切片，不在缓存内的日期会自动取最近可用日)",
            foreground="#888",
        ).pack(side=tk.LEFT, padx=(8, 0))
        self._concept_hype_status = ttk.Label(action, text="尚未分析", foreground="#666")
        self._concept_hype_status.pack(side=tk.RIGHT, padx=8)

        # ---- 筛选栏 ----
        filt = ttk.Frame(hype_tab)
        filt.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(filt, text="来源:").pack(side=tk.LEFT)
        self._concept_hype_source_var = tk.StringVar(value="全部")
        src_combo = ttk.Combobox(
            filt, textvariable=self._concept_hype_source_var,
            values=("全部", "行业", "概念", "LLM题材"),
            width=10, state="readonly",
        )
        src_combo.pack(side=tk.LEFT, padx=(2, 10))
        src_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_concept_hype_list())
        ttk.Label(filt, text="阶段:").pack(side=tk.LEFT)
        self._concept_hype_phase_var = tk.StringVar(value="全部")
        phase_combo = ttk.Combobox(
            filt, textvariable=self._concept_hype_phase_var,
            values=("全部", "萌芽", "主升", "末期", "退潮"),
            width=8, state="readonly",
        )
        phase_combo.pack(side=tk.LEFT, padx=(2, 10))
        phase_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_concept_hype_list())
        self._concept_hype_active_only = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            filt, text="仅今日仍有涨停", variable=self._concept_hype_active_only,
            command=self._refresh_concept_hype_list,
        ).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Label(filt, text="关键词:").pack(side=tk.LEFT)
        self._concept_hype_keyword_var = tk.StringVar(value="")
        kw = ttk.Entry(filt, textvariable=self._concept_hype_keyword_var, width=12)
        kw.pack(side=tk.LEFT, padx=(2, 4))
        kw.bind("<KeyRelease>", lambda _e: self._refresh_concept_hype_list())
        self._concept_hype_count_label = ttk.Label(filt, text="", foreground="#666")
        self._concept_hype_count_label.pack(side=tk.RIGHT, padx=8)

        # ---- 主区域 ----
        body = ttk.PanedWindow(hype_tab, orient=tk.HORIZONTAL)
        body.pack(fill=tk.BOTH, expand=True)

        # 左：摘要面板
        summary_frame = ttk.LabelFrame(body, text="主线 / 萌芽", padding=6)
        self._concept_hype_summary = scrolledtext.ScrolledText(
            summary_frame, width=38, height=24, wrap=tk.WORD,
        )
        self._concept_hype_summary.pack(fill=tk.BOTH, expand=True)
        self._concept_hype_summary.insert(
            tk.END,
            "点击「开始分析」识别近 N 个交易日正在被炒作的概念/行业。\n\n"
            "数据维度：\n"
            "  · 行业（涨停池自带，必有）\n"
            "  · 概念（依赖“刷新概念库”，未刷则跳过）\n"
            "  · LLM 题材（依赖涨停对比 tab AI 聚类缓存）\n\n"
            "判定口径：\n"
            "  · 起爆日：单日涨停数 ≥ 3 且 ≥ 前 3 日均值的 2 倍\n"
            "  · 持续天数：起爆日 → 基准日\n"
            "  · 阶段：萌芽(≤2d) / 主升(3-7d) / 末期 / 退潮\n\n"
            "  · 双击表格行 → 展开题材成份股\n"
            "  · 双击成份股 → 跳转股票详情",
        )
        self._concept_hype_summary.config(state=tk.DISABLED)
        body.add(summary_frame, weight=2)

        # 右：上下分栏
        right = ttk.PanedWindow(body, orient=tk.VERTICAL)

        # 右上：题材排行表
        rank_frame = ttk.LabelFrame(right, text="题材排行", padding=2)
        rank_cols = (
            "score", "name", "source", "today", "total", "active", "ignite",
            "duration", "phase", "trend", "leaders",
        )
        self._concept_hype_rank_tree = ttk.Treeview(
            rank_frame, columns=rank_cols, show="headings",
            height=12, style="Predict.Treeview",
        )
        rank_headings = {
            "score": ("机会分", 60),
            "name": ("题材", 130), "source": ("来源", 70),
            "today": ("今涨停", 60), "total": ("累计", 55),
            "active": ("活跃日", 60), "ignite": ("起爆日", 80),
            "duration": ("持续", 50), "phase": ("阶段", 55),
            "trend": ("趋势", 55), "leaders": ("龙头(连板)", 280),
        }
        for col, (heading, w) in rank_headings.items():
            self._concept_hype_rank_tree.heading(
                col, text=heading,
                command=lambda c=col: self._on_concept_hype_sort(c),
            )
            self._concept_hype_rank_tree.column(
                col, width=w,
                anchor=tk.CENTER if col not in ("name", "leaders") else tk.W,
            )
        sb_rank = ttk.Scrollbar(
            rank_frame, orient=tk.VERTICAL,
            command=self._concept_hype_rank_tree.yview,
        )
        self._concept_hype_rank_tree.configure(yscrollcommand=sb_rank.set)
        sb_rank.pack(side=tk.RIGHT, fill=tk.Y)
        self._concept_hype_rank_tree.pack(fill=tk.BOTH, expand=True)
        self._concept_hype_rank_tree.bind(
            "<<TreeviewSelect>>", self._on_concept_hype_select,
        )
        # 阶段配色
        self._concept_hype_rank_tree.tag_configure(
            "phase_萌芽", background="#bbdefb", foreground="#0d47a1",
        )
        self._concept_hype_rank_tree.tag_configure(
            "phase_主升", background="#c8e6c9", foreground="#1b5e20",
        )
        self._concept_hype_rank_tree.tag_configure(
            "phase_末期", background="#ffe0b2", foreground="#bf360c",
        )
        self._concept_hype_rank_tree.tag_configure(
            "phase_退潮", background="#eceff1", foreground="#616161",
        )
        right.add(rank_frame, weight=3)

        # 右下：选中题材的成份股
        members_frame = ttk.LabelFrame(right, text="题材成份股（点击上方某行查看）", padding=2)
        self._concept_hype_members_label = members_frame
        member_cols = (
            "code", "name", "industry", "boards", "change_pct",
            "close", "turnover", "lu_count", "lu_dates",
        )
        self._concept_hype_members_tree = ttk.Treeview(
            members_frame, columns=member_cols, show="headings",
            height=10, style="Predict.Treeview",
        )
        member_headings = {
            "code": ("代码", 70), "name": ("名称", 90),
            "industry": ("所属行业", 90), "boards": ("最高板", 60),
            "change_pct": ("涨幅%", 65), "close": ("收盘", 70),
            "turnover": ("换手%", 60),
            "lu_count": ("窗口涨停", 70),
            "lu_dates": ("涨停日历", 320),
        }
        for col, (heading, w) in member_headings.items():
            self._concept_hype_members_tree.heading(col, text=heading)
            self._concept_hype_members_tree.column(
                col, width=w,
                anchor=tk.CENTER if col not in ("name", "industry", "lu_dates") else tk.W,
            )
        sb_mem = ttk.Scrollbar(
            members_frame, orient=tk.VERTICAL,
            command=self._concept_hype_members_tree.yview,
        )
        self._concept_hype_members_tree.configure(yscrollcommand=sb_mem.set)
        sb_mem.pack(side=tk.RIGHT, fill=tk.Y)
        self._concept_hype_members_tree.pack(fill=tk.BOTH, expand=True)
        self._concept_hype_members_tree.bind(
            "<Double-1>", self._on_concept_hype_member_double_click,
        )
        right.add(members_frame, weight=2)

        body.add(right, weight=5)

        # 状态
        self._concept_hype_result: Optional[Dict[str, Any]] = None
        self._concept_hype_thread: Optional[threading.Thread] = None
        self._concept_hype_sort_col: str = "score"
        self._concept_hype_sort_reverse: bool = True

    def _start_concept_hype_analysis(self) -> None:
        """点击"开始分析"：后台跑 concept_hype_service.analyze_concept_hype。"""
        if self._concept_hype_thread is not None and self._concept_hype_thread.is_alive():
            self._log("概念炒作分析已在运行中，请稍候")
            return
        try:
            lookback = max(3, min(30, int(self._concept_hype_lookback_var.get() or "10")))
        except ValueError:
            lookback = 10
        end_date = (self._concept_hype_end_date_var.get() or "").strip().replace("-", "")
        self._concept_hype_status.config(text="分析中...", foreground="#1565c0")

        def _worker():
            try:
                result = concept_hype_service.analyze_concept_hype(
                    end_date=end_date or None,
                    lookback=lookback,
                    log=lambda s: self._post_to_ui(lambda m=s: self._log(m)),
                )
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
                logger_msg = f"概念炒作分析失败: {err}"
                self._post_to_ui(lambda m=logger_msg: self._log(m))
                self._post_to_ui(
                    lambda: self._concept_hype_status.config(
                        text="分析失败", foreground="#c62828",
                    ),
                )
                return
            self._post_to_ui(lambda r=result: self._apply_concept_hype_result(r))

        t = threading.Thread(target=_worker, daemon=True)
        self._concept_hype_thread = t
        t.start()

    def _apply_concept_hype_result(self, result: Dict[str, Any]) -> None:
        """主线程回调：保存结果 + 刷新摘要 + 刷新列表。"""
        self._concept_hype_result = result or {}
        ml = (result or {}).get("main_line") or {}
        stats = (result or {}).get("stats") or {}
        dates = (result or {}).get("trade_dates") or []
        # 摘要
        self._concept_hype_summary.config(state=tk.NORMAL)
        self._concept_hype_summary.delete("1.0", tk.END)
        lines: List[str] = []
        if dates:
            lines.append(
                f"窗口：{dates[0]} ~ {dates[-1]}（{len(dates)} 个交易日）"
            )
        lines.append(
            f"题材数 {stats.get('total_concepts', 0)}（今日活跃 "
            f"{stats.get('active_concepts', 0)}） | 累计涨停 "
            f"{stats.get('total_limit_ups', 0)} 次"
        )
        lines.append("")
        if ml.get("name"):
            lines.append("【主线】")
            lines.append(ml.get("summary", ""))
        else:
            lines.append(ml.get("summary", "无主线"))
        lines.append("")
        fresh = stats.get("fresh_concepts") or []
        if fresh:
            lines.append(f"【萌芽题材 ({len(fresh)})】")
            for f in fresh:
                lines.append(
                    f"  • {f['name']} ({f['source']}) "
                    f"起爆 {f['ignite_date']} 今 {f['today_count']} 只"
                )
        else:
            lines.append("【萌芽题材】无")
        self._concept_hype_summary.insert(tk.END, "\n".join(lines))
        self._concept_hype_summary.config(state=tk.DISABLED)
        # 状态
        self._concept_hype_status.config(
            text=f"已分析 · {result.get('generated_at', '')}",
            foreground="#2e7d32",
        )
        # 列表
        self._refresh_concept_hype_list()
        # 清空成员表
        for it in self._concept_hype_members_tree.get_children():
            self._concept_hype_members_tree.delete(it)

    def _filtered_concepts(self) -> List[Dict[str, Any]]:
        result = self._concept_hype_result or {}
        concepts = list(result.get("concepts") or [])
        if not concepts:
            return []
        src = self._concept_hype_source_var.get()
        phase = self._concept_hype_phase_var.get()
        kw = (self._concept_hype_keyword_var.get() or "").strip().lower()
        active_only = bool(self._concept_hype_active_only.get())
        out: List[Dict[str, Any]] = []
        for c in concepts:
            if src != "全部" and c.get("source") != src:
                continue
            if phase != "全部" and c.get("phase") != phase:
                continue
            if active_only and int(c.get("today_count", 0)) <= 0:
                continue
            if kw:
                hay = (
                    f"{c.get('name', '')} {c.get('source', '')} "
                    + " ".join(m.get("name", "") for m in (c.get("leaders") or []))
                ).lower()
                if kw not in hay:
                    continue
            out.append(c)
        # 排序
        col = self._concept_hype_sort_col
        rev = self._concept_hype_sort_reverse
        key_map = {
            "score": lambda c: int(c.get("opportunity_score", 0)),
            "name": lambda c: str(c.get("name", "")),
            "source": lambda c: str(c.get("source", "")),
            "today": lambda c: int(c.get("today_count", 0)),
            "total": lambda c: int(c.get("total_limit_ups", 0)),
            "active": lambda c: int(c.get("active_days", 0)),
            "ignite": lambda c: str(c.get("ignite_date", "")),
            "duration": lambda c: int(c.get("duration", 0)),
            "phase": lambda c: str(c.get("phase", "")),
            "trend": lambda c: str(c.get("trend", "")),
            "leaders": lambda c: -len(c.get("leaders") or []),
        }
        out.sort(key=key_map.get(col, key_map["score"]), reverse=rev)
        return out

    def _refresh_concept_hype_list(self) -> None:
        for it in self._concept_hype_rank_tree.get_children():
            self._concept_hype_rank_tree.delete(it)
        rows = self._filtered_concepts()
        for c in rows:
            leaders_str = " / ".join(
                f"{m.get('name', '')}({m.get('boards', 1)}板)"
                for m in (c.get("leaders") or [])[:4]
            )
            self._concept_hype_rank_tree.insert(
                "", tk.END,
                values=(
                    c.get("opportunity_score", 0),
                    c.get("name", ""),
                    c.get("source", ""),
                    c.get("today_count", 0),
                    c.get("total_limit_ups", 0),
                    c.get("active_days", 0),
                    c.get("ignite_date", ""),
                    f"{c.get('duration', 0)}d",
                    c.get("phase", ""),
                    c.get("trend", ""),
                    leaders_str,
                ),
                tags=(f"phase_{c.get('phase', '')}",),
            )
        total = len((self._concept_hype_result or {}).get("concepts") or [])
        self._concept_hype_count_label.config(
            text=f"显示 {len(rows)} / 共 {total}",
        )

    def _on_concept_hype_sort(self, col: str) -> None:
        if self._concept_hype_sort_col == col:
            self._concept_hype_sort_reverse = not self._concept_hype_sort_reverse
        else:
            self._concept_hype_sort_col = col
            # 数字类列默认降序，文本类列默认升序
            self._concept_hype_sort_reverse = col not in (
                "name", "source", "phase", "trend",
            )
        self._refresh_concept_hype_list()

    def _on_concept_hype_select(self, _event=None) -> None:
        """点击题材行 → 在下方表格展开成员明细。"""
        sel = self._concept_hype_rank_tree.selection()
        if not sel:
            return
        values = self._concept_hype_rank_tree.item(sel[0]).get("values") or []
        if not values:
            return
        name = str(values[0])
        source = str(values[1])
        concept = next(
            (
                c for c in (self._concept_hype_result or {}).get("concepts") or []
                if c.get("name") == name and c.get("source") == source
            ),
            None,
        )
        # 清表
        for it in self._concept_hype_members_tree.get_children():
            self._concept_hype_members_tree.delete(it)
        if not concept:
            return
        members = concept.get("members") or []
        for m in members:
            lu_dates = m.get("limit_up_dates") or []
            self._concept_hype_members_tree.insert(
                "", tk.END,
                values=(
                    m.get("code", ""),
                    m.get("name", ""),
                    m.get("industry", ""),
                    m.get("boards", 1),
                    f"{float(m.get('change_pct', 0)):.2f}",
                    f"{float(m.get('close', 0)):.2f}",
                    f"{float(m.get('turnover', 0)):.2f}",
                    len(lu_dates),
                    " ".join(lu_dates),
                ),
            )
        try:
            self._concept_hype_members_label.configure(
                text=f"题材成份股【{name} · {source}】 共 {len(members)} 只 · 双击跳详情",
            )
        except Exception:
            pass

    def _on_concept_hype_member_double_click(self, _event=None) -> None:
        sel = self._concept_hype_members_tree.selection()
        if not sel:
            return
        values = self._concept_hype_members_tree.item(sel[0]).get("values") or []
        if not values:
            return
        code = str(values[0]).strip().zfill(6)
        if not code:
            return
        try:
            self._cancel_scheduled_detail()
        except Exception:
            pass
        try:
            self.show_stock_detail(code, force_refresh=True)
            self.notebook.select(self.detail_tab_frame)
        except Exception as exc:  # noqa: BLE001
            self._log(f"打开股票详情失败 {code}: {exc}")

    # ============== 市场情绪 + 仓位建议 ==============
    def _refresh_sentiment_async(self) -> None:
        """后台刷新市场情绪条；同一只票同日已缓存则秒回。"""
        if (
            getattr(self, "_sentiment_thread", None) is not None
            and self._sentiment_thread.is_alive()
        ):
            return
        target = (self._predict_date_var.get() or "").strip().replace("-", "") or None
        try:
            self._sentiment_score_label.config(
                text="情绪: 分析中...", foreground="#1565c0",
            )
            self._sentiment_advice_label.config(text="", foreground="#1565c0")
            self._sentiment_summary_label.config(text="", foreground="#666")
        except Exception:
            return

        def _worker():
            try:
                r = market_sentiment_service.analyze_market_sentiment(
                    target,
                    fetch_external=True,
                    log=lambda s: self._post_to_ui(lambda m=s: self._log(m)),
                )
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
                self._post_to_ui(lambda m=err: self._log(f"市场情绪分析失败: {m}"))
                self._post_to_ui(lambda: self._sentiment_score_label.config(
                    text="情绪: 失败", foreground="#c62828",
                ))
                return
            self._post_to_ui(lambda res=r: self._apply_sentiment_result(res))

        t = threading.Thread(target=_worker, daemon=True)
        self._sentiment_thread = t
        t.start()

    def _apply_sentiment_result(self, r: Dict[str, Any]) -> None:
        self._sentiment_result = r or {}
        score = int(r.get("score", 50))
        advice = r.get("position_suggest") or {}
        color = advice.get("color", "#1f1f1f")
        try:
            self._sentiment_score_label.config(
                text=f"情绪 {r.get('trade_date', '-')} : {score}/100",
                foreground=color,
            )
            self._sentiment_advice_label.config(
                text=f"→ {advice.get('label', '-')}", foreground=color,
            )
            # 显示 5 个最重要指标的缩略
            sigs = r.get("signals") or []
            key_names = ("涨停数", "晋级率", "最高连板", "大盘", "跌停数")
            parts: List[str] = []
            for s in sigs:
                if s.get("name") in key_names:
                    delta = int(s.get("delta", 0))
                    sign = "+" if delta > 0 else ""
                    parts.append(f"{s.get('name', '')} {s.get('value', '')}({sign}{delta})")
            self._sentiment_summary_label.config(
                text=" · ".join(parts), foreground="#444",
            )
        except Exception:
            pass

    def _show_sentiment_detail(self) -> None:
        r = getattr(self, "_sentiment_result", None) or {}
        if not r:
            messagebox.showinfo(
                "市场情绪", "请先点击「刷新」分析市场情绪。", parent=self.root,
            )
            return
        win = tk.Toplevel(self.root)
        win.title(f"市场情绪明细 · {r.get('trade_date', '-')}")
        win.geometry("560x460")
        win.transient(self.root)

        body = ttk.Frame(win, padding=10)
        body.pack(fill=tk.BOTH, expand=True)

        score = int(r.get("score", 50))
        advice = r.get("position_suggest") or {}
        color = advice.get("color", "#1f1f1f")

        header = ttk.Frame(body)
        header.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(
            header, text=f"综合 {score}/100", font=("", 18, "bold"),
            foreground=color,
        ).pack(side=tk.LEFT)
        ttk.Label(
            header, text=f"→ 建议 {advice.get('label', '-')}",
            font=("", 13), foreground=color,
        ).pack(side=tk.LEFT, padx=10)

        ttk.Label(
            body, text=r.get("summary", ""), wraplength=520,
            foreground="#333",
        ).pack(anchor=tk.W, pady=(0, 8))

        # 信号明细表
        cols = ("name", "value", "delta", "note")
        tree = ttk.Treeview(body, columns=cols, show="headings", height=10)
        for col, (h, w, anc) in {
            "name": ("信号", 90, tk.W),
            "value": ("数值", 110, tk.W),
            "delta": ("加减分", 60, tk.CENTER),
            "note": ("解读", 270, tk.W),
        }.items():
            tree.heading(col, text=h)
            tree.column(col, width=w, anchor=anc)
        for s in r.get("signals") or []:
            d = int(s.get("delta", 0))
            tree.insert("", tk.END, values=(
                s.get("name", ""), s.get("value", ""),
                f"+{d}" if d > 0 else (str(d) if d < 0 else "0"),
                s.get("note", ""),
            ))
        tree.pack(fill=tk.BOTH, expand=True)

        ttk.Label(
            body, text=f"生成于: {r.get('generated_at', '-')}",
            foreground="#888",
        ).pack(anchor=tk.W, pady=(8, 0))

        bottom = ttk.Frame(body)
        bottom.pack(fill=tk.X, pady=(6, 0))
        ttk.Button(
            bottom, text="刷新（强制重拉外部数据）",
            command=lambda: (win.destroy(), self._force_refresh_sentiment()),
        ).pack(side=tk.LEFT)
        ttk.Button(bottom, text="关闭", command=win.destroy).pack(side=tk.RIGHT)

    def _force_refresh_sentiment(self) -> None:
        """清掉外部数据缓存后重算（解决盘中跌停/指数尚未更新到最新的情况）。"""
        td = (self._predict_date_var.get() or "").strip().replace("-", "")
        if td:
            try:
                stock_store.save_app_config(
                    f"{market_sentiment_service.CACHE_KEY_PREFIX}{td}", None,
                )
            except Exception:
                pass
        self._refresh_sentiment_async()

    # ============== AI 博弈短报 ==============
    def _open_daily_brief_window(self) -> None:
        """打开 AI 博弈短报窗口，展示融合后的语言化建议。

        基于内存中的 self._predict_result + self._concept_hype_result，
        优雅降级 — 任意一个有就能跑，全无则给出明确提示。
        """
        if (
            (not self._predict_result or not isinstance(self._predict_result, dict))
            and not getattr(self, "_concept_hype_result", None)
        ):
            messagebox.showinfo(
                "AI 博弈短报",
                "需要先运行至少一项分析：\n"
                "  · 涨停预测：点击「开始预测」\n"
                "  · 概念炒作：在「概念炒作」tab 点击「开始分析」\n\n"
                "短报会基于这些结果给出综合建议。",
                parent=self.root,
            )
            return

        win = tk.Toplevel(self.root)
        win.title("AI 博弈短报")
        win.geometry("680x520")
        win.transient(self.root)

        body = ttk.Frame(win, padding=10)
        body.pack(fill=tk.BOTH, expand=True)

        # 头部信息栏
        header = ttk.Frame(body)
        header.pack(fill=tk.X, pady=(0, 6))
        td = (self._predict_date_var.get() or "").strip().replace("-", "")
        if not td:
            td = datetime.now().strftime("%Y%m%d")
        ttk.Label(header, text=f"基准日: {td}", font=("", 10, "bold")).pack(side=tk.LEFT)
        meta_var = tk.StringVar(value="模型: -  生成于: -")
        ttk.Label(header, textvariable=meta_var, foreground="#666").pack(side=tk.LEFT, padx=10)

        status_var = tk.StringVar(value="正在调用 NIM 生成中...")
        ttk.Label(body, textvariable=status_var, foreground="#1565c0").pack(anchor=tk.W)

        text_widget = scrolledtext.ScrolledText(
            body, wrap=tk.WORD, font=("Microsoft YaHei", 10),
        )
        text_widget.pack(fill=tk.BOTH, expand=True, pady=(6, 6))
        text_widget.config(state=tk.DISABLED)

        # 底部按钮
        btn_bar = ttk.Frame(body)
        btn_bar.pack(fill=tk.X)
        regen_btn = ttk.Button(btn_bar, text="重新生成（强制忽略缓存）")
        regen_btn.pack(side=tk.LEFT)

        def _copy_brief():
            text = text_widget.get("1.0", tk.END).strip()
            if not text:
                return
            try:
                self.root.clipboard_clear()
                self.root.clipboard_append(text)
                status_var.set("已复制到剪贴板")
            except Exception as exc:  # noqa: BLE001
                status_var.set(f"复制失败: {exc}")

        ttk.Button(btn_bar, text="复制", command=_copy_brief).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(btn_bar, text="关闭", command=win.destroy).pack(side=tk.RIGHT)

        def _set_text(content: str, color: str = "#1f1f1f") -> None:
            text_widget.config(state=tk.NORMAL)
            text_widget.delete("1.0", tk.END)
            text_widget.insert(tk.END, content)
            text_widget.tag_configure("body", foreground=color)
            text_widget.tag_add("body", "1.0", tk.END)
            text_widget.config(state=tk.DISABLED)

        def _run(use_cache: bool) -> None:
            from src.services import daily_brief_service as svc
            try:
                from llm_client import LlmConfigError, LlmRequestError
            except ImportError:
                LlmConfigError = LlmRequestError = Exception  # type: ignore

            status_var.set("正在调用 NIM 生成中..." if use_cache else "强制重新生成中...")
            regen_btn.config(state=tk.DISABLED)

            def _worker():
                try:
                    result = svc.generate_daily_brief(
                        td,
                        predict_result=self._predict_result,
                        hype_result=getattr(self, "_concept_hype_result", None),
                        sentiment_result=getattr(self, "_sentiment_result", None),
                        use_cache=use_cache,
                        log=lambda s: self._post_to_ui(lambda m=s: self._log(m)),
                    )
                except LlmConfigError as exc:
                    msg = str(exc)
                    self._post_to_ui(lambda: status_var.set("失败：未配置 API Key"))
                    self._post_to_ui(lambda m=msg: _set_text(
                        f"未能生成短报：\n\n{m}\n\n"
                        "请到设置 → 保存 NIM API Key，或设置环境变量 NVIDIA_API_KEY。",
                        color="#c62828",
                    ))
                    self._post_to_ui(lambda: regen_btn.config(state=tk.NORMAL))
                    return
                except LlmRequestError as exc:
                    msg = str(exc)
                    self._post_to_ui(lambda: status_var.set("失败：NIM 调用错误"))
                    self._post_to_ui(lambda m=msg: _set_text(
                        f"NIM 调用失败：\n\n{m}",
                        color="#c62828",
                    ))
                    self._post_to_ui(lambda: regen_btn.config(state=tk.NORMAL))
                    return
                except Exception as exc:  # noqa: BLE001
                    msg = str(exc)
                    self._post_to_ui(lambda: status_var.set("失败"))
                    self._post_to_ui(lambda m=msg: _set_text(
                        f"未知错误：\n\n{m}", color="#c62828",
                    ))
                    self._post_to_ui(lambda: regen_btn.config(state=tk.NORMAL))
                    return

                def _apply():
                    _set_text(result.get("brief") or "(空)")
                    cache_tag = " · 来自缓存" if result.get("from_cache") else ""
                    meta_var.set(
                        f"模型: {result.get('model', '-')}  "
                        f"生成于: {result.get('generated_at', '-')}{cache_tag}  "
                        f"候选数: {result.get('candidates_count', 0)}  "
                        f"题材数: {result.get('hype_concepts_count', 0)}"
                    )
                    status_var.set("完成" + cache_tag)
                    regen_btn.config(state=tk.NORMAL)

                self._post_to_ui(_apply)

            threading.Thread(target=_worker, daemon=True).start()

        regen_btn.config(command=lambda: _run(use_cache=False))
        # 首次打开：尝试缓存优先
        _run(use_cache=True)

    # ============== 批量回测 ==============
    def _open_predict_backtest_dialog(self) -> None:
        """打开批量回测对话框：输入日期范围，预校验可用数据，确认后后台跑。"""
        win = tk.Toplevel(self.root)
        win.title("批量回测")
        win.geometry("520x340")
        win.transient(self.root)

        body = ttk.Frame(win, padding=10)
        body.pack(fill=tk.BOTH, expand=True)

        ttk.Label(body, text="对历史日期回放预测并统计成功率。",
                  foreground="#555").pack(anchor=tk.W, pady=(0, 6))
        ttk.Label(body, text="数据依赖：history 表 + limit_up_pool 表均需缓存。",
                  foreground="#888", font=("", 9)).pack(anchor=tk.W, pady=(0, 8))

        # 日期输入
        form = ttk.Frame(body)
        form.pack(fill=tk.X, pady=4)
        ttk.Label(form, text="起始日期:").grid(row=0, column=0, sticky=tk.W, padx=(0, 4))
        # 默认起始：最近一个有 pool 缓存的 14 天前
        try:
            pool_dates = stock_store.list_limit_up_pool_trade_dates()
        except Exception:
            pool_dates = []
        default_end = pool_dates[-1] if pool_dates else datetime.now().strftime("%Y%m%d")
        default_start = pool_dates[0] if pool_dates else default_end
        from_var = tk.StringVar(value=default_start)
        to_var = tk.StringVar(value=default_end)
        ttk.Entry(form, textvariable=from_var, width=12).grid(row=0, column=1, padx=(0, 8))
        ttk.Label(form, text="结束日期:").grid(row=0, column=2, sticky=tk.W, padx=(0, 4))
        ttk.Entry(form, textvariable=to_var, width=12).grid(row=0, column=3, padx=(0, 8))
        ttk.Label(form, text="回溯天数:").grid(row=0, column=4, sticky=tk.W, padx=(0, 4))
        lookback_var = tk.StringVar(value="5")
        ttk.Entry(form, textvariable=lookback_var, width=4).grid(row=0, column=5)

        status_var = tk.StringVar(value="")
        ttk.Label(body, textvariable=status_var, foreground="#1565c0",
                  wraplength=480, justify=tk.LEFT).pack(anchor=tk.W, pady=(8, 0))

        log_text = scrolledtext.ScrolledText(body, height=10, wrap=tk.WORD)
        log_text.pack(fill=tk.BOTH, expand=True, pady=(8, 6))
        log_text.config(state=tk.DISABLED)

        btn_bar = ttk.Frame(body)
        btn_bar.pack(fill=tk.X, pady=(2, 0))

        def _append_log(s: str) -> None:
            log_text.config(state=tk.NORMAL)
            log_text.insert(tk.END, s + "\n")
            log_text.see(tk.END)
            log_text.config(state=tk.DISABLED)

        def _check_feasible_dates() -> List[str]:
            s = from_var.get().strip()
            e = to_var.get().strip()
            if len(s) != 8 or len(e) != 8 or not s.isdigit() or not e.isdigit():
                messagebox.showwarning("日期格式", "请输入 YYYYMMDD 格式", parent=win)
                return []
            if s > e:
                messagebox.showwarning("日期范围", "起始日期不能晚于结束日期", parent=win)
                return []
            try:
                hist = set(stock_store.list_history_trade_dates_in_range(s, e))
                pool = set(stock_store.list_limit_up_pool_trade_dates())
            except Exception as exc:
                messagebox.showerror("数据检查失败", str(exc), parent=win)
                return []
            # 可回测 = 该日有 history & limit_up_pool & T+1 也有 history
            feasible_set = hist & pool
            feasible = sorted(feasible_set)
            # 去掉没有 T+1 history 的日期（无法评估准确率）
            evaluable = [d for d in feasible if any(h > d for h in hist)]
            return evaluable

        def _on_check():
            ds = _check_feasible_dates()
            log_text.config(state=tk.NORMAL)
            log_text.delete("1.0", tk.END)
            log_text.config(state=tk.DISABLED)
            if not ds:
                status_var.set("范围内没有可回测的日期（需要 history + limit_up_pool 都有缓存，并有 T+1 数据）")
                _append_log("可回测日期：0 天")
                return
            status_var.set(f"范围内可回测 {len(ds)} 天")
            _append_log(f"可回测日期（{len(ds)} 天）：")
            for d in ds:
                _append_log(f"  {d}")

        def _run_worker(dates: List[str], lookback: int):
            evaluated_dates: List[str] = []
            for i, d in enumerate(dates, 1):
                self._post_to_ui(lambda x=i, n=len(dates), td=d:
                                 status_var.set(f"[{x}/{n}] 回测 {td} ..."))
                try:
                    result = self.stock_filter.predict_limit_up_candidates(
                        d, lookback_days=lookback, historical_mode=True,
                    )
                    self._post_to_ui(lambda td=d, r=result, x=i, n=len(dates):
                                     _append_log(
                                         f"[{x}/{n}] {td} → cont={len(r.get('continuation_candidates', []))} "
                                         f"first={len(r.get('first_board_candidates', []))} "
                                         f"fresh={len(r.get('fresh_first_board_candidates', []))} "
                                         f"wrap={len(r.get('broken_board_wrap_candidates', []))} "
                                         f"trend={len(r.get('trend_limit_up_candidates', []))}"
                                     ))
                    evaluated_dates.append(d)
                except Exception as exc:
                    err = str(exc)
                    self._post_to_ui(lambda td=d, e=err:
                                     _append_log(f"  {td} 失败: {e}"))
            # 全部跑完 → 强制评估这批日期
            self._post_to_ui(lambda: status_var.set(f"预测完成，正在评估准确率..."))
            try:
                from src.services import prediction_accuracy_service as svc
                for d in evaluated_dates:
                    try:
                        svc.evaluate(d)
                    except Exception as exc:
                        self._post_to_ui(lambda td=d, e=str(exc):
                                         _append_log(f"  评估 {td} 失败: {e}"))
                # 汇总最近 N 日命中率
                stats = svc.query_category_stats(lookback_dates=max(1, len(evaluated_dates)))
            except Exception as exc:
                self._post_to_ui(lambda e=str(exc):
                                 status_var.set(f"评估失败: {e}"))
                return

            def _show_done():
                status_var.set(f"完成。共 {len(evaluated_dates)} 天预测、评估完成")
                _append_log("")
                _append_log("=== 命中率汇总（按类别）===")
                for cat, lbl in [("cont", "保留涨停"), ("first", "二波接力"),
                                 ("fresh", "首板涨停"), ("wrap", "反包/承接"),
                                 ("trend", "趋势涨停")]:
                    d = stats.get(cat) or {}
                    b = int(d.get("buyable") or 0)
                    h = int(d.get("hit_primary") or d.get("hit_strict") or 0)
                    r = float(d.get("primary_rate") or d.get("strict_rate") or 0.0)
                    if b > 0:
                        _append_log(f"  {lbl}: {r:.1f}% ({h}/{b})")
                    else:
                        _append_log(f"  {lbl}: 暂无可买样本")
                # 刷新主页面
                try:
                    self._refresh_predict_accuracy_async("")
                except Exception:
                    pass

            self._post_to_ui(_show_done)

        def _on_start():
            try:
                lookback = max(2, min(int(lookback_var.get().strip() or "5"), 15))
            except ValueError:
                lookback = 5
            lookback_var.set(str(lookback))
            ds = _check_feasible_dates()
            if not ds:
                status_var.set("没有可回测的日期")
                return
            if len(ds) < 3:
                if not messagebox.askyesno(
                    "样本过少",
                    f"范围内只有 {len(ds)} 天可回测，样本量较少。是否继续？",
                    parent=win,
                ):
                    return
            start_btn.config(state=tk.DISABLED)
            check_btn.config(state=tk.DISABLED)
            log_text.config(state=tk.NORMAL)
            log_text.delete("1.0", tk.END)
            log_text.config(state=tk.DISABLED)
            _append_log(f"开始回测 {len(ds)} 天 (lookback={lookback})...")
            threading.Thread(
                target=_run_worker, args=(ds, lookback), daemon=True,
            ).start()

        check_btn = ttk.Button(btn_bar, text="检查可用数据", command=_on_check)
        check_btn.pack(side=tk.LEFT)
        start_btn = ttk.Button(btn_bar, text="开始回测", command=_on_start)
        start_btn.pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(btn_bar, text="关闭", command=win.destroy).pack(side=tk.RIGHT)

    _PROFILE_LABELS = {
        "change_pct_t1": "T-1涨跌幅%",
        "vol_ratio_t1": "T-1量比",
        "amt_ratio_t1": "T-1额比",
        "shrink_ratio_t1": "前3日/前5日缩量比",
        "dist_ma5_pct": "距MA5%",
        "dist_ma10_pct": "距MA10%",
        "trend_5d": "5日涨幅%",
        "trend_10d": "10日涨幅%",
        "position_60d": "60日位置%",
        "volatility_10d": "10日波动率%",
        "turnover_t1": "T-1换手率%",
    }

    def _apply_predict_result(self, result: Dict[str, Any]):
        self._predict_result = result
        # 先加载该日期的命中结果，再注入到 record（让"结果"列可参与排序）
        current_date = str(result.get("trade_date") or "").strip()
        self._predict_results_map = {}
        if current_date:
            try:
                self._predict_results_map = (
                    prediction_accuracy_service.get_per_code_results(current_date)
                )
            except Exception:
                self._predict_results_map = {}

        def _enrich(records: List[Dict[str, Any]], cat: str) -> List[Dict[str, Any]]:
            for rec in records:
                key = (str(rec.get("code") or "").zfill(6), cat)
                row = self._predict_results_map.get(key)
                if row:
                    # 用"开盘买、收盘卖"口径作为结果排序值；老记录回退到 t1_pct
                    pct = row.get("t1_open_close_pct")
                    if pct is None:
                        pct = row.get("t1_pct")
                    rec["_t1_pct"] = pct
                else:
                    rec["_t1_pct"] = None
            return records

        cont_list = self._sort_predict_records(_enrich(list(result.get("continuation_candidates", [])), "cont"), "cont")
        first_list = self._sort_predict_records(_enrich(list(result.get("first_board_candidates", [])), "first"), "first")
        fresh_list = self._sort_predict_records(_enrich(list(result.get("fresh_first_board_candidates", [])), "fresh"), "fresh")
        wrap_list = self._sort_predict_records(_enrich(list(result.get("broken_board_wrap_candidates", [])), "wrap"), "wrap")
        trend_list = self._sort_predict_records(_enrich(list(result.get("trend_limit_up_candidates", [])), "trend"), "trend")
        hot_industries = result.get("hot_industries", {})
        profile = result.get("profile", {})
        compare_context = result.get("compare_context", {})

        # ---- 填充摘要 ----
        self._predict_summary_text.config(state=tk.NORMAL)
        self._predict_summary_text.delete("1.0", tk.END)
        txt = self._predict_summary_text

        txt.insert(tk.END, result.get("summary", "") + "\n")

        # 兼容旧结果：若存在画像字段则仍展示
        if profile:
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, "  涨停前兆画像（T-1日特征统计）\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for key, label in self._PROFILE_LABELS.items():
                p = profile.get(key, {})
                if not p or p.get("count", 0) == 0:
                    continue
                txt.insert(tk.END,
                    f"  {label:14s}  中位={p['median']:>7s}  "
                    f"区间=[{p['p25']}, {p['p75']}]  "
                    f"均值={p['mean']}  样本={p['count']}\n".format_map({})
                    if False else
                    f"  {label:14s}  中位={p.get('median', '-')}  "
                    f"[{p.get('p25', '-')}~{p.get('p75', '-')}]  "
                    f"均值={p.get('mean', '-')}  n={p.get('count', 0)}\n")
            # 布尔特征
            for key, label in [("ma_bullish", "多头排列"), ("above_ma5", "站上MA5"), ("ma5_pullback", "回踩MA5")]:
                p = profile.get(key, {})
                if p:
                    txt.insert(tk.END,
                        f"  {label:14s}  {p.get('true_count', 0)}/{p.get('total', 0)}只  "
                        f"占比={p.get('ratio', 0):.1f}%\n")

        if compare_context.get("pair_stats"):
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, "  最近涨停对比环境\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for item in compare_context.get("pair_stats", [])[-5:]:
                rate = item.get("continuation_rate")
                rate_text = f"{rate:.1f}%" if rate is not None else "-"
                txt.insert(
                    tk.END,
                    f"  {item.get('yesterday_date', '-')}"
                    f"→{item.get('today_date', '-')}"
                    f"  昨首板{item.get('yesterday_first_count', 0):2d}只  "
                    f"晋级{item.get('continued_count', 0):2d}只  "
                    f"晋级率={rate_text}\n",
                )

        # 保留涨停 TOP10
        if cont_list:
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, f"  保留涨停候选 TOP10\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for rec in cont_list[:10]:
                boards_text = f"{rec['consecutive_boards']}板" if rec.get("consecutive_boards", 1) > 1 else "首板"
                txt.insert(tk.END,
                    f"  {rec['code']} {rec.get('name', ''):6s}  {boards_text:4s}  "
                    f"分={rec['score']:3d}  {rec.get('reasons', '')}\n")

        if first_list:
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, f"  二波接力候选 TOP10\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for rec in first_list[:10]:
                chg = rec.get("change_pct")
                chg_text = f"{chg:.1f}%" if chg is not None else "-"
                txt.insert(tk.END,
                    f"  {rec['code']} {rec.get('name', ''):6s}  涨{chg_text:6s}  "
                    f"分={rec['score']:3d}  {rec.get('reasons', '')}\n")

        if fresh_list:
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, f"  首板涨停候选 TOP10\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for rec in fresh_list[:10]:
                chg = rec.get("change_pct")
                chg_text = f"{chg:.1f}%" if chg is not None else "-"
                txt.insert(tk.END,
                    f"  {rec['code']} {rec.get('name', ''):6s}  涨{chg_text:6s}  "
                    f"分={rec['score']:3d}  {rec.get('reasons', '')}\n")

        if wrap_list:
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, f"  反包/承接候选 TOP10\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for rec in wrap_list[:10]:
                chg = rec.get("change_pct")
                chg_text = f"{chg:.1f}%" if chg is not None else "-"
                gap = rec.get("wrap_gap_pct")
                gap_text = f"差{gap:.1f}%" if gap is not None else "-"
                txt.insert(tk.END,
                    f"  {rec['code']} {rec.get('name', ''):6s}  涨{chg_text:6s} {gap_text:7s}  "
                    f"分={rec['score']:3d}  {rec.get('reasons', '')}\n")

        if trend_list:
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, f"  趋势涨停候选 TOP10\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for rec in trend_list[:10]:
                chg = rec.get("change_pct")
                chg_text = f"{chg:.1f}%" if chg is not None else "-"
                spread = rec.get("ma_spread_pct")
                spread_text = f"开口{spread:.1f}%" if spread is not None else "-"
                txt.insert(tk.END,
                    f"  {rec['code']} {rec.get('name', ''):6s}  涨{chg_text:6s} {spread_text:9s}  "
                    f"分={rec['score']:3d}  {rec.get('reasons', '')}\n")

        # 明日热点板块预测（基于今日涨停股的行业分布；今日热点延续到明日）
        if hot_industries:
            sorted_inds = sorted(hot_industries.items(), key=lambda x: -x[1])
            total_zt = sum(hot_industries.values()) or 1
            top5 = sorted_inds[:5]
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, f"  明日热点板块预测（TOP5 · 基于今日涨停股分布）\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for k, v in top5:
                ratio = v / total_zt * 100.0
                bar = "█" * min(v, 24)
                txt.insert(tk.END, f"  {k:12s}  {v:2d} 只 ({ratio:4.1f}%)  {bar}\n")
            # 11+ 名次的行业以紧凑形式追加
            if len(sorted_inds) > 5:
                tail = sorted_inds[5:10]
                tail_str = "、".join(f"{k}({v})" for k, v in tail)
                txt.insert(tk.END, f"  其他: {tail_str}\n")

        txt.insert(tk.END, f"\n{'='*36}\n")
        txt.insert(tk.END, "说明：预测基于最近涨停对比环境，以及“今日已启动+\n"
                           "收盘强势+距前涨停≤5日”的二波接力形态，仅供参考。\n"
                           "请结合次日竞价、盘口、板块情绪\n"
                           "综合判断。\n")
        self._predict_summary_text.config(state=tk.DISABLED)

        # 保存原始 5 类候选，供筛选实时重渲染
        self._predict_lists = {
            "cont": cont_list, "first": first_list, "fresh": fresh_list,
            "wrap": wrap_list, "trend": trend_list,
        }
        self._predict_compare_context = compare_context

        # 刷新行业下拉选项
        self._refresh_predict_industry_options()

        # 渲染 5 个候选表（应用当前筛选）
        self._render_predict_trees()

        # 同步刷新历史记录下拉，并选中当前结果对应的日期
        self._refresh_predict_history_dates(select=current_date or None)

        # 异步刷新命中率统计 + 触发未回填日期的回填
        self._refresh_predict_accuracy_async(current_date)

        # 启动后台预热：把所有候选股的分时 + 资金流缓存起来，方便用户后续秒开
        self._start_predict_prewarm(result)

        # 冷启动检测：保留涨停有数据但其他 4 类全空，通常是本地历史K线缓存还没预热
        # （这些类目都依赖 65 日 K 线评分，cache_only 模式拿不到就直接被过滤）
        if (
            len(cont_list) > 0
            and len(first_list) == 0
            and len(fresh_list) == 0
            and len(wrap_list) == 0
            and len(trend_list) == 0
        ):
            messagebox.showwarning(
                "历史数据未就绪",
                "本地历史 K 线缓存尚未预热，\n"
                "「二波接力 / 首板涨停 / 断板反包 / 趋势涨停」候选暂时为空。\n\n"
                "首次点击会触发后台缓存预取，\n"
                "请稍等几秒后再次点击「预测涨停数据」按钮，\n"
                "即可看到完整候选列表。",
                parent=self.root,
            )

    # ============== 后台预热：分时 + 资金流缓存 ==============
    def _start_predict_prewarm(self, result: Dict[str, Any]) -> None:
        """预测完成后，对所有候选股票预热分时 + 资金流缓存，让后续点击秒开。

        - 自动去重 5 类候选的所有 code
        - 串行执行：先分时（用户最先点的多），再资金流
        - 每个任务内部并发 4 worker，可被新预测/取消令牌打断
        """
        if not isinstance(result, dict):
            return
        # 旧的预热在跑就先取消（新预测意味着新的候选集）
        old = getattr(self, "_predict_prewarm_thread", None)
        old_token = getattr(self, "_predict_prewarm_token", None)
        if old is not None and old.is_alive() and old_token is not None:
            try:
                old_token.cancel()
            except Exception:
                pass

        codes: set = set()
        # 同时按分数排序，挑 top-N 做上层 payload 预热（写入 GUI LRU 缓存，秒开）
        code_best_score: Dict[str, int] = {}
        for key in (
            "continuation_candidates", "first_board_candidates",
            "fresh_first_board_candidates", "broken_board_wrap_candidates",
            "trend_limit_up_candidates",
        ):
            for cand in result.get(key, []) or []:
                if not isinstance(cand, dict):
                    continue
                c = str(cand.get("code") or "").strip().zfill(6)
                if not c:
                    continue
                codes.add(c)
                try:
                    sc = int(cand.get("score") or 0)
                except (TypeError, ValueError):
                    sc = 0
                if sc > code_best_score.get(c, -1):
                    code_best_score[c] = sc
        if not codes:
            return
        try:
            lookback = max(1, min(int(self._predict_lookback_var.get().strip() or "5"), 5))
        except (ValueError, AttributeError, tk.TclError):
            lookback = 5
        codes_list = sorted(codes)
        # 按分数降序取 top-N 做上层 payload 预热，N 与 LRU 上限对齐避免互相挤掉
        top_codes_for_payload = [
            c for c, _ in sorted(
                code_best_score.items(), key=lambda kv: (-kv[1], kv[0]),
            )
        ][: self._DETAIL_CACHE_MAX]
        thread, token = self._start_background_job(
            self._run_predict_prewarm,
            name="predict-prewarm",
            args=(codes_list, lookback, top_codes_for_payload),
        )
        self._predict_prewarm_thread = thread
        self._predict_prewarm_token = token

    def _run_predict_prewarm(
        self, codes: List[str], lookback: int,
        top_codes_for_payload: List[str],
        cancel_token: CancelToken,
    ) -> None:
        """预热 worker：分时 → 资金流 → 上层 payload (top-N，写 GUI LRU)。"""
        total = len(codes)
        if total == 0:
            return
        fetcher = self.stock_filter.fetcher

        def _report_intraday(done: int, n: int, _code: str) -> None:
            msg = f"预热分时 {done}/{n} · 资金流待跑"
            self._post_to_ui(lambda m=msg: self.status_var.set(m))

        def _report_flow(done: int, n: int, _code: str) -> None:
            msg = f"预热资金流 {done}/{n}（分时完成）"
            self._post_to_ui(lambda m=msg: self.status_var.set(m))

        try:
            self._post_to_ui(
                lambda: self.status_var.set(f"预热缓存启动：分时 0/{total}")
            )
            intraday_stat = fetcher.prewarm_intraday_for_codes(
                codes,
                ndays=max(1, min(int(lookback), 5)),
                max_workers=4,
                cancel_check=lambda: cancel_token.is_cancelled(),
                progress_cb=_report_intraday,
            )
            if cancel_token.is_cancelled():
                self._post_to_ui(lambda: self.status_var.set("预热已取消"))
                return
            flow_stat = fetcher.prewarm_fund_flow_for_codes(
                codes,
                days=30,
                max_workers=4,
                cancel_check=lambda: cancel_token.is_cancelled(),
                progress_cb=_report_flow,
            )
            if cancel_token.is_cancelled():
                self._post_to_ui(lambda: self.status_var.set("预热已取消"))
                return
            # 底层缓存就绪后，对 top-N 跑完整 get_stock_detail + get_stock_intraday，
            # 写到 GUI LRU 缓存 —— 用户点击 top-N 候选直接秒开
            payload_stat = self._prewarm_upper_payloads(
                top_codes_for_payload, cancel_token,
            )
            summary = (
                f"预热完成 · 分时 {intraday_stat['done']-intraday_stat['failed']}/{intraday_stat['total']}"
                f"，资金流 {flow_stat['done']-flow_stat['failed']}/{flow_stat['total']}"
                f"，详情payload {payload_stat['done']-payload_stat['failed']}/{payload_stat['total']}"
            )
            self._post_to_ui(lambda s=summary: self.status_var.set(s))
        except Exception as exc:
            err = str(exc)
            self._post_to_ui(lambda e=err: self.status_var.set(f"预热失败: {e}"))

    def _prewarm_upper_payloads(
        self, codes: List[str], cancel_token: CancelToken,
    ) -> Dict[str, int]:
        """对 top-N 候选直接调上层 get_stock_detail/get_stock_intraday，结果写 GUI LRU。

        底层数据已被 prewarm_intraday/prewarm_fund_flow 缓存，这里主要在补齐：
        - analyze_history + 技术指标 (MACD/KDJ/RSI/BOLL) 的计算
        - _resolve_intraday_prev_close 的计算
        - GUI LRU 缓存的填充

        用户点击 top-N 候选时 → LRU 命中 → 秒开。
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        total = len(codes or [])
        stat = {"total": total, "done": 0, "failed": 0}
        if total == 0:
            return stat
        self._post_to_ui(
            lambda n=total: self.status_var.set(f"预热详情 payload 0/{n}")
        )

        def _one(code: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
            if cancel_token.is_cancelled():
                return code, None, None
            detail = None
            intraday = None
            try:
                detail = self.stock_filter.get_stock_detail(code)
            except Exception:
                detail = None
            if cancel_token.is_cancelled():
                return code, detail, None
            try:
                intraday = self.stock_filter.get_stock_intraday(code, day_offset=0)
            except Exception:
                intraday = None
            return code, detail, intraday

        def _write_to_lru(code: str, detail, intraday) -> None:
            now = time.time()
            if isinstance(detail, dict):
                self._detail_payload_cache[code] = (now, detail)
                self._detail_payload_cache.move_to_end(code)
                while len(self._detail_payload_cache) > self._DETAIL_CACHE_MAX:
                    self._detail_payload_cache.popitem(last=False)
            if isinstance(intraday, dict):
                key = (code, 0, "")
                self._intraday_payload_cache[key] = (now, intraday, True)
                self._intraday_payload_cache.move_to_end(key)
                while len(self._intraday_payload_cache) > self._INTRADAY_CACHE_MAX:
                    self._intraday_payload_cache.popitem(last=False)

        # 3 worker 并行：底层缓存已就绪，主要瓶颈是上层分析 + 锁竞争
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="payload-prewarm") as pool:
            futs = {pool.submit(_one, c): c for c in codes}
            for fut in as_completed(futs):
                if cancel_token.is_cancelled():
                    break
                try:
                    code, detail, intraday = fut.result()
                except Exception:
                    stat["failed"] += 1
                    stat["done"] += 1
                    continue
                if detail is None and intraday is None:
                    stat["failed"] += 1
                else:
                    self._post_to_ui(
                        lambda c=code, d=detail, i=intraday: _write_to_lru(c, d, i),
                    )
                stat["done"] += 1
                self._post_to_ui(
                    lambda done=stat["done"], n=total:
                    self.status_var.set(f"预热详情 payload {done}/{n}")
                )
        return stat

    # ============== 候选筛选与表格渲染 ==============
    def _refresh_predict_industry_options(self) -> None:
        """根据当前 5 类候选，刷新行业下拉选项。"""
        industries: set = set()
        for lst in (self._predict_lists or {}).values():
            for rec in lst or []:
                ind = (rec.get("industry") or "").strip()
                if ind:
                    industries.add(ind)
        values = ("全部",) + tuple(sorted(industries))
        try:
            self._predict_filter_industry_combo.configure(values=values)
        except Exception:
            return
        if self._predict_filter_industry.get() not in values:
            self._predict_filter_industry.set("全部")

    def _reset_predict_filters(self) -> None:
        self._predict_filter_min_score.set(0)
        self._predict_filter_keyword.set("")
        self._predict_filter_industry.set("全部")
        self._predict_filter_lhb_only.set(False)
        self._predict_filter_northbound_only.set(False)
        self._predict_filter_theme_only.set(False)
        self._render_predict_trees()

    def _on_predict_filter_changed(self) -> None:
        """筛选条件变化时重渲染表格（不重跑预测）。"""
        self._render_predict_trees()

    def _on_predict_sort_mode_changed(self) -> None:
        """切换"按历史命中段排序"开关：清空缓存并重排 5 类列表。"""
        # 切换模式时强制重新读取（lookback 内最新累计的命中数据可能已变化）
        self._predict_bucket_rates_cache = {}
        if self._predict_result:
            self._apply_predict_result(self._predict_result)

    def _matches_predict_filters(self, rec: Dict[str, Any]) -> bool:
        """记录是否通过当前筛选条件。"""
        try:
            min_score = int(self._predict_filter_min_score.get() or 0)
        except (TypeError, ValueError, tk.TclError):
            min_score = 0
        if int(rec.get("score") or 0) < min_score:
            return False

        # 顶部全局价格过滤（与扫描/涨停对比保持一致）
        try:
            min_price_raw = (self.min_price_var.get() or "").strip()
            min_price = float(min_price_raw) if min_price_raw else None
        except (ValueError, AttributeError, tk.TclError):
            min_price = None
        try:
            max_price_raw = (self.max_price_var.get() or "").strip()
            max_price = float(max_price_raw) if max_price_raw else None
        except (ValueError, AttributeError, tk.TclError):
            max_price = None
        close = rec.get("close")
        if isinstance(close, (int, float)):
            if min_price is not None and close < min_price:
                return False
            if max_price is not None and close > max_price:
                return False

        # 顶部全局板块过滤
        try:
            allowed_boards = {str(b).strip() for b in self._selected_boards() if str(b).strip()}
        except Exception:
            allowed_boards = set()
        if allowed_boards:
            code = str(rec.get("code", "")).strip().zfill(6)
            board = self._infer_board_from_code(code)
            if board not in allowed_boards:
                return False

        kw = (self._predict_filter_keyword.get() or "").strip().lower()
        if kw:
            haystack = " ".join(str(rec.get(f, "") or "") for f in
                                ("code", "name", "industry", "reasons", "predict_type"))
            if kw not in haystack.lower():
                return False

        ind_filter = (self._predict_filter_industry.get() or "全部").strip()
        if ind_filter and ind_filter != "全部":
            if (rec.get("industry") or "").strip() != ind_filter:
                return False

        ctx = self._predict_compare_context or {}
        code = (rec.get("code") or "").strip().zfill(6)

        if self._predict_filter_lhb_only.get():
            lhb = (ctx.get("lhb_map") or {}).get(code)
            if not lhb or float((lhb or {}).get("net_buy") or 0) <= 0:
                return False

        if self._predict_filter_northbound_only.get():
            nb = (ctx.get("northbound_map") or {}).get(code, 0)
            if not isinstance(nb, (int, float)) or nb < 200:
                return False

        if self._predict_filter_theme_only.get():
            theme_map = ctx.get("code_theme_map") or {}
            industry_heat = ctx.get("industry_theme_heat") or {}
            in_theme = code in theme_map
            ind_heat = industry_heat.get((rec.get("industry") or ""), 0)
            if not in_theme and ind_heat < 2:
                return False

        return True

    def _filter_predict_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [r for r in (records or []) if self._matches_predict_filters(r)]

    def _render_predict_trees(self) -> None:
        """根据当前筛选条件渲染 5 个候选表。"""
        if not getattr(self, "_predict_lists", None):
            return

        cont_list = self._filter_predict_records(self._predict_lists.get("cont", []))
        first_list = self._filter_predict_records(self._predict_lists.get("first", []))
        fresh_list = self._filter_predict_records(self._predict_lists.get("fresh", []))
        wrap_list = self._filter_predict_records(self._predict_lists.get("wrap", []))
        trend_list = self._filter_predict_records(self._predict_lists.get("trend", []))

        # 取本次预测对应日期的命中结果（若已回填）
        results_map = getattr(self, "_predict_results_map", {}) or {}

        cont_cat_keys = {"cont", "cont_1to2", "cont_2to3", "cont_3to4", "cont_4to5", "cont_5plus"}

        def _result_cell(category: str, code: str):
            """返回 (cell_text, hit_tag_or_None)。

            cont 类（保留涨停）必须涨停才算 hit；其他类别用「开盘买、收盘卖 ≥ 5%」或涨停。
            数字部分一律展示 t1_open_close_pct（实盘可达盈亏），老记录回退到 t1_pct。
            """
            row = results_map.get((str(code).zfill(6), category))
            if not row:
                return ("—", None)
            if row.get("t1_suspended"):
                return ("⏸停牌", None)
            if row.get("t1_one_word"):
                return ("一字未买", None)
            pct_oc = row.get("t1_open_close_pct")
            if pct_oc is None:
                pct_oc = row.get("t1_pct")  # 老数据回退
            if row.get("hit_strict"):
                t1pct = row.get("t1_pct")
                txt = "✓涨停" if t1pct is None else f"✓涨停 +{t1pct:.1f}%"
                return (txt, "hit")
            if pct_oc is None:
                return ("—", None)
            is_cont = category in cont_cat_keys
            if not is_cont and pct_oc >= 5:
                return (f"↑+{pct_oc:.1f}%", "hit")  # 弱命中：开盘买、收盘 ≥ 5%
            sign = "+" if pct_oc >= 0 else ""
            return (f"{sign}{pct_oc:.1f}%", "miss" if pct_oc < 0 else None)

        # ---- 填充连板延续表格 ----
        self._predict_cont_tree.delete(*self._predict_cont_tree.get_children())
        for rec in cont_list:
            res_text, hit_tag = _result_cell("cont", rec.get("code", ""))
            tag = self._predict_row_tag("cont", hit_tag, rec.get("score", 0))
            vals = (
                rec.get("code", ""),
                rec.get("name", ""),
                rec.get("industry", ""),
                str(rec.get("consecutive_boards", 1)),
                f"{rec['change_pct']:.2f}" if rec.get("change_pct") is not None else "-",
                f"{rec['close']:.2f}" if rec.get("close") is not None else "-",
                rec.get("first_board_time", "-"),
                str(rec.get("break_count", 0)),
                f"{rec['turnover']:.1f}" if rec.get("turnover") is not None else "-",
                str(rec.get("score", 0)),
                res_text,
                rec.get("reasons", ""),
            )
            self._predict_cont_tree.insert("", tk.END, values=vals, tags=(tag,))

        # ---- 填充首板候选表格 ----
        self._predict_first_tree.delete(*self._predict_first_tree.get_children())
        for rec in first_list:
            res_text, hit_tag = _result_cell("first", rec.get("code", ""))
            tag = self._predict_row_tag("first", hit_tag, rec.get("score", 0))
            vals = (
                rec.get("code", ""),
                rec.get("name", ""),
                rec.get("industry", ""),
                f"{rec['change_pct']:.2f}" if rec.get("change_pct") is not None else "-",
                f"{rec['close']:.2f}" if rec.get("close") is not None else "-",
                rec.get("burst_date", "-") or "-",
                f"{rec['volume_ratio']:.2f}" if rec.get("volume_ratio") is not None else "-",
                f"{rec['dist_ma5_pct']:.1f}" if rec.get("dist_ma5_pct") is not None else "-",
                str(rec.get("days_since_burst", 0)) if rec.get("days_since_burst") is not None else "-",
                str(rec.get("score", 0)),
                res_text,
                rec.get("reasons", ""),
            )
            self._predict_first_tree.insert("", tk.END, values=vals, tags=(tag,))

        # ---- 填充首板涨停候选表格 ----
        self._predict_fresh_tree.delete(*self._predict_fresh_tree.get_children())
        for rec in fresh_list:
            res_text, hit_tag = _result_cell("fresh", rec.get("code", ""))
            tag = self._predict_row_tag("fresh", hit_tag, rec.get("score", 0))
            vals = (
                rec.get("code", ""),
                rec.get("name", ""),
                rec.get("industry", ""),
                f"{rec['change_pct']:.2f}" if rec.get("change_pct") is not None else "-",
                f"{rec['close']:.2f}" if rec.get("close") is not None else "-",
                f"{rec['volume_ratio']:.2f}" if rec.get("volume_ratio") is not None else "-",
                f"{rec['dist_ma5_pct']:.1f}" if rec.get("dist_ma5_pct") is not None else "-",
                f"{rec['trend_5d']:.1f}" if rec.get("trend_5d") is not None else "-",
                f"{rec['position_60d']:.0f}" if rec.get("position_60d") is not None else "-",
                f"{rec['turnover']:.1f}" if rec.get("turnover") is not None else "-",
                str(rec.get("score", 0)),
                res_text,
                rec.get("reasons", ""),
            )
            self._predict_fresh_tree.insert("", tk.END, values=vals, tags=(tag,))

        # ---- 填充反包/承接候选表格 ----
        self._predict_wrap_tree.delete(*self._predict_wrap_tree.get_children())
        _PATTERN_LABELS = {"wrap": "断板反包", "hold_strong": "强势承接"}
        for rec in wrap_list:
            res_text, hit_tag = _result_cell("wrap", rec.get("code", ""))
            tag = self._predict_row_tag("wrap", hit_tag, rec.get("score", 0))
            vals = (
                rec.get("code", ""),
                rec.get("name", ""),
                rec.get("industry", ""),
                _PATTERN_LABELS.get(rec.get("pattern_kind", ""), rec.get("predict_type", "-")),
                f"{rec['change_pct']:.2f}" if rec.get("change_pct") is not None else "-",
                f"{rec['close']:.2f}" if rec.get("close") is not None else "-",
                rec.get("prior_lu_date", "-") or "-",
                f"{rec['prior_lu_close']:.2f}" if rec.get("prior_lu_close") is not None else "-",
                f"{rec['wrap_gap_pct']:.1f}" if rec.get("wrap_gap_pct") is not None else "-",
                str(rec.get("days_since_lu", "-")) if rec.get("days_since_lu") is not None else "-",
                f"{rec['worst_drop']:.1f}" if rec.get("worst_drop") is not None else "-",
                f"{rec['volume_ratio']:.2f}" if rec.get("volume_ratio") is not None else "-",
                str(rec.get("score", 0)),
                res_text,
                rec.get("reasons", ""),
            )
            self._predict_wrap_tree.insert("", tk.END, values=vals, tags=(tag,))

        # ---- 填充趋势涨停候选表格 ----
        self._predict_trend_tree.delete(*self._predict_trend_tree.get_children())
        for rec in trend_list:
            res_text, hit_tag = _result_cell("trend", rec.get("code", ""))
            tag = self._predict_row_tag("trend", hit_tag, rec.get("score", 0))
            vals = (
                rec.get("code", ""),
                rec.get("name", ""),
                rec.get("industry", ""),
                f"{rec['change_pct']:.2f}" if rec.get("change_pct") is not None else "-",
                f"{rec['close']:.2f}" if rec.get("close") is not None else "-",
                f"{rec['ma_spread_pct']:.2f}" if rec.get("ma_spread_pct") is not None else "-",
                f"{rec['dist_ma5_pct']:+.1f}" if rec.get("dist_ma5_pct") is not None else "-",
                f"{rec['ma20_slope_pct']:.2f}" if rec.get("ma20_slope_pct") is not None else "-",
                f"{rec['trend_5d']:+.1f}" if rec.get("trend_5d") is not None else "-",
                f"{rec['trend_10d']:+.1f}" if rec.get("trend_10d") is not None else "-",
                f"{rec['position_60d']:.0f}" if rec.get("position_60d") is not None else "-",
                f"{rec['volume_ratio']:.2f}" if rec.get("volume_ratio") is not None else "-",
                str(rec.get("score", 0)),
                res_text,
                rec.get("reasons", ""),
            )
            self._predict_trend_tree.insert("", tk.END, values=vals, tags=(tag,))

        # 更新 Tab 标题：显示「筛选后/总数」
        raw = getattr(self, "_predict_lists", {}) or {}
        total_cont = len(raw.get("cont", []))
        total_first = len(raw.get("first", []))
        total_fresh = len(raw.get("fresh", []))
        total_wrap = len(raw.get("wrap", []))
        total_trend = len(raw.get("trend", []))
        def _label(name: str, shown: int, total: int) -> str:
            return f"{name}({shown}/{total})" if shown != total else f"{name}({total})"
        self._predict_table_nb.tab(0, text=_label("保留涨停候选", len(cont_list), total_cont))
        self._predict_table_nb.tab(1, text=_label("二波接力候选", len(first_list), total_first))
        self._predict_table_nb.tab(2, text=_label("首板涨停候选", len(fresh_list), total_fresh))
        self._predict_table_nb.tab(3, text=_label("反包/承接候选", len(wrap_list), total_wrap))
        self._predict_table_nb.tab(4, text=_label("趋势涨停候选", len(trend_list), total_trend))

        shown_total = len(cont_list) + len(first_list) + len(fresh_list) + len(wrap_list) + len(trend_list)
        raw_total = total_cont + total_first + total_fresh + total_wrap + total_trend
        if shown_total != raw_total:
            self._predict_filter_count_label.config(text=f"筛选后 {shown_total}/{raw_total}")
        else:
            self._predict_filter_count_label.config(text=f"共 {raw_total} 只")

        self._predict_status_label.config(text="")
        self.status_var.set(
            f"涨停预测完成: 保留涨停{total_cont} / 二波接力{total_first} / "
            f"首板{total_fresh} / 反包{total_wrap} / 趋势{total_trend}"
        )

    def _on_predict_stock_select(self, event):
        tree = event.widget
        sel = tree.selection()
        if not sel:
            return
        vals = tree.item(sel[0], "values")
        if vals:
            code = str(vals[0]).strip().zfill(6)
            self.status_var.set(f"预测候选: {code} {vals[1] if len(vals) > 1 else ''}")

    def _on_predict_stock_double_click(self, event):
        tree = event.widget
        stock_code = self._get_tree_selected_code(tree)
        if not stock_code:
            return
        self._cancel_scheduled_detail()
        self.show_stock_detail(stock_code, force_refresh=True)
        self.notebook.select(self.detail_tab_frame)

    # ============== 涨停预测准确率（命中对比） ==============
    def _refresh_predict_accuracy_async(self, current_date: str = "") -> None:
        """后台触发待回填的准确率，并刷新 5 个 tab 的命中率标签。"""
        def _worker():
            try:
                # 仅评估尚未评估且 T+1 已就绪的日期，幂等
                # refresh_stale=True：早上 K 线没到位时会按需 fetch 最新数据，
                # 避免"昨日命中率"因本地缓存陈旧而显示一片 suspended
                prediction_accuracy_service.evaluate_all_pending(refresh_stale=True)
            except Exception:
                pass
            # 拉取分类统计：近 20 日 + 昨日（最近一个已评估交易日）
            try:
                stats = prediction_accuracy_service.query_category_stats(lookback_dates=20)
            except Exception:
                stats = {}
            try:
                stats_yesterday = prediction_accuracy_service.query_category_stats_yesterday()
            except Exception:
                stats_yesterday = {}
            # 重新加载当前日期的逐行结果
            results_map = {}
            if current_date:
                try:
                    results_map = prediction_accuracy_service.get_per_code_results(current_date)
                except Exception:
                    results_map = {}
            # 提前拉取每类的分数段命中率（在 worker 线程做 DB 查询，避免 UI 卡顿）
            # 用于 _apply_predict_accuracy 后续计算"历史最优段"
            bucket_rates_by_cat: Dict[str, Dict[Tuple[int, int], Dict[str, Any]]] = {}
            for cat in ("cont", "first", "fresh", "wrap", "trend"):
                try:
                    bucket_rates_by_cat[cat] = prediction_accuracy_service.get_score_bucket_rates(
                        category=cat, lookback_dates=20, min_samples=5,
                    )
                except Exception:
                    bucket_rates_by_cat[cat] = {}
            self._post_to_ui(
                lambda s=stats, y=stats_yesterday, m=results_map, br=bucket_rates_by_cat:
                self._apply_predict_accuracy(s, m, y, br)
            )

        threading.Thread(target=_worker, daemon=True).start()

    def _apply_predict_accuracy(
        self,
        stats: Dict[str, Dict[str, Any]],
        results_map: Dict,
        stats_yesterday: Optional[Dict[str, Dict[str, Any]]] = None,
        bucket_rates_by_cat: Optional[
            Dict[str, Dict[Tuple[int, int], Dict[str, Any]]]
        ] = None,
    ) -> None:
        """更新 5 个 tab 顶部的命中率标签 + 刷新 result 列。

        stats: 近 N 日命中率（默认 N=20）
        stats_yesterday: 最近一个已评估交易日的命中率（"昨日"）
        bucket_rates_by_cat: worker 线程预取的分数段命中率，避免 UI 卡顿
        """
        # 用 worker 预取结果重置缓存；若调用方没传则按旧行为清空，懒加载兜底
        if bucket_rates_by_cat is not None:
            self._predict_bucket_rates_cache = dict(bucket_rates_by_cat)
        else:
            self._predict_bucket_rates_cache = {}
        labels = getattr(self, "_predict_stat_labels", {}) or {}
        stats_yesterday = stats_yesterday or {}
        category_names = {
            "cont": "保留涨停", "first": "二波接力", "fresh": "首板涨停",
            "wrap": "反包/承接", "trend": "趋势涨停",
            "cont_1to2": "1进2", "cont_2to3": "2进3", "cont_3to4": "3进4",
            "cont_4to5": "4进5", "cont_5plus": "5进6+",
        }
        sub_keys = {"cont_1to2", "cont_2to3", "cont_3to4", "cont_4to5", "cont_5plus"}

        def _fmt_pair(d: Dict[str, Any]) -> str:
            """昨日命中率紧凑串。无样本时返回 '-'。"""
            d = d or {}
            b = int(d.get("buyable") or 0)
            if b <= 0:
                return "-"
            # primary 口径：cont 走 hit_strict（涨停），其他类别走 hit_loose（开→收 ≥ 5% 或涨停）
            h = int(d.get("hit_primary") or d.get("hit_strict") or 0)
            r = float(d.get("primary_rate") or d.get("strict_rate") or 0.0)
            return f"{r:.1f}% ({h}/{b})"

        for cat, lbl in labels.items():
            data = stats.get(cat) or {}
            y_data = stats_yesterday.get(cat) or {}
            buyable = int(data.get("buyable") or 0)
            hit = int(data.get("hit_primary") or data.get("hit_strict") or 0)
            rate = float(data.get("primary_rate") or data.get("strict_rate") or 0.0)
            avg_pct = float(data.get("avg_pct") or 0.0)
            dates = int(data.get("dates") or 0)
            y_str = _fmt_pair(y_data)
            name = category_names.get(cat, cat)
            if cat in sub_keys:
                # 子类别紧凑格式：1进2 · 昨 60.0% (3/5) | 近20d 42.1% (8/19)
                if buyable <= 0:
                    txt = f"{name}: 昨{y_str} | 近-"
                else:
                    txt = (
                        f"{name}: 昨{y_str} | "
                        f"近{dates}d {rate:.1f}% ({hit}/{buyable})"
                    )
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
            try:
                lbl.configure(text=txt)
            except Exception:
                pass

        # 计算每个主类别的"历史最优分数段"并刷新黄色提示标签
        self._refresh_predict_best_bucket_labels()

        # 当前日期的逐行结果（用于 result 列着色）
        if results_map:
            self._predict_results_map = results_map
            try:
                self._render_predict_trees()
            except Exception:
                pass
        else:
            # 即使没有逐行结果，best_bucket 也可能因数据更新而变化 —— 刷新一次行高亮
            try:
                if getattr(self, "_predict_lists", None):
                    self._render_predict_trees()
            except Exception:
                pass

    def _refresh_predict_best_bucket_labels(self) -> None:
        """读取近 20 日的分数段命中率，找出每类历史命中率最高的桶并更新标签。

        eligible=True 的桶里挑 rate 最大者；同 rate 时挑分数段更高的（高分往往更稳）。
        样本不足或无回填时显示 "-"。结果同步写入 self._predict_best_buckets，
        供 _render_predict_trees 给落在该段的行打 best_bucket tag。
        """
        category_display = {
            "cont": "保留涨停", "first": "二波接力", "fresh": "首板涨停",
            "wrap": "反包/承接", "trend": "趋势涨停",
        }
        best_map: Dict[str, Optional[Tuple[int, int]]] = {}
        for cat in ("cont", "first", "fresh", "wrap", "trend"):
            rates = self._get_predict_bucket_rates(cat)
            best_bucket: Optional[Tuple[int, int]] = None
            best_info: Optional[Dict[str, Any]] = None
            for bucket, info in rates.items():
                if not info or not info.get("eligible"):
                    continue
                if best_info is None:
                    best_bucket, best_info = bucket, info
                    continue
                if (
                    float(info.get("rate") or 0) > float(best_info.get("rate") or 0)
                    or (
                        float(info.get("rate") or 0) == float(best_info.get("rate") or 0)
                        and bucket[0] > (best_bucket or (0, 0))[0]
                    )
                ):
                    best_bucket, best_info = bucket, info
            best_map[cat] = best_bucket
            lbl = self._predict_best_bucket_labels.get(cat)
            if lbl is None:
                continue
            if best_bucket is None or best_info is None:
                txt = f"历史最优段: -（{category_display.get(cat, cat)} 样本不足）"
            else:
                lo, hi = best_bucket
                txt = (
                    f"历史最优段: {lo}-{hi} 命中率 "
                    f"{float(best_info.get('rate') or 0):.1f}% "
                    f"(样本 {int(best_info.get('buyable') or 0)}) "
                    f"— 表中此段行已金色高亮"
                )
            try:
                lbl.configure(text=txt)
            except Exception:
                pass
        self._predict_best_buckets = best_map

    def _open_predict_compare_window(self) -> None:
        """弹窗：今日实际涨停 与 上次预测候选 的命中对比。"""
        # 默认对比"当前选中预测日期"或"最新预测"
        trade_date = (self._predict_history_var.get() or "").strip()
        if not trade_date:
            trade_date = (self._predict_date_var.get() or "").strip()
        if not trade_date:
            messagebox.showinfo("命中对比", "请先选择历史预测日期或先执行一次预测。",
                                parent=self.root)
            return

        # 先尝试同步评估（若 T+1 已就绪），再查询
        try:
            prediction_accuracy_service.evaluate(trade_date, refresh_stale=True)
        except Exception:
            pass
        try:
            data = prediction_accuracy_service.query_compare(trade_date)
        except Exception as exc:
            messagebox.showerror("命中对比", f"加载对比数据失败: {exc}",
                                 parent=self.root)
            return

        if not data.get("candidates"):
            verify_date = data.get("verify_date") or "—"
            messagebox.showinfo(
                "命中对比",
                f"{trade_date} 的预测候选尚未回填准确率。\n"
                f"验证日: {verify_date}\n\n"
                f"请确认 T+1 已收盘，且本地 K 线已同步到该日期。",
                parent=self.root,
            )
            return

        self._build_predict_compare_window(data)

    def _build_predict_compare_window(self, data: Dict[str, Any]) -> None:
        win = tk.Toplevel(self.root)
        win.title(f"命中对比 - {data.get('trade_date', '')} → {data.get('verify_date', '')}")
        win.geometry("1100x640")
        win.transient(self.root)

        # 顶部统计
        stats = data.get("stats", {}) or {}
        top = ttk.Frame(win, padding=(10, 8))
        top.pack(fill=tk.X)
        info = (
            f"预测日: {data.get('trade_date', '-')}    "
            f"验证日: {data.get('verify_date', '-')}    "
            f"预测候选: {stats.get('predicted', 0)} 只    "
            f"可买入: {stats.get('buyable', 0)}    "
            f"命中: {stats.get('hit', 0)}    "
            f"命中率: {stats.get('hit_rate', 0.0):.1f}%    "
            f"实际涨停: {stats.get('actual_count', 0)} 只    "
            f"漏报: {stats.get('missed_predict', 0)}"
        )
        ttk.Label(top, text=info, font=("Microsoft YaHei", 10, "bold")).pack(side=tk.LEFT)

        ttk.Button(top, text="导出CSV",
                   command=lambda d=data: self._export_compare_csv(d)).pack(side=tk.RIGHT)
        ttk.Button(top, text="刷新",
                   command=lambda td=data.get("trade_date", ""):
                   self._reopen_compare_window(td, win)).pack(side=tk.RIGHT, padx=(0, 6))

        # 主体：左右分栏
        body = ttk.PanedWindow(win, orient=tk.HORIZONTAL)
        body.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        # 左：候选 + 命中
        left_frame = ttk.LabelFrame(body, text="预测候选（按预测分降序）", padding=4)
        left_cols = ("code", "name", "industry", "categories", "score", "result", "t1_pct")
        left_tree = ttk.Treeview(left_frame, columns=left_cols, show="headings", height=24)
        for col, (label, w, anc) in {
            "code": ("代码", 70, tk.CENTER),
            "name": ("名称", 90, tk.W),
            "industry": ("行业", 90, tk.W),
            "categories": ("类别", 140, tk.W),
            "score": ("预测分", 60, tk.CENTER),
            "result": ("结果", 90, tk.CENTER),
            "t1_pct": ("次日开→收%", 90, tk.CENTER),
        }.items():
            left_tree.heading(col, text=label)
            left_tree.column(col, width=w, anchor=anc)
        left_sb = ttk.Scrollbar(left_frame, orient=tk.VERTICAL, command=left_tree.yview)
        left_tree.configure(yscrollcommand=left_sb.set)
        left_sb.pack(side=tk.RIGHT, fill=tk.Y)
        left_tree.pack(fill=tk.BOTH, expand=True)
        left_tree.tag_configure("hit", background="#a5d6a7", foreground="#1f1f1f")
        left_tree.tag_configure("miss", background="#ffcdd2", foreground="#1f1f1f")
        left_tree.tag_configure("info", background="#eceff1", foreground="#1f1f1f")
        body.add(left_frame, weight=3)

        # 右：实际涨停名单
        right_frame = ttk.LabelFrame(body, text="次日实际涨停（含漏报标记）", padding=4)
        right_cols = ("code", "name", "industry", "boards", "predicted")
        right_tree = ttk.Treeview(right_frame, columns=right_cols, show="headings", height=24)
        for col, (label, w, anc) in {
            "code": ("代码", 70, tk.CENTER),
            "name": ("名称", 90, tk.W),
            "industry": ("行业", 90, tk.W),
            "boards": ("连板", 60, tk.CENTER),
            "predicted": ("是否预测", 90, tk.CENTER),
        }.items():
            right_tree.heading(col, text=label)
            right_tree.column(col, width=w, anchor=anc)
        right_sb = ttk.Scrollbar(right_frame, orient=tk.VERTICAL, command=right_tree.yview)
        right_tree.configure(yscrollcommand=right_sb.set)
        right_sb.pack(side=tk.RIGHT, fill=tk.Y)
        right_tree.pack(fill=tk.BOTH, expand=True)
        right_tree.tag_configure("hit", background="#a5d6a7", foreground="#1f1f1f")
        right_tree.tag_configure("miss", background="#ffe0b2", foreground="#1f1f1f")
        body.add(right_frame, weight=2)

        # 填充左侧
        cont_label = "保留涨停"  # 与 prediction_accuracy_service.CATEGORY_LABELS["cont"] 保持一致
        for c in data.get("candidates", []):
            cats = c.get("categories", []) or []
            has_non_cont = any(lbl != cont_label for lbl in cats)
            pct_oc = c.get("t1_open_close_pct")
            if pct_oc is None:
                pct_oc = c.get("t1_pct")  # 老记录回退
            if c.get("t1_suspended"):
                res, tag = "⏸停牌", "info"
            elif c.get("t1_one_word"):
                res, tag = "一字未买", "info"
            elif c.get("hit_strict"):
                res, tag = "✓涨停", "hit"
            elif has_non_cont and pct_oc is not None and pct_oc >= 5:
                # 非保留涨停类别走开盘买、收盘卖 ≥ 5% 的弱命中
                res, tag = f"↑+{pct_oc:.1f}%", "hit"
            elif pct_oc is None:
                res, tag = "—", None
            else:
                res = f"{'+' if pct_oc >= 0 else ''}{pct_oc:.1f}%"
                tag = "miss" if pct_oc < 0 else None
            pct_text = f"{pct_oc:+.2f}" if isinstance(pct_oc, (int, float)) else "-"
            cats_text = " / ".join(cats)
            left_tree.insert("", tk.END, values=(
                c.get("code", ""), c.get("name", ""), c.get("industry", ""),
                cats_text, c.get("max_score", 0), res, pct_text,
            ), tags=((tag,) if tag else ()))

        # 填充右侧（次日实际涨停）
        candidate_codes = {str(c.get("code", "")).zfill(6) for c in data.get("candidates", [])}
        for entry in data.get("actual_lu", []):
            code = str(entry.get("code", "")).zfill(6)
            in_pred = code in candidate_codes
            tag = "hit" if in_pred else "miss"
            right_tree.insert("", tk.END, values=(
                code, entry.get("name", ""), entry.get("industry", ""),
                entry.get("consecutive_boards", "-"),
                "✓ 已预测" if in_pred else "✗ 漏报",
            ), tags=(tag,))

        try:
            _attach_tree_enhancers(win)
        except Exception:
            pass

    def _reopen_compare_window(self, trade_date: str, old_win: tk.Toplevel) -> None:
        try:
            old_win.destroy()
        except Exception:
            pass
        # 强制重新评估
        try:
            prediction_accuracy_service.evaluate(trade_date, refresh_stale=True)
        except Exception:
            pass
        try:
            data = prediction_accuracy_service.query_compare(trade_date)
        except Exception as exc:
            messagebox.showerror("命中对比", f"刷新失败: {exc}", parent=self.root)
            return
        if not data.get("candidates"):
            messagebox.showinfo("命中对比", "暂无可对比数据", parent=self.root)
            return
        self._build_predict_compare_window(data)

    def _export_compare_csv(self, data: Dict[str, Any]) -> None:
        td = data.get("trade_date", "")
        path = filedialog.asksaveasfilename(
            parent=self.root,
            defaultextension=".csv",
            initialfile=f"compare_{td}.csv",
            filetypes=[("CSV", "*.csv"), ("All", "*.*")],
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f)
                w.writerow([
                    "code", "name", "industry", "categories", "score",
                    "hit_strict", "hit_loose", "buyable",
                    "t1_pct", "t1_open_close_pct", "t1_open", "t1_close",
                    "one_word", "suspended",
                ])
                for c in data.get("candidates", []):
                    w.writerow([
                        c.get("code", ""), c.get("name", ""), c.get("industry", ""),
                        " / ".join(c.get("categories", [])), c.get("max_score", 0),
                        c.get("hit_strict", 0), c.get("hit_loose", 0), c.get("hit_buyable", 0),
                        c.get("t1_pct"), c.get("t1_open_close_pct"),
                        c.get("t1_open"), c.get("t1_close"),
                        c.get("t1_one_word", 0), c.get("t1_suspended", 0),
                    ])
            self.status_var.set(f"已导出对比CSV: {path}")
        except Exception as exc:
            messagebox.showerror("导出失败", str(exc), parent=self.root)

    # ============== 策略分析（分数段 / 行业 / 失败归因） ==============
    def _open_predict_strategy_window(self) -> None:
        """弹窗：分数段命中率柱状图 / 行业命中率排行 / 失败归因分布。"""
        win = tk.Toplevel(self.root)
        win.title("策略分析 - 命中率多维度复盘")
        win.geometry("1080x680")
        win.transient(self.root)

        # 顶部控制栏
        top = ttk.Frame(win, padding=(10, 8))
        top.pack(fill=tk.X)
        ttk.Label(top, text="回看交易日:").pack(side=tk.LEFT)
        lookback_var = tk.IntVar(value=20)
        ttk.Spinbox(top, from_=5, to=120, increment=5, width=5,
                    textvariable=lookback_var).pack(side=tk.LEFT, padx=(2, 12))

        ttk.Label(top, text="类别:").pack(side=tk.LEFT)
        category_var = tk.StringVar(value="全部")
        cat_options = ["全部", "保留涨停", "二波接力", "首板涨停", "反包/承接", "趋势涨停"]
        cat_combo = ttk.Combobox(top, textvariable=category_var, values=cat_options,
                                 width=12, state="readonly")
        cat_combo.pack(side=tk.LEFT, padx=(2, 12))

        ttk.Button(top, text="刷新",
                   command=lambda: _reload()).pack(side=tk.LEFT)
        info_label = ttk.Label(top, text="", foreground="#666")
        info_label.pack(side=tk.LEFT, padx=(12, 0))

        # 主体：3 个 tab
        nb = ttk.Notebook(win)
        nb.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        # Tab 1: 分数段
        score_tab = ttk.Frame(nb)
        nb.add(score_tab, text="分数段命中率")
        score_chart_holder = ttk.Frame(score_tab)
        score_chart_holder.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        score_table_holder = ttk.Frame(score_tab)
        score_table_holder.pack(side=tk.BOTTOM, fill=tk.X)

        # Tab 2: 行业
        ind_tab = ttk.Frame(nb)
        nb.add(ind_tab, text="行业命中率")
        ind_cols = ("rank", "industry", "rate", "hit_buyable", "avg_pct", "total")
        ind_tree = ttk.Treeview(ind_tab, columns=ind_cols, show="headings", height=24)
        for col, (label, w, anc) in {
            "rank": ("名次", 50, tk.CENTER),
            "industry": ("行业", 200, tk.W),
            "rate": ("命中率", 90, tk.CENTER),
            "hit_buyable": ("命中/可买", 110, tk.CENTER),
            "avg_pct": ("平均次日涨幅", 110, tk.CENTER),
            "total": ("总样本", 80, tk.CENTER),
        }.items():
            ind_tree.heading(col, text=label)
            ind_tree.column(col, width=w, anchor=anc)
        ind_sb = ttk.Scrollbar(ind_tab, orient=tk.VERTICAL, command=ind_tree.yview)
        ind_tree.configure(yscrollcommand=ind_sb.set)
        ind_sb.pack(side=tk.RIGHT, fill=tk.Y)
        ind_tree.pack(fill=tk.BOTH, expand=True)
        ind_tree.tag_configure("hot", background="#a5d6a7", foreground="#1f1f1f")
        ind_tree.tag_configure("cold", background="#ffcdd2", foreground="#1f1f1f")

        # Tab 3: 失败归因
        fail_tab = ttk.Frame(nb)
        nb.add(fail_tab, text="失败归因")
        fail_summary = ttk.Label(fail_tab, text="", padding=(8, 6),
                                 font=("Microsoft YaHei", 10))
        fail_summary.pack(side=tk.TOP, fill=tk.X)
        fail_chart_holder = ttk.Frame(fail_tab)
        fail_chart_holder.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        fail_text = scrolledtext.ScrolledText(fail_tab, height=10, wrap=tk.WORD,
                                              font=("Consolas", 10))
        fail_text.pack(side=tk.BOTTOM, fill=tk.X)

        # 渲染逻辑
        cat_label_to_key = {
            "全部": None, "保留涨停": "cont", "二波接力": "first",
            "首板涨停": "fresh", "反包/承接": "wrap", "趋势涨停": "trend",
        }

        def _reload():
            try:
                lookback = int(lookback_var.get() or 20)
            except (TypeError, ValueError, tk.TclError):
                lookback = 20
            cat_key = cat_label_to_key.get(category_var.get(), None)

            from src.services import prediction_accuracy_service as svc

            # ---- Tab 1：分数段柱状图 + 表格 ----
            buckets = svc.query_score_bucket_stats(category=cat_key, lookback_dates=lookback)
            self._render_score_bucket_chart(score_chart_holder, score_table_holder, buckets)

            # ---- Tab 2：行业排行 ----
            inds = svc.query_industry_stats(lookback_dates=lookback, min_samples=3)
            ind_tree.delete(*ind_tree.get_children())
            for i, x in enumerate(inds, start=1):
                if x["rate"] >= 40:
                    tag = "hot"
                elif x["rate"] < 15 and x["buyable"] >= 5:
                    tag = "cold"
                else:
                    tag = ""
                ind_tree.insert("", tk.END, values=(
                    i, x["industry"], f"{x['rate']:.1f}%",
                    f"{x['hit']}/{x['buyable']}",
                    f"{x['avg_pct']:+.2f}%", x["total"],
                ), tags=((tag,) if tag else ()))

            # ---- Tab 3：失败归因 ----
            fr = svc.query_failure_reasons(category=cat_key, lookback_dates=lookback)
            self._render_failure_reasons(
                fail_chart_holder, fail_text, fail_summary, fr,
            )

            # 顶部 info
            info_label.config(text=(
                f"近 {lookback} 个交易日 · 类别: {category_var.get()} · "
                f"未命中样本 {fr.get('total_miss', 0)} 只"
            ))

            # 每次 _reload 都会重建桶明细表，幂等挂载新的 Treeview
            try:
                _attach_tree_enhancers(win)
            except Exception:
                pass

        cat_combo.bind("<<ComboboxSelected>>", lambda _e: _reload())
        _reload()

    def _render_score_bucket_chart(
        self, chart_holder: ttk.Frame, table_holder: ttk.Frame,
        buckets: List[Dict[str, Any]],
    ) -> None:
        """并排展示「分数段命中率」与「成功分布占比」，下方明细表。

        左图：bucket.hit / bucket.buyable —— 衡量该分数段命中"质量"。
        右图：bucket.hit / 总命中数 —— 衡量成功集中在哪些分数段。
        两个视角互补：高分段可能命中率高但样本少，中段命中率不高但贡献了更多成功。
        """
        for w in chart_holder.winfo_children():
            w.destroy()
        for w in table_holder.winfo_children():
            w.destroy()

        labels = [b["label"] for b in buckets]
        rates = [b["rate"] for b in buckets]
        counts = [b["buyable"] for b in buckets]
        hits = [int(b.get("hit") or 0) for b in buckets]
        total_hits = sum(hits)
        dist_pcts = [
            (h / total_hits * 100.0) if total_hits > 0 else 0.0
            for h in hits
        ]

        fig = Figure(figsize=(10, 4), dpi=100)
        ax_rate = fig.add_subplot(1, 2, 1)
        ax_dist = fig.add_subplot(1, 2, 2)

        # 左：命中率
        rate_colors = []
        for r, c in zip(rates, counts):
            if c == 0:
                rate_colors.append("#bdbdbd")
            elif r >= 40:
                rate_colors.append("#43a047")
            elif r >= 25:
                rate_colors.append("#fb8c00")
            else:
                rate_colors.append("#e53935")
        bars_l = ax_rate.bar(labels, rates, color=rate_colors,
                             edgecolor="#333", linewidth=0.6)
        for bar, rate, cnt in zip(bars_l, rates, counts):
            ax_rate.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                         f"{rate:.1f}%\nn={cnt}", ha="center", va="bottom", fontsize=8)
        ax_rate.set_ylabel("命中率 %")
        ax_rate.set_xlabel("预测分段")
        ax_rate.set_title("命中率（hit / 可买）")
        ax_rate.set_ylim(0, max(max(rates + [10]) * 1.25, 20))
        ax_rate.grid(axis="y", linestyle="--", alpha=0.4)

        # 右：成功分布占比
        bars_r = ax_dist.bar(labels, dist_pcts, color="#1976d2",
                             edgecolor="#333", linewidth=0.6)
        for bar, pct, h in zip(bars_r, dist_pcts, hits):
            if pct > 0:
                ax_dist.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                             f"{pct:.1f}%\nhit={h}", ha="center", va="bottom", fontsize=8)
        ax_dist.set_ylabel("占总命中 %")
        ax_dist.set_xlabel("预测分段")
        ax_dist.set_title(f"成功分布（共 {total_hits} 命中）")
        ax_dist.set_ylim(0, max(max(dist_pcts + [10]) * 1.25, 20))
        ax_dist.grid(axis="y", linestyle="--", alpha=0.4)

        fig.tight_layout()
        canvas = FigureCanvasTkAgg(fig, master=chart_holder)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # 下方明细表（多加一列：占总命中%）
        cols = ("label", "total", "buyable", "hit", "rate", "dist_pct", "avg_pct")
        tree = ttk.Treeview(table_holder, columns=cols, show="headings", height=6)
        for col, (label, w, anc) in {
            "label": ("分段", 80, tk.CENTER),
            "total": ("总样本", 70, tk.CENTER),
            "buyable": ("可买入", 70, tk.CENTER),
            "hit": ("命中", 60, tk.CENTER),
            "rate": ("命中率", 80, tk.CENTER),
            "dist_pct": ("占总命中%", 90, tk.CENTER),
            "avg_pct": ("平均次日涨幅", 110, tk.CENTER),
        }.items():
            tree.heading(col, text=label)
            tree.column(col, width=w, anchor=anc)
        for b, dist in zip(buckets, dist_pcts):
            tree.insert("", tk.END, values=(
                b["label"], b["total"], b["buyable"], b["hit"],
                f"{b['rate']:.1f}%", f"{dist:.1f}%",
                f"{b['avg_pct']:+.2f}%",
            ))
        tree.pack(fill=tk.X)

    def _render_failure_reasons(
        self, chart_holder: ttk.Frame, fail_text: scrolledtext.ScrolledText,
        summary_label: ttk.Label, data: Dict[str, Any],
    ) -> None:
        for w in chart_holder.winfo_children():
            w.destroy()

        total_miss = int(data.get("total_miss") or 0)
        reasons = data.get("by_reason") or []
        summary_label.config(text=f"未命中样本总数: {total_miss}  ·  归因分布如下")

        # 横向条形图
        fig = Figure(figsize=(8, 3.4), dpi=100)
        ax = fig.add_subplot(111)
        labels = [r["reason"] for r in reasons]
        ratios = [r["ratio"] for r in reasons]
        counts = [r["count"] for r in reasons]
        # 按 ratio 降序展示更直观
        order = sorted(range(len(labels)), key=lambda i: ratios[i])
        labels = [labels[i] for i in order]
        ratios = [ratios[i] for i in order]
        counts = [counts[i] for i in order]
        bars = ax.barh(labels, ratios, color="#5c6bc0", edgecolor="#333", linewidth=0.5)
        for bar, ratio, cnt in zip(bars, ratios, counts):
            ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                    f"{ratio:.1f}% ({cnt})", va="center", fontsize=9)
        ax.set_xlabel("占比 %")
        ax.set_title("未命中候选 → 失败模式分布")
        ax.set_xlim(0, max(ratios + [10]) * 1.25)
        ax.grid(axis="x", linestyle="--", alpha=0.4)
        fig.tight_layout()
        canvas = FigureCanvasTkAgg(fig, master=chart_holder)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # 下方文字明细
        fail_text.config(state=tk.NORMAL)
        fail_text.delete("1.0", tk.END)
        fail_text.insert(tk.END, "归因解释:\n")
        fail_text.insert(tk.END,
            "  冲高回落 = T+1 盘中冲高 ≥ +3% 但收盘 ≤ 昨收（情绪退潮）\n"
            "  低开低走 = T+1 低开 ≥ 1% 且收盘 ≤ 开盘价（资金提前出货）\n"
            "  弱势震荡 = T+1 涨跌幅在 [-2%, +2%]（缺乏接力）\n"
            "  大跌/跌停 = T+1 涨跌幅 ≤ -5%（板块塌方）\n\n"
        )
        for r in data.get("by_reason") or []:
            inds_text = " · ".join(
                f"{x['industry']}({x['count']})" for x in r.get("top_industries", [])
            ) or "-"
            fail_text.insert(tk.END,
                f"  {r['reason']:>10s}  {r['count']:3d} 只 ({r['ratio']:5.1f}%)  "
                f"平均涨幅 {r['avg_pct']:+.2f}%  Top 行业: {inds_text}\n"
            )
        fail_text.config(state=tk.DISABLED)

    def setup_watchlist_tab(self):
        watch_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(watch_frame, text="自选池")
        self.watchlist_tab = watch_frame

        action_bar = ttk.Frame(watch_frame)
        action_bar.pack(fill=tk.X, pady=(0, 6))
        ttk.Button(action_bar, text="刷新自选池", command=self.refresh_watchlist_view).pack(side=tk.LEFT)
        ttk.Button(action_bar, text="加入当前详情", command=self.add_current_detail_to_watchlist).pack(side=tk.LEFT, padx=6)
        ttk.Button(action_bar, text="编辑备注", command=self.edit_selected_watchlist_item).pack(side=tk.LEFT, padx=6)
        ttk.Button(action_bar, text="移除", command=self.remove_selected_watchlist_item).pack(side=tk.LEFT)
        self.watchlist_summary_var = tk.StringVar(value="自选 0 只")
        ttk.Label(action_bar, textvariable=self.watchlist_summary_var).pack(side=tk.RIGHT)

        columns = ("code", "name", "status", "score", "latest_close", "board", "note", "updated_at")
        self._watch_tree = ttk.Treeview(watch_frame, columns=columns, show="headings", height=20)
        headings = {
            "code": ("代码", 80),
            "name": ("名称", 110),
            "status": ("状态", 90),
            "score": ("评分", 70),
            "latest_close": ("最新收盘", 90),
            "board": ("板块", 110),
            "note": ("备注", 320),
            "updated_at": ("更新时间", 150),
        }
        for col, (label, width) in headings.items():
            self._watch_tree.heading(col, text=label)
            anchor = tk.W if col in {"name", "note"} else tk.CENTER
            self._watch_tree.column(col, width=width, anchor=anchor)
        sb = ttk.Scrollbar(watch_frame, orient=tk.VERTICAL, command=self._watch_tree.yview)
        self._watch_tree.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self._watch_tree.pack(fill=tk.BOTH, expand=True)
        self._watch_tree.bind("<<TreeviewSelect>>", self.on_watchlist_select)
        self._watch_tree.bind("<Double-1>", self.on_watchlist_double_click)
        self.refresh_watchlist_view()

    def setup_log_tab(self):
        log_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(log_frame, text="运行日志")
        self.log_tab = log_frame

        self.log_text = scrolledtext.ScrolledText(log_frame, height=30, width=100)
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def setup_status_bar(self):
        self.status_var = tk.StringVar(value="就绪")
        self.progress_text_var = tk.StringVar(value="")

        status_frame = ttk.Frame(self.root)
        status_frame.pack(side=tk.BOTTOM, fill=tk.X)

        status_bar = ttk.Label(status_frame, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.LEFT, fill=tk.X, expand=True)

        progress_text = ttk.Label(status_frame, textvariable=self.progress_text_var, relief=tk.SUNKEN, anchor=tk.E, width=28)
        progress_text.pack(side=tk.RIGHT)

        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(self.root, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def _set_progress_text(self, current: int, total: int, extra: str = "") -> None:
        base = f"{current}/{total}" if total > 0 else ""
        self.progress_text_var.set(f"{base} {extra}".strip())

    def _set_progressbar_indeterminate(self, active: bool) -> None:
        if active:
            self.progress_bar.configure(mode="indeterminate")
            self.progress_bar.start(10)
            return
        self.progress_bar.stop()
        self.progress_bar.configure(mode="determinate")

    def _open_run_log(self) -> None:
        log_dir = Path("data") / "run_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._run_log_file = log_dir / f"scan_{stamp}.log"
        self._run_log_file.write_text("", encoding="utf-8")

    def _close_run_log(self) -> None:
        self._run_log_file = None

    def _log(self, message: str) -> None:
        if self._is_closing:
            return
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n"
        self.log_text.insert(tk.END, line)
        self.log_text.see(tk.END)
        if self._run_log_file is not None:
            with self._run_log_file.open("a", encoding="utf-8") as f:
                f.write(line)

    def _log_async(self, message: str) -> None:
        self._log_drainer.enqueue(message)

    def _drain_log_queue(self) -> None:
        """保留公开方法名以便外部调用；实际转发到 LogDrainer。"""
        self._log_drainer.drain_once()

    @property
    def _is_closing(self) -> bool:
        """只读视图：真正的"关闭中"状态由 self._ui 独占管理。

        进入关闭流程请显式调用 `self._ui.mark_closing()`；不提供 setter，
        避免出现 `self._is_closing = False` 这种把关闭状态误"复活"的写法。
        """
        return self._ui.is_closing

    def _safe_after(self, delay_ms: int, callback) -> None:
        self._ui.safe_after(delay_ms, callback)

    def _post_to_ui(self, callback) -> None:
        """后台线程专用：把 UI 更新推到主线程。窗口已关时直接丢弃。"""
        self._ui.post(callback)

    def _register_cancel_token(self, token: CancelToken) -> None:
        self._cancel_registry.issue(token)

    def _unregister_cancel_token(self, token: CancelToken) -> None:
        self._cancel_registry.retire(token)

    def _cancel_all_background(self, reason: str = "") -> None:
        """广播取消到所有注册过的 token；扫描/缓存也同步失效。"""
        self.is_scanning = False
        self.is_updating_cache = False
        self._cancel_registry.broadcast_cancel(reason)

    def _start_background_job(
        self,
        target,
        *,
        name: str = "",
        args: tuple = (),
        include_token: bool = True,
    ) -> tuple[threading.Thread, CancelToken]:
        """为通用后台任务（详情/分时/涨停对比/涨停预测）创建并启动线程。

        返回 (thread, token)。线程会在 target 结束后自动摘掉 token。
        target 签名：如果 include_token=True，则要求最后一个参数接受 token。
        """
        token = self._cancel_registry.issue()

        def _runner():
            try:
                if include_token:
                    target(*args, token)
                else:
                    target(*args)
            finally:
                self._cancel_registry.retire(token)

        thread_kwargs = {"target": _runner, "daemon": True}
        if name:
            thread_kwargs["name"] = name
        t = threading.Thread(**thread_kwargs)
        t.start()
        return t, token

    def _selected_boards(self) -> List[str]:
        boards = [board for board, var in self.board_filter_vars.items() if var.get()]
        return boards or list(self.board_filter_vars.keys())

    def _parse_int_value(
        self,
        raw_value: str,
        field_name: str,
        minimum: int,
        maximum: Optional[int] = None,
        allow_zero: bool = False,
    ) -> int:
        text = str(raw_value).strip()
        try:
            value = int(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} 必须是整数") from exc
        if allow_zero and value == 0:
            return 0
        if value < minimum:
            raise ValueError(f"{field_name} 不能小于 {minimum}")
        if maximum is not None and value > maximum:
            raise ValueError(f"{field_name} 不能大于 {maximum}")
        return value

    def _parse_float_value(
        self,
        raw_value: str,
        field_name: str,
        minimum: float,
        maximum: Optional[float] = None,
    ) -> float:
        text = str(raw_value).strip()
        try:
            value = float(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} 必须是数字") from exc
        if value < minimum:
            raise ValueError(f"{field_name} 不能小于 {minimum:g}")
        if maximum is not None and value > maximum:
            raise ValueError(f"{field_name} 不能大于 {maximum:g}")
        return value

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
            trend_days=self._parse_int_value(self.trend_days_var.get(), "连续天数", minimum=1, maximum=120),
            ma_period=self._parse_int_value(self.ma_period_var.get(), "MA周期", minimum=1, maximum=250),
            limit_up_lookback_days=self._parse_int_value(
                self.limit_up_lookback_var.get(),
                "近N日涨停",
                minimum=1,
                maximum=60,
            ),
            volume_lookback_days=self._parse_int_value(
                self.volume_lookback_var.get(),
                "放量观察天数",
                minimum=1,
                maximum=60,
            ),
            volume_expand_enabled=bool(self.volume_expand_enabled_var.get()),
            volume_expand_factor=self._parse_float_value(
                self.volume_expand_factor_var.get(),
                "放量倍数阈值",
                minimum=1.0,
                maximum=50.0,
            ),
            require_limit_up_within_days=bool(self.require_limit_up_var.get()),
            strong_ft_enabled=bool(self.strong_ft_enabled_var.get()),
            strong_ft_max_pullback_pct=self._parse_float_value(
                self.strong_ft_max_pullback_pct_var.get(),
                "承接强势-最大回撤%",
                minimum=0.0,
                maximum=20.0,
            ),
            strong_ft_max_volume_ratio=self._parse_float_value(
                self.strong_ft_max_volume_ratio_var.get(),
                "承接强势-次日量能上限",
                minimum=0.0,
                maximum=2.0,
            ),
            strong_ft_min_hold_days=self._parse_int_value(
                self.strong_ft_min_hold_days_var.get(),
                "承接强势-至少站稳天数",
                minimum=0,
                maximum=30,
                allow_zero=True,
            ),
        )

    def _apply_filter_settings_from_ui(self, show_error: bool = True) -> Optional[FilterSettings]:
        try:
            settings = self._build_filter_settings()
        except ValueError as exc:
            if show_error:
                messagebox.showerror("错误", str(exc))
            return None
        self.stock_filter.apply_settings(settings)
        self._refresh_detail_metric_labels()
        return settings

    def _build_scan_request(self) -> Optional[ScanRequest]:
        settings = self._apply_filter_settings_from_ui(show_error=True)
        if settings is None:
            return None
        try:
            max_stocks = self._parse_int_value(
                self.scan_count_var.get(),
                "扫描数量",
                minimum=1,
                maximum=10000,
                allow_zero=True,
            )
            scan_workers = self._parse_int_value(
                self.scan_workers_var.get(),
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
            history_source=str(self.history_source_var.get() or "auto").strip().lower() or "auto",
            allowed_boards=tuple(self._selected_boards()),
            refresh_universe=bool(self.refresh_universe_var.get()),
            ignore_result_snapshot=bool(self.ignore_result_snapshot_var.get()),
        )

    def _short_text(self, value: Any, max_len: int = 28) -> str:
        text = str(value or "").strip()
        if not text or text == "-":
            return "-"
        if len(text) <= max_len:
            return text
        return f"{text[: max_len - 1]}…"

    def _format_amount(self, value: Any) -> str:
        try:
            if value is None or value == "":
                return "-"
            amount = float(value)
        except (TypeError, ValueError):
            return "-"
        abs_amount = abs(amount)
        if abs_amount >= 1e8:
            return f"{amount / 1e8:.2f}亿"
        if abs_amount >= 1e4:
            return f"{amount / 1e4:.2f}万"
        return f"{amount:.0f}"

    def _format_volume(self, value: Any) -> str:
        try:
            if value is None or value == "":
                return "-"
            volume = float(value)
        except (TypeError, ValueError):
            return "-"
        abs_volume = abs(volume)
        if abs_volume >= 1e8:
            return f"{volume / 1e8:.2f}亿"
        if abs_volume >= 1e4:
            return f"{volume / 1e4:.2f}万"
        return f"{volume:.0f}"

    def _format_axis_volume(self, value: float, _pos: float = 0) -> str:
        text = self._format_volume(value)
        return text if text != "-" else "0"

    def _refresh_detail_metric_labels(self) -> None:
        ma_period = max(1, int(self.stock_filter.ma_period))
        if "latest_ma" in self.detail_label_caption_vars:
            self.detail_label_caption_vars["latest_ma"].set(f"MA{ma_period}:")
        volume_days = max(1, int(self.stock_filter.volume_lookback_days))
        if "latest_volume" in self.detail_label_caption_vars:
            self.detail_label_caption_vars["latest_volume"].set(f"成交量(近{volume_days}日均量占比):")

    def _sync_watchlist_with_scan_results(self) -> None:
        if not self.watchlist_items:
            return
        changed = False
        for code, item in list(self.watchlist_items.items()):
            result = self._lookup_result_by_code(code)
            if not result:
                continue
            payload = self._build_watchlist_item_payload(
                code,
                stock_name=str(item.get("name", "") or result.get("name", "") or ""),
                status=str(item.get("status", "") or "观察"),
                note=str(item.get("note", "") or ""),
            )
            save_watchlist_item(payload)
            self.watchlist_items[code] = {**item, **payload}
            changed = True
        if changed:
            self.refresh_watchlist_view()
            self._refresh_result_table_if_ready()

    def _filter_results_by_selected_boards(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        allowed = {str(board).strip() for board in self._selected_boards() if str(board).strip()}
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

    def _filter_results_by_price_range(
        self,
        results: List[Dict[str, Any]],
        raise_error: bool = False,
    ) -> List[Dict[str, Any]]:
        try:
            min_price = self._parse_optional_price_limit(self.min_price_var.get(), "最低价")
            max_price = self._parse_optional_price_limit(self.max_price_var.get(), "最高价")
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

    def _apply_result_filters(self, results: List[Dict[str, Any]], raise_price_error: bool = False) -> List[Dict[str, Any]]:
        filtered = self._filter_results_by_selected_boards(results)
        filtered = self._filter_results_by_price_range(filtered, raise_error=raise_price_error)
        filtered = self._filter_results_by_quick_filters(filtered)
        return filtered

    def _parse_optional_float(self, raw: str, field_name: str) -> Optional[float]:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} 必须是数字") from exc

    def _parse_optional_int(self, raw: str, field_name: str) -> Optional[int]:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} 必须是整数") from exc

    def _filter_results_by_quick_filters(
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
                return self._parse_optional_float(raw_var.get(), name)
            except (ValueError, AttributeError) as exc:
                if str(exc):
                    parse_errors.append(str(exc))
                return None

        def _parse_int(name: str, raw_var) -> Optional[int]:
            try:
                return self._parse_optional_int(raw_var.get(), name)
            except (ValueError, AttributeError) as exc:
                if str(exc):
                    parse_errors.append(str(exc))
                return None

        min_score = _parse_float("评分", self.min_score_var)
        min_five_day = _parse_float("5日涨幅", self.min_five_day_var)
        min_volume_ratio = _parse_float("放量倍数", self.min_volume_ratio_var)
        min_streak = _parse_int("连板数", self.min_streak_var)
        if parse_errors:
            # 只在 status_var 里提示,不弹窗打断实时输入
            try:
                self.status_var.set(
                    "快速过滤有非法输入(已忽略): " + "；".join(parse_errors[:3])
                )
            except (AttributeError, tk.TclError):
                pass

        def _get_bool_var(name: str) -> bool:
            var = getattr(self, name, None)
            try:
                return bool(var.get()) if var is not None else False
            except tk.TclError:
                return False

        needle = getattr(self, "search_var", None)
        needle_str = needle.get() if needle is not None else ""

        only_watch = _get_bool_var("only_watchlist_var")
        only_lu = _get_bool_var("only_limit_up_var")
        only_broken = _get_bool_var("only_broken_limit_up_var")
        only_vol = _get_bool_var("only_volume_expand_var")
        only_strong = _get_bool_var("only_strong_ft_var")

        watchlist_codes = set(self.watchlist_items.keys())

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
            if not result_filters.only_in_watchlist(item, only_watch, watchlist_codes):
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
                self.root.after_cancel(existing)
            except tk.TclError:
                pass
            self._quick_filter_after_id = None
        self._quick_filter_after_id = self._safe_after(250, self._quick_filter_tick)

    def _quick_filter_tick(self) -> None:
        self._quick_filter_after_id = None
        if not self.is_scanning:
            self.on_quick_filter_apply()

    def on_quick_filter_apply(self) -> None:
        """用户点"应用"或勾选复选框时触发。"""
        if self.is_scanning:
            return
        source = self.all_scan_results or self.filtered_stocks
        if not source:
            return
        try:
            self.update_result_table(source, announce=False, persist=False)
            self.status_var.set(f"已应用快速过滤，当前显示 {len(self.filtered_stocks)} 只")
        except ValueError as exc:
            messagebox.showerror("错误", str(exc))

    def clear_all_result_filters(self) -> None:
        """一键清空所有结果表过滤（板块、价格、快速过滤）。"""
        for board_var in self.board_filter_vars.values():
            board_var.set(True)
        self.min_price_var.set("")
        self.max_price_var.set("")
        self.search_var.set("")
        self.min_score_var.set("")
        self.min_five_day_var.set("")
        self.min_volume_ratio_var.set("")
        self.min_streak_var.set("")
        self.only_watchlist_var.set(False)
        self.only_limit_up_var.set(False)
        self.only_broken_limit_up_var.set(False)
        self.only_volume_expand_var.set(False)
        self.only_strong_ft_var.set(False)
        self.on_quick_filter_apply()

    def on_board_filter_changed(self):
        self._save_board_filter_layout()
        if self.is_scanning:
            return
        source = self.all_scan_results or self.filtered_stocks
        if source:
            try:
                self.update_result_table(source, announce=False, persist=False)
                self.status_var.set(f"已按筛选条件更新，当前显示 {len(self.filtered_stocks)} 只")
            except ValueError as exc:
                messagebox.showerror("错误", str(exc))
        else:
            self.status_var.set("已保存显示板块筛选设置")
        # 同步刷新涨停对比
        self._refresh_zt_compare_display()
        # 同步刷新涨停预测
        self._refresh_predict_display_if_ready()

    def on_price_filter_changed(self, event=None):
        if self.is_scanning:
            return "break" if event is not None else None
        source = self.all_scan_results or self.filtered_stocks
        if source:
            try:
                self._apply_result_filters(source, raise_price_error=True)
                self.update_result_table(source, announce=False, persist=False)
                self.status_var.set(f"已按价格过滤，当前显示 {len(self.filtered_stocks)} 只")
            except ValueError as exc:
                messagebox.showerror("错误", str(exc))
                return "break" if event is not None else None
        # 同步刷新涨停对比
        self._refresh_zt_compare_display()
        # 同步刷新涨停预测
        self._refresh_predict_display_if_ready()
        return "break" if event is not None else None

    def clear_price_filter(self):
        self.min_price_var.set("")
        self.max_price_var.set("")
        self.on_price_filter_changed()

    def on_history_source_changed(self, event=None):
        self._save_app_settings()
        source = str(self.history_source_var.get() or "auto").strip().lower() or "auto"
        self._apply_source_preferences()
        self.status_var.set(f"历史数据源已切换为 {source}")
        self._log(
            f"数据源设置已更新: history={self.history_source_var.get()}, intraday={self.intraday_source_var.get()}, fund_flow={self.fund_flow_source_var.get()}, limit_up_reason={self.limit_up_reason_source_var.get()}"
        )
        return None

    def _scan_signature(self, request: ScanRequest) -> Dict[str, Any]:
        return request.to_signature()

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
        signature_request = request or self._active_scan_request
        if signature_request is None:
            current_settings = self.stock_filter.get_settings()
            signature_request = ScanRequest(
                filter_settings=current_settings,
                max_stocks=int(self._current_scan_max_stocks),
                allowed_boards=tuple(self._current_scan_allowed_boards),
            )
        payload = {
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scan_date": datetime.now().strftime("%Y-%m-%d"),
            "complete": bool(complete),
            "row_count": len(payload_results),
            "signature": self._scan_signature(signature_request),
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
        self.all_scan_results = list(results)
        self.filtered_stocks = list(results)
        self.update_result_table(results, announce=False, persist=False)
        self.status_var.set("已从本地结果恢复")

    def _can_use_snapshot(self, request: ScanRequest) -> bool:
        if request.ignore_result_snapshot:
            return False
        if request.refresh_universe:
            return False
        signature = self._scan_signature(request)
        payload = load_scan_snapshot(json.dumps(signature, ensure_ascii=False, sort_keys=True))
        return bool(payload and payload.get("complete") and payload.get("results"))

    def update_filter_params(self) -> bool:
        return self._apply_filter_settings_from_ui(show_error=True) is not None

    def _history_cache_summary_text(self) -> str:
        summary = self.stock_filter.fetcher.get_history_cache_summary()
        return (
            f"历史缓存 {summary.get('covered_count', 0)}/{summary.get('universe_count', 0)} "
            f"({summary.get('coverage_ratio', 0.0) * 100:.1f}%)，最新交易日 {summary.get('latest_trade_date') or '-'}"
        )

    def start_scan(self):
        if self.is_scanning or self.is_updating_cache:
            return
        request = self._build_scan_request()
        if request is None:
            return

        self._active_scan_request = request
        self._current_scan_allowed_boards = list(request.allowed_boards)
        self._current_scan_max_stocks = int(request.max_stocks)

        if self._can_use_snapshot(request):
            self._log("命中本地结果快照，直接恢复上次扫描结果。")
            signature = json.dumps(self._scan_signature(request), ensure_ascii=False, sort_keys=True)
            payload = load_scan_snapshot(signature)
            if payload:
                self.all_scan_results = payload.get("results", []) or []
                self.update_result_table(self.all_scan_results, announce=False, persist=False)
                self.status_var.set("已从本地结果恢复")
                self.progress_var.set(100)
                self.scan_finished("已从本地结果恢复")
                return

        self.is_scanning = True
        self.scan_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.all_scan_results.clear()
        self.filtered_stocks.clear()
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        self._open_run_log()
        self._log(
            f"开始扫描：最近{request.filter_settings.trend_days}日收盘 > MA{request.filter_settings.ma_period}，"
            f"近{request.filter_settings.limit_up_lookback_days}日内涨停过滤={'开' if request.filter_settings.require_limit_up_within_days else '关'}。"
        )
        self._log(self._history_cache_summary_text())
        self._log(f"本轮历史数据源：{request.history_source}")
        if request.max_stocks <= 0:
            self._log("本次为全量扫描，建议优先在收盘后执行，并尽量复用本地结果快照。")
        if request.scan_workers >= 4:
            self._log(
                f"你当前设置了 {request.scan_workers} 个扫描线程；程序会自动做并发保护，但较大的线程数仍可能增加外部接口压力。"
            )
        if request.refresh_universe:
            self._log("已开启“重新拉取股票池”，本轮会刷新股票池缓存，整体耗时会更长。")
        if request.ignore_result_snapshot:
            self._log("已开启“忽略本地结果快照”，本轮不会直接复用上次扫描结果。")
        self._log("扫描阶段只拉历史日线，不拉实时、资金流或内外盘。")
        self.status_var.set("正在扫描...")
        self.progress_var.set(0)
        self._set_progress_text(0, 0)

        token = CancelToken()
        self._scan_cancel_token = token
        self._register_cancel_token(token)
        self._scan_thread = threading.Thread(
            target=self.scan_stocks, args=(request, token), daemon=True
        )
        self._scan_thread.start()

    def start_history_cache_update(self):
        if self.is_scanning or self.is_updating_cache:
            return
        request = self._build_scan_request()
        if request is None:
            return
        self.is_updating_cache = True
        self.scan_btn.config(state=tk.DISABLED)
        self.update_cache_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self._open_run_log()
        self._log("开始更新历史缓存。")
        self._log(self._history_cache_summary_text())
        self.status_var.set("正在统计待更新股票范围...")
        self.progress_var.set(0)
        self._set_progress_text(0, 0, "准备中")
        self._set_progressbar_indeterminate(True)
        token = CancelToken()
        self._cache_cancel_token = token
        self._register_cancel_token(token)
        self._cache_thread = threading.Thread(
            target=self.update_history_cache, args=(request, token), daemon=True
        )
        self._cache_thread.start()

    def stop_scan(self):
        # 布尔标记保留做兼容；CancelToken 才是真正可传播的停止信号
        self.is_scanning = False
        self.is_updating_cache = False
        for token in (self._scan_cancel_token, self._cache_cancel_token):
            if token is not None:
                token.cancel("user_stop")
        self.status_var.set("正在停止...")
        self._log("已请求停止，正在等待当前任务结束。")

    def scan_stocks(self, request: ScanRequest, cancel_token: Optional[CancelToken] = None):
        import time as _time
        token = cancel_token or CancelToken()
        try:
            scan_filter = StockFilter()
            scan_filter.apply_settings(request.filter_settings)
            scan_filter.set_log_callback(self._log_async)
            self._log_async(
                f"扫描参数：数量={'全量' if request.max_stocks <= 0 else request.max_stocks}，并发线程={request.scan_workers}，历史源={request.history_source}"
            )

            scan_t0 = _time.time()

            def progress_callback(current, total, code, name):
                if token.is_cancelled() or not self.is_scanning:
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
                self._post_to_ui(lambda: self.progress_var.set(progress))
                self._post_to_ui(lambda c=current, t=total: self._set_progress_text(c, t))
                self._post_to_ui(lambda s=status_text: self.status_var.set(s))

            results = scan_filter.scan_all_stocks(
                max_stocks=request.max_stocks,
                progress_callback=progress_callback,
                max_workers=request.scan_workers,
                history_source=request.history_source,
                local_history_only=True,
                cancel_token=token,
                should_stop=lambda: not self.is_scanning,
                refresh_universe=request.refresh_universe,
                allowed_boards=list(request.allowed_boards),
            )
            if token.is_cancelled() or not self.is_scanning:
                self._post_to_ui(lambda: self._log("扫描已停止。"))
                self._post_to_ui(lambda: self.scan_finished("扫描已停止"))
                return
            self.all_scan_results = results
            self._post_to_ui(lambda res=results, req=request: self.update_result_table(res, request=req))
            self._post_to_ui(self._sync_watchlist_with_scan_results)
            self._post_to_ui(lambda count=len(results): self.scan_finished(f"扫描完成，命中 {count} 只。"))
        except StopIteration:
            self._post_to_ui(lambda: self._log("扫描已停止。"))
            self._post_to_ui(lambda: self.scan_finished("扫描已停止"))
        except Exception as e:
            error_text = str(e)
            self._post_to_ui(lambda: self._log(f"扫描出错: {error_text}"))
            self._post_to_ui(lambda: self.scan_finished(f"扫描失败: {error_text}"))
            self._post_to_ui(lambda et=error_text: self._show_network_error_alert(et))
        finally:
            self._unregister_cancel_token(token)

    def update_history_cache(self, request: ScanRequest, cancel_token: Optional[CancelToken] = None):
        import time as _time
        token = cancel_token or CancelToken()
        try:
            scan_filter = StockFilter()
            scan_filter.apply_settings(request.filter_settings)
            scan_filter.set_log_callback(self._log_async)
            scan_filter.set_history_source_preference(request.history_source)
            self._log_async(
                f"缓存更新参数：数量={'全量' if request.max_stocks <= 0 else request.max_stocks}，并发线程={request.scan_workers}，历史源={request.history_source}"
            )

            self._post_to_ui(lambda: self.status_var.set("正在加载股票池并统计总数..."))
            universe = scan_filter.fetcher.get_all_stocks(force_refresh=request.refresh_universe)
            if token.is_cancelled():
                self._post_to_ui(lambda: self._set_progressbar_indeterminate(False))
                self._post_to_ui(lambda: self.scan_finished("历史缓存更新已停止"))
                return
            if universe is None or universe.empty:
                self._post_to_ui(lambda: self._set_progressbar_indeterminate(False))
                self._post_to_ui(lambda: self.scan_finished("历史缓存更新失败: 股票池为空"))
                return
            if request.allowed_boards and "board" in universe.columns:
                allowed = {str(x).strip() for x in request.allowed_boards if str(x).strip()}
                if allowed:
                    universe = universe[universe["board"].astype(str).isin(allowed)].reset_index(drop=True)
            if request.max_stocks and request.max_stocks > 0:
                universe = universe.head(request.max_stocks).reset_index(drop=True)
            estimated_total = int(len(universe))

            self._post_to_ui(lambda: self._set_progressbar_indeterminate(False))
            self._post_to_ui(lambda: self.progress_var.set(0))
            self._post_to_ui(
                lambda total=estimated_total: self._set_progress_text(0, total, "等待任务启动"),
            )
            self._post_to_ui(
                lambda total=estimated_total: self.status_var.set(f"准备更新历史缓存，共 {total} 只股票..."),
            )

            cache_t0 = _time.time()
            cache_updated = 0
            cache_failed = 0
            cache_skipped = 0
            last_updated = 0
            last_failed = 0
            last_skipped = 0

            def progress_callback(current, total, code, name, updated, failed, skipped):
                nonlocal last_updated, last_failed, last_skipped
                if token.is_cancelled() or not self.is_updating_cache:
                    raise StopIteration
                progress = (current / total) * 100 if total else 0
                elapsed = _time.time() - cache_t0
                speed = current / elapsed if elapsed > 0 else 0
                eta_sec = (total - current) / speed if speed > 0 else 0
                if eta_sec >= 60:
                    eta_text = f"{int(eta_sec // 60)}分{int(eta_sec % 60)}秒"
                else:
                    eta_text = f"{int(eta_sec)}秒"
                remaining = max(0, total - current)
                if updated > last_updated:
                    outcome_text = "成功"
                elif failed > last_failed:
                    outcome_text = "失败"
                elif skipped > last_skipped:
                    outcome_text = "跳过"
                else:
                    outcome_text = "完成"
                last_updated = updated
                last_failed = failed
                last_skipped = skipped
                status_text = (
                    f"更新缓存 {current}/{total} ({progress:.0f}%) "
                    f"| 速度 {speed:.1f}只/秒 | 预计剩余 {eta_text} "
                    f"| 当前 {code} {name}".strip()
                )
                self._log_async(
                    f"缓存进度 {current}/{total}，剩余 {remaining} 只，"
                    f"{outcome_text} {code} {name}；成功{updated} 跳过{skipped} 失败{failed}"
                )
                self._post_to_ui(lambda: self.progress_var.set(progress))
                self._post_to_ui(
                    lambda c=current, t=total, u=updated, s=skipped, f=failed:
                        self._set_progress_text(c, t, f"成功{u} 跳过{s} 失败{f}"),
                )
                self._post_to_ui(lambda s=status_text: self.status_var.set(s))

            result = scan_filter.fetcher.update_history_cache(
                max_stocks=request.max_stocks,
                days=max(60, request.filter_settings.ma_period + request.filter_settings.limit_up_lookback_days + 20),
                source=request.history_source,
                workers=request.scan_workers,
                progress_callback=progress_callback,
                should_stop=lambda: token.is_cancelled() or not self.is_updating_cache,
                refresh_universe=request.refresh_universe,
                allowed_boards=list(request.allowed_boards),
            )
            cache_updated = result.get("updated", 0)
            cache_failed = result.get("failed", 0)
            cache_skipped = result.get("skipped", 0)
            if token.is_cancelled() or not self.is_updating_cache:
                self._post_to_ui(lambda: self._log("历史缓存更新已停止。"))
                self._post_to_ui(lambda: self.scan_finished("历史缓存更新已停止"))
                return
            total_time = _time.time() - cache_t0
            if total_time >= 60:
                time_text = f"{int(total_time // 60)}分{int(total_time % 60)}秒"
            else:
                time_text = f"{total_time:.1f}秒"
            summary_msg = (
                f"历史缓存更新完成：总计 {result.get('total', 0)}，"
                f"成功 {cache_updated}，跳过(已新鲜) {cache_skipped}，失败 {cache_failed}，"
                f"耗时 {time_text}。"
            )
            self._post_to_ui(lambda m=summary_msg: self._log(m))
            self._post_to_ui(lambda: self._log(self._history_cache_summary_text()))
            self._post_to_ui(lambda: self.scan_finished("历史缓存更新完成"))
        except StopIteration:
            self._post_to_ui(lambda: self._log("历史缓存更新已停止。"))
            self._post_to_ui(lambda: self.scan_finished("历史缓存更新已停止"))
        except Exception as e:
            error_text = str(e)
            self._post_to_ui(lambda: self._log(f"历史缓存更新出错: {error_text}"))
            self._post_to_ui(lambda: self.scan_finished(f"历史缓存更新失败: {error_text}"))
            self._post_to_ui(lambda et=error_text: self._show_network_error_alert(et))
        finally:
            self._unregister_cancel_token(token)

    def _sort_value_for_column(self, item: Dict[str, Any], column: str):
        col_def = self._result_columns_map.get(column)
        context = {"watchlist_items": self.watchlist_items}
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
        sort_column = column or self.sort_column
        sort_reverse = self.sort_reverse if reverse is None else bool(reverse)
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

    def on_result_heading_click(self, column: str):
        if column == self.sort_column:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = column
            self.sort_reverse = column in desc_by_default_ids()
        if self.filtered_stocks:
            self.update_result_table(self.filtered_stocks, announce=False, persist=False)

    def update_result_table(
        self,
        results: List[Dict[str, Any]],
        announce: bool = True,
        persist: bool = True,
        request: Optional[ScanRequest] = None,
    ):
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        results = self._apply_result_filters(results)
        results = self._sort_results(results)

        for result in results:
            row_values = self._format_result_row_values(result)
            values = tuple(row_values.get(col, "-") for col in self.result_columns)
            self.result_tree.insert("", tk.END, values=values)

        self.filtered_stocks = results
        if persist:
            self._save_last_results(results, complete=True, request=request)
        if announce:
            self._log(f"扫描完成，命中 {len(results)} 只。")

    def scan_finished(self, status_text: str = "扫描完成"):
        self._set_progressbar_indeterminate(False)
        self.scan_btn.config(state=tk.NORMAL)
        self.update_cache_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.status_var.set(status_text)
        if self.progress_var.get() >= 100:
            progress_total_text = self.progress_text_var.get().strip()
            self.progress_text_var.set(progress_total_text)
        elif not self.is_scanning and not self.is_updating_cache:
            self.progress_text_var.set("")
        self.is_scanning = False
        self.is_updating_cache = False
        self._scan_thread = None
        self._cache_thread = None
        self._active_scan_request = None
        self.refresh_universe_var.set(False)
        self._close_run_log()

    def on_stock_select(self, event):
        try:
            selection = self.result_tree.selection()
            if not selection:
                return
            item = self.result_tree.item(selection[0])
            values = item.get("values") or []
            if not values:
                return
            self._schedule_show_stock_detail(values[0])
        except Exception as e:
            self._log(f"选择股票详情失败: {e}")

    def on_stock_double_click(self, event):
        selection = self.result_tree.selection()
        if selection:
            item = self.result_tree.item(selection[0])
            stock_code = item["values"][0]
            self._cancel_scheduled_detail()
            self.show_stock_detail(stock_code, force_refresh=True)
            self.notebook.select(self.detail_tab_frame)

    def _get_tree_selected_code(self, tree) -> str:
        selection = tree.selection()
        if not selection:
            return ""
        item = tree.item(selection[0])
        values = item.get("values") or []
        if not values:
            return ""
        return str(values[0]).strip().zfill(6)

    def on_zt_stock_select(self, event):
        try:
            tree = event.widget
            stock_code = self._get_tree_selected_code(tree)
            if not stock_code:
                return
            self._schedule_show_stock_detail(stock_code)
        except Exception as e:
            self._log(f"选择涨停对比股票详情失败: {e}")

    def on_zt_stock_double_click(self, event):
        tree = event.widget
        stock_code = self._get_tree_selected_code(tree)
        if not stock_code:
            return
        self._cancel_scheduled_detail()
        self.show_stock_detail(stock_code, force_refresh=True)
        self.notebook.select(self.detail_tab_frame)

    def _get_selected_watchlist_code(self) -> str:
        if not hasattr(self, "_watch_tree"):
            return ""
        return self._get_tree_selected_code(self._watch_tree)

    def add_selected_result_to_watchlist(self) -> None:
        selection = self._get_selected_result_identity()
        if selection is None:
            messagebox.showwarning("提示", "请先在结果表中选中一只股票")
            return
        stock_code, stock_name = selection
        self._add_code_to_watchlist(stock_code, stock_name=stock_name)

    def remove_selected_result_from_watchlist(self) -> None:
        selection = self._get_selected_result_identity()
        if selection is None:
            messagebox.showwarning("提示", "请先在结果表中选中一只股票")
            return
        stock_code, _ = selection
        self._remove_code_from_watchlist(stock_code)

    def add_current_detail_to_watchlist(self) -> None:
        stock_code = str(self._current_detail_code or self._detail_request_code or "").strip().zfill(6)
        if not stock_code:
            messagebox.showwarning("提示", "请先打开一只股票详情")
            return
        stock_name = self.detail_labels.get("name").cget("text") if "name" in self.detail_labels else ""
        detail_payload = {
            "code": stock_code,
            "name": stock_name,
            "board": self._lookup_result_by_code(stock_code).get("data", {}).get("board", "") if self._lookup_result_by_code(stock_code) else "",
            "analysis": getattr(self, "_detail_chart_analysis", {}) or {},
        }
        self._add_code_to_watchlist(stock_code, stock_name=stock_name, detail=detail_payload)

    def toggle_current_detail_watchlist(self) -> None:
        stock_code = str(self._current_detail_code or self._detail_request_code or "").strip().zfill(6)
        if not stock_code:
            messagebox.showwarning("提示", "请先打开一只股票详情")
            return
        if stock_code in self.watchlist_items:
            self._remove_code_from_watchlist(stock_code)
        else:
            self.add_current_detail_to_watchlist()

    def edit_current_detail_watch_note(self) -> None:
        stock_code = str(self._current_detail_code or self._detail_request_code or "").strip().zfill(6)
        if not stock_code:
            messagebox.showwarning("提示", "请先打开一只股票详情")
            return
        if stock_code not in self.watchlist_items:
            self.add_current_detail_to_watchlist()
        self._edit_watchlist_item(stock_code)

    def _add_code_to_watchlist(
        self,
        stock_code: str,
        stock_name: str = "",
        detail: Optional[Dict[str, Any]] = None,
        status: str = "观察",
        note: Optional[str] = None,
    ) -> None:
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return
        payload = self._build_watchlist_item_payload(
            code,
            stock_name=stock_name,
            status=status,
            note=note,
            detail=detail,
        )
        save_watchlist_item(payload)
        self.watchlist_items[code] = {**self.watchlist_items.get(code, {}), **payload}
        self.refresh_watchlist_view()
        self._refresh_result_table_if_ready()
        self._update_detail_watch_state(code)
        self.status_var.set(f"{code} 已加入自选池")

    def _remove_code_from_watchlist(self, stock_code: str) -> None:
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return
        if code not in self.watchlist_items:
            return
        delete_watchlist_item(code)
        self.watchlist_items.pop(code, None)
        self.refresh_watchlist_view()
        self._refresh_result_table_if_ready()
        self._update_detail_watch_state(code)
        self.status_var.set(f"{code} 已移除自选池")

    def _edit_watchlist_item(self, stock_code: str) -> None:
        code = str(stock_code or "").strip().zfill(6)
        item = self.watchlist_items.get(code) or load_watchlist_item(code)
        if not item:
            return
        current_status = str(item.get("status", "") or "观察")
        new_status = simpledialog.askstring("自选状态", "请输入状态（观察/重点/持仓/淘汰）:", initialvalue=current_status, parent=self.root)
        if new_status is None:
            return
        new_note = simpledialog.askstring("自选备注", "请输入备注:", initialvalue=str(item.get("note", "") or ""), parent=self.root)
        if new_note is None:
            return
        payload = self._build_watchlist_item_payload(
            code,
            stock_name=str(item.get("name", "") or ""),
            status=str(new_status).strip() or current_status,
            note=str(new_note).strip(),
        )
        save_watchlist_item(payload)
        self.watchlist_items[code] = {**item, **payload}
        self.refresh_watchlist_view()
        self._refresh_result_table_if_ready()
        self._update_detail_watch_state(code)
        self.status_var.set(f"{code} 自选备注已更新")

    def edit_selected_watchlist_item(self) -> None:
        code = self._get_selected_watchlist_code()
        if not code:
            messagebox.showwarning("提示", "请先在自选池中选中一只股票")
            return
        self._edit_watchlist_item(code)

    def remove_selected_watchlist_item(self) -> None:
        code = self._get_selected_watchlist_code()
        if not code:
            messagebox.showwarning("提示", "请先在自选池中选中一只股票")
            return
        self._remove_code_from_watchlist(code)

    def refresh_watchlist_view(self) -> None:
        if not hasattr(self, "_watch_tree"):
            return
        self._watch_tree.delete(*self._watch_tree.get_children())
        items = sorted(
            self.watchlist_items.values(),
            key=lambda item: (str(item.get("status", "") or ""), str(item.get("updated_at", "") or "")),
            reverse=True,
        )
        for item in items:
            latest_close = item.get("latest_close")
            score = item.get("score")
            self._watch_tree.insert(
                "",
                tk.END,
                values=(
                    item.get("code", ""),
                    item.get("name", ""),
                    item.get("status", ""),
                    "-" if score is None else str(int(score)),
                    "-" if latest_close is None else f"{float(latest_close):.2f}",
                    item.get("board", ""),
                    item.get("note", ""),
                    item.get("updated_at", ""),
                ),
            )
        self.watchlist_summary_var.set(f"自选 {len(items)} 只")

    def on_watchlist_select(self, event):
        stock_code = self._get_selected_watchlist_code()
        if not stock_code:
            return
        self._schedule_show_stock_detail(stock_code)

    def on_watchlist_double_click(self, event):
        stock_code = self._get_selected_watchlist_code()
        if not stock_code:
            return
        self._cancel_scheduled_detail()
        self.show_stock_detail(stock_code, force_refresh=True)
        self.notebook.select(self.detail_tab_frame)

    def _refresh_result_table_if_ready(self) -> None:
        if hasattr(self, "result_tree") and self.filtered_stocks:
            self.update_result_table(self.filtered_stocks, announce=False, persist=False)

    def query_single_stock(self):
        stock_code = self.stock_code_var.get().strip()
        if not stock_code:
            messagebox.showwarning("警告", "请输入股票代码")
            return
        if not self.update_filter_params():
            return
        self._cancel_scheduled_detail()
        self.show_stock_detail(stock_code, force_refresh=True)
        self.notebook.select(self.detail_tab_frame)

    def _cancel_scheduled_detail(self) -> None:
        if self._detail_after_id is None:
            return
        try:
            self.root.after_cancel(self._detail_after_id)
        except tk.TclError:
            pass
        self._detail_after_id = None

    def _schedule_show_stock_detail(self, stock_code: str, delay_ms: int = 180) -> None:
        code = str(stock_code).strip().zfill(6)
        self._cancel_scheduled_detail()
        if self._is_closing:
            return
        try:
            if self.root.winfo_exists():
                self._detail_after_id = self.root.after(delay_ms, lambda c=code: self.show_stock_detail(c))
        except tk.TclError:
            self._detail_after_id = None

    def _trigger_detail_revalidate(self, code: str) -> None:
        """缓存命中后悄悄拉一次最新详情，5s 节流避免重复。

        新数据回来会通过 _apply_detail_if_current 静默更新面板（用户无感）。
        如果用户已切到别的票，过滤掉过期请求。
        """
        c = str(code or "").strip().zfill(6)
        if not c:
            return
        last = self._detail_last_revalidate_ts.get(c, 0.0)
        if (time.time() - last) < self._DETAIL_REVALIDATE_THROTTLE_SEC:
            return
        self._detail_last_revalidate_ts[c] = time.time()
        try:
            self._start_background_job(
                self._load_detail, name=f"detail-revalidate-{c}", args=(c,),
            )
        except Exception as exc:  # noqa: BLE001
            self._log(f"详情后台刷新启动失败 {c}: {exc}")

    def show_stock_detail(self, stock_code: str, force_refresh: bool = False):
        code = str(stock_code).strip().zfill(6)
        self._cancel_scheduled_detail()
        self._detail_request_code = code
        self._show_detail_loading(code)
        # 进入详情前先用扫描结果中的名称占位（若有）
        prefilled_name = ""
        for result in self.filtered_stocks:
            if str(result.get("code", "")).strip().zfill(6) == code:
                prefilled_name = str(result.get("name", "") or "")
                break
        self._set_top_header_for_code(code, prefilled_name)

        # ---- GUI 层 LRU 缓存命中：反复点击同一只股票秒开 ----
        if not force_refresh:
            cached = self._detail_payload_cache.get(code)
            if cached is not None:
                ts, payload = cached
                if (time.time() - ts) < self._DETAIL_CACHE_TTL_SEC:
                    self._detail_payload_cache.move_to_end(code)
                    try:
                        self._update_detail_ui(payload)
                        age = int(time.time() - ts)
                        self.status_var.set(f"{code} 详情（内存缓存 {age}s，正在后台刷新）")
                        self._detail_loading_code = ""
                        # stale-while-revalidate：后台再拉一次，新数据回来静默替换
                        self._trigger_detail_revalidate(code)
                        return
                    except Exception as e:
                        self._log(f"渲染内存缓存详情失败，回退到完整加载: {e}")

        detail_payload = None
        for result in self.filtered_stocks:
            if str(result.get("code", "")).strip().zfill(6) == code:
                data = result.get("data", {}) or {}
                detail_payload = {
                    "code": code,
                    "name": result.get("name", ""),
                    "industry": result.get("industry", ""),
                    "board": data.get("board", ""),
                    "exchange": data.get("exchange", ""),
                    "history": data.get("history"),
                    "analysis": data.get("analysis") or {},
                }
                break

        if detail_payload is not None:
            self._log(f"显示扫描结果中的股票 {code} 详情。")
            try:
                self._update_detail_ui(detail_payload)
            except Exception as e:
                self._log(f"渲染缓存详情失败: {e}")
                self._show_detail_error(code, f"渲染详情失败: {e}")
            if not force_refresh:
                # 扫描快照可能是几分钟到几小时前拉的，先秒开再后台 revalidate，
                # 让用户既快又新（盘后场景下尤其重要 —— 14:50 扫描的快照在
                # 15:00 收盘后应当被替换为收盘价）
                self.status_var.set(f"{code} 详情（扫描快照，正在后台刷新）")
                self._detail_loading_code = ""
                self._trigger_detail_revalidate(code)
                return
            self.status_var.set(f"正在刷新 {code} 最新详情...")
            self._detail_loading_code = code
            self._start_background_job(
                self._load_detail, name=f"detail-{code}", args=(code,)
            )
            return

        if not force_refresh and self._detail_loading_code == code:
            self.status_var.set(f"{code} 详情正在加载...")
            return

        self._log(f"查询股票 {code} 的历史详情...")
        self.status_var.set(f"正在查询 {code}...")
        self._detail_loading_code = code
        self._start_background_job(
            self._load_detail, name=f"detail-{code}", args=(code,)
        )
    def _load_detail(self, stock_code: str, cancel_token: CancelToken):
        try:
            if cancel_token.is_cancelled():
                return
            quick_detail = self.stock_filter.get_stock_detail_quick(stock_code)
            quick_history = None
            if isinstance(quick_detail, dict):
                quick_history = quick_detail.get("history")
                if quick_history is not None and not getattr(quick_history, "empty", True):
                    self._post_to_ui(lambda: self._apply_quick_detail_if_current(stock_code, quick_detail))

            if cancel_token.is_cancelled():
                return
            detail = self.stock_filter.get_stock_detail(stock_code, preloaded_history=quick_history)
            if cancel_token.is_cancelled():
                return
            self._post_to_ui(lambda: self._apply_detail_if_current(stock_code, detail))
        except Exception as e:
            error_text = str(e)
            self._post_to_ui(lambda: self._log(f"查询详情出错: {error_text}"))
            self._post_to_ui(lambda: self._show_detail_error(stock_code, f"详情加载失败: {error_text}"))
        finally:
            self._post_to_ui(lambda: self._finish_detail_status(stock_code))

    def _apply_quick_detail_if_current(self, stock_code: str, detail: Dict[str, Any]) -> None:
        if str(stock_code).strip().zfill(6) != self._detail_request_code:
            return
        try:
            self._update_detail_ui(detail)
            self.status_var.set(f"{stock_code} 详情（历史缓存）")
        except Exception as e:
            self._log(f"更新缓存详情失败: {e}")

    def _apply_detail_if_current(self, stock_code: str, detail: Dict[str, Any]) -> None:
        code = str(stock_code).strip().zfill(6)
        if code != self._detail_request_code:
            return
        try:
            self._update_detail_ui(detail)
        except Exception as e:
            self._log(f"更新详情面板失败: {e}")
            self._show_detail_error(stock_code, f"详情渲染失败: {e}")
            return
        # 写入 LRU 缓存：下次点击该股票走秒开路径
        try:
            self._detail_payload_cache[code] = (time.time(), detail)
            self._detail_payload_cache.move_to_end(code)
            while len(self._detail_payload_cache) > self._DETAIL_CACHE_MAX:
                self._detail_payload_cache.popitem(last=False)
        except Exception:
            pass
    def _finish_detail_status(self, stock_code: str) -> None:
        code = str(stock_code).strip().zfill(6)
        if code == self._detail_loading_code:
            self._detail_loading_code = ""
        if code != self._detail_request_code:
            return
        self.status_var.set("查询完成")

    def _show_detail_loading(self, stock_code: str) -> None:
        self._detail_chart_dates = []
        self._detail_chart_history = None
        self._detail_chart_analysis = {}
        self._detail_chart_window_start = 0
        self._detail_chart_loaded_days = 0
        self._detail_chart_loading_more = False
        placeholders = {
            "code": str(stock_code).strip().zfill(6),
            "name": "加载中...",
            "industry": "加载中...",
            "latest_date": "加载中...",
            "quote_time": "加载中...",
            "latest_close": "加载中...",
            "score": "加载中...",
            "watch_status": "加载中...",
            "latest_ma": "加载中...",
            "latest_ma10": "加载中...",
            "latest_volume": "加载中...",
            "latest_amount": "加载中...",
            "five_day_return": "加载中...",
            "limit_up": "加载中...",
            "big_order_amount": "加载中...",
            "main_force_amount": "加载中...",
            "summary": "正在加载详情数据...",
        }
        for key, value in placeholders.items():
            if key in self.detail_labels:
                self.detail_labels[key].config(text=value)
        self._update_detail_watch_state(stock_code)
        self.price_ax.clear()
        self.volume_ax.clear()
        self.flow_ax.clear()
        self.price_ax.text(0.5, 0.5, "正在加载详情...", ha="center", va="center", fontsize=14)
        self.volume_ax.text(0.5, 0.5, "请稍候", ha="center", va="center", fontsize=11)
        self.flow_ax.text(0.5, 0.5, "正在加载大单净额...", ha="center", va="center", fontsize=11)
        self.price_ax.set_axis_off()
        self.volume_ax.set_axis_off()
        self.flow_ax.set_axis_off()
        self.canvas.draw()

    def _show_detail_error(self, stock_code: str, message: str) -> None:
        self._detail_chart_dates = []
        self._detail_chart_history = None
        self._detail_chart_analysis = {}
        self._detail_chart_window_start = 0
        self._detail_chart_loaded_days = 0
        self._detail_chart_loading_more = False
        code_text = str(stock_code).strip().zfill(6)
        if "code" in self.detail_labels:
            self.detail_labels["code"].config(text=code_text)
        if "name" in self.detail_labels:
            self.detail_labels["name"].config(text="加载失败")
        if "score" in self.detail_labels:
            self.detail_labels["score"].config(text="-")
        if "watch_status" in self.detail_labels:
            self.detail_labels["watch_status"].config(text="-")
        if "summary" in self.detail_labels:
            self.detail_labels["summary"].config(text=message or "详情加载失败")
        self.price_ax.clear()
        self.volume_ax.clear()
        self.flow_ax.clear()
        self.price_ax.text(0.5, 0.5, "详情加载失败", ha="center", va="center", fontsize=14, color="#b22222")
        self.volume_ax.text(0.5, 0.5, "请查看运行日志", ha="center", va="center", fontsize=11)
        self.flow_ax.text(0.5, 0.5, "请稍后重试", ha="center", va="center", fontsize=11)
        self.price_ax.set_axis_off()
        self.volume_ax.set_axis_off()
        self.flow_ax.set_axis_off()
        self.canvas.draw()
    def _update_detail_ui(self, detail: Dict[str, Any]):
        analysis = detail.get("analysis") or {}
        history = detail.get("history")
        self._current_detail_code = str(detail.get("code", "") or "").strip().zfill(6)
        self._detail_chart_loading_more = False
        self._refresh_detail_metric_labels()
        self._set_top_header_for_code(self._current_detail_code, str(detail.get("name", "") or ""))

        self.detail_labels["code"].config(text=detail.get("code", "-"))
        self.detail_labels["name"].config(text=detail.get("name", "-"))
        industry = str(detail.get("industry", "") or "").strip()
        last_limit_up_trade_date = str(detail.get("last_limit_up_trade_date", "") or "").strip()
        industry_text = industry or "-"
        if industry and last_limit_up_trade_date:
            industry_text = f"{industry} (涨停缓存 {last_limit_up_trade_date})"
        self.detail_labels["industry"].config(text=industry_text)
        self.detail_labels["latest_date"].config(text=analysis.get("latest_date", "-"))
        self.detail_labels["quote_time"].config(text=analysis.get("quote_time", "-") or "-")
        self.detail_labels["latest_close"].config(
            text="-" if analysis.get("latest_close") is None else f"{analysis['latest_close']:.2f}"
        )
        self.detail_labels["score"].config(
            text="-" if analysis.get("score") is None else f"{int(analysis['score'])}"
        )
        self.detail_labels["latest_ma"].config(
            text="-" if analysis.get("latest_ma") is None else f"{analysis['latest_ma']:.2f}"
        )
        self.detail_labels["latest_ma10"].config(
            text="-" if analysis.get("latest_ma10") is None else f"{analysis['latest_ma10']:.2f}"
        )
        latest_volume_text = self._format_volume(analysis.get("latest_volume"))
        latest_volume_ratio = analysis.get("latest_volume_ratio")
        if latest_volume_text != "-" and latest_volume_ratio is not None:
            latest_volume_text = f"{latest_volume_text} ({latest_volume_ratio:.1f}%)"
        self.detail_labels["latest_volume"].config(text=latest_volume_text)
        self.detail_labels["latest_amount"].config(text=self._format_amount(analysis.get("latest_amount")))
        self.detail_labels["five_day_return"].config(
            text="-" if analysis.get("five_day_return") is None else f"{analysis['five_day_return']:.2f}%"
        )
        self.detail_labels["limit_up"].config(text="是" if analysis.get("limit_up") else "否")
        self.detail_labels["big_order_amount"].config(text=self._format_amount(analysis.get("big_order_amount")))
        self.detail_labels["main_force_amount"].config(text=self._format_amount(analysis.get("main_force_amount")))

        # 技术指标
        dif = analysis.get("macd_dif")
        dea = analysis.get("macd_dea")
        macd_bar = analysis.get("macd_bar")
        if dif is not None and dea is not None:
            self.detail_labels["macd"].config(text=f"DIF {dif:.3f} / DEA {dea:.3f}")
        else:
            self.detail_labels["macd"].config(text="-")

        kdj_k = analysis.get("kdj_k")
        kdj_d = analysis.get("kdj_d")
        kdj_j = analysis.get("kdj_j")
        if kdj_k is not None:
            self.detail_labels["kdj"].config(text=f"K {kdj_k:.1f} / D {kdj_d:.1f} / J {kdj_j:.1f}")
        else:
            self.detail_labels["kdj"].config(text="-")

        rsi6 = analysis.get("rsi_6")
        rsi12 = analysis.get("rsi_12")
        if rsi6 is not None:
            self.detail_labels["rsi"].config(text=f"RSI6 {rsi6:.1f} / RSI12 {rsi12:.1f}")
        else:
            self.detail_labels["rsi"].config(text="-")

        boll_upper = analysis.get("boll_upper")
        boll_mid = analysis.get("boll_mid")
        boll_lower = analysis.get("boll_lower")
        if boll_mid is not None:
            self.detail_labels["boll"].config(text=f"U {boll_upper:.2f} / M {boll_mid:.2f} / L {boll_lower:.2f}")
        else:
            self.detail_labels["boll"].config(text="-")

        self.detail_labels["summary"].config(text=analysis.get("summary", "-"))
        self._update_detail_watch_state(self._current_detail_code)

        self._draw_chart(history, analysis)

    def _update_detail_watch_state(self, stock_code: str) -> None:
        code = str(stock_code or "").strip().zfill(6)
        item = self.watchlist_items.get(code) or {}
        if "watch_status" in self.detail_labels:
            if item:
                text = str(item.get("status", "") or "观察")
                note = str(item.get("note", "") or "").strip()
                if note:
                    text = f"{text} | {self._short_text(note, max_len=18)}"
                self.detail_labels["watch_status"].config(text=text)
            else:
                self.detail_labels["watch_status"].config(text="未加入")
        if hasattr(self, "detail_watch_btn"):
            self.detail_watch_btn.config(text="移除自选" if item else "加入自选")

    def _reset_detail_chart_axes(self) -> None:
        self._detail_chart_dates = []
        self.price_ax.clear()
        self.volume_ax.clear()
        self.flow_ax.clear()
        self.price_ax.set_axis_on()
        self.volume_ax.set_axis_on()
        self.flow_ax.set_axis_on()

    def _show_empty_detail_chart(self, message: str) -> None:
        self._detail_chart_history = None
        self._detail_chart_analysis = {}
        self._detail_chart_window_start = 0
        self.detail_chart_window_scale.config(from_=0, to=0, state=tk.DISABLED)
        self.detail_chart_window_var.set(0)
        self.detail_chart_window_label_var.set("窗口: -")
        self.price_ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=14)
        self.canvas.draw()

    def _prepare_detail_chart_dataset(self, history, analysis):
        if history is None or getattr(history, "empty", True):
            return None

        df = history.copy()
        if "date" in df.columns:
            df = df.sort_values("date").reset_index(drop=True)
        else:
            df = df.reset_index(drop=True)
        if df.empty:
            return None

        self._detail_chart_history = df
        self._detail_chart_analysis = dict(analysis or {})
        self._detail_chart_loaded_days = max(self._detail_chart_loaded_days, len(df))

        x = list(range(len(df)))
        dates = df["date"].astype(str).tolist() if "date" in df.columns else [str(i) for i in x]
        self._detail_chart_dates = list(dates)
        opens = pd.to_numeric(df["open"], errors="coerce") if "open" in df.columns else pd.Series([None] * len(df))
        closes = pd.to_numeric(df["close"], errors="coerce") if "close" in df.columns else pd.Series([None] * len(df))
        highs = pd.to_numeric(df["high"], errors="coerce") if "high" in df.columns else pd.Series([None] * len(df))
        lows = pd.to_numeric(df["low"], errors="coerce") if "low" in df.columns else pd.Series([None] * len(df))
        volumes = pd.to_numeric(df["volume"], errors="coerce") if "volume" in df.columns else pd.Series([0] * len(df))
        return {
            "df": df,
            "x": x,
            "dates": dates,
            "opens": opens,
            "closes": closes,
            "highs": highs,
            "lows": lows,
            "volumes": volumes,
        }

    def _draw_detail_price_panel(self, chart_data: Dict[str, Any]) -> None:
        x = chart_data["x"]
        opens = chart_data["opens"]
        closes = chart_data["closes"]
        highs = chart_data["highs"]
        lows = chart_data["lows"]

        for idx, (open_price, close_price, high_price, low_price) in enumerate(zip(opens, closes, highs, lows)):
            if pd.isna(open_price) or pd.isna(close_price) or pd.isna(high_price) or pd.isna(low_price):
                continue
            color = "#d94b4b" if close_price >= open_price else "#1f8b4c"
            body_low = min(open_price, close_price)
            body_height = abs(close_price - open_price)
            if body_height == 0:
                body_height = max(close_price * 0.001, 0.01)
            self.price_ax.vlines(idx, low_price, high_price, color=color, linewidth=1.0)
            self.price_ax.bar(idx, body_height, bottom=body_low, width=0.6, color=color, edgecolor=color, alpha=0.85)

        ma_period = max(1, int(self.stock_filter.ma_period))
        ma = closes.rolling(window=ma_period, min_periods=ma_period).mean()
        ma10 = closes.rolling(window=10, min_periods=10).mean()
        latest_close = closes.dropna().iloc[-1] if not closes.dropna().empty else None
        latest_ma = ma.dropna().iloc[-1] if not ma.dropna().empty else None
        latest_ma10 = ma10.dropna().iloc[-1] if not ma10.dropna().empty else None
        close_label = "收盘价" if latest_close is None else f"收盘价 {latest_close:.2f}"
        ma_label = f"MA{ma_period}" if latest_ma is None else f"MA{ma_period} {latest_ma:.2f}"
        ma10_label = "MA10" if latest_ma10 is None else f"MA10 {latest_ma10:.2f}"
        self.price_ax.plot(x, closes, color="#2f6fd6", linewidth=1.0, alpha=0.35, label=close_label)
        self.price_ax.plot(x, ma, color="#f08a24", linewidth=1.4, label=ma_label)
        self.price_ax.plot(x, ma10, color="#7b52ab", linewidth=1.2, label=ma10_label)
        self.price_ax.set_ylabel("价\n格", rotation=0, labelpad=14, va="center")
        self.price_ax.set_title("K线图（滚轮左右滑动，点击K线进入分时）")
        self.price_ax.legend(loc="upper left")
        self.price_ax.grid(True, alpha=0.25)

    def _build_detail_flow_series(self, dates: List[str], analysis: Dict[str, Any]):
        flow_history = (analysis or {}).get("fund_flow_history") or []
        flow_map = {}
        for item in flow_history:
            if not isinstance(item, dict):
                continue
            flow_date = str(item.get("date", "") or "").strip()
            if flow_date:
                flow_map[flow_date] = item

        values = []
        colors = []
        for date_str in dates:
            flow_item = flow_map.get(date_str, {})
            amount = pd.to_numeric(pd.Series([flow_item.get("big_order_amount")]), errors="coerce").iloc[0]
            if pd.isna(amount):
                amount = 0.0
            values.append(float(amount))
            colors.append("#d94b4b" if amount >= 0 else "#1f8b4c")
        return values, colors

    def _draw_detail_volume_panel(self, chart_data: Dict[str, Any]) -> None:
        x = chart_data["x"]
        opens = chart_data["opens"]
        closes = chart_data["closes"]
        volumes = chart_data["volumes"]
        volume_colors = [
            "#d94b4b" if (not pd.isna(c) and not pd.isna(o) and c >= o) else "#1f8b4c"
            for o, c in zip(opens, closes)
        ]
        self.volume_ax.bar(x, volumes.fillna(0), width=0.6, color=volume_colors, alpha=0.85)
        volume_compare_window = volumes.iloc[-6:-1].dropna() if len(volumes) > 1 else pd.Series(dtype=float)
        if volume_compare_window.empty:
            volume_compare_window = volumes.dropna()
        latest_volume_value = volumes.dropna().iloc[-1] if not volumes.dropna().empty else None
        latest_volume_ratio_text = ""
        if latest_volume_value is not None and not volume_compare_window.empty:
            avg_volume = float(volume_compare_window.mean())
            if avg_volume > 0:
                latest_volume_ratio_text = f"  最新 {self._format_volume(latest_volume_value)} / 均量 {latest_volume_value / avg_volume * 100.0:.1f}%"
        self.volume_ax.set_ylabel("成\n交\n量", rotation=0, labelpad=14, va="center")
        self.volume_ax.set_title(f"成交量{latest_volume_ratio_text}" if latest_volume_ratio_text else "成交量")
        self.volume_ax.yaxis.set_major_formatter(FuncFormatter(self._format_axis_volume))
        self.volume_ax.yaxis.get_offset_text().set_visible(False)
        self.volume_ax.grid(True, alpha=0.2)

    def _draw_detail_flow_panel(self, chart_data: Dict[str, Any], analysis: Dict[str, Any]) -> None:
        if not self._detail_flow_expanded:
            self.flow_ax.set_visible(False)
            self.flow_ax.set_axis_off()
            return

        flow_values, flow_colors = self._build_detail_flow_series(chart_data["dates"], analysis)
        self.flow_ax.set_visible(True)
        self.flow_ax.bar(chart_data["x"], flow_values, width=0.6, color=flow_colors, alpha=0.85)
        self.flow_ax.axhline(0, color="#666666", linewidth=0.8, alpha=0.6)
        self.flow_ax.set_ylabel("大\n单\n净\n额", rotation=0, labelpad=14, va="center")
        self.flow_ax.set_xlabel("日期")
        self.flow_ax.grid(True, alpha=0.2)

    def _resolve_detail_chart_window(self, total: int, keep_window: bool = False) -> Dict[str, int]:
        window = max(15, min(int(self._detail_chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        start = max(0, min(int(self._detail_chart_window_start), max_start)) if keep_window else max_start
        end = min(total, start + window)
        return {"window": window, "max_start": max_start, "start": start, "end": end}

    def _apply_detail_chart_window(self, chart_data: Dict[str, Any], window_meta: Dict[str, int]) -> None:
        x = chart_data["x"]
        dates = chart_data["dates"]
        start = window_meta["start"]
        end = window_meta["end"]
        max_start = window_meta["max_start"]

        self._detail_chart_window_start = start
        self.detail_chart_window_scale.config(from_=0, to=max_start, state=(tk.NORMAL if max_start > 0 else tk.DISABLED))
        self._detail_chart_slider_updating = True
        try:
            self.detail_chart_window_var.set(start)
        finally:
            self._detail_chart_slider_updating = False

        if x:
            self.price_ax.set_xlim(start - 0.5, end - 0.5)
            self.volume_ax.set_xlim(start - 0.5, end - 0.5)
            if self._detail_flow_expanded:
                self.flow_ax.set_xlim(start - 0.5, end - 0.5)

        view_len = max(1, end - start)
        tick_step = max(1, view_len // 8)
        tick_positions = list(range(start, end, tick_step))
        if tick_positions and tick_positions[-1] != end - 1:
            tick_positions.append(end - 1)
        elif not tick_positions and end > start:
            tick_positions = [end - 1]
        tick_labels = [dates[pos][5:] if len(dates[pos]) >= 10 else dates[pos] for pos in tick_positions]

        self.price_ax.set_xticks(tick_positions)
        self.price_ax.tick_params(axis="x", labelbottom=False)
        if self._detail_flow_expanded:
            self.volume_ax.set_xticklabels([""] * len(tick_positions))
            self.volume_ax.tick_params(axis="x", labelbottom=False)
            self.flow_ax.set_xticklabels(tick_labels, rotation=45, ha="right")
            self.flow_ax.tick_params(axis="x", labelbottom=True)
        else:
            self.flow_ax.tick_params(axis="x", labelbottom=False)
            self.volume_ax.set_xticklabels(tick_labels, rotation=45, ha="right")
            self.volume_ax.tick_params(axis="x", labelbottom=True)

        if dates:
            self.detail_chart_window_label_var.set(f"窗口: {dates[start]} ~ {dates[end - 1]}")
        else:
            self.detail_chart_window_label_var.set("窗口: -")

    def _draw_chart(self, history, analysis, keep_window: bool = False):
        self._reset_detail_chart_axes()

        chart_data = self._prepare_detail_chart_dataset(history, analysis)
        if chart_data is None:
            self._show_empty_detail_chart("暂无历史数据")
            return

        self._draw_detail_price_panel(chart_data)
        self._draw_detail_volume_panel(chart_data)
        self._draw_detail_flow_panel(chart_data, analysis or {})
        window_meta = self._resolve_detail_chart_window(len(chart_data["x"]), keep_window=keep_window)
        self._apply_detail_chart_window(chart_data, window_meta)
        self.fig.tight_layout()
        self.canvas.draw()

    def toggle_detail_summary_section(self):
        self._detail_summary_expanded = not self._detail_summary_expanded
        self._apply_detail_section_visibility()

    def toggle_detail_chart_section(self):
        self._detail_chart_expanded = not self._detail_chart_expanded
        self._apply_detail_section_visibility()

    def _apply_detail_section_visibility(self):
        if self._detail_summary_expanded:
            if not self.info_frame.winfo_ismapped():
                self.info_frame.pack(fill=tk.X, pady=5, after=self.detail_info_header)
            self.detail_summary_toggle_btn.config(text="收起历史摘要")
            self.detail_summary_status_var.set("历史摘要已展开")
        else:
            if self.info_frame.winfo_ismapped():
                self.info_frame.pack_forget()
            self.detail_summary_toggle_btn.config(text="展开历史摘要")
            self.detail_summary_status_var.set("历史摘要已收起")

        if self._detail_chart_expanded:
            if not self.chart_frame.winfo_ismapped():
                self.chart_frame.pack(fill=tk.BOTH, expand=True, pady=5, after=self.detail_chart_header)
            self.detail_chart_toggle_btn.config(text="收起历史K线")
            self.detail_chart_status_var.set("历史K线已展开")
        else:
            if self.chart_frame.winfo_ismapped():
                self.chart_frame.pack_forget()
            self.detail_chart_toggle_btn.config(text="展开历史K线")
            self.detail_chart_status_var.set("历史K线已收起")

    def toggle_detail_flow_section(self):
        self._detail_flow_expanded = not self._detail_flow_expanded
        if self._detail_flow_expanded:
            self.detail_flow_toggle_btn.config(text="收起大单净额")
            self.detail_flow_status_var.set("大单净额已展开")
        else:
            self.detail_flow_toggle_btn.config(text="展开大单净额")
            self.detail_flow_status_var.set("大单净额已收起")
        if self._detail_chart_history is not None:
            self._draw_chart(self._detail_chart_history, self._detail_chart_analysis, keep_window=True)

    def on_detail_chart_window_changed(self, value):
        if self._detail_chart_slider_updating:
            return
        if self._detail_chart_history is None or getattr(self._detail_chart_history, "empty", True):
            return
        try:
            new_start = int(float(value))
        except (TypeError, ValueError):
            return
        total = len(self._detail_chart_history)
        window = max(15, min(int(self._detail_chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        new_start = max(0, min(new_start, max_start))
        if new_start == self._detail_chart_window_start:
            return
        self._detail_chart_window_start = new_start
        self._draw_chart(self._detail_chart_history, self._detail_chart_analysis, keep_window=True)

    def _bind_detail_chart_scroll(self, widget) -> None:
        if self._detail_chart_scroll_bound:
            return
        try:
            widget.bind("<MouseWheel>", self.on_detail_chart_mousewheel)
            widget.bind("<Shift-MouseWheel>", self.on_detail_chart_mousewheel)
            widget.bind("<Button-4>", self.on_detail_chart_mousewheel)
            widget.bind("<Button-5>", self.on_detail_chart_mousewheel)
            widget.bind("<Left>", lambda _e: self._move_detail_chart_window(-12))
            widget.bind("<Right>", lambda _e: self._move_detail_chart_window(12))
            widget.bind("<Enter>", lambda _e: widget.focus_set())
            widget.bind("<Button-1>", lambda _e: widget.focus_set())
            self._detail_chart_scroll_bound = True
        except tk.TclError:
            pass

    def _move_detail_chart_window(self, delta: int) -> bool:
        if self._detail_chart_history is None or getattr(self._detail_chart_history, "empty", True):
            return False
        total = len(self._detail_chart_history)
        window = max(15, min(int(self._detail_chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        if max_start <= 0:
            self._maybe_load_more_detail_history(need_older=True)
            return False
        try:
            shift = int(delta)
        except (TypeError, ValueError):
            return False
        if shift == 0:
            return False
        new_start = max(0, min(self._detail_chart_window_start + shift, max_start))
        if new_start == self._detail_chart_window_start:
            if new_start == 0 and shift < 0:
                self._maybe_load_more_detail_history(need_older=True)
            return False
        self._detail_chart_window_start = new_start
        self._draw_chart(self._detail_chart_history, self._detail_chart_analysis, keep_window=True)
        if self._detail_chart_window_start <= max(0, min(12, max_start)) and shift < 0:
            self._maybe_load_more_detail_history(need_older=True)
        return True

    def _maybe_load_more_detail_history(self, need_older: bool = False) -> None:
        if not need_older:
            return
        if self._detail_chart_loading_more:
            return
        code = str(self._current_detail_code or self._detail_request_code or "").strip().zfill(6)
        if not code:
            return
        current_rows = 0
        if self._detail_chart_history is not None and not getattr(self._detail_chart_history, "empty", True):
            current_rows = len(self._detail_chart_history)
        request_days = max(120, self._detail_chart_loaded_days + 120, current_rows + 120)
        self._detail_chart_loading_more = True
        self.detail_chart_status_var.set("历史K线补载中...")
        self._start_background_job(
            self._load_more_detail_history,
            name=f"detail-history-{code}",
            args=(code, request_days, current_rows),
        )

    def _load_more_detail_history(
        self,
        stock_code: str,
        request_days: int,
        previous_rows: int,
        cancel_token: CancelToken,
    ) -> None:
        if cancel_token.is_cancelled():
            return
        history = self.stock_filter.get_stock_detail_history(stock_code, request_days)
        if cancel_token.is_cancelled():
            return
        self._post_to_ui(
            lambda: self._apply_more_detail_history_if_current(stock_code, history, request_days, previous_rows),
        )

    def _apply_more_detail_history_if_current(self, stock_code: str, history, request_days: int, previous_rows: int) -> None:
        self._detail_chart_loading_more = False
        code = str(stock_code).strip().zfill(6)
        if code != str(self._current_detail_code or self._detail_request_code or "").strip().zfill(6):
            return
        if history is None or getattr(history, "empty", True):
            self.detail_chart_status_var.set("历史K线补载失败")
            return
        if len(history) <= previous_rows:
            self._detail_chart_loaded_days = max(self._detail_chart_loaded_days, request_days)
            self.detail_chart_status_var.set("历史K线已展开")
            return
        old_start = int(self._detail_chart_window_start)
        added_rows = len(history) - previous_rows
        self._detail_chart_history = history
        self._detail_chart_window_start = old_start + added_rows
        self.detail_chart_status_var.set("历史K线已展开")
        self._draw_chart(history, self._detail_chart_analysis, keep_window=True)

    def _detail_scroll_delta(self, event) -> int:
        button = str(getattr(event, "button", "") or "").lower()
        if button == "up":
            return -6
        if button == "down":
            return 6
        num = getattr(event, "num", None)
        if num == 4:
            return -6
        if num == 5:
            return 6
        delta = getattr(event, "delta", 0) or 0
        if delta > 0:
            return -6
        if delta < 0:
            return 6
        return 0

    def on_detail_chart_scroll(self, event):
        if event is None:
            return
        inaxes = getattr(event, "inaxes", None)
        if inaxes is not None and inaxes not in (self.price_ax, self.volume_ax, self.flow_ax):
            return
        if self._detail_chart_history is None or getattr(self._detail_chart_history, "empty", True):
            return
        total = len(self._detail_chart_history)
        window = max(15, min(int(self._detail_chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        if max_start <= 0:
            return

        delta = self._detail_scroll_delta(event)
        if delta == 0:
            return
        self._move_detail_chart_window(delta)

    def on_detail_chart_mousewheel(self, event):
        self.on_detail_chart_scroll(event)

    def on_detail_chart_click(self, event):
        if event is None:
            return
        if event.inaxes not in (self.price_ax, self.volume_ax, self.flow_ax):
            return
        if getattr(event, "button", None) not in (1, None):
            return
        self._detail_chart_click_target_date = ""
        if self._detail_chart_dates and event.xdata is not None:
            try:
                idx = int(round(float(event.xdata)))
                idx = max(0, min(idx, len(self._detail_chart_dates) - 1))
                self._detail_chart_click_target_date = str(self._detail_chart_dates[idx] or "").strip()
            except (TypeError, ValueError):
                self._detail_chart_click_target_date = ""
        if event.x is None:
            self._detail_chart_dragging = False
            return
        self._detail_chart_dragging = True
        self._detail_chart_drag_moved = False
        self._detail_chart_drag_start_x = float(event.x)
        self._detail_chart_drag_start_window = int(self._detail_chart_window_start)

    def on_detail_chart_drag_motion(self, event):
        if not self._detail_chart_dragging:
            return
        if event is None or getattr(event, "inaxes", None) not in (self.price_ax, self.volume_ax, self.flow_ax):
            return
        if self._detail_chart_history is None or getattr(self._detail_chart_history, "empty", True):
            return
        if event.x is None:
            return
        total = len(self._detail_chart_history)
        window = max(15, min(int(self._detail_chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        if max_start <= 0:
            return
        pixel_per_bar = max(3.0, self.canvas.get_tk_widget().winfo_width() / max(1, window))
        bars = int(round((float(event.x) - self._detail_chart_drag_start_x) / pixel_per_bar))
        new_start = max(0, min(self._detail_chart_drag_start_window - bars, max_start))
        if new_start == self._detail_chart_window_start:
            return
        self._detail_chart_drag_moved = True
        self._detail_chart_window_start = new_start
        self._draw_chart(self._detail_chart_history, self._detail_chart_analysis, keep_window=True)

    def on_detail_chart_drag_release(self, event):
        if not self._detail_chart_dragging:
            return
        self._detail_chart_dragging = False
        if self._detail_chart_drag_moved:
            self._detail_chart_drag_moved = False
            return
        code = str(self._current_detail_code or self._detail_request_code or "").strip().zfill(6)
        if not code:
            return
        self.open_intraday_view_with_offset(code, day_offset=0, target_trade_date=self._detail_chart_click_target_date)
        self._detail_chart_drag_moved = False

    def open_intraday_view(self, stock_code: str):
        self.open_intraday_view_with_offset(stock_code, day_offset=0)

    def navigate_intraday_day(self, delta: int):
        code = str(self._intraday_request_code or self._current_detail_code or "").strip().zfill(6)
        if not code:
            return
        try:
            step = int(delta)
        except (TypeError, ValueError):
            return
        if step == 0:
            return
        if not self._intraday_available_dates:
            self.open_intraday_view_with_offset(code, day_offset=self._intraday_day_offset + step)
            return
        try:
            current_idx = self._intraday_available_dates.index(self._intraday_selected_date)
        except ValueError:
            current_idx = len(self._intraday_available_dates) - 1
        target_idx = max(0, min(current_idx + step, len(self._intraday_available_dates) - 1))
        target_date = self._intraday_available_dates[target_idx]
        self.open_intraday_view_with_offset(code, day_offset=0, target_trade_date=target_date)

    def _refresh_intraday_nav_buttons(self):
        has_code = bool(str(self._intraday_request_code or "").strip())
        has_dates = len(self._intraday_available_dates) > 0
        can_prev = has_code and has_dates and (self._intraday_day_offset > -(len(self._intraday_available_dates) - 1))
        can_next = has_code and has_dates and (self._intraday_day_offset < 0)
        self.intraday_prev_btn.config(state=(tk.NORMAL if can_prev else tk.DISABLED))
        self.intraday_next_btn.config(state=(tk.NORMAL if can_next else tk.DISABLED))

    def open_intraday_view_with_offset(self, stock_code: str, day_offset: int = 0, target_trade_date: str = ""):
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return
        last_code = str(self._intraday_request_code or "").strip().zfill(6)
        if last_code and last_code != code:
            self._intraday_available_dates = []
            self._intraday_selected_date = ""

        try:
            requested_offset = int(day_offset)
        except (TypeError, ValueError):
            requested_offset = 0
        if requested_offset > 0:
            requested_offset = 0
        if self._intraday_available_dates:
            requested_offset = max(requested_offset, -(len(self._intraday_available_dates) - 1))

        self._intraday_request_code = code
        self._intraday_request_offset = requested_offset
        self._intraday_day_offset = requested_offset
        self.intraday_title_var.set(f"分时图 - {code}")
        self._set_top_header_for_code(code)
        self.intraday_day_var.set("交易日: 加载中...")
        self._refresh_intraday_nav_buttons()
        self._draw_intraday_loading(f"正在加载 {code} 分时...")
        self.notebook.select(self.intraday_tab)

        normalized_target_date = str(target_trade_date or "").strip()
        self._intraday_request_target_date = normalized_target_date

        # ---- GUI 层 LRU 缓存命中：历史日期永久有效，实时请求 30s TTL ----
        cache_key = (code, requested_offset, normalized_target_date)
        cached = self._intraday_payload_cache.get(cache_key)
        if cached is not None:
            ts, payload, is_live = cached
            age = time.time() - ts
            fresh = (not is_live) or (age < self._INTRADAY_LIVE_TTL_SEC)
            if fresh:
                self._intraday_payload_cache.move_to_end(cache_key)
                try:
                    self._apply_intraday_if_current(
                        code, requested_offset, normalized_target_date, payload,
                    )
                    return
                except Exception as e:
                    self._log(f"渲染内存缓存分时失败，回退到完整加载: {e}")

        if (
            self._intraday_loading_code == code
            and self._intraday_loading_offset == requested_offset
            and self._intraday_loading_target_date == normalized_target_date
        ):
            return
        self._intraday_loading_code = code
        self._intraday_loading_offset = requested_offset
        self._intraday_loading_target_date = normalized_target_date
        self._start_background_job(
            self._load_intraday,
            name=f"intraday-{code}",
            args=(code, requested_offset, normalized_target_date),
        )

    def _load_intraday(self, stock_code: str, day_offset: int, target_trade_date: str, cancel_token: CancelToken):
        try:
            if cancel_token.is_cancelled():
                return
            payload = self.stock_filter.get_stock_intraday(
                stock_code,
                day_offset=day_offset,
                target_trade_date=target_trade_date,
            )
            if cancel_token.is_cancelled():
                return
            code = str(stock_code).strip().zfill(6)
            normalized_target = str(target_trade_date or "").strip()
            # 历史日（非"最新"）数据不变，可永久缓存；day_offset=0 且无指定日才算 live
            is_live = (int(day_offset) == 0 and not normalized_target)
            cache_key = (code, int(day_offset), normalized_target)

            def _apply():
                # UI 线程写缓存 + 渲染（避免 OrderedDict 跨线程争用）
                try:
                    self._intraday_payload_cache[cache_key] = (time.time(), payload, is_live)
                    self._intraday_payload_cache.move_to_end(cache_key)
                    while len(self._intraday_payload_cache) > self._INTRADAY_CACHE_MAX:
                        self._intraday_payload_cache.popitem(last=False)
                except Exception:
                    pass
                self._apply_intraday_if_current(stock_code, day_offset, target_trade_date, payload)

            self._post_to_ui(_apply)
        except Exception as e:
            self._post_to_ui(lambda: self._draw_intraday_error(stock_code, f"分时加载失败: {e}"))
            self._post_to_ui(lambda: self._log(f"分时加载失败 {stock_code}: {e}"))
        finally:
            self._post_to_ui(lambda: self._finish_intraday_status(stock_code, day_offset))

    def _apply_intraday_if_current(self, stock_code: str, day_offset: int, target_trade_date: str, payload: Dict[str, Any]) -> None:
        code = str(stock_code).strip().zfill(6)
        if (
            code != self._intraday_request_code
            or int(day_offset) != int(self._intraday_request_offset)
            or str(target_trade_date or "").strip() != self._intraday_request_target_date
        ):
            return
        intraday_df = payload.get("intraday")
        prev_close = payload.get("prev_close")
        auction_snapshot = payload.get("auction")
        selected_trade_date = str(payload.get("selected_trade_date") or "")
        available_trade_dates = [str(d) for d in (payload.get("available_trade_dates") or [])]
        try:
            applied_day_offset = int(payload.get("applied_day_offset") or 0)
        except (TypeError, ValueError):
            applied_day_offset = int(day_offset)
        self._intraday_available_dates = available_trade_dates
        self._intraday_day_offset = applied_day_offset
        self._intraday_request_offset = applied_day_offset
        self._intraday_selected_date = selected_trade_date
        self.intraday_day_var.set(f"交易日: {selected_trade_date or '-'}")
        # 调试日志：检查分时数据内容和竞价点
        try:
            if intraday_df is not None and not intraday_df.empty:
                times = pd.to_datetime(intraday_df["time"], errors="coerce")
                time_strs = [t.strftime("%H:%M") for t in times if not pd.isna(t)]
                first_label = time_strs[0] if time_strs else "-"
                last_label = time_strs[-1] if time_strs else "-"
                has_auction = isinstance(auction_snapshot, dict) and auction_snapshot.get("price") is not None
                self._log(
                    f"【分时调试】{code} 数据共 {len(intraday_df)} 行，区间 {first_label}~{last_label}，竞价标记: {'是' if has_auction else '否'}"
                )
                if has_auction:
                    self._log(
                        f"   竞价数据: 时间={auction_snapshot.get('time')}, 价格={auction_snapshot.get('price')}, 成交量={auction_snapshot.get('volume')}"
                    )
            else:
                self._log(f"【分时调试】{code} 分时数据为空")
        except Exception as e:
            self._log(f"【分时调试】记录日志出错: {e}")

        self._refresh_intraday_nav_buttons()
        self._draw_intraday_chart(code, intraday_df, prev_close=prev_close, auction_snapshot=auction_snapshot)

    def _finish_intraday_status(self, stock_code: str, day_offset: int) -> None:
        code = str(stock_code).strip().zfill(6)
        if code == self._intraday_loading_code and int(day_offset) == int(self._intraday_loading_offset):
            self._intraday_loading_code = ""
            self._intraday_loading_offset = 0
            self._intraday_loading_target_date = ""

    def _draw_intraday_loading(self, message: str):
        self.intraday_price_ax.clear()
        self.intraday_volume_ax.clear()
        self.intraday_dist_ax.clear()
        self.intraday_price_ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=13)
        self.intraday_volume_ax.text(0.5, 0.5, "请稍候", ha="center", va="center", fontsize=11)
        self.intraday_dist_ax.text(0.5, 0.5, "等待分时数据", ha="center", va="center", fontsize=11)
        self.intraday_price_ax.set_axis_off()
        self.intraday_volume_ax.set_axis_off()
        self.intraday_dist_ax.set_axis_off()
        self.intraday_canvas.draw()
    def _draw_intraday_error(self, stock_code: str, message: str):
        code = str(stock_code).strip().zfill(6)
        self.intraday_title_var.set(f"分时图 - {code}")
        self._set_top_header_for_code(code)
        if not self._intraday_selected_date:
            self.intraday_day_var.set("交易日: -")
        self.intraday_price_ax.clear()
        self.intraday_volume_ax.clear()
        self.intraday_dist_ax.clear()
        self.intraday_price_ax.text(0.5, 0.5, "分时数据加载失败", ha="center", va="center", fontsize=13, color="#b22222")
        self.intraday_volume_ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=10)
        self.intraday_dist_ax.text(0.5, 0.5, "无分布数据", ha="center", va="center", fontsize=10)
        self.intraday_price_ax.set_axis_off()
        self.intraday_volume_ax.set_axis_off()
        self.intraday_dist_ax.set_axis_off()
        self._refresh_intraday_nav_buttons()
        self.intraday_canvas.draw()

    def _resolve_intraday_base_price(self, close_series, prev_close: Optional[float]) -> float:
        if prev_close is not None and pd.notna(prev_close) and float(prev_close) > 0:
            return float(prev_close)
        first_close = pd.to_numeric(close_series, errors="coerce").dropna()
        if not first_close.empty:
            return max(float(first_close.iloc[0]), 1.0)
        return 1.0

    def _resolve_intraday_average_price(self, df, close_series, volume_series):
        avg_price = pd.to_numeric(df.get("avg_price"), errors="coerce")
        if not avg_price.isna().all():
            return avg_price
        cumulative_volume = volume_series.cumsum()
        if (cumulative_volume > 0).any():
            weighted_amount = (close_series.ffill().fillna(0) * volume_series).cumsum()
            return pd.to_numeric(weighted_amount / cumulative_volume.replace(0, pd.NA), errors="coerce")
        return close_series.expanding(min_periods=1).mean()

    def _normalize_intraday_auction_snapshot(self, auction_snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        context = {
            "time_label": "",
            "price": None,
            "amount": None,
            "volume": None,
            "has_auction": False,
            "x": None,
        }
        if not isinstance(auction_snapshot, dict):
            return context

        auction_time = pd.to_datetime(auction_snapshot.get("time"), errors="coerce")
        if not pd.isna(auction_time):
            context["time_label"] = auction_time.strftime("%H:%M")

        for source_key, target_key in [("price", "price"), ("amount", "amount"), ("volume", "volume")]:
            raw_value = pd.to_numeric(pd.Series([auction_snapshot.get(source_key)]), errors="coerce").iloc[0]
            if pd.notna(raw_value) and float(raw_value) > 0:
                context[target_key] = float(raw_value)

        context["has_auction"] = context["price"] is not None
        if context["has_auction"]:
            context["x"] = -0.6
        return context

    def _build_intraday_tick_positions(self, time_labels: List[str], x: List[int], auction_context: Dict[str, Any]):
        key_times = ["09:30", "10:30", "11:30", "13:00", "14:00", "15:00"]
        tick_map: Dict[int, str] = {}
        for idx, text in enumerate(time_labels):
            if text in key_times and idx not in tick_map:
                tick_map[idx] = text

        if x:
            if not auction_context["has_auction"]:
                tick_map[0] = time_labels[0]
            tick_map[len(x) - 1] = time_labels[-1]

        raw_tick_positions = sorted(tick_map.keys())
        tick_positions: List[Any] = []
        min_tick_gap = max(12, len(x) // 10) if x else 12
        for pos in raw_tick_positions:
            if not tick_positions or pos - tick_positions[-1] >= min_tick_gap or pos == len(x) - 1:
                tick_positions.append(pos)
        tick_labels = [tick_map[pos] for pos in tick_positions]

        if len(tick_positions) < 5 and x:
            tick_step = max(1, len(x) // 6)
            tick_positions = x[::tick_step]
            if tick_positions[-1] != x[-1]:
                tick_positions.append(x[-1])
            tick_labels = [time_labels[pos] for pos in tick_positions]

        if auction_context["has_auction"] and auction_context["x"] is not None:
            tick_positions = [auction_context["x"]] + tick_positions
            tick_labels = [auction_context["time_label"] or "09:25"] + tick_labels
            if len(tick_positions) >= 2 and tick_positions[1] - tick_positions[0] < 12:
                tick_positions.pop(1)
                tick_labels.pop(1)
        return tick_positions, tick_labels

    def _draw_intraday_price_panel(
        self,
        x: List[int],
        pct_close,
        pct_avg,
        pct_ma5,
        base_price: float,
        first_close,
        auction_context: Dict[str, Any],
    ) -> None:
        self.intraday_price_ax.plot(x, pct_close, color="#2f6fd6", linewidth=1.4, label="分时")
        self.intraday_price_ax.plot(x, pct_avg, color="#f08a24", linewidth=1.3, label="均价线")
        self.intraday_price_ax.plot(x, pct_ma5, color="#7b52ab", linewidth=1.0, linestyle="--", alpha=0.85, label="MA5")
        self.intraday_price_ax.axhline(0.0, color="#888888", linewidth=0.9, linestyle="--", alpha=0.85, label="昨收")
        self.intraday_price_ax.grid(True, alpha=0.25)
        self.intraday_price_ax.set_ylabel("涨\n跌\n幅\n(%)", rotation=0, labelpad=14, va="center")
        self.intraday_price_ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:.1f}%"))

        valid_pct_parts = [pct_close, pct_avg, pct_ma5]
        if auction_context["has_auction"] and auction_context["price"] is not None:
            valid_pct_parts.append(pd.Series([(auction_context["price"] / base_price - 1.0) * 100.0]))
        valid_pct = pd.concat(valid_pct_parts, ignore_index=True)
        valid_pct = pd.to_numeric(valid_pct, errors="coerce").dropna()
        if valid_pct.empty or first_close.empty:
            self.intraday_price_ax.set_ylim(-2.0, 2.0)
        else:
            # 同花顺风格：围绕 0%（昨收）对称，上下等幅
            max_abs = float(valid_pct.abs().max())
            pad = max(max_abs * 0.08, 0.1)
            m = min(max_abs + pad, 35.0)
            if m < 1.0:
                m = 1.0
            self.intraday_price_ax.set_ylim(-m, m)

        secax = self.intraday_price_ax.secondary_yaxis(
            "right",
            functions=(
                lambda y: base_price * (1.0 + y / 100.0),
                lambda p: (p / base_price - 1.0) * 100.0,
            ),
        )
        secax.set_ylabel("价\n格\n(元)", rotation=0, labelpad=12, va="center")
        secax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:.2f}"))

        self.intraday_price_ax.set_title("分时走势（含竞价标记）" if auction_context["has_auction"] else "分时走势")
        self.intraday_price_ax.legend(loc="upper right", fontsize=8, framealpha=0.85)

        if auction_context["has_auction"] and auction_context["x"] is not None and auction_context["price"] is not None:
            q_pct = (auction_context["price"] / base_price - 1.0) * 100.0
            self.intraday_price_ax.axvline(auction_context["x"] + 0.1, color="#888888", linewidth=0.9, alpha=0.7, linestyle=":")
            self.intraday_price_ax.scatter([auction_context["x"]], [q_pct], s=26, color="#555555", zorder=5, label="_nolegend_")
            first_intraday_pct = pd.to_numeric(pd.Series([pct_close.iloc[0]]), errors="coerce").iloc[0] if len(pct_close) else None
            if first_intraday_pct is not None and pd.notna(first_intraday_pct):
                self.intraday_price_ax.plot(
                    [auction_context["x"], 0],
                    [q_pct, float(first_intraday_pct)],
                    color="#777777",
                    linewidth=0.9,
                    linestyle=":",
                )
            self.intraday_price_ax.text(
                auction_context["x"],
                self.intraday_price_ax.get_ylim()[1],
                "竞价",
                ha="center",
                va="bottom",
                fontsize=9,
                color="#666666",
            )

        auction_info_text = "竞价: 无可靠数据"
        if auction_context["has_auction"] and auction_context["price"] is not None:
            parts = [auction_context["time_label"] or "09:25", f"{auction_context['price']:.2f}"]
            if auction_context["volume"] is not None:
                parts.append(f"量 {self._format_volume(auction_context['volume'])}")
            elif auction_context["amount"] is not None:
                parts.append(f"额 {self._format_amount(auction_context['amount'])}")
            auction_info_text = "竞价: " + " / ".join(parts)
        self.intraday_price_ax.text(
            0.995,
            0.98,
            auction_info_text,
            transform=self.intraday_price_ax.transAxes,
            ha="right",
            va="top",
            fontsize=8,
            color="#555555",
            bbox=dict(boxstyle="round,pad=0.2", fc="#f2f2f2", ec="#c9c9c9", alpha=0.9),
        )

    def _draw_intraday_volume_panel(
        self,
        x: List[int],
        open_series,
        close_series,
        volume_series,
        tick_positions,
        tick_labels,
        auction_context: Dict[str, Any],
    ) -> None:
        colors = [
            "#d94b4b" if (not pd.isna(c) and not pd.isna(o) and c >= o) else "#1f8b4c"
            for o, c in zip(open_series, close_series)
        ]
        self.intraday_volume_ax.bar(x, volume_series, width=0.65, color=colors, alpha=0.85)
        self.intraday_volume_ax.grid(True, alpha=0.2)
        self.intraday_volume_ax.set_ylabel("成\n交\n量", rotation=0, labelpad=10, va="center")
        self.intraday_volume_ax.set_xlabel("时间")
        if auction_context["has_auction"] and auction_context["x"] is not None:
            self.intraday_volume_ax.axvline(auction_context["x"] + 0.1, color="#888888", linewidth=0.9, alpha=0.65, linestyle=":")

        self.intraday_price_ax.set_xticks(tick_positions)
        self.intraday_price_ax.set_xticklabels([])
        self.intraday_price_ax.tick_params(axis="x", which="both", length=0)

        self.intraday_volume_ax.set_xticks(tick_positions)
        self.intraday_volume_ax.set_xticklabels(tick_labels, rotation=45, ha="right", fontsize=8)
        if x:
            left_limit = -1.0 if auction_context["has_auction"] else -0.5
            right_limit = x[-1] + 0.5
            self.intraday_price_ax.set_xlim(left_limit, right_limit)
            self.intraday_volume_ax.set_xlim(left_limit, right_limit)

    def _draw_intraday_distribution_panel(self, close_series, volume_series) -> None:
        dist_df = pd.DataFrame({"price": close_series, "volume": volume_series}).dropna(subset=["price"])
        dist_df["volume"] = pd.to_numeric(dist_df["volume"], errors="coerce").fillna(0)
        dist_df = dist_df[dist_df["volume"] > 0]

        if dist_df.empty:
            self.intraday_dist_ax.text(0.5, 0.5, "暂无成交分布数据", ha="center", va="center", fontsize=11)
            self.intraday_dist_ax.set_axis_off()
            return

        dist_df["price"] = dist_df["price"].round(2)
        grouped = dist_df.groupby("price", as_index=False)["volume"].sum()
        total_volume = float(grouped["volume"].sum())
        if total_volume <= 0:
            self.intraday_dist_ax.text(0.5, 0.5, "暂无成交分布数据", ha="center", va="center", fontsize=11)
            self.intraday_dist_ax.set_axis_off()
            return

        grouped["ratio"] = grouped["volume"] / total_volume
        grouped = grouped.sort_values("ratio", ascending=False).reset_index(drop=True).head(min(12, len(grouped)))
        y = list(range(len(grouped)))
        ratios_pct = (grouped["ratio"] * 100.0).tolist()
        labels = [f"{p:.2f}" for p in grouped["price"].tolist()]
        self.intraday_dist_ax.barh(y, ratios_pct, color="#5b7bd5", alpha=0.9)
        self.intraday_dist_ax.set_yticks(y)
        self.intraday_dist_ax.set_yticklabels(labels, fontsize=9)
        self.intraday_dist_ax.invert_yaxis()
        self.intraday_dist_ax.yaxis.tick_right()
        self.intraday_dist_ax.tick_params(axis="y", labelright=True, labelleft=False, pad=4)
        self.intraday_dist_ax.set_xlabel("占比(%)")
        self.intraday_dist_ax.set_ylabel("价\n位\n(元)", rotation=0, labelpad=14, va="center")
        self.intraday_dist_ax.yaxis.set_label_position("right")
        self.intraday_dist_ax.set_title("成交价格分布")
        self.intraday_dist_ax.grid(True, axis="x", alpha=0.2)

        for yi, value in zip(y, ratios_pct):
            text_x = max(value - 0.35, 0.12)
            text_color = "white" if value >= 3.0 else "#222222"
            self.intraday_dist_ax.text(
                text_x,
                yi,
                f"{value:.2f}%",
                va="center",
                ha="right",
                fontsize=8,
                color=text_color,
            )

    def _draw_intraday_chart(
        self,
        stock_code: str,
        intraday_df,
        prev_close: Optional[float] = None,
        auction_snapshot: Optional[Dict[str, Any]] = None,
    ):
        code = str(stock_code).strip().zfill(6)
        self.intraday_title_var.set(f"分时图 - {code}")
        self._set_top_header_for_code(code)
        self.intraday_price_ax.clear()
        self.intraday_volume_ax.clear()
        self.intraday_dist_ax.clear()
        self.intraday_price_ax.set_axis_on()
        self.intraday_volume_ax.set_axis_on()
        self.intraday_dist_ax.set_axis_on()

        if intraday_df is None or getattr(intraday_df, "empty", True):
            self._draw_intraday_error(code, "暂无分时数据")
            return

        df = intraday_df.copy().reset_index(drop=True)
        close_series = pd.to_numeric(df.get("close"), errors="coerce")
        open_series = pd.to_numeric(df.get("open"), errors="coerce")
        volume_series = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0)
        times = pd.to_datetime(df.get("time"), errors="coerce")

        if close_series.isna().all() or times.isna().all():
            self._draw_intraday_error(code, "分时数据无有效价格")
            return

        base_price = self._resolve_intraday_base_price(close_series, prev_close)
        first_close = close_series.dropna()
        avg_price = self._resolve_intraday_average_price(df, close_series, volume_series)
        ma5 = close_series.rolling(window=5, min_periods=1).mean()
        auction_context = self._normalize_intraday_auction_snapshot(auction_snapshot)
        x = list(range(len(df)))
        pct_close = (close_series / base_price - 1.0) * 100.0
        pct_avg = (avg_price / base_price - 1.0) * 100.0
        pct_ma5 = (ma5 / base_price - 1.0) * 100.0
        time_labels = [t.strftime("%H:%M") if not pd.isna(t) else "" for t in times]
        self._draw_intraday_price_panel(
            x,
            pct_close,
            pct_avg,
            pct_ma5,
            base_price,
            first_close,
            auction_context,
        )
        tick_positions, tick_labels = self._build_intraday_tick_positions(time_labels, x, auction_context)
        self._draw_intraday_volume_panel(
            x,
            open_series,
            close_series,
            volume_series,
            tick_positions,
            tick_labels,
            auction_context,
        )
        self._draw_intraday_distribution_panel(close_series, volume_series)
        self.intraday_fig.tight_layout(rect=[0.02, 0.06, 0.98, 0.98], h_pad=1.2, w_pad=0.8)
        self.intraday_canvas.draw()
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
            self._log(f"结果已导出到 {file_path}")
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
            row_values = self._format_result_row_values(result)
            rows.append([str(row_values.get(col, "-")) for col, _ in export_columns])

        if not rows:
            messagebox.showwarning("警告", "没有可导出的结果")
            return

        output_path = Path(file_path)
        headings = [heading for _, heading in export_columns]
        column_widths = [max(self.result_headings.get(col, ("", 100))[1], 80) for col, _ in export_columns]
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
            self._log(f"结果图片已导出到 {output_path}")
        except Exception as e:
            messagebox.showerror("错误", f"导出图片失败: {e}")

    def copy_selected_stock_code_name(self, event=None):
        selection = self._get_selected_result_identity()
        if selection is None:
            messagebox.showwarning("提示", "请先在结果表中选中一只股票")
            return "break" if event is not None else None

        stock_code, _ = selection
        payload = stock_code

        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(payload)
            self.root.update_idletasks()
            self.status_var.set(f"已复制: {payload}")
            self._log(f"已复制股票代码: {payload}")
        except tk.TclError as e:
            messagebox.showerror("错误", f"复制失败: {e}")

        return "break" if event is not None else None

    def show_settings(self):
        settings_window = tk.Toplevel(self.root)
        settings_window.title("扫描参数")
        settings_window.geometry("600x700")
        settings_window.transient(self.root)
        settings_window.grab_set()

        frame = ttk.Frame(settings_window, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="扫描数量(0=全量):").grid(row=0, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.scan_count_var, width=15).grid(row=0, column=1, pady=8)

        ttk.Label(frame, text="并发线程:").grid(row=1, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.scan_workers_var, width=15).grid(row=1, column=1, pady=8)

        ttk.Label(frame, text="连续天数:").grid(row=2, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.trend_days_var, width=15).grid(row=2, column=1, pady=8)

        ttk.Label(frame, text="MA周期:").grid(row=3, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.ma_period_var, width=15).grid(row=3, column=1, pady=8)

        ttk.Label(frame, text="近N日涨停:").grid(row=4, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.limit_up_lookback_var, width=15).grid(row=4, column=1, pady=8)

        ttk.Label(frame, text="放量观察天数:").grid(row=5, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.volume_lookback_var, width=15).grid(row=5, column=1, pady=8)

        ttk.Checkbutton(frame, text="启用放量倍数", variable=self.volume_expand_enabled_var).grid(
            row=6, column=0, columnspan=2, pady=8
        )

        ttk.Label(frame, text="放量倍数阈值:").grid(row=7, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.volume_expand_factor_var, width=15).grid(row=7, column=1, pady=8)

        ttk.Label(frame, text="备注：放量倍数=最近N天最大成交量/最小成交量").grid(
            row=8, column=0, columnspan=2, pady=8
        )

        ttk.Checkbutton(frame, text="仅显示近N日内有涨停", variable=self.require_limit_up_var).grid(
            row=9, column=0, columnspan=2, pady=8
        )

        ttk.Checkbutton(frame, text="重新拉取股票池", variable=self.refresh_universe_var).grid(
            row=10, column=0, columnspan=2, pady=8
        )

        ttk.Label(frame, text="历史数据源:").grid(row=11, column=0, sticky=tk.E, pady=8)
        ttk.Combobox(
            frame,
            textvariable=self.history_source_var,
            width=15,
            state="readonly",
            values=DATA_SOURCE_OPTIONS["history"],
        ).grid(row=11, column=1, pady=8)

        ttk.Label(frame, text="分时数据源:").grid(row=12, column=0, sticky=tk.E, pady=8)
        ttk.Combobox(
            frame,
            textvariable=self.intraday_source_var,
            width=15,
            state="readonly",
            values=DATA_SOURCE_OPTIONS["intraday"],
        ).grid(row=12, column=1, pady=8)

        ttk.Label(frame, text="资金流数据源:").grid(row=13, column=0, sticky=tk.E, pady=8)
        ttk.Combobox(
            frame,
            textvariable=self.fund_flow_source_var,
            width=15,
            state="readonly",
            values=DATA_SOURCE_OPTIONS["fund_flow"],
        ).grid(row=13, column=1, pady=8)

        ttk.Label(frame, text="涨停原因源:").grid(row=14, column=0, sticky=tk.E, pady=8)
        reason_box = ttk.Frame(frame)
        reason_box.grid(row=14, column=1, pady=8, sticky=tk.W)
        ttk.Combobox(
            reason_box,
            textvariable=self.limit_up_reason_source_var,
            width=12,
            state="readonly",
            values=DATA_SOURCE_OPTIONS["limit_up_reason"],
        ).pack(side=tk.LEFT)
        ttk.Button(
            reason_box, text="刷新概念库",
            command=self._refresh_concept_index_dialog,
        ).pack(side=tk.LEFT, padx=(6, 0))

        # ==== 承接强势形态 ====
        ttk.Separator(frame, orient="horizontal").grid(
            row=15, column=0, columnspan=2, sticky="ew", pady=(12, 4)
        )
        ttk.Label(
            frame,
            text="承接强势：涨停 → 次日回落但缩量、且后续不破位",
            foreground="#666",
        ).grid(row=16, column=0, columnspan=2, pady=(0, 4))
        ttk.Checkbutton(
            frame,
            text="启用承接强势过滤",
            variable=self.strong_ft_enabled_var,
        ).grid(row=17, column=0, columnspan=2, pady=4)

        ttk.Label(frame, text="最大回撤%(次日最低 vs 涨停收盘):").grid(
            row=18, column=0, sticky=tk.E, pady=4
        )
        ttk.Entry(frame, textvariable=self.strong_ft_max_pullback_pct_var, width=15).grid(
            row=18, column=1, pady=4
        )
        ttk.Label(frame, text="次日量能上限(占涨停日):").grid(
            row=19, column=0, sticky=tk.E, pady=4
        )
        ttk.Entry(frame, textvariable=self.strong_ft_max_volume_ratio_var, width=15).grid(
            row=19, column=1, pady=4
        )
        ttk.Label(frame, text="至少站稳天数(0=允许次日就是今天):").grid(
            row=20, column=0, sticky=tk.E, pady=4
        )
        ttk.Entry(frame, textvariable=self.strong_ft_min_hold_days_var, width=15).grid(
            row=20, column=1, pady=4
        )

        ttk.Button(
            frame,
            text="保存",
            command=lambda: (self._save_app_settings(), self._apply_source_preferences(), settings_window.destroy()),
        ).grid(
            row=21, column=0, columnspan=2, pady=18
        )

    def show_about(self):
        messagebox.showinfo(
            "关于",
            "A股筛选\n\n"
            "功能:\n"
            "- 只使用历史日线数据\n"
            "- 筛选最近N日收盘全部高于MA\n"
            "- 可过滤近N日内出现过涨停的股票\n"
            "- 结果和历史数据都会保存到 data/stock_store.sqlite3\n",
        )

    def on_clear_universe_data(self):
        clear_universe_data()
        self._log("已清空股票池和结果快照。")

    def on_clear_history_data(self):
        clear_history_data()
        self._log("已清空历史数据。")

    # ============== 概念库刷新 ==============
    def _refresh_concept_index_dialog(self) -> None:
        """点击"刷新概念库"按钮：弹确认 → 后台拉取东财+同花顺概念板块，
        写入 stock_concept_tags 反查表。耗时 10-15 分钟，可取消。
        """
        from src.sources import concept_index
        stats = stock_store.concept_tags_stats()
        msg_lines = [
            "拉取东财 + 同花顺所有概念板块的成份股，",
            "建立股票→概念反查表，让涨停原因显示更细的题材标签。",
            "",
            f"当前已有：{stats.get('pairs_total', 0)} 对 (覆盖 {stats.get('codes_total', 0)} 只),",
            f"东财 {stats.get('em_pairs', 0)}，同花顺 {stats.get('ths_pairs', 0)}",
        ]
        last = stats.get("latest_updated_at") or ""
        if last:
            msg_lines.append(f"最近更新：{last}")
        msg_lines += [
            "",
            "本次刷新预计 10-15 分钟，后台执行，期间可正常使用。",
            "是否继续？",
        ]
        ok = messagebox.askyesno("刷新概念库", "\n".join(msg_lines), parent=self.root)
        if not ok:
            return

        # 后台执行
        thread, token = self._start_background_job(
            self._run_concept_index_refresh,
            name="concept-index-refresh",
            args=(),
        )
        self._concept_index_thread = thread
        self._concept_index_token = token

    def _run_concept_index_refresh(self, cancel_token: "CancelToken") -> None:
        from src.sources import concept_index

        def _progress(done: int, total: int, label: str) -> None:
            msg = f"刷新概念库 {done}/{total} · {label[:30]}"
            self._post_to_ui(lambda m=msg: self.status_var.set(m))

        try:
            self._post_to_ui(lambda: self.status_var.set("刷新概念库启动..."))
            result = concept_index.build_concept_reverse_index(
                sources=("em", "ths"),
                cancel_check=lambda: cancel_token.is_cancelled(),
                progress_cb=_progress,
            )
            if result.get("cancelled"):
                self._post_to_ui(lambda: self.status_var.set("概念库刷新已取消"))
                return
            summary = (
                f"概念库刷新完成 · 东财 {result.get('em_pairs', 0)} 对/"
                f"同花顺 {result.get('ths_pairs', 0)} 对，"
                f"覆盖 {result.get('total_codes', 0)} 只，"
                f"耗时 {result.get('duration_seconds', 0):.0f}s"
            )
            self._post_to_ui(lambda s=summary: self.status_var.set(s))
            self._post_to_ui(lambda s=summary: self._log(s))
        except Exception as exc:
            err = str(exc)
            self._post_to_ui(lambda e=err: self.status_var.set(f"概念库刷新失败: {e}"))
            self._post_to_ui(lambda e=err: self._log(f"概念库刷新失败: {e}"))

    # ================= 网络异常醒目提示 =================

    def _show_network_error_alert(self, error_text: str) -> None:
        """扫描/缓存更新失败时弹出醒目提示。"""
        if self._is_closing:
            return
        network_keywords = ("连接", "超时", "timeout", "connection", "refused", "reset", "网络", "ssl", "proxy", "limited")
        is_network = any(kw in error_text.lower() for kw in network_keywords)
        if is_network:
            messagebox.showerror(
                "网络异常",
                f"操作失败，疑似网络问题：\n\n{error_text[:300]}\n\n"
                "建议：\n"
                "1. 检查网络连接\n"
                "2. 尝试切换数据源（设置 -> 扫描参数）\n"
                "3. 稍后重试",
            )
        else:
            messagebox.showerror("操作失败", error_text[:500])

    # ================= 自选股导入/导出 =================

    def _export_watchlist_csv(self) -> None:
        file_path = filedialog.asksaveasfilename(
            title="导出自选股",
            defaultextension=".csv",
            filetypes=[("CSV 文件", "*.csv")],
            initialfile="watchlist.csv",
        )
        if not file_path:
            return
        try:
            count = export_watchlist_csv(file_path)
            if count > 0:
                messagebox.showinfo("成功", f"已导出 {count} 只自选股到\n{file_path}")
            else:
                messagebox.showwarning("提示", "自选股列表为空，无内容可导出")
        except Exception as exc:
            messagebox.showerror("导出失败", str(exc))

    def _import_watchlist_csv(self) -> None:
        file_path = filedialog.askopenfilename(
            title="导入自选股",
            filetypes=[("CSV 文件", "*.csv")],
        )
        if not file_path:
            return
        try:
            count = import_watchlist_csv(file_path)
            messagebox.showinfo("成功", f"已导入 {count} 只自选股")
            self._load_watchlist_items()
        except Exception as exc:
            messagebox.showerror("导入失败", str(exc))

    # ================= 数据清理 =================

    def _on_cleanup_data(self) -> None:
        result = cleanup_all()
        total = sum(result.values())
        detail = (
            f"历史数据：删除 {result.get('history', 0)} 条\n"
            f"分时缓存：删除 {result.get('intraday', 0)} 条\n"
            f"扫描快照：删除 {result.get('scan_snapshots', 0)} 条"
        )
        messagebox.showinfo("清理完成", f"共清理 {total} 条过期数据。\n\n{detail}")
        self._log(f"数据清理完成：{result}")

    # ================= 数据库备份/恢复 =================

    def _on_backup_database(self) -> None:
        try:
            path = backup_database()
            messagebox.showinfo("备份成功", f"数据库已备份到:\n{path}")
            self._log(f"数据库备份完成：{path}")
        except Exception as exc:
            messagebox.showerror("备份失败", str(exc))

    def _on_restore_database(self) -> None:
        if self.is_scanning or self.is_updating_cache:
            messagebox.showwarning(
                "恢复前请先停止",
                "检测到扫描或缓存更新仍在进行。请先点“停止”并等待任务结束后再执行恢复。",
            )
            return
        file_path = filedialog.askopenfilename(
            title="选择备份文件",
            filetypes=[("SQLite 数据库", "*.sqlite3")],
            initialdir=str(Path("data") / "backups"),
        )
        if not file_path:
            return
        confirm = messagebox.askyesno(
            "确认恢复",
            f"将从以下文件恢复数据库:\n{file_path}\n\n当前数据库会先自动备份。是否继续？",
        )
        if not confirm:
            return

        from src.services.db_admin_service import SafeRestoreOrchestrator
        orchestrator = SafeRestoreOrchestrator(
            broadcast_cancel=lambda: self._cancel_all_background("database_restore"),
            thread_sources=lambda: (self._scan_thread, self._cache_thread),
            wait_timeout_sec=5.0,
        )
        ok = orchestrator.execute(file_path)
        if ok:
            self.all_scan_results = []
            self.filtered_stocks = []
            messagebox.showinfo("恢复成功", "数据库已恢复。建议重启应用以确保数据一致。")
            self._log(f"数据库恢复完成：{file_path}")
        else:
            messagebox.showerror("恢复失败", "恢复过程出错，请检查日志。")

    def on_close(self):
        self._ui.mark_closing()
        # 广播取消到所有后台任务，让它们尽快退出
        self._cancel_all_background("window_close")
        self._detail_request_code = ""
        self._detail_loading_code = ""
        self._intraday_request_code = ""
        self._intraday_loading_code = ""
        self._cancel_scheduled_detail()
        self._close_run_log()
        for t in (self._scan_thread, self._cache_thread):
            if t is not None and t.is_alive():
                t.join(timeout=3.0)
        try:
            self.root.quit()
            self.root.destroy()
        except Exception:
            pass


def run_app():
    root = tk.Tk()
    app = StockMonitorApp(root)
    root.mainloop()


if __name__ == "__main__":
    run_app()
