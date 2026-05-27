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
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from scan_models import FilterSettings, ScanRequest
from data_source_models import DATA_SOURCE_OPTIONS
from src.gui.log_drainer import LogDrainer
from src.gui.tree_enhancer import attach_enhancers_recursively as _attach_tree_enhancers
from src.gui.ui_dispatch import UIDispatcher
from src.gui.tabs.log import LogTab
from src.gui.tabs.intraday import IntradayTab
from src.gui.tabs.detail import DetailTab
from src.gui.tabs.predict import PredictTab
from src.gui.tabs.result import ResultTab
from src.utils.cancel_token import CancelToken, CancelTokenRegistry
import stock_store
from stock_filter import StockFilter
from stock_data import clear_history_data, clear_universe_data
from stock_store import (
    backup_database,
    cleanup_all,
    ensure_store_ready,
    load_app_config,
    load_scan_snapshot,
    save_app_config,
)

plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False


class StockMonitorApp:
    # 日志环形缓冲上限：超过后裁掉最旧的，避免长时间运行内存膨胀
    _LOG_BUFFER_MAX = 5000
    # 命中即标记为错误（红色），并保留在「只显示警告/错误」过滤中
    _LOG_ERROR_KEYWORDS = (
        "失败", "错误", "异常", "Traceback", "崩溃", "[ERROR]",
        "Exception", "无法", "拒绝", "超时", "未能",
    )
    # 命中即标记为警告（橙色），并保留在「只显示警告/错误」过滤中
    _LOG_WARN_KEYWORDS = (
        "警告", "[WARN]", "Warning", "重试", "回退", "降级",
        "弃用", "补位",
    )

    def __init__(self, root: tk.Tk):
        ensure_store_ready()
        self.root = root
        self.root.title("A股筛选")
        self.root.minsize(1280, 820)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self._set_initial_window_geometry()

        self.stock_filter = StockFilter()
        self.stock_filter.set_log_callback(self._log_async)
        # 扫描结果 / 过滤后列表 全部迁移到 ResultTab（self.result.all_results / filtered_stocks）
        # 详情 tab 相关状态全部迁移到 DetailTab（self.detail.xxx）
        # 涨停预测 tab 相关状态全部迁移到 PredictTab（self.predict.xxx）
        self.sort_column = "score"
        self.sort_reverse = True
        # 详情/分时 payload 的 GUI 层 LRU 缓存已迁移到各 Tab 内
        # 详情：self.detail.payload_cache / DetailTab._DETAIL_CACHE_MAX / _DETAIL_CACHE_TTL_SEC
        # 分时：self.intraday.payload_cache / IntradayTab._INTRADAY_CACHE_MAX
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
        # 结果表列定义/排序状态全部迁移到 ResultTab（self.result.columns / headings / column_vars / column_order）
        # GUI 设置统一存储在 SQLite app_config 表中（key: result_column_layout / board_filter_layout / app_settings）
        self._main_thread_id = threading.get_ident()
        self._ui = UIDispatcher(self.root)
        # 日志环形缓冲：每条 = (完整行包含时间戳, 级别 "err"/"warn"/"")。
        # 切换「只显示警告/错误」时按缓冲重渲染，不会丢历史。
        self._log_buffer: List[Tuple[str, str]] = []
        self._log_drainer = LogDrainer(
            dispatcher=self._ui,
            main_thread_id=self._main_thread_id,
            sink=self._log,
            poll_interval_ms=100,
        )

        self.setup_ui()
        self._load_app_settings()
        self._apply_source_preferences()
        self.result._load_column_layout()
        self._load_board_filter_layout()
        self.result.apply_display_columns(save=False)
        self.result._load_last_results()
        self.predict._load_last_prediction()
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
        file_menu.add_command(label="导出结果 CSV", command=lambda: self.result.export_results())
        file_menu.add_command(label="导出结果图片", command=lambda: self.result.export_results_image())
        file_menu.add_command(label="复制代码", command=lambda: self.result.copy_selected_code_name(), accelerator="Ctrl+C")
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
        """顶部参数行：只放 更新历史缓存 必需的参数（扫描数量/并发线程/重新拉取股票池）。

        扫描专属参数（连续天数/MA周期/近N日涨停/放量观察天数/启用放量倍数/放量倍数阈值）
        通过 _build_scan_only_params_row 渲染到扫描结果 tab 内。
        """
        row1 = ttk.Frame(control_frame)
        row1.pack(fill=tk.X, pady=5)
        ttk.Label(row1, text="扫描数量(0=全量):").pack(side=tk.LEFT, padx=5)
        self.scan_count_var = tk.StringVar(value="0")
        ttk.Entry(row1, textvariable=self.scan_count_var, width=8).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="并发线程:").pack(side=tk.LEFT, padx=5)
        self.scan_workers_var = tk.StringVar(value="3")
        ttk.Entry(row1, textvariable=self.scan_workers_var, width=6).pack(side=tk.LEFT, padx=5)

        self.refresh_universe_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row1,
            text="重新拉取股票池",
            variable=self.refresh_universe_var,
        ).pack(side=tk.LEFT, padx=15)

        # 扫描专属变量先在这里声明（保证任何路径都能读到），实体控件移到扫描结果 tab。
        self.trend_days_var = tk.StringVar(value="5")
        self.ma_period_var = tk.StringVar(value="5")
        self.limit_up_lookback_var = tk.StringVar(value="5")
        self.volume_lookback_var = tk.StringVar(value="5")
        self.volume_expand_enabled_var = tk.BooleanVar(value=True)
        self.volume_expand_factor_var = tk.StringVar(value="2.0")

        # 承接强势形态相关的变量在这里先声明，实体控件放在"扫描参数"弹窗里（show_settings）。
        self.strong_ft_enabled_var = tk.BooleanVar(value=False)
        self.strong_ft_max_pullback_pct_var = tk.StringVar(value="3.0")
        self.strong_ft_max_volume_ratio_var = tk.StringVar(value="0.7")
        self.strong_ft_min_hold_days_var = tk.StringVar(value="1")

    def _build_control_actions_row(self, control_frame) -> None:
        """顶部动作行：保留涨停预测前置需要的更新缓存 + 停止 + 单股查询 + 历史源。

        '开始扫描' 按钮移到扫描结果 tab 内部（与扫描参数同处一处），
        通过 _build_scan_only_actions_row 渲染。
        """
        row2 = ttk.Frame(control_frame)
        row2.pack(fill=tk.X, pady=5)

        self.update_cache_btn = ttk.Button(row2, text="更新历史缓存", command=self.start_history_cache_update)
        self.update_cache_btn.pack(side=tk.LEFT, padx=5)

        self.backfill_industry_btn = ttk.Button(
            row2, text="补全行业(THS)", command=self.start_industry_backfill,
        )
        self.backfill_industry_btn.pack(side=tk.LEFT, padx=5)

        self.backfill_industry_baostock_btn = ttk.Button(
            row2, text="补全行业(证监会)",
            command=self.start_industry_backfill_baostock,
        )
        self.backfill_industry_baostock_btn.pack(side=tk.LEFT, padx=5)

        self.stop_btn = ttk.Button(row2, text="停止", command=lambda: self.result.stop_scan(), state=tk.DISABLED)
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
                command=lambda: self.result.on_board_filter_changed(),
            ).pack(side=tk.LEFT, padx=8)

        # 扫描专属过滤变量先声明（实体控件移到扫描结果 tab）
        self.require_limit_up_var = tk.BooleanVar(value=False)
        self.ignore_result_snapshot_var = tk.BooleanVar(value=False)

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
        ttk.Button(row4, text="应用价格过滤", command=lambda: self.result.on_price_filter_changed()).pack(side=tk.LEFT, padx=4)
        ttk.Button(row4, text="清空价格过滤", command=lambda: self.result.clear_price_filter()).pack(side=tk.LEFT, padx=4)
        min_price_entry.bind("<Return>", lambda e: self.result.on_price_filter_changed(e))
        max_price_entry.bind("<Return>", lambda e: self.result.on_price_filter_changed(e))

    def _build_control_quick_filter_row(self, control_frame) -> None:
        """结果表客户端快速过滤：对扫描出来的几百条再细筛。"""
        row5 = ttk.Frame(control_frame)
        row5.pack(fill=tk.X, pady=5)

        ttk.Label(row5, text="搜索:").pack(side=tk.LEFT, padx=(5, 2))
        self.search_var = tk.StringVar(value="")
        search_entry = ttk.Entry(row5, textvariable=self.search_var, width=14)
        search_entry.pack(side=tk.LEFT, padx=(0, 10))
        # 输入时即时过滤（debounce 交给 _schedule_quick_filter 的 after）
        self.search_var.trace_add("write", lambda *_: self.result._schedule_quick_filter())

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

        ttk.Button(row5, text="应用", command=lambda: self.result.on_quick_filter_apply()).pack(side=tk.LEFT, padx=4)
        ttk.Button(row5, text="清空全部", command=lambda: self.result.clear_all_filters()).pack(side=tk.LEFT, padx=4)

        row6 = ttk.Frame(control_frame)
        row6.pack(fill=tk.X, pady=2)
        ttk.Label(row6, text="只显示:").pack(side=tk.LEFT, padx=5)

        self.only_limit_up_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row6, text="涨停", variable=self.only_limit_up_var,
            command=lambda: self.result.on_quick_filter_apply(),
        ).pack(side=tk.LEFT, padx=4)

        self.only_broken_limit_up_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row6, text="断板", variable=self.only_broken_limit_up_var,
            command=lambda: self.result.on_quick_filter_apply(),
        ).pack(side=tk.LEFT, padx=4)

        self.only_volume_expand_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row6, text="放量", variable=self.only_volume_expand_var,
            command=lambda: self.result.on_quick_filter_apply(),
        ).pack(side=tk.LEFT, padx=4)

        self.only_strong_ft_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row6, text="承接强势", variable=self.only_strong_ft_var,
            command=lambda: self.result.on_quick_filter_apply(),
        ).pack(side=tk.LEFT, padx=4)

        search_entry.bind("<Return>", lambda e: self.result.on_quick_filter_apply())

    def setup_control_panel(self, parent):
        control_frame = ttk.LabelFrame(parent, text="控制面板", padding="10")
        control_frame.pack(fill=tk.X, pady=5)
        self._build_control_scan_params_row(control_frame)
        self._build_control_actions_row(control_frame)
        self._build_control_board_filter_row(control_frame)
        # 价格过滤为全局生效（涨停预测/扫描结果共用 self.min_price_var / max_price_var）
        self._build_control_price_filter_row(control_frame)
        # 扫描专属面板（开始扫描/扫描参数/扫描过滤/快速过滤）放在 setup_result_tab 内部，
        # 顶部只保留通用的（更新缓存/查询股票/板块/历史源/价格过滤）

    def setup_notebook(self, parent):
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=5)

        self.predict = PredictTab(self, self.notebook)
        self.detail = DetailTab(self, self.notebook)
        self.intraday = IntradayTab(self, self.notebook)
        self.result = ResultTab(self, self.notebook)
        self.log = LogTab(self, self.notebook)

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
        if current is getattr(getattr(self, "detail", None), "frame", None):
            self._set_top_header_for_code(getattr(self.detail, "current_code", "") or "")
        elif current is getattr(getattr(self, "intraday", None), "frame", None):
            self._set_top_header_for_code(
                getattr(self.intraday, "request_code", "")
                or getattr(getattr(self, "detail", None), "current_code", "")
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
            ("predict", self.predict.frame, "涨停预测", False),
            ("detail", self.detail.frame, "股票详情", False),
            ("intraday", self.intraday.frame, "分时", False),
            ("result", self.result.frame, "扫描结果", True),
            ("log", self.log.frame, "运行日志", False),
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
            for col in self.result.column_order:
                label, _ = self.result.headings[col]
                flag = "显示" if self.result.column_vars[col].get() else "隐藏"
                column_listbox.insert(tk.END, f"[{flag}] {label}")
            if keep_selection is None and self.result.column_order:
                keep_selection = 0
            if keep_selection is not None and self.result.column_order:
                keep_selection = max(0, min(keep_selection, len(self.result.column_order) - 1))
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
            if new_index < 0 or new_index >= len(self.result.column_order):
                return
            self.result.column_order[index], self.result.column_order[new_index] = (
                self.result.column_order[new_index],
                self.result.column_order[index],
            )
            self.result.apply_display_columns()
            refresh_column_listbox(new_index)

        def toggle_selected() -> None:
            index = selected_index()
            if index is None:
                return
            col = self.result.column_order[index]
            self.result.column_vars[col].set(not self.result.column_vars[col].get())
            self.result.apply_display_columns()
            refresh_column_listbox(index)

        def show_all() -> None:
            for var in self.result.column_vars.values():
                var.set(True)
            self.result.apply_display_columns()
            refresh_column_listbox(selected_index())

        def show_core() -> None:
            core = {
                "code",
                "name",
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
            for col, var in self.result.column_vars.items():
                var.set(col in core)
            self.result.apply_display_columns()
            refresh_column_listbox(selected_index())

        def reset_columns() -> None:
            self.result.reset_columns()
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

    def _infer_board_from_code(self, code: str) -> str:
        """根据股票代码前缀推断板块归属。被涨停预测的板块筛选共用。"""
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
        level = self._classify_log_level(message)

        self._log_buffer.append((line, level))
        overflow = len(self._log_buffer) - self._LOG_BUFFER_MAX
        if overflow > 0:
            del self._log_buffer[:overflow]

        if self._run_log_file is not None:
            with self._run_log_file.open("a", encoding="utf-8") as f:
                f.write(line)

        if self.log.show_only_errors_var.get() and level not in ("err", "warn"):
            return

        self._append_log_line(line, level)

    def _classify_log_level(self, message: str) -> str:
        for kw in self._LOG_ERROR_KEYWORDS:
            if kw in message:
                return "err"
        for kw in self._LOG_WARN_KEYWORDS:
            if kw in message:
                return "warn"
        return ""

    def _append_log_line(self, line: str, level: str) -> None:
        start = self.log.text.index("end-1c")
        self.log.text.insert(tk.END, line)
        if level:
            end = self.log.text.index("end-1c")
            self.log.text.tag_add(level, start, end)
        self.log.text.see(tk.END)

    def _rerender_log(self) -> None:
        """根据当前「只显示警告/错误」开关重渲染整个日志区域。在主线程调用。"""
        if self._is_closing:
            return
        self.log.text.delete("1.0", tk.END)
        only_errors = self.log.show_only_errors_var.get()
        for line, level in self._log_buffer:
            if only_errors and level not in ("err", "warn"):
                continue
            self._append_log_line(line, level)

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

    def _apply_filter_settings_from_ui(self, show_error: bool = True) -> Optional[FilterSettings]:
        try:
            settings = self.result._build_filter_settings()
        except ValueError as exc:
            if show_error:
                messagebox.showerror("错误", str(exc))
            return None
        self.stock_filter.apply_settings(settings)
        if getattr(self, "detail", None) is not None:
            self.detail.refresh_metric_labels()
        return settings

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

    def start_history_cache_update(self):
        if self.is_scanning or self.is_updating_cache:
            return
        request = self.result._build_scan_request()
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

    def start_industry_backfill(self):
        """从东财一级行业反向遍历成分股，回填 universe.industry 字段。"""
        def _industry_coverage():
            import sqlite3
            from pathlib import Path
            db_path = Path("data/stock_store.sqlite3")
            if not db_path.is_file():
                return (0, 0)
            with sqlite3.connect(str(db_path)) as conn:
                total = conn.execute("SELECT COUNT(*) FROM universe").fetchone()[0]
                with_ind = conn.execute(
                    "SELECT COUNT(*) FROM universe WHERE industry != ''"
                ).fetchone()[0]
            return (total, with_ind)

        if getattr(self, "_industry_backfill_running", False):
            return
        self._industry_backfill_running = True
        self.backfill_industry_btn.config(state=tk.DISABLED)
        self._open_run_log()
        self._log("开始补全 universe.industry（从东财一级行业反向遍历成分股）...")
        self.status_var.set("补全行业信息：拉取行业列表...")

        def _run():
            try:
                # 用户主动点了按钮就清掉之前 EM 熔断状态，给一次完整重试机会；
                # 如果真的还是连挂，熔断器会在 3 次失败后自动重新跳闸。
                from src.utils import em_circuit_breaker as _emcb
                if _emcb.is_open():
                    _emcb.reset()
                    self._log_async("补全行业：检测到东财熔断中，已手动重置（用户主动操作）")

                from src.services.scoring import first_board as _fb

                # 前后对比，方便用户判断是否真的有效果
                before_total, before_with = _industry_coverage()
                self._log_async(
                    f"补全行业：当前 universe {before_total} 只，"
                    f"已有行业 {before_with} 只 ({before_with / max(before_total,1) * 100:.1f}%)"
                )

                def _progress(cur, total, msg):
                    self._post_to_ui(
                        lambda c=cur, t=total, m=msg: self.status_var.set(
                            f"补全行业 {c}/{t} · {m}"
                        )
                    )

                result = _fb.backfill_universe_industries(
                    log_fn=self._log_async, progress_callback=_progress,
                )
                after_total, after_with = _industry_coverage()
                msg = (
                    f"补全行业完成：覆盖 {result.get('industries', 0)} 个 THS 行业，"
                    f"映射 {result.get('mapped_codes', 0)} 只票，"
                    f"DB 写入 {result.get('updated', 0)} 行 · "
                    f"行业覆盖 {before_with} → {after_with} / {after_total} "
                    f"({after_with / max(after_total,1) * 100:.1f}%)"
                )
                errors = result.get("errors") or []
                if errors:
                    msg += f"，{len(errors)} 个行业拉取失败"
                self._log_async(msg)
                if errors:
                    # 单独把失败明细打到日志（前 5 条），便于排查
                    self._log_async("补全行业 失败明细（前 5 条）：")
                    for line in errors[:5]:
                        self._log_async(f"  · {line}")
                self._post_to_ui(lambda m=msg: self.status_var.set(m))
            except Exception as exc:
                self._log_async(f"补全行业失败：{exc}")
                self._post_to_ui(lambda e=exc: self.status_var.set(f"补全行业失败：{e}"))
            finally:
                self._industry_backfill_running = False
                self._post_to_ui(
                    lambda: self.backfill_industry_btn.config(state=tk.NORMAL)
                )

        threading.Thread(target=_run, daemon=True).start()

    def start_industry_backfill_baostock(self):
        """用 Baostock 一次拉全市场证监会行业，回填 universe.industry。

        相比 THS scrape 版：单接口拉 5500+ 票，10s 完成、不会限流；
        命名是证监会标准（如"计算机、通信和其他电子设备制造业"），比 THS 短名粗。
        """
        def _industry_coverage():
            import sqlite3
            from pathlib import Path
            db_path = Path("data/stock_store.sqlite3")
            if not db_path.is_file():
                return (0, 0)
            with sqlite3.connect(str(db_path)) as conn:
                total = conn.execute("SELECT COUNT(*) FROM universe").fetchone()[0]
                with_ind = conn.execute(
                    "SELECT COUNT(*) FROM universe WHERE industry != ''"
                ).fetchone()[0]
            return (total, with_ind)

        if getattr(self, "_industry_backfill_running", False):
            return
        self._industry_backfill_running = True
        self.backfill_industry_baostock_btn.config(state=tk.DISABLED)
        self.backfill_industry_btn.config(state=tk.DISABLED)
        self._open_run_log()
        self._log("开始补全 universe.industry（Baostock 证监会行业，单接口拉全市场）...")
        self.status_var.set("补全行业(证监会)：登录 Baostock...")

        def _run():
            try:
                from src.services.scoring import first_board as _fb

                before_total, before_with = _industry_coverage()
                self._log_async(
                    f"补全行业(证监会)：当前 universe {before_total} 只，"
                    f"已有行业 {before_with} 只 ({before_with / max(before_total,1) * 100:.1f}%)"
                )

                def _progress(cur, total, msg):
                    self._post_to_ui(
                        lambda c=cur, t=total, m=msg: self.status_var.set(
                            f"补全行业(证监会) · {m}"
                        )
                    )

                result = _fb.backfill_universe_industries_baostock(
                    log_fn=self._log_async, progress_callback=_progress,
                )
                after_total, after_with = _industry_coverage()
                msg = (
                    f"补全行业(证监会)完成：覆盖 {result.get('industries', 0)} 个证监会行业，"
                    f"映射 {result.get('mapped_codes', 0)} 只票，"
                    f"DB 写入 {result.get('updated', 0)} 行 · "
                    f"行业覆盖 {before_with} → {after_with} / {after_total} "
                    f"({after_with / max(after_total,1) * 100:.1f}%)"
                )
                errors = result.get("errors") or []
                if errors:
                    msg += f"，{len(errors)} 项问题"
                self._log_async(msg)
                if errors:
                    self._log_async("补全行业(证监会) 失败明细（前 5 条）：")
                    for line in errors[:5]:
                        self._log_async(f"  · {line}")
                self._post_to_ui(lambda m=msg: self.status_var.set(m))
            except Exception as exc:
                self._log_async(f"补全行业(证监会)失败：{exc}")
                self._post_to_ui(lambda e=exc: self.status_var.set(f"补全行业(证监会)失败：{e}"))
            finally:
                self._industry_backfill_running = False
                self._post_to_ui(
                    lambda: self.backfill_industry_baostock_btn.config(state=tk.NORMAL)
                )
                self._post_to_ui(
                    lambda: self.backfill_industry_btn.config(state=tk.NORMAL)
                )

        threading.Thread(target=_run, daemon=True).start()

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
                self._post_to_ui(lambda: self.result.scan_finished("历史缓存更新已停止"))
                return
            if universe is None or universe.empty:
                self._post_to_ui(lambda: self._set_progressbar_indeterminate(False))
                self._post_to_ui(lambda: self.result.scan_finished("历史缓存更新失败: 股票池为空"))
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
                self._post_to_ui(lambda: self.result.scan_finished("历史缓存更新已停止"))
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
            self._post_to_ui(lambda: self.result.scan_finished("历史缓存更新完成"))
        except StopIteration:
            self._post_to_ui(lambda: self._log("历史缓存更新已停止。"))
            self._post_to_ui(lambda: self.result.scan_finished("历史缓存更新已停止"))
        except Exception as e:
            error_text = str(e)
            self._post_to_ui(lambda: self._log(f"历史缓存更新出错: {error_text}"))
            self._post_to_ui(lambda: self.result.scan_finished(f"历史缓存更新失败: {error_text}"))
            self._post_to_ui(lambda et=error_text: self._show_network_error_alert(et))
        finally:
            self._unregister_cancel_token(token)

    def _get_tree_selected_code(self, tree) -> str:
        selection = tree.selection()
        if not selection:
            return ""
        item = tree.item(selection[0])
        values = item.get("values") or []
        if not values:
            return ""
        return str(values[0]).strip().zfill(6)

    def query_single_stock(self):
        stock_code = self.stock_code_var.get().strip()
        if not stock_code:
            messagebox.showwarning("警告", "请输入股票代码")
            return
        if not self.update_filter_params():
            return
        self.detail._cancel_scheduled()
        self.detail.show(stock_code, force_refresh=True)
        self.notebook.select(self.detail.frame)

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
            "建立股票→概念反查表，让强势标签显示更细的题材标签。",
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
            self.result.all_results = []
            self.result.filtered_stocks = []
            messagebox.showinfo("恢复成功", "数据库已恢复。建议重启应用以确保数据一致。")
            self._log(f"数据库恢复完成：{file_path}")
        else:
            messagebox.showerror("恢复失败", "恢复过程出错，请检查日志。")

    def on_close(self):
        self._ui.mark_closing()
        # 广播取消到所有后台任务，让它们尽快退出
        self._cancel_all_background("window_close")
        if getattr(self, "detail", None) is not None:
            self.detail.request_code = ""
            self.detail.loading_code = ""
            self.detail._cancel_scheduled()
        if getattr(self, "intraday", None) is not None:
            self.intraday.request_code = ""
            self.intraday.loading_code = ""
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
