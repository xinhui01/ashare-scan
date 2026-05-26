"""涨停预测 Tab：4 sub-tab 候选 + 预测/对比/策略/回测/AI 短报。

这是项目最复杂的 tab，包含：
- 涨停预测 4 sub-tab notebook（保留涨停/二波接力/首板涨停/反包）
  以及 概念炒作 sub-tab
- 顶部 action_bar（开始预测/历史日期/命中对比/策略分析/批量回测/AI 博弈短报/NIM Key）
- 市场情绪条（sent_bar：评分/建议/详情/刷新）
- 数据源指示标签
- 筛选栏（filter_bar）
- 多个子窗口（命中对比/策略分析/批量回测/AI 短报）

跨 tab 引用走 self.app.xxx：
- self.app.notebook
- self.app.status_var / top_header_var
- self.app.stock_filter
- self.app._ui / _post_to_ui / _log_async / _log
- self.app.detail.show(...)（双击跳详情）
- self.app.min_price_var / max_price_var / selected_boards（全局过滤 var）
- self.app.history_source_var（数据源偏好）
"""
from __future__ import annotations

import csv
import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, simpledialog, filedialog

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

from src.gui.tabs.detail import DetailTab
from src.gui.tree_enhancer import attach_enhancers_recursively as _attach_tree_enhancers
from src.services import (
    concept_hype_service,
    market_sentiment_service,
    prediction_accuracy_service,
)
from src.utils.cancel_token import CancelToken
from src.utils.trade_calendar import (
    _get_trade_calendar,
    _is_trading_day,
    _previous_trading_day,
)

import stock_store
from llm_client import LlmConfigError, save_api_key as llm_save_api_key
from stock_store import (
    list_limit_up_prediction_dates,
    load_last_limit_up_prediction,
    load_limit_up_prediction_by_date,
)

if TYPE_CHECKING:
    from src.gui.app import StockMonitorApp


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


class PredictTab:
    """涨停预测 tab：5 sub-tab 候选 + 预测/对比/策略/回测/AI 短报。"""

    def __init__(self, app: "StockMonitorApp", notebook: ttk.Notebook) -> None:
        self.app = app
        # ---- 5 类候选各自的排序状态 ----
        self.cont_sort_column: str = "score"
        self.cont_sort_reverse: bool = True
        self.first_sort_column: str = "score"
        self.first_sort_reverse: bool = True
        self.fresh_sort_column: str = "score"
        self.fresh_sort_reverse: bool = True
        self.wrap_sort_column: str = "score"
        self.wrap_sort_reverse: bool = True
        # 按历史命中段排序时，缓存每个类别的 {(lo, hi): {rate, eligible, ...}}
        self.bucket_rates_cache: Dict[str, Dict[Tuple[int, int], Dict[str, Any]]] = {}
        # 每个主类别的"历史最优分数段" (lo, hi)
        self.best_buckets: Dict[str, Optional[Tuple[int, int]]] = {}
        self.best_bucket_labels: Dict[str, Any] = {}
        # 运行时填充
        self.lists: Optional[Dict[str, List[Dict[str, Any]]]] = None
        self.compare_context: Dict[str, Any] = {}
        self.thread: Optional[threading.Thread] = None
        self.result: Optional[Dict[str, Any]] = None
        self.results_map: Dict = {}
        self.prewarm_thread: Optional[threading.Thread] = None
        self.prewarm_token: Optional[CancelToken] = None
        self.concept_index_thread: Optional[threading.Thread] = None
        self.concept_index_token: Optional[CancelToken] = None
        # sentiment
        self.sentiment_result: Optional[Dict[str, Any]] = None
        self.sentiment_thread: Optional[threading.Thread] = None
        # concept hype
        self.concept_hype_result: Optional[Dict[str, Any]] = None
        self.concept_hype_thread: Optional[threading.Thread] = None
        self.concept_hype_sort_col: str = "score"
        self.concept_hype_sort_reverse: bool = True

        self._build(notebook)

    # ============================== UI 构建 ==============================

    def _build(self, notebook: ttk.Notebook) -> None:
        """构建 widget。从原 setup_predict_tab 整体迁移。"""
        predict_frame = ttk.Frame(notebook, padding="5")
        notebook.add(predict_frame, text="涨停预测")
        self.frame = predict_frame

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
        ttk.Button(action_bar, text="开始预测", command=self.start).pack(side=tk.LEFT)
        ttk.Button(
            action_bar, text="命中对比", command=self.open_compare_window,
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            action_bar, text="策略分析", command=self.open_strategy_window,
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            action_bar, text="批量回测", command=self.open_backtest_dialog,
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            action_bar, text="AI 博弈短报", command=self.open_daily_brief_window,
        ).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(
            action_bar, text="NIM Key", command=self.open_nim_key_dialog,
        ).pack(side=tk.LEFT, padx=(2, 0))
        ttk.Label(action_bar, text="基准日期:").pack(side=tk.LEFT, padx=(12, 2))
        self.date_var = tk.StringVar(value=datetime.now().strftime("%Y%m%d"))
        ttk.Entry(action_bar, textvariable=self.date_var, width=10).pack(side=tk.LEFT)
        ttk.Button(
            action_bar, text="今天", width=5,
            command=lambda: self.date_var.set(datetime.now().strftime("%Y%m%d")),
        ).pack(side=tk.LEFT, padx=(2, 0))
        ttk.Label(action_bar, text="回溯天数:").pack(side=tk.LEFT, padx=(10, 2))
        self.lookback_var = tk.StringVar(value="5")
        ttk.Entry(action_bar, textvariable=self.lookback_var, width=4).pack(side=tk.LEFT)
        ttk.Label(action_bar, text="(回看N日涨停对比环境 + 识别二波接力)").pack(side=tk.LEFT, padx=6)
        self.status_label = ttk.Label(action_bar, text="")
        self.status_label.pack(side=tk.RIGHT, padx=8)

        # 历史记录选择：可按日期查看每天的预测数据
        ttk.Button(
            action_bar, text="刷新此日期",
            command=self.refresh_selected_date,
        ).pack(side=tk.RIGHT, padx=(4, 0))
        ttk.Label(action_bar, text="历史记录:").pack(side=tk.RIGHT, padx=(12, 2))
        self.history_var = tk.StringVar(value="")
        self.history_combo = ttk.Combobox(
            action_bar, textvariable=self.history_var,
            width=12, state="readonly", values=(),
        )
        self.history_combo.pack(side=tk.RIGHT)
        self.history_combo.bind(
            "<<ComboboxSelected>>", self._on_history_selected,
        )

        # ---- 市场情绪条 ----
        sent_bar = ttk.Frame(predict_frame, padding=(4, 3))
        sent_bar.pack(fill=tk.X, pady=(0, 4))
        try:
            sent_bar.configure(relief=tk.GROOVE, borderwidth=1)
        except tk.TclError:
            pass
        self.sentiment_score_label = ttk.Label(
            sent_bar, text="情绪: -/100", font=("", 11, "bold"),
        )
        self.sentiment_score_label.pack(side=tk.LEFT, padx=(0, 6))
        self.sentiment_advice_label = ttk.Label(sent_bar, text="→ -")
        self.sentiment_advice_label.pack(side=tk.LEFT, padx=(0, 12))
        self.sentiment_summary_label = ttk.Label(
            sent_bar, text="点击右侧「刷新」分析市场情绪", foreground="#666",
        )
        self.sentiment_summary_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        # 数据源指示标签（在预测完成后由 _refresh_data_source_label 更新）
        self.data_source_label = ttk.Label(
            sent_bar, text="", foreground="#888",
        )
        self.data_source_label.pack(side=tk.RIGHT, padx=(8, 4))
        ttk.Button(
            sent_bar, text="详情", width=6,
            command=self._show_sentiment_detail,
        ).pack(side=tk.RIGHT)
        ttk.Button(
            sent_bar, text="刷新", width=6,
            command=self._refresh_sentiment_async,
        ).pack(side=tk.RIGHT, padx=(0, 4))
        # 启动后 1.5s 自动算一次（有 app_config 缓存时秒回）
        try:
            self.app.root.after(1500, self._refresh_sentiment_async)
        except Exception:
            pass

        # ---- 筛选栏 ----
        filter_bar = ttk.Frame(predict_frame)
        filter_bar.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(filter_bar, text="最低分≥").pack(side=tk.LEFT)
        self.filter_min_score = tk.IntVar(value=0)
        min_score_spin = ttk.Spinbox(
            filter_bar, from_=0, to=100, increment=5, width=5,
            textvariable=self.filter_min_score,
            command=self._on_filter_changed,
        )
        min_score_spin.pack(side=tk.LEFT, padx=(2, 4))
        # 让"键盘输入"也触发筛选（Spinbox 默认 command 只响应上下箭头）
        min_score_spin.bind("<KeyRelease>", lambda _e: self._on_filter_changed())
        min_score_spin.bind("<FocusOut>", lambda _e: self._on_filter_changed())
        min_score_spin.bind("<Return>", lambda _e: self._on_filter_changed())
        # 一键预设按钮
        for preset in (50, 60, 70):
            ttk.Button(
                filter_bar, text=f"≥{preset}", width=4,
                command=lambda v=preset: (
                    self.filter_min_score.set(v),
                    self._on_filter_changed(),
                ),
            ).pack(side=tk.LEFT, padx=1)
        ttk.Frame(filter_bar, width=8).pack(side=tk.LEFT)

        ttk.Label(filter_bar, text="关键词:").pack(side=tk.LEFT)
        self.filter_keyword = tk.StringVar(value="")
        kw_entry = ttk.Entry(filter_bar, textvariable=self.filter_keyword, width=14)
        kw_entry.pack(side=tk.LEFT, padx=(2, 10))
        kw_entry.bind("<KeyRelease>", lambda _e: self._on_filter_changed())

        ttk.Label(filter_bar, text="行业:").pack(side=tk.LEFT)
        self.filter_industry = tk.StringVar(value="全部")
        self.filter_industry_combo = ttk.Combobox(
            filter_bar, textvariable=self.filter_industry,
            width=14, state="readonly", values=("全部",),
        )
        self.filter_industry_combo.pack(side=tk.LEFT, padx=(2, 10))
        self.filter_industry_combo.bind(
            "<<ComboboxSelected>>", lambda _e: self._on_filter_changed(),
        )

        self.filter_lhb_only = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            filter_bar, text="仅 LHB", variable=self.filter_lhb_only,
            command=self._on_filter_changed,
        ).pack(side=tk.LEFT, padx=(0, 8))

        self.filter_northbound_only = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            filter_bar, text="仅北向加仓", variable=self.filter_northbound_only,
            command=self._on_filter_changed,
        ).pack(side=tk.LEFT, padx=(0, 8))

        self.filter_theme_only = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            filter_bar, text="仅命中题材", variable=self.filter_theme_only,
            command=self._on_filter_changed,
        ).pack(side=tk.LEFT, padx=(0, 8))

        # 按历史命中段排序：以策略分析的"分数段命中率"做主排序键，
        # 把历史高命中的分数段顶到表头（每个 tab 用各自类别的命中率）
        self.sort_by_hit_bucket = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            filter_bar, text="按历史命中段排序",
            variable=self.sort_by_hit_bucket,
            command=self._on_sort_mode_changed,
        ).pack(side=tk.LEFT, padx=(0, 8))

        ttk.Button(
            filter_bar, text="重置筛选",
            command=self._reset_filters,
        ).pack(side=tk.LEFT, padx=(4, 0))

        self.filter_count_label = ttk.Label(filter_bar, text="", foreground="#666")
        self.filter_count_label.pack(side=tk.RIGHT, padx=8)

        # ---- 主区域：左侧摘要 + 右侧表格 ----
        body = ttk.PanedWindow(predict_frame, orient=tk.HORIZONTAL)
        body.pack(fill=tk.BOTH, expand=True)

        # 左侧：摘要面板
        summary_frame = ttk.LabelFrame(body, text="预测摘要", padding="6")
        self.summary_text = scrolledtext.ScrolledText(summary_frame, width=42, height=30, wrap=tk.WORD)
        self.summary_text.pack(fill=tk.BOTH, expand=True)
        self.summary_text.insert(tk.END,
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
        self.summary_text.config(state=tk.DISABLED)
        body.add(summary_frame, weight=2)

        # 右侧：表格区
        table_frame = ttk.Frame(body)
        self.table_nb = ttk.Notebook(table_frame)
        self.table_nb.pack(fill=tk.BOTH, expand=True)

        # 用于在每个 tab 顶部展示历史命中率
        self.stat_labels: Dict[str, ttk.Label] = {}

        # 连板延续候选 Tab
        cont_tab = ttk.Frame(self.table_nb)
        self.table_nb.add(cont_tab, text="保留涨停候选")
        cont_stat = ttk.Label(cont_tab, text="历史命中率: -", foreground="#444",
                              anchor=tk.W, padding=(6, 2))
        cont_stat.pack(side=tk.TOP, fill=tk.X)
        self.stat_labels["cont"] = cont_stat
        cont_best = ttk.Label(cont_tab, text="历史最优段: -",
                              foreground="#b8860b", anchor=tk.W, padding=(6, 1))
        cont_best.pack(side=tk.TOP, fill=tk.X)
        self.best_bucket_labels["cont"] = cont_best

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
        self.subcategory_best_labels: Dict[str, ttk.Label] = {}
        # 拆开"昨日"和"近20d"两列，原来的 self.stat_labels[sub_key] 只放一个
        # Label 不够，现在改放 (yest_label, recent_label) 元组
        self.subcategory_stat_labels: Dict[str, Tuple[ttk.Label, ttk.Label]] = {}

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
            self.subcategory_stat_labels[sub_key] = (yest_lbl, recent_lbl)
            self.subcategory_best_labels[sub_key] = best_lbl
            # 兼容旧 stat_labels：让 sub_key 指向 recent_lbl（"近20d"列），
            # 这样 _apply_accuracy 现有 sub_key 分支的 fallback 仍能工作
            self.stat_labels[sub_key] = recent_lbl
        cont_cols = ("code", "name", "industry", "boards", "change_pct", "close",
                     "seal_time", "breaks", "turnover", "score", "result", "reasons")
        self.cont_tree = ttk.Treeview(
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
            self.cont_tree.heading(
                col, text=heading,
                command=lambda c=col: self._on_heading_click("cont", c),
            )
            self.cont_tree.column(col, width=w, anchor=tk.CENTER if col != "reasons" else tk.W)
        sb_cont = ttk.Scrollbar(cont_tab, orient=tk.VERTICAL, command=self.cont_tree.yview)
        self.cont_tree.configure(yscrollcommand=sb_cont.set)
        sb_cont.pack(side=tk.RIGHT, fill=tk.Y)
        self.cont_tree.pack(fill=tk.BOTH, expand=True)
        self.cont_tree.bind("<<TreeviewSelect>>", self._on_stock_select)
        self.cont_tree.bind("<Double-1>", self._on_stock_double_click)
        # 行标签色：按分数段
        self.cont_tree.tag_configure("score_high", background="#c8e6c9", foreground="#1f1f1f")
        self.cont_tree.tag_configure("score_mid", background="#fff9c4", foreground="#1f1f1f")
        self.cont_tree.tag_configure("score_low", background="#ffecb3", foreground="#1f1f1f")
        self.cont_tree.tag_configure("hit", background="#a5d6a7", foreground="#1f1f1f")
        self.cont_tree.tag_configure("miss", background="#ffcdd2", foreground="#1f1f1f")
        self.cont_tree.tag_configure("best_bucket", background="#ffd54f", foreground="#1f1f1f")

        # 首板候选 Tab
        first_tab = ttk.Frame(self.table_nb)
        self.table_nb.add(first_tab, text="二波接力候选")
        first_stat = ttk.Label(first_tab, text="历史命中率: -", foreground="#444",
                               anchor=tk.W, padding=(6, 2))
        first_stat.pack(side=tk.TOP, fill=tk.X)
        self.stat_labels["first"] = first_stat
        first_best = ttk.Label(first_tab, text="历史最优段: -",
                               foreground="#b8860b", anchor=tk.W, padding=(6, 1))
        first_best.pack(side=tk.TOP, fill=tk.X)
        self.best_bucket_labels["first"] = first_best
        first_cols = ("code", "name", "industry", "change_pct", "close",
                      "burst_date", "burst_ratio", "dist_ma5", "days_since_burst",
                      "score", "result", "reasons")
        self.first_tree = ttk.Treeview(
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
            self.first_tree.heading(
                col, text=heading,
                command=lambda c=col: self._on_heading_click("first", c),
            )
            self.first_tree.column(col, width=w, anchor=tk.CENTER if col != "reasons" else tk.W)
        sb_first = ttk.Scrollbar(first_tab, orient=tk.VERTICAL, command=self.first_tree.yview)
        self.first_tree.configure(yscrollcommand=sb_first.set)
        sb_first.pack(side=tk.RIGHT, fill=tk.Y)
        self.first_tree.pack(fill=tk.BOTH, expand=True)
        self.first_tree.bind("<<TreeviewSelect>>", self._on_stock_select)
        self.first_tree.bind("<Double-1>", self._on_stock_double_click)
        self.first_tree.tag_configure("score_high", background="#c8e6c9", foreground="#1f1f1f")
        self.first_tree.tag_configure("score_mid", background="#fff9c4", foreground="#1f1f1f")
        self.first_tree.tag_configure("score_low", background="#ffecb3", foreground="#1f1f1f")
        self.first_tree.tag_configure("hit", background="#a5d6a7", foreground="#1f1f1f")
        self.first_tree.tag_configure("miss", background="#ffcdd2", foreground="#1f1f1f")
        self.first_tree.tag_configure("best_bucket", background="#ffd54f", foreground="#1f1f1f")

        # 首板涨停候选 Tab（最近 N 日未涨停、今日量价启动）
        fresh_tab = ttk.Frame(self.table_nb)
        self.table_nb.add(fresh_tab, text="首板涨停候选")
        fresh_stat = ttk.Label(fresh_tab, text="历史命中率: -", foreground="#444",
                               anchor=tk.W, padding=(6, 2))
        fresh_stat.pack(side=tk.TOP, fill=tk.X)
        self.stat_labels["fresh"] = fresh_stat
        fresh_best = ttk.Label(fresh_tab, text="历史最优段: -",
                               foreground="#b8860b", anchor=tk.W, padding=(6, 1))
        fresh_best.pack(side=tk.TOP, fill=tk.X)
        self.best_bucket_labels["fresh"] = fresh_best
        fresh_cols = ("code", "name", "industry", "change_pct", "close",
                      "volume_ratio", "dist_ma5", "trend_5d", "position_60d",
                      "turnover", "score", "result", "reasons")
        self.fresh_tree = ttk.Treeview(
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
            self.fresh_tree.heading(
                col, text=heading,
                command=lambda c=col: self._on_heading_click("fresh", c),
            )
            self.fresh_tree.column(col, width=w, anchor=tk.CENTER if col != "reasons" else tk.W)
        sb_fresh = ttk.Scrollbar(fresh_tab, orient=tk.VERTICAL, command=self.fresh_tree.yview)
        self.fresh_tree.configure(yscrollcommand=sb_fresh.set)
        sb_fresh.pack(side=tk.RIGHT, fill=tk.Y)
        self.fresh_tree.pack(fill=tk.BOTH, expand=True)
        self.fresh_tree.bind("<<TreeviewSelect>>", self._on_stock_select)
        self.fresh_tree.bind("<Double-1>", self._on_stock_double_click)
        self.fresh_tree.tag_configure("score_high", background="#c8e6c9", foreground="#1f1f1f")
        self.fresh_tree.tag_configure("score_mid", background="#fff9c4", foreground="#1f1f1f")
        self.fresh_tree.tag_configure("score_low", background="#ffecb3", foreground="#1f1f1f")
        self.fresh_tree.tag_configure("hit", background="#a5d6a7", foreground="#1f1f1f")
        self.fresh_tree.tag_configure("miss", background="#ffcdd2", foreground="#1f1f1f")
        self.fresh_tree.tag_configure("best_bucket", background="#ffd54f", foreground="#1f1f1f")

        # 断板反包候选 Tab（近期涨停被打掉，今日逼近反包）
        wrap_tab = ttk.Frame(self.table_nb)
        self.table_nb.add(wrap_tab, text="反包候选")
        wrap_stat = ttk.Label(wrap_tab, text="历史命中率: -", foreground="#444",
                              anchor=tk.W, padding=(6, 2))
        wrap_stat.pack(side=tk.TOP, fill=tk.X)
        self.stat_labels["wrap"] = wrap_stat
        wrap_best = ttk.Label(wrap_tab, text="历史最优段: -",
                              foreground="#b8860b", anchor=tk.W, padding=(6, 1))
        wrap_best.pack(side=tk.TOP, fill=tk.X)
        self.best_bucket_labels["wrap"] = wrap_best
        wrap_cols = ("code", "name", "industry", "pattern_kind", "change_pct", "close",
                     "prior_lu_date", "prior_lu_close", "wrap_gap", "days_since_lu",
                     "worst_drop", "volume_ratio", "score", "result", "reasons")
        self.wrap_tree = ttk.Treeview(
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
            self.wrap_tree.heading(
                col, text=heading,
                command=lambda c=col: self._on_heading_click("wrap", c),
            )
            self.wrap_tree.column(col, width=w, anchor=tk.CENTER if col != "reasons" else tk.W)
        sb_wrap = ttk.Scrollbar(wrap_tab, orient=tk.VERTICAL, command=self.wrap_tree.yview)
        self.wrap_tree.configure(yscrollcommand=sb_wrap.set)
        sb_wrap.pack(side=tk.RIGHT, fill=tk.Y)
        self.wrap_tree.pack(fill=tk.BOTH, expand=True)
        self.wrap_tree.bind("<<TreeviewSelect>>", self._on_stock_select)
        self.wrap_tree.bind("<Double-1>", self._on_stock_double_click)
        self.wrap_tree.tag_configure("score_high", background="#c8e6c9", foreground="#1f1f1f")
        self.wrap_tree.tag_configure("score_mid", background="#fff9c4", foreground="#1f1f1f")
        self.wrap_tree.tag_configure("score_low", background="#ffecb3", foreground="#1f1f1f")
        self.wrap_tree.tag_configure("hit", background="#a5d6a7", foreground="#1f1f1f")
        self.wrap_tree.tag_configure("miss", background="#ffcdd2", foreground="#1f1f1f")
        self.wrap_tree.tag_configure("best_bucket", background="#ffd54f", foreground="#1f1f1f")

        # 概念炒作 Tab（按"题材"维度看：哪些概念在被炒、持续多久、主线/龙头/潜伏）
        self._setup_concept_hype_subtab(self.table_nb)

        body.add(table_frame, weight=4)

        # 启动时即刻把 5 个 tab 的"历史命中率"标签填上
        self._refresh_accuracy_async("")

    # ============== 概念炒作 sub-tab ==============
    def _setup_concept_hype_subtab(self, parent_nb: ttk.Notebook) -> None:
        """在涨停预测的内层 notebook 里加一个'概念炒作'子 tab。

        左侧为摘要面板（主线 + 萌芽题材），右侧上下分栏：
        - 上：题材排行表（按 今日涨停 / 累计 / 持续 排序）
        - 下：选中题材的涨停成员表（双击跳详情）
        """
        hype_tab = ttk.Frame(parent_nb)
        parent_nb.add(hype_tab, text="概念炒作")
        self.concept_hype_tab = hype_tab

        # ---- 操作栏 ----
        action = ttk.Frame(hype_tab)
        action.pack(fill=tk.X, pady=(2, 4))
        ttk.Button(
            action, text="开始分析", command=self._start_concept_hype_analysis,
        ).pack(side=tk.LEFT)
        ttk.Label(action, text="基准日期:").pack(side=tk.LEFT, padx=(10, 2))
        self.concept_hype_end_date_var = tk.StringVar(
            value=datetime.now().strftime("%Y%m%d"),
        )
        ttk.Entry(
            action, textvariable=self.concept_hype_end_date_var, width=10,
        ).pack(side=tk.LEFT)
        ttk.Button(
            action, text="今日", width=5,
            command=lambda: self.concept_hype_end_date_var.set(
                datetime.now().strftime("%Y%m%d"),
            ),
        ).pack(side=tk.LEFT, padx=(2, 0))
        ttk.Button(
            action, text="同步上方", width=8,
            command=lambda: self.concept_hype_end_date_var.set(
                (self.date_var.get() or "").strip().replace("-", ""),
            ),
        ).pack(side=tk.LEFT, padx=(2, 0))
        ttk.Label(action, text="回看交易日:").pack(side=tk.LEFT, padx=(10, 2))
        self.concept_hype_lookback_var = tk.StringVar(value="10")
        ttk.Spinbox(
            action, from_=3, to=30, width=4,
            textvariable=self.concept_hype_lookback_var,
        ).pack(side=tk.LEFT)
        ttk.Label(
            action, text="(按 limit_up_pool 已缓存日切片，不在缓存内的日期会自动取最近可用日)",
            foreground="#888",
        ).pack(side=tk.LEFT, padx=(8, 0))
        self.concept_hype_status = ttk.Label(action, text="尚未分析", foreground="#666")
        self.concept_hype_status.pack(side=tk.RIGHT, padx=8)

        # ---- 筛选栏 ----
        filt = ttk.Frame(hype_tab)
        filt.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(filt, text="来源:").pack(side=tk.LEFT)
        self.concept_hype_source_var = tk.StringVar(value="全部")
        src_combo = ttk.Combobox(
            filt, textvariable=self.concept_hype_source_var,
            values=("全部", "行业", "概念", "LLM题材"),
            width=10, state="readonly",
        )
        src_combo.pack(side=tk.LEFT, padx=(2, 10))
        src_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_concept_hype_list())
        ttk.Label(filt, text="阶段:").pack(side=tk.LEFT)
        self.concept_hype_phase_var = tk.StringVar(value="全部")
        phase_combo = ttk.Combobox(
            filt, textvariable=self.concept_hype_phase_var,
            values=("全部", "萌芽", "主升", "末期", "退潮"),
            width=8, state="readonly",
        )
        phase_combo.pack(side=tk.LEFT, padx=(2, 10))
        phase_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_concept_hype_list())
        self.concept_hype_active_only = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            filt, text="仅今日仍有涨停", variable=self.concept_hype_active_only,
            command=self._refresh_concept_hype_list,
        ).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Label(filt, text="关键词:").pack(side=tk.LEFT)
        self.concept_hype_keyword_var = tk.StringVar(value="")
        kw = ttk.Entry(filt, textvariable=self.concept_hype_keyword_var, width=12)
        kw.pack(side=tk.LEFT, padx=(2, 4))
        kw.bind("<KeyRelease>", lambda _e: self._refresh_concept_hype_list())
        self.concept_hype_count_label = ttk.Label(filt, text="", foreground="#666")
        self.concept_hype_count_label.pack(side=tk.RIGHT, padx=8)

        # ---- 主区域 ----
        body = ttk.PanedWindow(hype_tab, orient=tk.HORIZONTAL)
        body.pack(fill=tk.BOTH, expand=True)

        # 左：摘要面板
        summary_frame = ttk.LabelFrame(body, text="主线 / 萌芽", padding=6)
        self.concept_hype_summary = scrolledtext.ScrolledText(
            summary_frame, width=38, height=24, wrap=tk.WORD,
        )
        self.concept_hype_summary.pack(fill=tk.BOTH, expand=True)
        self.concept_hype_summary.insert(
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
        self.concept_hype_summary.config(state=tk.DISABLED)
        body.add(summary_frame, weight=2)

        # 右：上下分栏
        right = ttk.PanedWindow(body, orient=tk.VERTICAL)

        # 右上：题材排行表
        rank_frame = ttk.LabelFrame(right, text="题材排行", padding=2)
        rank_cols = (
            "score", "name", "source", "today", "total", "active", "ignite",
            "duration", "phase", "trend", "leaders",
        )
        self.concept_hype_rank_tree = ttk.Treeview(
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
            self.concept_hype_rank_tree.heading(
                col, text=heading,
                command=lambda c=col: self._on_concept_hype_sort(c),
            )
            self.concept_hype_rank_tree.column(
                col, width=w,
                anchor=tk.CENTER if col not in ("name", "leaders") else tk.W,
            )
        sb_rank = ttk.Scrollbar(
            rank_frame, orient=tk.VERTICAL,
            command=self.concept_hype_rank_tree.yview,
        )
        self.concept_hype_rank_tree.configure(yscrollcommand=sb_rank.set)
        sb_rank.pack(side=tk.RIGHT, fill=tk.Y)
        self.concept_hype_rank_tree.pack(fill=tk.BOTH, expand=True)
        self.concept_hype_rank_tree.bind(
            "<<TreeviewSelect>>", self._on_concept_hype_select,
        )
        # 阶段配色
        self.concept_hype_rank_tree.tag_configure(
            "phase_萌芽", background="#bbdefb", foreground="#0d47a1",
        )
        self.concept_hype_rank_tree.tag_configure(
            "phase_主升", background="#c8e6c9", foreground="#1b5e20",
        )
        self.concept_hype_rank_tree.tag_configure(
            "phase_末期", background="#ffe0b2", foreground="#bf360c",
        )
        self.concept_hype_rank_tree.tag_configure(
            "phase_退潮", background="#eceff1", foreground="#616161",
        )
        right.add(rank_frame, weight=3)

        # 右下：选中题材的成份股
        members_frame = ttk.LabelFrame(right, text="题材成份股（点击上方某行查看）", padding=2)
        self.concept_hype_members_label = members_frame
        member_cols = (
            "code", "name", "industry", "boards", "change_pct",
            "close", "turnover", "lu_count", "lu_dates",
        )
        self.concept_hype_members_tree = ttk.Treeview(
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
            self.concept_hype_members_tree.heading(col, text=heading)
            self.concept_hype_members_tree.column(
                col, width=w,
                anchor=tk.CENTER if col not in ("name", "industry", "lu_dates") else tk.W,
            )
        sb_mem = ttk.Scrollbar(
            members_frame, orient=tk.VERTICAL,
            command=self.concept_hype_members_tree.yview,
        )
        self.concept_hype_members_tree.configure(yscrollcommand=sb_mem.set)
        sb_mem.pack(side=tk.RIGHT, fill=tk.Y)
        self.concept_hype_members_tree.pack(fill=tk.BOTH, expand=True)
        self.concept_hype_members_tree.bind(
            "<Double-1>", self._on_concept_hype_member_double_click,
        )
        right.add(members_frame, weight=2)

        body.add(right, weight=5)

    # ============================== 历史预测加载 ==============================

    def _load_last_prediction(self) -> None:
        payload = load_last_limit_up_prediction()
        self._refresh_history_dates()
        if not isinstance(payload, dict):
            return
        trade_date = str(payload.get("trade_date") or "").strip()
        if trade_date:
            self.date_var.set(trade_date)
            if hasattr(self, "history_var"):
                self.history_var.set(trade_date)
        self._apply_result(payload)

    def _refresh_history_dates(self, select: Optional[str] = None) -> None:
        """刷新历史记录下拉框；可选地选中指定日期。"""
        if not hasattr(self, "history_combo"):
            return
        try:
            dates = list_limit_up_prediction_dates()
        except Exception:
            dates = []
        self.history_combo["values"] = dates
        if select and select in dates:
            self.history_var.set(select)
        elif not self.history_var.get() and dates:
            self.history_var.set(dates[0])

    def _on_history_selected(self, _event=None) -> None:
        trade_date = (self.history_var.get() or "").strip()
        if not trade_date:
            return
        payload = load_limit_up_prediction_by_date(trade_date)
        if not isinstance(payload, dict):
            self.status_label.config(text=f"无 {trade_date} 的历史预测")
            return
        self.date_var.set(trade_date)
        self._apply_result(payload)
        self.app.status_var.set(f"已加载 {trade_date} 的涨停预测历史")

    def refresh_selected_date(self) -> None:
        """重新预测下拉中选中的历史日期，并覆盖原记录。"""
        trade_date = (self.history_var.get() or "").strip()
        if not trade_date:
            trade_date = (self.date_var.get() or "").strip()
        if not trade_date:
            self.status_label.config(text="请先选择要刷新的日期")
            return
        self.date_var.set(trade_date)
        # 在重新预测前，主动清除内存中的涨停池缓存，确保会重新走数据源
        try:
            fetcher = getattr(self.app.stock_filter, "fetcher", None)
            if fetcher is not None:
                date_key = fetcher._normalize_trade_date(trade_date)
                if date_key:
                    try:
                        fetcher._limit_up_pool_cache.pop(date_key, None)
                    except Exception:
                        pass
                    try:
                        fetcher._prev_limit_up_pool_cache.pop(date_key, None)
                    except Exception:
                        pass
                    if getattr(self.app.stock_filter, "_log", None):
                        try:
                            self.app.stock_filter._log(f"已清除内存涨停池缓存 {date_key}，将重新联网拉取")
                        except Exception:
                            pass
        except Exception:
            pass

        self.start(historical_mode=True)

    def _refresh_display_if_ready(self):
        """顶部价格/板块筛选变化时同步刷新涨停预测表（如已有预测结果）。"""
        if not self.lists:
            return
        try:
            self._render_trees()
        except Exception:
            pass

    # ============================== NVIDIA NIM API Key ==============================

    def open_nim_key_dialog(self) -> None:
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
            parent=self.app.root,
            show="*",
        )
        if not new_key or not str(new_key).strip():
            return
        try:
            llm_save_api_key(str(new_key).strip())
            messagebox.showinfo("已保存", "NIM API Key 已保存到本地配置。", parent=self.app.root)
        except Exception as e:
            messagebox.showerror("保存失败", f"无法保存 API Key: {e}", parent=self.app.root)

    # ============================== 行 tag / 排序值 ==============================

    @staticmethod
    def _score_tag(score: int) -> str:
        if score >= 70:
            return "score_high"
        elif score >= 50:
            return "score_mid"
        return "score_low"

    def _row_tag(self, category: str, hit_tag: Optional[str], score: Any) -> str:
        """决定预测候选行的背景色 tag。

        优先级：hit/miss（已回填的次日结果）> best_bucket（历史最优分数段）> 分数段色。
        """
        if hit_tag:
            return hit_tag
        best = (self.best_buckets or {}).get(category)
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
    def _sort_value(record: Dict[str, Any], column: str):
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

    def _sort_records(
        self,
        records: List[Dict[str, Any]],
        table_kind: str,
    ) -> List[Dict[str, Any]]:
        if table_kind == "cont":
            column = self.cont_sort_column
            reverse = self.cont_sort_reverse
            secondary = ["score", "boards", "change_pct", "turnover"]
        elif table_kind == "fresh":
            column = self.fresh_sort_column
            reverse = self.fresh_sort_reverse
            secondary = ["score", "volume_ratio", "change_pct", "turnover"]
        elif table_kind == "wrap":
            column = self.wrap_sort_column
            reverse = self.wrap_sort_reverse
            secondary = ["score", "wrap_gap", "change_pct", "volume_ratio"]
        else:
            column = self.first_sort_column
            reverse = self.first_sort_reverse
            secondary = ["score", "burst_ratio", "dist_ma5", "change_pct"]
        if column in secondary:
            secondary = [c for c in secondary if c != column]

        # "按历史命中段排序"模式：把当前类别下"历史命中率最高的分数段"顶到最上
        # 桶内按 score 降序（再叠加原 secondary 列），保证细排仍然可控
        bucket_priority = self._bucket_priority_for(table_kind)
        if bucket_priority is not None:
            return sorted(
                records,
                key=lambda rec: tuple(
                    [bucket_priority(rec)]
                    + [self._sort_value(rec, "score")]
                    + [self._sort_value(rec, c) for c in secondary
                       if c != "score"]
                    + [str(rec.get("code", ""))]
                ),
                reverse=True,
            )

        return sorted(
            records,
            key=lambda rec: tuple(
                [self._sort_value(rec, column)]
                + [self._sort_value(rec, c) for c in secondary]
                + [str(rec.get("code", ""))]
            ),
            reverse=reverse,
        )

    def _bucket_priority_for(self, table_kind: str):
        """返回桶排序的 key 函数；若关闭模式或拿不到数据则返回 None。

        样本不足（< 5）的桶 priority 退化为 -1，让它们沉到分数排序之后；
        其它桶用其历史命中率（0-100），越大越靠前。
        """
        try:
            if not self.sort_by_hit_bucket.get():
                return None
        except (AttributeError, tk.TclError):
            return None
        cat_key = {
            "cont": "cont", "first": "first", "fresh": "fresh",
            "wrap": "wrap",
        }.get(table_kind)
        if cat_key is None:
            return None
        rates = self._get_bucket_rates(cat_key)
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

    def _get_bucket_rates(self, category: str) -> Dict[Tuple[int, int], Dict[str, Any]]:
        """读取类别的历史分数段命中率（带缓存，lookback=20）。"""
        cache = self.bucket_rates_cache
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

    def _on_heading_click(self, table_kind: str, column: str) -> None:
        if table_kind == "cont":
            if column == self.cont_sort_column:
                self.cont_sort_reverse = not self.cont_sort_reverse
            else:
                self.cont_sort_column = column
                self.cont_sort_reverse = column in {"score", "boards", "change_pct", "close", "turnover", "breaks"}
        elif table_kind == "fresh":
            if column == self.fresh_sort_column:
                self.fresh_sort_reverse = not self.fresh_sort_reverse
            else:
                self.fresh_sort_column = column
                self.fresh_sort_reverse = column in {"score", "volume_ratio", "change_pct", "close", "trend_5d", "position_60d", "turnover"}
        elif table_kind == "wrap":
            if column == self.wrap_sort_column:
                self.wrap_sort_reverse = not self.wrap_sort_reverse
            else:
                self.wrap_sort_column = column
                # 反包缺口越小越好，因此 wrap_gap / days_since_lu 默认升序
                self.wrap_sort_reverse = column in {"score", "change_pct", "close", "volume_ratio", "prior_lu_close"}
        else:
            if column == self.first_sort_column:
                self.first_sort_reverse = not self.first_sort_reverse
            else:
                self.first_sort_column = column
                self.first_sort_reverse = column in {"score", "burst_ratio", "change_pct", "close", "dist_ma5", "days_since_burst"}

        if self.result:
            self._apply_result(self.result)

    # ============================== 预测启停 + 主流程 ==============================

    def start(self, historical_mode: bool = False):
        if self.thread is not None and self.thread.is_alive():
            return
        trade_date = self.date_var.get().strip()
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
        self.date_var.set(trade_date)
        try:
            lookback = max(2, min(int(self.lookback_var.get().strip() or "5"), 15))
        except ValueError:
            lookback = 5
        self.lookback_var.set(str(lookback))

        self.summary_text.config(state=tk.NORMAL)
        self.summary_text.delete("1.0", tk.END)
        self.summary_text.insert(tk.END,
            f"正在回溯最近 {lookback} 天涨停股特征，基于 {trade_date} 数据预测...\n")
        self.summary_text.config(state=tk.DISABLED)
        self.status_label.config(text="正在回溯涨停前兆画像...")
        self.app.status_var.set("正在执行涨停预测...")

        self.thread, _ = self.app._start_background_job(
            self._load,
            name="limit-up-predict",
            args=(trade_date, lookback, historical_mode),
        )

    def _load(
        self,
        trade_date: str,
        lookback_days: int,
        historical_mode: bool,
        cancel_token: CancelToken,
    ):
        try:
            if cancel_token.is_cancelled():
                return
            def _progress(cur, tot, info):
                if cancel_token.is_cancelled():
                    raise StopIteration
                self.app._post_to_ui(lambda c=cur, t=tot, i=info:
                    self.status_label.config(text=f"预测分析 {c}/{t}: {i}"))

            result = self.app.stock_filter.predict_limit_up_candidates(
                trade_date,
                lookback_days=lookback_days,
                progress_callback=_progress,
                historical_mode=historical_mode,
            )
            if cancel_token.is_cancelled():
                return
            self.app._post_to_ui(lambda r=result: self._apply_result(r))
        except StopIteration:
            self.app._post_to_ui(lambda: self.status_label.config(text="已取消"))
        except Exception as e:
            err = str(e)
            self.app._post_to_ui(lambda: self._show_error(f"涨停预测失败: {err}"))

    def _show_error(self, msg: str):
        self.summary_text.config(state=tk.NORMAL)
        self.summary_text.delete("1.0", tk.END)
        self.summary_text.insert(tk.END, msg)
        self.summary_text.config(state=tk.DISABLED)
        self.status_label.config(text="")
        self.app.status_var.set("涨停预测失败")


    # ============== 概念炒作 业务方法 ==============

    def _start_concept_hype_analysis(self) -> None:
        """点击"开始分析"：后台跑 concept_hype_service.analyze_concept_hype。"""
        if self.concept_hype_thread is not None and self.concept_hype_thread.is_alive():
            self.app._log("概念炒作分析已在运行中，请稍候")
            return
        try:
            lookback = max(3, min(30, int(self.concept_hype_lookback_var.get() or "10")))
        except ValueError:
            lookback = 10
        end_date = (self.concept_hype_end_date_var.get() or "").strip().replace("-", "")
        self.concept_hype_status.config(text="分析中...", foreground="#1565c0")

        def _worker():
            try:
                result = concept_hype_service.analyze_concept_hype(
                    end_date=end_date or None,
                    lookback=lookback,
                    log=lambda s: self.app._post_to_ui(lambda m=s: self.app._log(m)),
                )
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
                logger_msg = f"概念炒作分析失败: {err}"
                self.app._post_to_ui(lambda m=logger_msg: self.app._log(m))
                self.app._post_to_ui(
                    lambda: self.concept_hype_status.config(
                        text="分析失败", foreground="#c62828",
                    ),
                )
                return
            self.app._post_to_ui(lambda r=result: self._apply_concept_hype_result(r))

        t = threading.Thread(target=_worker, daemon=True)
        self.concept_hype_thread = t
        t.start()

    def _apply_concept_hype_result(self, result: Dict[str, Any]) -> None:
        """主线程回调：保存结果 + 刷新摘要 + 刷新列表。"""
        self.concept_hype_result = result or {}
        ml = (result or {}).get("main_line") or {}
        stats = (result or {}).get("stats") or {}
        dates = (result or {}).get("trade_dates") or []
        # 摘要
        self.concept_hype_summary.config(state=tk.NORMAL)
        self.concept_hype_summary.delete("1.0", tk.END)
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
        self.concept_hype_summary.insert(tk.END, "\n".join(lines))
        self.concept_hype_summary.config(state=tk.DISABLED)
        # 状态
        self.concept_hype_status.config(
            text=f"已分析 · {result.get('generated_at', '')}",
            foreground="#2e7d32",
        )
        # 列表
        self._refresh_concept_hype_list()
        # 清空成员表
        for it in self.concept_hype_members_tree.get_children():
            self.concept_hype_members_tree.delete(it)

    def _filtered_concepts(self) -> List[Dict[str, Any]]:
        result = self.concept_hype_result or {}
        concepts = list(result.get("concepts") or [])
        if not concepts:
            return []
        src = self.concept_hype_source_var.get()
        phase = self.concept_hype_phase_var.get()
        kw = (self.concept_hype_keyword_var.get() or "").strip().lower()
        active_only = bool(self.concept_hype_active_only.get())
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
        col = self.concept_hype_sort_col
        rev = self.concept_hype_sort_reverse
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
            "trend": lambda c: concept_hype_service.trend_label(c.get("trend", "")),
            "leaders": lambda c: -len(c.get("leaders") or []),
        }
        out.sort(key=key_map.get(col, key_map["score"]), reverse=rev)
        return out

    def _refresh_concept_hype_list(self) -> None:
        for it in self.concept_hype_rank_tree.get_children():
            self.concept_hype_rank_tree.delete(it)
        rows = self._filtered_concepts()
        for c in rows:
            leaders_str = " / ".join(
                f"{m.get('name', '')}({m.get('boards', 1)}板)"
                for m in (c.get("leaders") or [])[:4]
            )
            trend_text = concept_hype_service.trend_label(c.get("trend", ""))
            self.concept_hype_rank_tree.insert(
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
                    trend_text,
                    leaders_str,
                ),
                tags=(f"phase_{c.get('phase', '')}",),
            )
        total = len((self.concept_hype_result or {}).get("concepts") or [])
        self.concept_hype_count_label.config(
            text=f"显示 {len(rows)} / 共 {total}",
        )

    def _on_concept_hype_sort(self, col: str) -> None:
        if self.concept_hype_sort_col == col:
            self.concept_hype_sort_reverse = not self.concept_hype_sort_reverse
        else:
            self.concept_hype_sort_col = col
            # 数字类列默认降序，文本类列默认升序
            self.concept_hype_sort_reverse = col not in (
                "name", "source", "phase", "trend",
            )
        self._refresh_concept_hype_list()

    def _on_concept_hype_select(self, _event=None) -> None:
        """点击题材行 → 在下方表格展开成员明细。"""
        sel = self.concept_hype_rank_tree.selection()
        if not sel:
            return
        values = self.concept_hype_rank_tree.item(sel[0]).get("values") or []
        if not values:
            return
        # rank_cols = ("score", "name", "source", ...)
        # 这里要按列位取题材名/来源，不能误把机会分当成题材名。
        name = str(values[1])
        source = str(values[2])
        concept = next(
            (
                c for c in (self.concept_hype_result or {}).get("concepts") or []
                if c.get("name") == name and c.get("source") == source
            ),
            None,
        )
        # 清表
        for it in self.concept_hype_members_tree.get_children():
            self.concept_hype_members_tree.delete(it)
        if not concept:
            return
        members = concept.get("members") or []
        for m in members:
            lu_dates = m.get("limit_up_dates") or []
            self.concept_hype_members_tree.insert(
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
            self.concept_hype_members_label.configure(
                text=f"题材成份股【{name} · {source}】 共 {len(members)} 只 · 双击跳详情",
            )
        except Exception:
            pass

    def _on_concept_hype_member_double_click(self, _event=None) -> None:
        sel = self.concept_hype_members_tree.selection()
        if not sel:
            return
        values = self.concept_hype_members_tree.item(sel[0]).get("values") or []
        if not values:
            return
        code = str(values[0]).strip().zfill(6)
        if not code:
            return
        try:
            self.app.detail._cancel_scheduled()
        except Exception:
            pass
        try:
            self.app.detail.show(code, force_refresh=True)
            self.app.notebook.select(self.app.detail.frame)
        except Exception as exc:  # noqa: BLE001
            self.app._log(f"打开股票详情失败 {code}: {exc}")

    # ============== 市场情绪 + 仓位建议 ==============
    def _refresh_sentiment_async(self) -> None:
        """后台刷新市场情绪条；同一只票同日已缓存则秒回。"""
        if (
            self.sentiment_thread is not None
            and self.sentiment_thread.is_alive()
        ):
            return
        target = (self.date_var.get() or "").strip().replace("-", "") or None
        try:
            self.sentiment_score_label.config(
                text="情绪: 分析中...", foreground="#1565c0",
            )
            self.sentiment_advice_label.config(text="", foreground="#1565c0")
            self.sentiment_summary_label.config(text="", foreground="#666")
        except Exception:
            return

        def _worker():
            try:
                r = market_sentiment_service.analyze_market_sentiment(
                    target,
                    fetch_external=True,
                    log=lambda s: self.app._post_to_ui(lambda m=s: self.app._log(m)),
                )
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
                self.app._post_to_ui(lambda m=err: self.app._log(f"市场情绪分析失败: {m}"))
                self.app._post_to_ui(lambda: self.sentiment_score_label.config(
                    text="情绪: 失败", foreground="#c62828",
                ))
                return
            self.app._post_to_ui(lambda res=r: self._apply_sentiment_result(res))

        t = threading.Thread(target=_worker, daemon=True)
        self.sentiment_thread = t
        t.start()

    def _apply_sentiment_result(self, r: Dict[str, Any]) -> None:
        self.sentiment_result = r or {}
        score = int(r.get("score", 50))
        advice = r.get("position_suggest") or {}
        color = advice.get("color", "#1f1f1f")
        try:
            self.sentiment_score_label.config(
                text=f"情绪 {r.get('trade_date', '-')} : {score}/100",
                foreground=color,
            )
            self.sentiment_advice_label.config(
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
            self.sentiment_summary_label.config(
                text=" · ".join(parts), foreground="#444",
            )
        except Exception:
            pass

    def _show_sentiment_detail(self) -> None:
        r = self.sentiment_result or {}
        if not r:
            messagebox.showinfo(
                "市场情绪", "请先点击「刷新」分析市场情绪。", parent=self.app.root,
            )
            return
        win = tk.Toplevel(self.app.root)
        win.title(f"市场情绪明细 · {r.get('trade_date', '-')}")
        win.geometry("760x640")
        win.transient(self.app.root)

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
            body, text=r.get("summary", ""), wraplength=720,
            foreground="#333",
        ).pack(anchor=tk.W, pady=(0, 8))

        # 信号明细表
        cols = ("name", "value", "delta", "note")
        signals = r.get("signals") or []
        tree_height = max(len(signals), 7)
        tree = ttk.Treeview(body, columns=cols, show="headings", height=tree_height)
        col_specs = {
            "name": ("信号", 90, tk.W, False),
            "value": ("数值", 110, tk.W, False),
            "delta": ("加减分", 60, tk.CENTER, False),
            "note": ("解读", 460, tk.W, True),
        }
        for col, (h, w, anc, stretch) in col_specs.items():
            tree.heading(col, text=h)
            tree.column(col, width=w, anchor=anc, stretch=stretch)
        for s in signals:
            d = int(s.get("delta", 0))
            tree.insert("", tk.END, values=(
                s.get("name", ""), s.get("value", ""),
                f"+{d}" if d > 0 else (str(d) if d < 0 else "0"),
                s.get("note", ""),
            ))
        tree.pack(fill=tk.X)

        # ===== 数据明细（铺出 raw 里 7 个信号未体现的字段）=====
        raw = r.get("raw") or {}
        today_raw = raw.get("today") or {}
        external_raw = raw.get("external") or {}
        prior_counts = raw.get("prior_counts") or {}
        yest_date = raw.get("yesterday_date") or "-"
        yest_lu = int(raw.get("yesterday_lu") or 0)
        today_continued = int(raw.get("today_continued") or 0)
        avg5 = float(raw.get("avg5") or 0.0)

        detail = ttk.LabelFrame(body, text="数据明细", padding=8)
        detail.pack(fill=tk.BOTH, expand=True, pady=(8, 0))

        cont_rate = (today_continued / yest_lu * 100) if yest_lu else 0.0
        lines: List[str] = []
        lines.append(
            f"基准日: {r.get('trade_date', '-')}  ·  "
            f"对比日: {yest_date}（昨日涨停 {yest_lu} 只，今日继续 {today_continued} 只"
            + (f" = {cont_rate:.1f}%）" if yest_lu else "）")
        )
        if prior_counts:
            parts = [f"{d}={c}" for d, c in sorted(prior_counts.items())]
            lines.append(
                f"前 {len(prior_counts)} 日涨停数: " + "  ·  ".join(parts)
                + f"   → 平均 {avg5:.1f}"
            )
        elif avg5:
            lines.append(f"5 日均值: {avg5:.1f}（每日明细缺失）")

        lines.append(
            f"今日明细: 涨停 {today_raw.get('lu_count', 0)} 只  ·  "
            f"炸过板 {today_raw.get('broken_count', 0)} 只 / "
            f"炸板总次数 {today_raw.get('broken_total_times', 0)}  ·  "
            f"最高 {today_raw.get('max_boards', 0)} 板  ·  "
            f"4+板 {today_raw.get('high_board_count_4plus', 0)} 只"
        )

        codes = today_raw.get("codes") or []
        if codes:
            shown = "、".join(codes[:20])
            tail = f" … 共 {len(codes)} 只" if len(codes) > 20 else ""
            lines.append(f"今日涨停代码: {shown}{tail}")

        sh_pct = external_raw.get("sh_index_pct")
        dt_cnt = external_raw.get("down_limit_count")
        fetched_at = external_raw.get("fetched_at") or "-"
        sh_disp = f"{sh_pct:+.2f}%" if sh_pct is not None else "—"
        dt_disp = f"{dt_cnt} 只" if dt_cnt is not None else "—"
        lines.append(
            f"外部数据: 上证 {sh_disp}  ·  跌停 {dt_disp}  ·  拉取于 {fetched_at}"
        )

        for line in lines:
            ttk.Label(
                detail, text=line, anchor=tk.W, foreground="#444",
                wraplength=700, justify=tk.LEFT,
            ).pack(anchor=tk.W, pady=1)

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
        td = (self.date_var.get() or "").strip().replace("-", "")
        if td:
            try:
                stock_store.save_app_config(
                    f"{market_sentiment_service.CACHE_KEY_PREFIX}{td}", None,
                )
            except Exception:
                pass
        self._refresh_sentiment_async()

    # ============== AI 博弈短报 ==============
    def open_daily_brief_window(self) -> None:
        """打开 AI 博弈短报窗口，展示融合后的语言化建议。

        基于内存中的 self.result + self.concept_hype_result，
        优雅降级 — 任意一个有就能跑，全无则给出明确提示。
        """
        if (
            (not self.result or not isinstance(self.result, dict))
            and not self.concept_hype_result
        ):
            messagebox.showinfo(
                "AI 博弈短报",
                "需要先运行至少一项分析：\n"
                "  · 涨停预测：点击「开始预测」\n"
                "  · 概念炒作：在「概念炒作」tab 点击「开始分析」\n\n"
                "短报会基于这些结果给出综合建议。",
                parent=self.app.root,
            )
            return

        win = tk.Toplevel(self.app.root)
        win.title("AI 博弈短报")
        win.geometry("680x520")
        win.transient(self.app.root)

        body = ttk.Frame(win, padding=10)
        body.pack(fill=tk.BOTH, expand=True)

        # 头部信息栏
        header = ttk.Frame(body)
        header.pack(fill=tk.X, pady=(0, 6))
        td = (self.date_var.get() or "").strip().replace("-", "")
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
                self.app.root.clipboard_clear()
                self.app.root.clipboard_append(text)
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
                from llm_client import LlmConfigError as _LCE, LlmRequestError as _LRE
            except ImportError:
                _LCE = _LRE = Exception  # type: ignore

            status_var.set("正在调用 NIM 生成中..." if use_cache else "强制重新生成中...")
            regen_btn.config(state=tk.DISABLED)

            def _worker():
                # 先拉今日新闻（按日缓存，二次秒回）
                news_payload = None
                try:
                    from src.services import news_feed_service
                    news_payload = news_feed_service.fetch_today_news(
                        td, use_cache=use_cache,
                        log=lambda s: self.app._post_to_ui(lambda m=s: self.app._log(m)),
                    )
                except Exception as exc:  # noqa: BLE001
                    self.app._post_to_ui(
                        lambda m=str(exc): self.app._log(f"今日新闻拉取失败（继续无新闻短报）: {m}")
                    )
                try:
                    result = svc.generate_daily_brief(
                        td,
                        predict_result=self.result,
                        hype_result=self.concept_hype_result,
                        sentiment_result=self.sentiment_result,
                        news_result=news_payload,
                        use_cache=use_cache,
                        log=lambda s: self.app._post_to_ui(lambda m=s: self.app._log(m)),
                    )
                except _LCE as exc:
                    msg = str(exc)
                    self.app._post_to_ui(lambda: status_var.set("失败：未配置 API Key"))
                    self.app._post_to_ui(lambda m=msg: _set_text(
                        f"未能生成短报：\n\n{m}\n\n"
                        "请到设置 → 保存 NIM API Key，或设置环境变量 NVIDIA_API_KEY.",
                        color="#c62828",
                    ))
                    self.app._post_to_ui(lambda: regen_btn.config(state=tk.NORMAL))
                    return
                except _LRE as exc:
                    msg = str(exc)
                    self.app._post_to_ui(lambda: status_var.set("失败：NIM 调用错误"))
                    self.app._post_to_ui(lambda m=msg: _set_text(
                        f"NIM 调用失败：\n\n{m}",
                        color="#c62828",
                    ))
                    self.app._post_to_ui(lambda: regen_btn.config(state=tk.NORMAL))
                    return
                except Exception as exc:  # noqa: BLE001
                    msg = str(exc)
                    self.app._post_to_ui(lambda: status_var.set("失败"))
                    self.app._post_to_ui(lambda m=msg: _set_text(
                        f"未知错误：\n\n{m}", color="#c62828",
                    ))
                    self.app._post_to_ui(lambda: regen_btn.config(state=tk.NORMAL))
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

                self.app._post_to_ui(_apply)

            threading.Thread(target=_worker, daemon=True).start()

        regen_btn.config(command=lambda: _run(use_cache=False))
        # 首次打开：尝试缓存优先
        _run(use_cache=True)

    # ============== 批量回测 ==============
    def open_backtest_dialog(self) -> None:
        """打开批量回测对话框：输入日期范围，预校验可用数据，确认后后台跑。"""
        win = tk.Toplevel(self.app.root)
        win.title("批量回测")
        win.geometry("520x340")
        win.transient(self.app.root)

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
                self.app._post_to_ui(lambda x=i, n=len(dates), td=d:
                                 status_var.set(f"[{x}/{n}] 回测 {td} ..."))
                try:
                    result = self.app.stock_filter.predict_limit_up_candidates(
                        d, lookback_days=lookback, historical_mode=True,
                    )
                    self.app._post_to_ui(lambda td=d, r=result, x=i, n=len(dates):
                                     _append_log(
                                         f"[{x}/{n}] {td} → cont={len(r.get('continuation_candidates', []))} "
                                         f"first={len(r.get('first_board_candidates', []))} "
                                         f"fresh={len(r.get('fresh_first_board_candidates', []))} "
                                         f"wrap={len(r.get('broken_board_wrap_candidates', []))}"
                                     ))
                    evaluated_dates.append(d)
                except Exception as exc:
                    err = str(exc)
                    self.app._post_to_ui(lambda td=d, e=err:
                                     _append_log(f"  {td} 失败: {e}"))
            # 全部跑完 → 强制评估这批日期
            self.app._post_to_ui(lambda: status_var.set(f"预测完成，正在评估准确率..."))
            try:
                from src.services import prediction_accuracy_service as svc
                for d in evaluated_dates:
                    try:
                        svc.evaluate(d)
                    except Exception as exc:
                        self.app._post_to_ui(lambda td=d, e=str(exc):
                                         _append_log(f"  评估 {td} 失败: {e}"))
                # 汇总最近 N 日命中率
                stats = svc.query_category_stats(lookback_dates=max(1, len(evaluated_dates)))
            except Exception as exc:
                self.app._post_to_ui(lambda e=str(exc):
                                 status_var.set(f"评估失败: {e}"))
                return

            def _show_done():
                status_var.set(f"完成。共 {len(evaluated_dates)} 天预测、评估完成")
                _append_log("")
                _append_log("=== 命中率汇总（按类别）===")
                for cat, lbl in [("cont", "保留涨停"), ("first", "二波接力"),
                                 ("fresh", "首板涨停"), ("wrap", "反包")]:
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
                    self._refresh_accuracy_async("")
                except Exception:
                    pass

            self.app._post_to_ui(_show_done)

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

    def _refresh_data_source_label(self, trade_date: str) -> None:
        """根据涨停池数据源更新顶部指示标签。

        调用时机：每次 _apply_result 后。
        """
        if not hasattr(self, "data_source_label"):
            return
        try:
            source = self.app.stock_filter.fetcher.get_pool_source(trade_date)
        except Exception:
            source = "unknown"
        label = self.data_source_label
        source_text = {
            "eastmoney": ("数据: 东财", "#888"),
            "cache_memory": ("数据: 本地缓存", "#888"),
            "cache_db": ("数据: 本地缓存", "#888"),
            "spot_fallback": ("数据: spot 兜底 ⚠️", "#d08000"),  # 橙色
            "empty": ("数据: 无 ❌", "#c62828"),  # 红色
            "unknown": ("", "#888"),
        }
        text, fg = source_text.get(source, ("", "#888"))
        try:
            label.configure(text=text, foreground=fg)
        except Exception:
            pass


    def _apply_result(self, result: Dict[str, Any]):
        self.result = result
        # 先加载该日期的命中结果，再注入到 record（让"结果"列可参与排序）
        current_date = str(result.get("trade_date") or "").strip()
        self.results_map = {}
        if current_date:
            try:
                self.results_map = (
                    prediction_accuracy_service.get_per_code_results(current_date)
                )
            except Exception:
                self.results_map = {}

        def _enrich(records: List[Dict[str, Any]], cat: str) -> List[Dict[str, Any]]:
            for rec in records:
                key = (str(rec.get("code") or "").zfill(6), cat)
                row = self.results_map.get(key)
                if row:
                    # 用"开盘买、收盘卖"口径作为结果排序值；老记录回退到 t1_pct
                    pct = row.get("t1_open_close_pct")
                    if pct is None:
                        pct = row.get("t1_pct")
                    rec["_t1_pct"] = pct
                else:
                    rec["_t1_pct"] = None
            return records

        cont_list = self._sort_records(_enrich(list(result.get("continuation_candidates", [])), "cont"), "cont")
        first_list = self._sort_records(_enrich(list(result.get("first_board_candidates", [])), "first"), "first")
        fresh_list = self._sort_records(_enrich(list(result.get("fresh_first_board_candidates", [])), "fresh"), "fresh")
        wrap_list = self._sort_records(_enrich(list(result.get("broken_board_wrap_candidates", [])), "wrap"), "wrap")
        hot_industries = result.get("hot_industries", {})
        profile = result.get("profile", {})
        compare_context = result.get("compare_context", {})

        # ---- 填充摘要 ----
        self.summary_text.config(state=tk.NORMAL)
        self.summary_text.delete("1.0", tk.END)
        txt = self.summary_text

        # 中止预测时（数据未就位）顶部状态条变红 + 标题更明显
        dq = result.get("data_quality") or {}
        if dq.get("blocked"):
            try:
                self.status_label.config(
                    text=f"⛔ 预测中止 — {len(dq.get('missing') or [])} 项数据未就位（详见摘要）",
                    foreground="#b71c1c",
                )
            except Exception:
                pass

        txt.insert(tk.END, result.get("summary", "") + "\n")

        # ---- 数据健康度（让用户一眼看出本次预测哪些维度是真数据、哪些是 fallback）----
        if dq:
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, "  数据健康度\n")
            txt.insert(tk.END, f"{'='*36}\n")
            mode = "历史模式" if dq.get("historical_mode") else "实时模式"
            txt.insert(tk.END, f"  运行模式: {mode}（生成于 {dq.get('generated_at', '-')}）\n")
            spot = dq.get("spot") or {}
            txt.insert(tk.END,
                f"  spot 快照: {spot.get('rows', 0)} 行 ({spot.get('source', '-')})"
                f" · 行业缺失 {spot.get('industry_missing', 0)} 只\n"
            )
            lup = dq.get("limit_up_pool") or {}
            txt.insert(tk.END, f"  涨停池: {lup.get('rows', 0)} 只 ({lup.get('source', '-')})\n")
            th = dq.get("themes") or {}
            th_state = "已加载" if th.get("loaded") else "未加载"
            txt.insert(tk.END,
                f"  AI 题材聚类: {th_state}（{th.get('themes', 0)} 个题材 / "
                f"覆盖 {th.get('covered_codes', 0)} 只涨停股）\n"
            )
            lhb = dq.get("lhb") or {}
            bs = dq.get("board_strength") or {}
            txt.insert(tk.END,
                f"  龙虎榜: {'已加载' if lhb.get('loaded') else '未启用'} "
                f"({lhb.get('rows', 0)} 只)   "
                f"板块强度: {'已加载' if bs.get('loaded') else '未启用'} "
                f"({bs.get('rows', 0)} 个)\n"
            )
            sent = dq.get("sentiment") or {}
            sent_disp = (
                f"{sent.get('score')}/100 {sent.get('label', '')}"
                if sent.get("loaded") else "未加载"
            )
            if sent.get("degraded"):
                sent_disp += "  ⚠降级"
            txt.insert(tk.END, f"  市场情绪: {sent_disp}\n")
            warnings = dq.get("warnings") or []
            if warnings:
                txt.insert(tk.END, f"\n  ⚠ 注意 ({len(warnings)} 条):\n")
                for w in warnings:
                    txt.insert(tk.END, f"    · {w}\n")

        # 兼容旧结果：若存在画像字段则仍展示
        if profile:
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, "  涨停前兆画像（T-1日特征统计）\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for key, label in _PROFILE_LABELS.items():
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
            txt.insert(tk.END, f"  反包候选 TOP10\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for rec in wrap_list[:10]:
                chg = rec.get("change_pct")
                chg_text = f"{chg:.1f}%" if chg is not None else "-"
                gap = rec.get("wrap_gap_pct")
                gap_text = f"差{gap:.1f}%" if gap is not None else "-"
                txt.insert(tk.END,
                    f"  {rec['code']} {rec.get('name', ''):6s}  涨{chg_text:6s} {gap_text:7s}  "
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
        self.summary_text.config(state=tk.DISABLED)

        # 保存原始 4 类候选，供筛选实时重渲染
        self.lists = {
            "cont": cont_list, "first": first_list, "fresh": fresh_list,
            "wrap": wrap_list,
        }
        self.compare_context = compare_context

        # 刷新行业下拉选项
        self._refresh_industry_options()

        # 渲染 4 个候选表（应用当前筛选）
        self._render_trees()

        # 同步刷新历史记录下拉，并选中当前结果对应的日期
        self._refresh_history_dates(select=current_date or None)

        # 异步刷新命中率统计 + 触发未回填日期的回填
        self._refresh_accuracy_async(current_date)

        # 启动后台预热：把候选股分时和详情 payload 缓起来，方便用户后续秒开
        self._start_prewarm(result)

        # 更新数据源指示标签
        predict_date = str(result.get("trade_date") or result.get("today_date") or "").strip()
        self._refresh_data_source_label(predict_date)

        # 冷启动检测：保留涨停有数据但其他 3 类全空，通常是本地历史K线缓存还没预热
        # （这些类目都依赖 65 日 K 线评分，cache_only 模式拿不到就直接被过滤）
        if (
            len(cont_list) > 0
            and len(first_list) == 0
            and len(fresh_list) == 0
            and len(wrap_list) == 0
        ):
            messagebox.showwarning(
                "历史数据未就绪",
                "本地历史 K 线缓存尚未预热，\n"
                "「二波接力 / 首板涨停 / 断板反包」候选暂时为空。\n\n"
                "首次点击会触发后台缓存预取，\n"
                "请稍等几秒后再次点击「预测涨停数据」按钮，\n"
                "即可看到完整候选列表。",
                parent=self.app.root,
            )

    # ============== 后台预热：分时 + 详情 payload 缓存 ==============
    def _start_prewarm(self, result: Dict[str, Any]) -> None:
        """预测完成后，对所有候选股票预热分时 + 详情 payload，让后续点击秒开。

        - 自动去重 5 类候选的所有 code
        - 串行执行：先分时，再详情 payload
        - 每个任务内部并发 4 worker，可被新预测/取消令牌打断
        """
        if not isinstance(result, dict):
            return
        # 旧的预热在跑就先取消（新预测意味着新的候选集）
        old = self.prewarm_thread
        old_token = self.prewarm_token
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
            lookback = max(1, min(int(self.lookback_var.get().strip() or "5"), 5))
        except (ValueError, AttributeError, tk.TclError):
            lookback = 5
        codes_list = sorted(codes)
        # 按分数降序取 top-N 做上层 payload 预热，N 与 LRU 上限对齐避免互相挤掉
        top_codes_for_payload = [
            c for c, _ in sorted(
                code_best_score.items(), key=lambda kv: (-kv[1], kv[0]),
            )
        ][: DetailTab._DETAIL_CACHE_MAX]
        thread, token = self.app._start_background_job(
            self._run_prewarm,
            name="predict-prewarm",
            args=(codes_list, lookback, top_codes_for_payload),
        )
        self.prewarm_thread = thread
        self.prewarm_token = token

    def _run_prewarm(
        self, codes: List[str], lookback: int,
        top_codes_for_payload: List[str],
        cancel_token: CancelToken,
    ) -> None:
        """预热 worker：分时 → 上层 payload (top-N，写 GUI LRU)。"""
        total = len(codes)
        if total == 0:
            return
        fetcher = self.app.stock_filter.fetcher

        def _report_intraday(done: int, n: int, _code: str) -> None:
            msg = f"预热分时 {done}/{n} · 详情待跑"
            self.app._post_to_ui(lambda m=msg: self.app.status_var.set(m))

        try:
            self.app._post_to_ui(
                lambda: self.app.status_var.set(f"预热缓存启动：分时 0/{total}")
            )
            intraday_stat = fetcher.prewarm_intraday_for_codes(
                codes,
                ndays=max(1, min(int(lookback), 5)),
                max_workers=4,
                cancel_check=lambda: cancel_token.is_cancelled(),
                progress_cb=_report_intraday,
            )
            if cancel_token.is_cancelled():
                self.app._post_to_ui(lambda: self.app.status_var.set("预热已取消"))
                return
            # 底层缓存就绪后，对 top-N 跑完整 get_stock_detail + get_stock_intraday，
            # 写到 GUI LRU 缓存 —— 用户点击 top-N 候选直接秒开
            payload_stat = self._prewarm_upper_payloads(
                top_codes_for_payload, cancel_token,
            )
            summary = (
                f"预热完成 · 分时 {intraday_stat['done']-intraday_stat['failed']}/{intraday_stat['total']}"
                f"，详情payload {payload_stat['done']-payload_stat['failed']}/{payload_stat['total']}"
            )
            self.app._post_to_ui(lambda s=summary: self.app.status_var.set(s))
        except Exception as exc:
            err = str(exc)
            self.app._post_to_ui(lambda e=err: self.app.status_var.set(f"预热失败: {e}"))

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
        self.app._post_to_ui(
            lambda n=total: self.app.status_var.set(f"预热详情 payload 0/{n}")
        )

        def _one(code: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
            if cancel_token.is_cancelled():
                return code, None, None
            detail = None
            intraday = None
            try:
                detail = self.app.stock_filter.get_stock_detail(code)
            except Exception:
                detail = None
            if cancel_token.is_cancelled():
                return code, detail, None
            try:
                intraday = self.app.stock_filter.get_stock_intraday(code, day_offset=0)
            except Exception:
                intraday = None
            return code, detail, intraday

        def _write_to_lru(code: str, detail, intraday) -> None:
            now = time.time()
            if isinstance(detail, dict):
                self.app.detail.payload_cache[code] = (now, detail)
                self.app.detail.payload_cache.move_to_end(code)
                while len(self.app.detail.payload_cache) > self.app.detail._DETAIL_CACHE_MAX:
                    self.app.detail.payload_cache.popitem(last=False)
            if isinstance(intraday, dict):
                key = (code, 0, "")
                self.app.intraday.payload_cache[key] = (now, intraday, True)
                self.app.intraday.payload_cache.move_to_end(key)
                while len(self.app.intraday.payload_cache) > self.app.intraday._INTRADAY_CACHE_MAX:
                    self.app.intraday.payload_cache.popitem(last=False)

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
                    self.app._post_to_ui(
                        lambda c=code, d=detail, i=intraday: _write_to_lru(c, d, i),
                    )
                stat["done"] += 1
                self.app._post_to_ui(
                    lambda done=stat["done"], n=total:
                    self.app.status_var.set(f"预热详情 payload {done}/{n}")
                )
        return stat


    # ============== 候选筛选与表格渲染 ==============
    def _refresh_industry_options(self) -> None:
        """根据当前 5 类候选，刷新行业下拉选项。"""
        industries: set = set()
        for lst in (self.lists or {}).values():
            for rec in lst or []:
                ind = (rec.get("industry") or "").strip()
                if ind:
                    industries.add(ind)
        values = ("全部",) + tuple(sorted(industries))
        try:
            self.filter_industry_combo.configure(values=values)
        except Exception:
            return
        if self.filter_industry.get() not in values:
            self.filter_industry.set("全部")

    def _reset_filters(self) -> None:
        self.filter_min_score.set(0)
        self.filter_keyword.set("")
        self.filter_industry.set("全部")
        self.filter_lhb_only.set(False)
        self.filter_northbound_only.set(False)
        self.filter_theme_only.set(False)
        self._render_trees()

    def _on_filter_changed(self) -> None:
        """筛选条件变化时重渲染表格（不重跑预测）。"""
        self._render_trees()

    def _on_sort_mode_changed(self) -> None:
        """切换"按历史命中段排序"开关：清空缓存并重排 5 类列表。"""
        # 切换模式时强制重新读取（lookback 内最新累计的命中数据可能已变化）
        self.bucket_rates_cache = {}
        if self.result:
            self._apply_result(self.result)

    def _matches_filters(self, rec: Dict[str, Any]) -> bool:
        """记录是否通过当前筛选条件。"""
        try:
            min_score = int(self.filter_min_score.get() or 0)
        except (TypeError, ValueError, tk.TclError):
            min_score = 0
        if int(rec.get("score") or 0) < min_score:
            return False

        # 顶部全局价格过滤（与扫描/涨停对比保持一致）
        try:
            min_price_raw = (self.app.min_price_var.get() or "").strip()
            min_price = float(min_price_raw) if min_price_raw else None
        except (ValueError, AttributeError, tk.TclError):
            min_price = None
        try:
            max_price_raw = (self.app.max_price_var.get() or "").strip()
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
            allowed_boards = {str(b).strip() for b in self.app._selected_boards() if str(b).strip()}
        except Exception:
            allowed_boards = set()
        if allowed_boards:
            code = str(rec.get("code", "")).strip().zfill(6)
            board = self.app._infer_board_from_code(code)
            if board not in allowed_boards:
                return False

        kw = (self.filter_keyword.get() or "").strip().lower()
        if kw:
            haystack = " ".join(str(rec.get(f, "") or "") for f in
                                ("code", "name", "industry", "reasons", "predict_type"))
            if kw not in haystack.lower():
                return False

        ind_filter = (self.filter_industry.get() or "全部").strip()
        if ind_filter and ind_filter != "全部":
            if (rec.get("industry") or "").strip() != ind_filter:
                return False

        ctx = self.compare_context or {}
        code = (rec.get("code") or "").strip().zfill(6)

        if self.filter_lhb_only.get():
            lhb = (ctx.get("lhb_map") or {}).get(code)
            if not lhb or float((lhb or {}).get("net_buy") or 0) <= 0:
                return False

        if self.filter_northbound_only.get():
            nb = (ctx.get("northbound_map") or {}).get(code, 0)
            if not isinstance(nb, (int, float)) or nb < 200:
                return False

        if self.filter_theme_only.get():
            theme_map = ctx.get("code_theme_map") or {}
            industry_heat = ctx.get("industry_theme_heat") or {}
            in_theme = code in theme_map
            ind_heat = industry_heat.get((rec.get("industry") or ""), 0)
            if not in_theme and ind_heat < 2:
                return False

        return True

    def _filter_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [r for r in (records or []) if self._matches_filters(r)]

    def _render_trees(self) -> None:
        """根据当前筛选条件渲染 5 个候选表。"""
        if not self.lists:
            return

        cont_list = self._filter_records(self.lists.get("cont", []))
        first_list = self._filter_records(self.lists.get("first", []))
        fresh_list = self._filter_records(self.lists.get("fresh", []))
        wrap_list = self._filter_records(self.lists.get("wrap", []))

        # 取本次预测对应日期的命中结果（若已回填）
        results_map = self.results_map or {}

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
        self.cont_tree.delete(*self.cont_tree.get_children())
        for rec in cont_list:
            res_text, hit_tag = _result_cell("cont", rec.get("code", ""))
            tag = self._row_tag("cont", hit_tag, rec.get("score", 0))
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
            self.cont_tree.insert("", tk.END, values=vals, tags=(tag,))

        # ---- 填充首板候选表格 ----
        self.first_tree.delete(*self.first_tree.get_children())
        for rec in first_list:
            res_text, hit_tag = _result_cell("first", rec.get("code", ""))
            tag = self._row_tag("first", hit_tag, rec.get("score", 0))
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
            self.first_tree.insert("", tk.END, values=vals, tags=(tag,))

        # ---- 填充首板涨停候选表格 ----
        self.fresh_tree.delete(*self.fresh_tree.get_children())
        for rec in fresh_list:
            res_text, hit_tag = _result_cell("fresh", rec.get("code", ""))
            tag = self._row_tag("fresh", hit_tag, rec.get("score", 0))
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
            self.fresh_tree.insert("", tk.END, values=vals, tags=(tag,))

        # ---- 填充反包候选表格 ----
        self.wrap_tree.delete(*self.wrap_tree.get_children())
        _PATTERN_LABELS = {"wrap": "断板反包"}
        for rec in wrap_list:
            res_text, hit_tag = _result_cell("wrap", rec.get("code", ""))
            tag = self._row_tag("wrap", hit_tag, rec.get("score", 0))
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
            self.wrap_tree.insert("", tk.END, values=vals, tags=(tag,))

        # 更新 Tab 标题：显示「筛选后/总数」
        raw = self.lists or {}
        total_cont = len(raw.get("cont", []))
        total_first = len(raw.get("first", []))
        total_fresh = len(raw.get("fresh", []))
        total_wrap = len(raw.get("wrap", []))
        def _label(name: str, shown: int, total: int) -> str:
            return f"{name}({shown}/{total})" if shown != total else f"{name}({total})"
        self.table_nb.tab(0, text=_label("保留涨停候选", len(cont_list), total_cont))
        self.table_nb.tab(1, text=_label("二波接力候选", len(first_list), total_first))
        self.table_nb.tab(2, text=_label("首板涨停候选", len(fresh_list), total_fresh))
        self.table_nb.tab(3, text=_label("反包候选", len(wrap_list), total_wrap))

        shown_total = len(cont_list) + len(first_list) + len(fresh_list) + len(wrap_list)
        raw_total = total_cont + total_first + total_fresh + total_wrap
        if shown_total != raw_total:
            self.filter_count_label.config(text=f"筛选后 {shown_total}/{raw_total}")
        else:
            self.filter_count_label.config(text=f"共 {raw_total} 只")

        self.status_label.config(text="")
        self.app.status_var.set(
            f"涨停预测完成: 保留涨停{total_cont} / 二波接力{total_first} / "
            f"首板{total_fresh} / 反包{total_wrap}"
        )

    def _on_stock_select(self, event):
        tree = event.widget
        sel = tree.selection()
        if not sel:
            return
        vals = tree.item(sel[0], "values")
        if vals:
            code = str(vals[0]).strip().zfill(6)
            self.app.status_var.set(f"预测候选: {code} {vals[1] if len(vals) > 1 else ''}")

    def _on_stock_double_click(self, event):
        tree = event.widget
        stock_code = self.app._get_tree_selected_code(tree)
        if not stock_code:
            return
        self.app.detail._cancel_scheduled()
        self.app.detail.show(stock_code, force_refresh=True)
        self.app.notebook.select(self.app.detail.frame)

    # ============== 涨停预测准确率（命中对比） ==============
    def _refresh_accuracy_async(self, current_date: str = "") -> None:
        """后台刷新 5 个 tab 的命中率标签 + 触发待回填的准确率。

        两阶段：
        1) 先用数据库里已有数据 query 一遍，立刻 push 到 UI（秒级，避免开 app 先看见一片 "-"）
        2) 再跑 evaluate_all_pending 把 T+1 已就绪但未评估的日期补上，完了再 query 一次
           push 一次，让 UI 静默刷新成最新数据
        """
        def _query_and_push():
            try:
                stats = prediction_accuracy_service.query_category_stats(lookback_dates=20)
            except Exception:
                stats = {}
            try:
                stats_yesterday = prediction_accuracy_service.query_category_stats_yesterday()
            except Exception:
                stats_yesterday = {}
            results_map = {}
            if current_date:
                try:
                    results_map = prediction_accuracy_service.get_per_code_results(current_date)
                except Exception:
                    results_map = {}
            bucket_rates_by_cat: Dict[str, Dict[Tuple[int, int], Dict[str, Any]]] = {}
            for cat in (
                "cont", "first", "fresh", "wrap",
                "cont_1to2", "cont_2to3", "cont_3to4", "cont_4to5", "cont_5plus",
            ):
                try:
                    bucket_rates_by_cat[cat] = prediction_accuracy_service.get_score_bucket_rates(
                        category=cat, lookback_dates=20, min_samples=5,
                    )
                except Exception:
                    bucket_rates_by_cat[cat] = {}
            self.app._post_to_ui(
                lambda s=stats, y=stats_yesterday, m=results_map, br=bucket_rates_by_cat:
                self._apply_accuracy(s, m, y, br)
            )

        def _worker():
            # 阶段 1：秒级用既有数据填 UI
            _query_and_push()
            # 阶段 2：refresh_stale=True 把早上 K 线没到位的"昨日命中率"补齐 + 新日期评估
            try:
                prediction_accuracy_service.evaluate_all_pending(refresh_stale=True)
            except Exception:
                pass
            # 评估完后再 query 一遍，静默把 UI 刷成最新
            _query_and_push()

        threading.Thread(target=_worker, daemon=True).start()

    def _apply_accuracy(
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
            self.bucket_rates_cache = dict(bucket_rates_by_cat)
        else:
            self.bucket_rates_cache = {}
        labels = self.stat_labels or {}
        stats_yesterday = stats_yesterday or {}
        category_names = {
            "cont": "保留涨停", "first": "二波接力", "fresh": "首板涨停",
            "wrap": "反包",
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
                # 子类别走 grid 表格，由独立 (yest_lbl, recent_lbl) 渲染
                yest_lbl, recent_lbl = self.subcategory_stat_labels.get(
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
            try:
                lbl.configure(text=txt)
            except Exception:
                pass

        # 计算每个主类别的"历史最优分数段"并刷新黄色提示标签
        self._refresh_best_bucket_labels()

        # 计算 5 个 cont 子类别各自的"最优分数段"并刷新（含颜色编码）
        self._refresh_subcategory_best_buckets()

        # 当前日期的逐行结果（用于 result 列着色）
        if results_map:
            self.results_map = results_map
            try:
                self._render_trees()
            except Exception:
                pass
        else:
            # 即使没有逐行结果，best_bucket 也可能因数据更新而变化 —— 刷新一次行高亮
            try:
                if self.lists:
                    self._render_trees()
            except Exception:
                pass

    def _find_best_bucket_for_category(
        self, cat: str,
    ) -> Optional[Tuple[Tuple[int, int], Dict[str, Any]]]:
        """通过 `_get_bucket_rates` 读取 cat 的所有 bucket rates，
        返回 eligible=True 中 rate 最大的桶，None 表示无 eligible 桶。
        同 rate 时取分数段更高的（高分往往更稳）。

        统一服务于主类别（cont/first/fresh/wrap）与 cont 子类别
        （cont_1to2/.../cont_5plus）。带 lazy-load 兜底（cache miss 时拉 DB）。
        """
        rates = self._get_bucket_rates(cat)
        best: Optional[Tuple[Tuple[int, int], Dict[str, Any]]] = None
        for bucket, info in rates.items():
            if not info or not info.get("eligible"):
                continue
            if best is None:
                best = (bucket, info)
                continue
            b_rate = float(best[1].get("rate") or 0.0)
            cur_rate = float(info.get("rate") or 0.0)
            if cur_rate > b_rate or (cur_rate == b_rate and bucket[0] > best[0][0]):
                best = (bucket, info)
        return best

    def _refresh_best_bucket_labels(self) -> None:
        """读取近 20 日的分数段命中率，找出每类历史命中率最高的桶并更新标签。

        eligible=True 的桶里挑 rate 最大者；同 rate 时挑分数段更高的（高分往往更稳）。
        样本不足或无回填时显示 "-"。结果同步写入 self.best_buckets，
        供 _render_trees 给落在该段的行打 best_bucket tag。
        """
        category_display = {
            "cont": "保留涨停", "first": "二波接力", "fresh": "首板涨停",
            "wrap": "反包",
        }
        best_map: Dict[str, Optional[Tuple[int, int]]] = {}
        for cat in ("cont", "first", "fresh", "wrap"):
            best = self._find_best_bucket_for_category(cat)
            best_bucket = best[0] if best else None
            best_info = best[1] if best else None
            best_map[cat] = best_bucket
            lbl = self.best_bucket_labels.get(cat)
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
        self.best_buckets = best_map

    def _refresh_subcategory_best_buckets(self) -> None:
        """刷新 5 个 cont 子类别（cont_1to2/.../cont_5plus）顶部的"最优分数段"Label。
        使用 self.bucket_rates_cache 里 worker 预取的 rates。
        按 rate 着色：≥40 绿，25-40 黄，<25 红，无数据/不足灰。
        """
        labels = self.subcategory_best_labels or {}
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


    # ============== 命中对比窗口 ==============
    def open_compare_window(self) -> None:
        """弹窗：今日实际涨停 与 上次预测候选 的命中对比。"""
        # 默认对比"当前选中预测日期"或"最新预测"
        trade_date = (self.history_var.get() or "").strip()
        if not trade_date:
            trade_date = (self.date_var.get() or "").strip()
        if not trade_date:
            messagebox.showinfo("命中对比", "请先选择历史预测日期或先执行一次预测。",
                                parent=self.app.root)
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
                                 parent=self.app.root)
            return

        if not data.get("candidates"):
            verify_date = data.get("verify_date") or "—"
            messagebox.showinfo(
                "命中对比",
                f"{trade_date} 的预测候选尚未回填准确率。\n"
                f"验证日: {verify_date}\n\n"
                f"请确认 T+1 已收盘，且本地 K 线已同步到该日期。",
                parent=self.app.root,
            )
            return

        self._build_compare_window(data)

    def _build_compare_window(self, data: Dict[str, Any]) -> None:
        win = tk.Toplevel(self.app.root)
        win.title(f"命中对比 - {data.get('trade_date', '')} → {data.get('verify_date', '')}")
        win.geometry("1100x640")
        win.transient(self.app.root)

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
            messagebox.showerror("命中对比", f"刷新失败: {exc}", parent=self.app.root)
            return
        if not data.get("candidates"):
            messagebox.showinfo("命中对比", "暂无可对比数据", parent=self.app.root)
            return
        self._build_compare_window(data)

    def _export_compare_csv(self, data: Dict[str, Any]) -> None:
        td = data.get("trade_date", "")
        path = filedialog.asksaveasfilename(
            parent=self.app.root,
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
            self.app.status_var.set(f"已导出对比CSV: {path}")
        except Exception as exc:
            messagebox.showerror("导出失败", str(exc), parent=self.app.root)

    # ============== 策略分析（分数段 / 行业 / 失败归因） ==============
    def open_strategy_window(self) -> None:
        """弹窗：分数段命中率柱状图 / 行业命中率排行 / 失败归因分布。"""
        win = tk.Toplevel(self.app.root)
        win.title("策略分析 - 命中率多维度复盘")
        win.geometry("1080x680")
        win.transient(self.app.root)

        # 顶部控制栏
        top = ttk.Frame(win, padding=(10, 8))
        top.pack(fill=tk.X)
        ttk.Label(top, text="回看交易日:").pack(side=tk.LEFT)
        lookback_var = tk.IntVar(value=20)
        ttk.Spinbox(top, from_=5, to=120, increment=5, width=5,
                    textvariable=lookback_var).pack(side=tk.LEFT, padx=(2, 12))

        ttk.Label(top, text="类别:").pack(side=tk.LEFT)
        category_var = tk.StringVar(value="全部")
        cat_options = ["全部", "保留涨停", "二波接力", "首板涨停", "反包"]
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
            "首板涨停": "fresh", "反包": "wrap",
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
