"""股票详情 Tab：历史摘要 + K 线图。

包含：
- ttk.Frame 容器（self.frame）
- 历史摘要折叠区（labels / label_caption_vars）
- matplotlib Figure / 2 个 axes（price / volume）
- 滑块控制 K 线窗口
- K 线点击/拖拽事件（触发分时 tab 跳转）
- 数据加载/缓存

跨 tab 引用走 self.app.xxx：
- self.app.notebook
- self.app.status_var
- self.app.stock_filter
- self.app._ui / _post_to_ui
- self.app._log / _log_async
- self.app._set_top_header_for_code / _clear_top_header
- self.app.intraday.open_view_with_offset（K 线点击跳分时）
- self.app.history_source_var（全局数据源偏好留在 App）
"""
from __future__ import annotations

import logging
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import messagebox, ttk

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.ticker import FuncFormatter

from src.services.holding_analysis_service import analyze_holding
from src.utils.cancel_token import CancelToken

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from src.gui.app import StockMonitorApp


class DetailTab:
    """股票详情 tab：K 线 + 摘要。"""

    # 详情 payload 的 GUI 层 LRU 缓存上限
    _DETAIL_CACHE_MAX = 20
    # LRU 命中后多久内视为新鲜（盘中/盘后兼顾）
    _DETAIL_CACHE_TTL_SEC = 120.0
    # stale-while-revalidate 节流：同一只票 N 秒内只触发一次后台 revalidate
    _DETAIL_REVALIDATE_THROTTLE_SEC = 5.0

    def __init__(self, app: "StockMonitorApp", notebook: ttk.Notebook) -> None:
        self.app = app
        # 状态字段（原 self._detail_* 和 self._current_detail_code）
        self.request_code: str = ""
        self.loading_code: str = ""
        self.after_id = None
        self.current_code: str = ""  # 原 _current_detail_code
        self.chart_dates: List[str] = []
        self.chart_window_size: int = 60
        self.chart_window_start: int = 0
        self.chart_history = None
        self.chart_analysis: Dict[str, Any] = {}
        self.chart_scroll_bound: bool = False
        self.chart_slider_updating: bool = False
        self.chart_dragging: bool = False
        self.chart_drag_moved: bool = False
        self.chart_drag_start_x: float = 0.0
        self.chart_drag_start_window: int = 0
        self.chart_click_target_date: str = ""
        self.chart_loading_more: bool = False
        self.chart_loaded_days: int = 0
        self.summary_expanded: bool = False
        self.chart_expanded: bool = True
        self.current_detail_payload: Dict[str, Any] = {}
        self.payload_cache: "OrderedDict[str, Tuple[float, Dict[str, Any]]]" = OrderedDict()
        self.last_revalidate_ts: Dict[str, float] = {}
        self._build(notebook)

    def _build(self, notebook: ttk.Notebook) -> None:
        """构建 widget。从原 setup_detail_tab 整体迁移。"""
        detail_frame = ttk.Frame(notebook, padding="5")
        notebook.add(detail_frame, text="股票详情")
        self.frame = detail_frame

        info_header = ttk.Frame(detail_frame)
        info_header.pack(fill=tk.X, pady=(5, 2))
        self.info_header = info_header
        self.summary_toggle_btn = ttk.Button(
            info_header,
            text="展开历史摘要",
            command=self.toggle_summary_section,
        )
        self.summary_toggle_btn.pack(side=tk.LEFT)
        self.holding_analysis_btn = ttk.Button(
            info_header,
            text="持有分析",
            command=self.show_holding_analysis,
        )
        self.holding_analysis_btn.pack(side=tk.LEFT, padx=(8, 0))
        self.summary_status_var = tk.StringVar(value="历史摘要已收起")
        ttk.Label(info_header, textvariable=self.summary_status_var).pack(side=tk.LEFT, padx=10)

        self.info_frame = ttk.LabelFrame(detail_frame, text="历史摘要", padding="10")
        self.info_frame.pack(fill=tk.X, pady=5)

        self.labels: Dict[str, ttk.Label] = {}
        self.label_caption_vars: Dict[str, tk.StringVar] = {}
        items = [
            ("code", "股票代码"),
            ("name", "股票名称"),
            ("industry", "行业"),
            ("latest_date", "最新日期"),
            ("quote_time", "刷新时间"),
            ("latest_close", "最新收盘"),
            ("score", "综合评分"),
            ("latest_ma", f"MA{max(1, int(self.app.stock_filter.ma_period))}"),
            ("latest_ma10", "MA10"),
            ("latest_volume", "成交量"),
            ("latest_amount", "成交额"),
            ("five_day_return", "5日涨幅"),
            ("limit_up", "涨停"),
            ("volume_expand", "放量"),
            ("volume_expand_ratio", "放量倍数"),
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
            self.label_caption_vars[key] = caption_var
            ttk.Label(self.info_frame, textvariable=caption_var).grid(row=row, column=col, padx=5, pady=5, sticky=tk.E)
            self.labels[key] = ttk.Label(self.info_frame, text="-", width=30)
            self.labels[key].grid(row=row, column=col + 1, padx=5, pady=5, sticky=tk.W)

        self.info_frame.pack_forget()

        chart_header = ttk.Frame(detail_frame)
        chart_header.pack(fill=tk.X, pady=(6, 2))
        self.chart_header = chart_header
        self.chart_toggle_btn = ttk.Button(
            chart_header,
            text="收起历史K线",
            command=self.toggle_chart_section,
        )
        self.chart_toggle_btn.pack(side=tk.LEFT)
        self.chart_status_var = tk.StringVar(value="历史K线已展开")
        ttk.Label(chart_header, textvariable=self.chart_status_var).pack(side=tk.LEFT, padx=10)

        self.chart_frame = ttk.LabelFrame(detail_frame, text="K线图", padding="5")
        self.chart_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.chart_body = ttk.Frame(self.chart_frame)
        self.chart_body.pack(fill=tk.BOTH, expand=True)

        self.fig, (self.price_ax, self.volume_ax) = plt.subplots(
            2,
            1,
            figsize=(12.8, 8.4),
            sharex=True,
            gridspec_kw={"height_ratios": [5.0, 1.5]},
        )
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.chart_body)
        canvas_widget = self.canvas.get_tk_widget()
        canvas_widget.pack(fill=tk.BOTH, expand=True)
        self.canvas.mpl_connect("button_press_event", self.on_chart_click)
        self.canvas.mpl_connect("scroll_event", self.on_chart_scroll)
        self.canvas.mpl_connect("motion_notify_event", self.on_chart_drag_motion)
        self.canvas.mpl_connect("button_release_event", self.on_chart_drag_release)
        self._bind_chart_scroll(canvas_widget)

        slider_row = ttk.Frame(self.chart_body)
        slider_row.pack(fill=tk.X, pady=(6, 0))
        ttk.Label(slider_row, text="左右滑动").pack(side=tk.LEFT)
        self.chart_window_var = tk.DoubleVar(value=0.0)
        self.chart_window_scale = tk.Scale(
            slider_row,
            orient=tk.HORIZONTAL,
            from_=0,
            to=0,
            resolution=1,
            showvalue=False,
            variable=self.chart_window_var,
            command=self.on_chart_window_changed,
            length=480,
        )
        self.chart_window_scale.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=8)
        self.chart_window_label_var = tk.StringVar(value="窗口: -")
        ttk.Label(slider_row, textvariable=self.chart_window_label_var).pack(side=tk.RIGHT)

        self.chart_placeholder = None
        self._apply_section_visibility()

    # ======================== 元数据/标签刷新 ========================

    def refresh_metric_labels(self) -> None:
        """根据 stock_filter 的 MA / 成交量参数刷新表头文案。被 App 在 settings 变化时调用。"""
        ma_period = max(1, int(self.app.stock_filter.ma_period))
        if "latest_ma" in self.label_caption_vars:
            self.label_caption_vars["latest_ma"].set(f"MA{ma_period}:")
        volume_days = max(1, int(self.app.stock_filter.volume_lookback_days))
        if "latest_volume" in self.label_caption_vars:
            self.label_caption_vars["latest_volume"].set(f"成交量(近{volume_days}日均量占比):")

    # ======================== 调度 / 公开入口 ========================

    def _cancel_scheduled(self) -> None:
        if self.after_id is None:
            return
        try:
            self.app.root.after_cancel(self.after_id)
        except tk.TclError:
            pass
        self.after_id = None

    def _schedule_show(self, stock_code: str, delay_ms: int = 180) -> None:
        code = str(stock_code).strip().zfill(6)
        self._cancel_scheduled()
        if self.app._is_closing:
            return
        try:
            if self.app.root.winfo_exists():
                self.after_id = self.app.root.after(delay_ms, lambda c=code: self.show(c))
        except tk.TclError:
            self.after_id = None

    def _trigger_revalidate(self, code: str) -> None:
        """缓存命中后悄悄拉一次最新详情，5s 节流避免重复。

        新数据回来会通过 _apply_if_current 静默更新面板（用户无感）。
        如果用户已切到别的票，过滤掉过期请求。
        """
        c = str(code or "").strip().zfill(6)
        if not c:
            return
        last = self.last_revalidate_ts.get(c, 0.0)
        if (time.time() - last) < self._DETAIL_REVALIDATE_THROTTLE_SEC:
            return
        self.last_revalidate_ts[c] = time.time()
        try:
            self.app._start_background_job(
                self._load, name=f"detail-revalidate-{c}", args=(c,),
            )
        except Exception as exc:  # noqa: BLE001
            self.app._log(f"详情后台刷新启动失败 {c}: {exc}")

    def show(self, stock_code: str, force_refresh: bool = False):
        code = str(stock_code).strip().zfill(6)
        self._cancel_scheduled()
        self.request_code = code
        self._show_loading(code)
        # 进入详情前先用扫描结果中的名称占位（若有）
        prefilled_name = ""
        for result in self.app.result.filtered_stocks:
            if str(result.get("code", "")).strip().zfill(6) == code:
                prefilled_name = str(result.get("name", "") or "")
                break
        self.app._set_top_header_for_code(code, prefilled_name)

        # ---- GUI 层 LRU 缓存命中：反复点击同一只股票秒开 ----
        if not force_refresh:
            cached = self.payload_cache.get(code)
            if cached is not None:
                ts, payload = cached
                if (time.time() - ts) < self._DETAIL_CACHE_TTL_SEC:
                    self.payload_cache.move_to_end(code)
                    try:
                        self._update_ui(payload)
                        age = int(time.time() - ts)
                        self.app.status_var.set(f"{code} 详情（内存缓存 {age}s，正在后台刷新）")
                        self.loading_code = ""
                        # stale-while-revalidate：后台再拉一次，新数据回来静默替换
                        self._trigger_revalidate(code)
                        return
                    except Exception as e:
                        self.app._log(f"渲染内存缓存详情失败，回退到完整加载: {e}")

        detail_payload = None
        for result in self.app.result.filtered_stocks:
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
            self.app._log(f"显示扫描结果中的股票 {code} 详情。")
            try:
                self._update_ui(detail_payload)
            except Exception as e:
                self.app._log(f"渲染缓存详情失败: {e}")
                self._show_error(code, f"渲染详情失败: {e}")
            if not force_refresh:
                # 扫描快照可能是几分钟到几小时前拉的，先秒开再后台 revalidate，
                # 让用户既快又新（盘后场景下尤其重要 —— 14:50 扫描的快照在
                # 15:00 收盘后应当被替换为收盘价）
                self.app.status_var.set(f"{code} 详情（扫描快照，正在后台刷新）")
                self.loading_code = ""
                self._trigger_revalidate(code)
                return
            self.app.status_var.set(f"正在刷新 {code} 最新详情...")
            self.loading_code = code
            self.app._start_background_job(
                self._load, name=f"detail-{code}", args=(code,)
            )
            return

        if not force_refresh and self.loading_code == code:
            self.app.status_var.set(f"{code} 详情正在加载...")
            return

        self.app._log(f"查询股票 {code} 的历史详情...")
        self.app.status_var.set(f"正在查询 {code}...")
        self.loading_code = code
        self.app._start_background_job(
            self._load, name=f"detail-{code}", args=(code,)
        )

    def _load(self, stock_code: str, cancel_token: CancelToken):
        try:
            if cancel_token.is_cancelled():
                return
            quick_detail = self.app.stock_filter.get_stock_detail_quick(stock_code)
            quick_history = None
            if isinstance(quick_detail, dict):
                quick_history = quick_detail.get("history")
                if quick_history is not None and not getattr(quick_history, "empty", True):
                    self.app._post_to_ui(lambda: self._apply_quick_if_current(stock_code, quick_detail))

            if cancel_token.is_cancelled():
                return
            detail = self.app.stock_filter.get_stock_detail(stock_code, preloaded_history=quick_history)
            if cancel_token.is_cancelled():
                return
            self.app._post_to_ui(lambda: self._apply_if_current(stock_code, detail))
        except Exception as e:
            error_text = str(e)
            self.app._post_to_ui(lambda: self.app._log(f"查询详情出错: {error_text}"))
            self.app._post_to_ui(lambda: self._show_error(stock_code, f"详情加载失败: {error_text}"))
        finally:
            self.app._post_to_ui(lambda: self._finish_status(stock_code))

    def _apply_quick_if_current(self, stock_code: str, detail: Dict[str, Any]) -> None:
        if str(stock_code).strip().zfill(6) != self.request_code:
            return
        try:
            self._update_ui(detail)
            self.app.status_var.set(f"{stock_code} 详情（历史缓存）")
        except Exception as e:
            self.app._log(f"更新缓存详情失败: {e}")

    def _apply_if_current(self, stock_code: str, detail: Dict[str, Any]) -> None:
        code = str(stock_code).strip().zfill(6)
        if code != self.request_code:
            return
        try:
            self._update_ui(detail)
        except Exception as e:
            self.app._log(f"更新详情面板失败: {e}")
            self._show_error(stock_code, f"详情渲染失败: {e}")
            return
        # 写入 LRU 缓存：下次点击该股票走秒开路径
        try:
            self.payload_cache[code] = (time.time(), detail)
            self.payload_cache.move_to_end(code)
            while len(self.payload_cache) > self._DETAIL_CACHE_MAX:
                self.payload_cache.popitem(last=False)
        except Exception as exc:
            logger.debug("详情 LRU 缓存写入失败 %s: %s", code, exc)

    def _finish_status(self, stock_code: str) -> None:
        code = str(stock_code).strip().zfill(6)
        if code == self.loading_code:
            self.loading_code = ""
        if code != self.request_code:
            return
        self.app.status_var.set("查询完成")

    # ======================== loading / error 占位 ========================

    def _show_loading(self, stock_code: str) -> None:
        self.chart_dates = []
        self.chart_history = None
        self.chart_analysis = {}
        self.current_detail_payload = {}
        self.chart_window_start = 0
        self.chart_loaded_days = 0
        self.chart_loading_more = False
        placeholders = {
            "code": str(stock_code).strip().zfill(6),
            "name": "加载中...",
            "industry": "加载中...",
            "latest_date": "加载中...",
            "quote_time": "加载中...",
            "latest_close": "加载中...",
            "score": "加载中...",
            "latest_ma": "加载中...",
            "latest_ma10": "加载中...",
            "latest_volume": "加载中...",
            "latest_amount": "加载中...",
            "five_day_return": "加载中...",
            "limit_up": "加载中...",
            "summary": "正在加载详情数据...",
        }
        for key, value in placeholders.items():
            if key in self.labels:
                self.labels[key].config(text=value)
        self.price_ax.clear()
        self.volume_ax.clear()
        self.price_ax.text(0.5, 0.5, "正在加载详情...", ha="center", va="center", fontsize=14)
        self.volume_ax.text(0.5, 0.5, "请稍候", ha="center", va="center", fontsize=11)
        self.price_ax.set_axis_off()
        self.volume_ax.set_axis_off()
        self.canvas.draw()

    def _show_error(self, stock_code: str, message: str) -> None:
        self.chart_dates = []
        self.chart_history = None
        self.chart_analysis = {}
        self.current_detail_payload = {}
        self.chart_window_start = 0
        self.chart_loaded_days = 0
        self.chart_loading_more = False
        code_text = str(stock_code).strip().zfill(6)
        if "code" in self.labels:
            self.labels["code"].config(text=code_text)
        if "name" in self.labels:
            self.labels["name"].config(text="加载失败")
        if "score" in self.labels:
            self.labels["score"].config(text="-")
        if "summary" in self.labels:
            self.labels["summary"].config(text=message or "详情加载失败")
        self.price_ax.clear()
        self.volume_ax.clear()
        self.price_ax.text(0.5, 0.5, "详情加载失败", ha="center", va="center", fontsize=14, color="#b22222")
        self.volume_ax.text(0.5, 0.5, "请查看运行日志", ha="center", va="center", fontsize=11)
        self.price_ax.set_axis_off()
        self.volume_ax.set_axis_off()
        self.canvas.draw()

    # ======================== UI 刷新 ========================

    def _update_ui(self, detail: Dict[str, Any]):
        analysis = detail.get("analysis") or {}
        history = detail.get("history")
        self.current_code = str(detail.get("code", "") or "").strip().zfill(6)
        self.current_detail_payload = dict(detail)
        self.chart_loading_more = False
        self.refresh_metric_labels()
        self.app._set_top_header_for_code(self.current_code, str(detail.get("name", "") or ""))

        self.labels["code"].config(text=detail.get("code", "-"))
        self.labels["name"].config(text=detail.get("name", "-"))
        industry = str(detail.get("industry", "") or "").strip()
        last_limit_up_trade_date = str(detail.get("last_limit_up_trade_date", "") or "").strip()
        industry_text = industry or "-"
        if industry and last_limit_up_trade_date:
            industry_text = f"{industry} (涨停缓存 {last_limit_up_trade_date})"
        self.labels["industry"].config(text=industry_text)
        self.labels["latest_date"].config(text=analysis.get("latest_date", "-"))
        self.labels["quote_time"].config(text=analysis.get("quote_time", "-") or "-")
        self.labels["latest_close"].config(
            text="-" if analysis.get("latest_close") is None else f"{analysis['latest_close']:.2f}"
        )
        self.labels["score"].config(
            text="-" if analysis.get("score") is None else f"{int(analysis['score'])}"
        )
        self.labels["latest_ma"].config(
            text="-" if analysis.get("latest_ma") is None else f"{analysis['latest_ma']:.2f}"
        )
        self.labels["latest_ma10"].config(
            text="-" if analysis.get("latest_ma10") is None else f"{analysis['latest_ma10']:.2f}"
        )
        latest_volume_text = self.app._format_volume(analysis.get("latest_volume"))
        latest_volume_ratio = analysis.get("latest_volume_ratio")
        if latest_volume_text != "-" and latest_volume_ratio is not None:
            latest_volume_text = f"{latest_volume_text} ({latest_volume_ratio:.1f}%)"
        self.labels["latest_volume"].config(text=latest_volume_text)
        self.labels["latest_amount"].config(text=self.app._format_amount(analysis.get("latest_amount")))
        self.labels["five_day_return"].config(
            text="-" if analysis.get("five_day_return") is None else f"{analysis['five_day_return']:.2f}%"
        )
        self.labels["limit_up"].config(text="是" if analysis.get("limit_up") else "否")
        # 技术指标
        dif = analysis.get("macd_dif")
        dea = analysis.get("macd_dea")
        if dif is not None and dea is not None:
            self.labels["macd"].config(text=f"DIF {dif:.3f} / DEA {dea:.3f}")
        else:
            self.labels["macd"].config(text="-")

        kdj_k = analysis.get("kdj_k")
        kdj_d = analysis.get("kdj_d")
        kdj_j = analysis.get("kdj_j")
        if kdj_k is not None:
            self.labels["kdj"].config(text=f"K {kdj_k:.1f} / D {kdj_d:.1f} / J {kdj_j:.1f}")
        else:
            self.labels["kdj"].config(text="-")

        rsi6 = analysis.get("rsi_6")
        rsi12 = analysis.get("rsi_12")
        if rsi6 is not None:
            self.labels["rsi"].config(text=f"RSI6 {rsi6:.1f} / RSI12 {rsi12:.1f}")
        else:
            self.labels["rsi"].config(text="-")

        boll_upper = analysis.get("boll_upper")
        boll_mid = analysis.get("boll_mid")
        boll_lower = analysis.get("boll_lower")
        if boll_mid is not None:
            self.labels["boll"].config(text=f"U {boll_upper:.2f} / M {boll_mid:.2f} / L {boll_lower:.2f}")
        else:
            self.labels["boll"].config(text="-")

        self.labels["summary"].config(text=analysis.get("summary", "-"))

        self._draw_chart(history, analysis)

    # ======================== 持有分析 ========================

    @staticmethod
    def _format_holding_analysis_message(result: Dict[str, Any]) -> str:
        key_levels = result.get("key_levels") or {}
        lines = [
            f"建议：{result.get('advice', '-')}",
            f"风险：{result.get('risk_level', '-')}    评分：{result.get('score', 0)}/100",
            "",
            "关键位：",
        ]
        labels = {
            "latest_close": "最新收盘",
            "ma5": "MA5",
            "ma10": "MA10",
            "five_day_return_pct": "近5日涨幅%",
        }
        has_level = False
        for key, label in labels.items():
            value = key_levels.get(key)
            if value is None:
                continue
            suffix = "%" if key == "five_day_return_pct" else ""
            lines.append(f"- {label}: {value}{suffix}")
            has_level = True
        if not has_level:
            lines.append("- 暂无可用关键位")

        lines.extend(["", "理由："])
        reasons = result.get("reasons") or []
        if reasons:
            lines.extend(f"- {reason}" for reason in reasons)
        else:
            lines.append("- 暂无明确理由")

        summary = str(result.get("summary") or "").strip()
        if summary:
            lines.extend(["", summary])
        return "\n".join(lines)

    def show_holding_analysis(self) -> None:
        payload = self.current_detail_payload or {}
        code = str(payload.get("code") or self.current_code or self.request_code or "").strip().zfill(6)
        history = payload.get("history")
        if not code or history is None or getattr(history, "empty", True):
            messagebox.showwarning(
                "持有分析",
                "请先查询股票，等待详情和K线加载完成后再分析。",
                parent=self.app.root,
            )
            return

        result = analyze_holding(history, payload.get("analysis") or {})
        title_name = str(payload.get("name") or "").strip()
        title = f"持有分析 - {code}"
        if title_name:
            title += f" {title_name}"
        messagebox.showinfo(
            title,
            self._format_holding_analysis_message(result),
            parent=self.app.root,
        )

    # ======================== chart 绘制 ========================

    def _reset_chart_axes(self) -> None:
        self.chart_dates = []
        self.price_ax.clear()
        self.volume_ax.clear()
        self.price_ax.set_axis_on()
        self.volume_ax.set_axis_on()

    def _show_empty_chart(self, message: str) -> None:
        self.chart_history = None
        self.chart_analysis = {}
        self.chart_window_start = 0
        self.chart_window_scale.config(from_=0, to=0, state=tk.DISABLED)
        self.chart_window_var.set(0)
        self.chart_window_label_var.set("窗口: -")
        self.price_ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=14)
        self.canvas.draw()

    def _prepare_chart_dataset(self, history, analysis):
        if history is None or getattr(history, "empty", True):
            return None

        df = history.copy()
        if "date" in df.columns:
            df = df.sort_values("date").reset_index(drop=True)
        else:
            df = df.reset_index(drop=True)
        if df.empty:
            return None

        self.chart_history = df
        self.chart_analysis = dict(analysis or {})
        self.chart_loaded_days = max(self.chart_loaded_days, len(df))

        x = list(range(len(df)))
        dates = df["date"].astype(str).tolist() if "date" in df.columns else [str(i) for i in x]
        self.chart_dates = list(dates)
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

    def _draw_price_panel(self, chart_data: Dict[str, Any]) -> None:
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

        ma_period = max(1, int(self.app.stock_filter.ma_period))
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

    def _draw_volume_panel(self, chart_data: Dict[str, Any]) -> None:
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
                latest_volume_ratio_text = f"  最新 {self.app._format_volume(latest_volume_value)} / 均量 {latest_volume_value / avg_volume * 100.0:.1f}%"
        self.volume_ax.set_ylabel("成\n交\n量", rotation=0, labelpad=14, va="center")
        self.volume_ax.set_title(f"成交量{latest_volume_ratio_text}" if latest_volume_ratio_text else "成交量")
        self.volume_ax.yaxis.set_major_formatter(FuncFormatter(self.app._format_axis_volume))
        self.volume_ax.yaxis.get_offset_text().set_visible(False)
        self.volume_ax.grid(True, alpha=0.2)

    def _resolve_chart_window(self, total: int, keep_window: bool = False) -> Dict[str, int]:
        window = max(15, min(int(self.chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        start = max(0, min(int(self.chart_window_start), max_start)) if keep_window else max_start
        end = min(total, start + window)
        return {"window": window, "max_start": max_start, "start": start, "end": end}

    def _apply_chart_window(self, chart_data: Dict[str, Any], window_meta: Dict[str, int]) -> None:
        x = chart_data["x"]
        dates = chart_data["dates"]
        start = window_meta["start"]
        end = window_meta["end"]
        max_start = window_meta["max_start"]

        self.chart_window_start = start
        self.chart_window_scale.config(from_=0, to=max_start, state=(tk.NORMAL if max_start > 0 else tk.DISABLED))
        self.chart_slider_updating = True
        try:
            self.chart_window_var.set(start)
        finally:
            self.chart_slider_updating = False

        if x:
            self.price_ax.set_xlim(start - 0.5, end - 0.5)
            self.volume_ax.set_xlim(start - 0.5, end - 0.5)

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
        self.volume_ax.set_xticks(tick_positions)
        self.volume_ax.set_xticklabels(tick_labels, rotation=45, ha="right")
        self.volume_ax.tick_params(axis="x", labelbottom=True)

        if dates:
            self.chart_window_label_var.set(f"窗口: {dates[start]} ~ {dates[end - 1]}")
        else:
            self.chart_window_label_var.set("窗口: -")

    def _draw_chart(self, history, analysis, keep_window: bool = False):
        self._reset_chart_axes()

        chart_data = self._prepare_chart_dataset(history, analysis)
        if chart_data is None:
            self._show_empty_chart("暂无历史数据")
            return

        self._draw_price_panel(chart_data)
        self._draw_volume_panel(chart_data)
        window_meta = self._resolve_chart_window(len(chart_data["x"]), keep_window=keep_window)
        self._apply_chart_window(chart_data, window_meta)
        self.fig.tight_layout()
        self.canvas.draw()

    # ======================== 折叠区切换 ========================

    def toggle_summary_section(self):
        self.summary_expanded = not self.summary_expanded
        self._apply_section_visibility()

    def toggle_chart_section(self):
        self.chart_expanded = not self.chart_expanded
        self._apply_section_visibility()

    def _apply_section_visibility(self):
        if self.summary_expanded:
            if not self.info_frame.winfo_ismapped():
                self.info_frame.pack(fill=tk.X, pady=5, after=self.info_header)
            self.summary_toggle_btn.config(text="收起历史摘要")
            self.summary_status_var.set("历史摘要已展开")
        else:
            if self.info_frame.winfo_ismapped():
                self.info_frame.pack_forget()
            self.summary_toggle_btn.config(text="展开历史摘要")
            self.summary_status_var.set("历史摘要已收起")

        if self.chart_expanded:
            if not self.chart_frame.winfo_ismapped():
                self.chart_frame.pack(fill=tk.BOTH, expand=True, pady=5, after=self.chart_header)
            self.chart_toggle_btn.config(text="收起历史K线")
            self.chart_status_var.set("历史K线已展开")
        else:
            if self.chart_frame.winfo_ismapped():
                self.chart_frame.pack_forget()
            self.chart_toggle_btn.config(text="展开历史K线")
            self.chart_status_var.set("历史K线已收起")

    # ======================== 滑块/滚动/拖拽事件 ========================

    def on_chart_window_changed(self, value):
        if self.chart_slider_updating:
            return
        if self.chart_history is None or getattr(self.chart_history, "empty", True):
            return
        try:
            new_start = int(float(value))
        except (TypeError, ValueError):
            return
        total = len(self.chart_history)
        window = max(15, min(int(self.chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        new_start = max(0, min(new_start, max_start))
        if new_start == self.chart_window_start:
            return
        self.chart_window_start = new_start
        self._draw_chart(self.chart_history, self.chart_analysis, keep_window=True)

    def _bind_chart_scroll(self, widget) -> None:
        if self.chart_scroll_bound:
            return
        try:
            widget.bind("<MouseWheel>", self.on_chart_mousewheel)
            widget.bind("<Shift-MouseWheel>", self.on_chart_mousewheel)
            widget.bind("<Button-4>", self.on_chart_mousewheel)
            widget.bind("<Button-5>", self.on_chart_mousewheel)
            widget.bind("<Left>", lambda _e: self._move_chart_window(-12))
            widget.bind("<Right>", lambda _e: self._move_chart_window(12))
            widget.bind("<Enter>", lambda _e: widget.focus_set())
            widget.bind("<Button-1>", lambda _e: widget.focus_set())
            self.chart_scroll_bound = True
        except tk.TclError:
            pass

    def _move_chart_window(self, delta: int) -> bool:
        if self.chart_history is None or getattr(self.chart_history, "empty", True):
            return False
        total = len(self.chart_history)
        window = max(15, min(int(self.chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        if max_start <= 0:
            self._maybe_load_more_history(need_older=True)
            return False
        try:
            shift = int(delta)
        except (TypeError, ValueError):
            return False
        if shift == 0:
            return False
        new_start = max(0, min(self.chart_window_start + shift, max_start))
        if new_start == self.chart_window_start:
            if new_start == 0 and shift < 0:
                self._maybe_load_more_history(need_older=True)
            return False
        self.chart_window_start = new_start
        self._draw_chart(self.chart_history, self.chart_analysis, keep_window=True)
        if self.chart_window_start <= max(0, min(12, max_start)) and shift < 0:
            self._maybe_load_more_history(need_older=True)
        return True

    def _maybe_load_more_history(self, need_older: bool = False) -> None:
        if not need_older:
            return
        if self.chart_loading_more:
            return
        code = str(self.current_code or self.request_code or "").strip().zfill(6)
        if not code:
            return
        current_rows = 0
        if self.chart_history is not None and not getattr(self.chart_history, "empty", True):
            current_rows = len(self.chart_history)
        request_days = max(120, self.chart_loaded_days + 120, current_rows + 120)
        self.chart_loading_more = True
        self.chart_status_var.set("历史K线补载中...")
        self.app._start_background_job(
            self._load_more_history,
            name=f"detail-history-{code}",
            args=(code, request_days, current_rows),
        )

    def _load_more_history(
        self,
        stock_code: str,
        request_days: int,
        previous_rows: int,
        cancel_token: CancelToken,
    ) -> None:
        if cancel_token.is_cancelled():
            return
        history = self.app.stock_filter.get_stock_detail_history(stock_code, request_days)
        if cancel_token.is_cancelled():
            return
        self.app._post_to_ui(
            lambda: self._apply_more_history_if_current(stock_code, history, request_days, previous_rows),
        )

    def _apply_more_history_if_current(self, stock_code: str, history, request_days: int, previous_rows: int) -> None:
        self.chart_loading_more = False
        code = str(stock_code).strip().zfill(6)
        if code != str(self.current_code or self.request_code or "").strip().zfill(6):
            return
        if history is None or getattr(history, "empty", True):
            self.chart_status_var.set("历史K线补载失败")
            return
        if len(history) <= previous_rows:
            self.chart_loaded_days = max(self.chart_loaded_days, request_days)
            self.chart_status_var.set("历史K线已展开")
            return
        old_start = int(self.chart_window_start)
        added_rows = len(history) - previous_rows
        self.chart_history = history
        self.chart_window_start = old_start + added_rows
        self.chart_status_var.set("历史K线已展开")
        self._draw_chart(history, self.chart_analysis, keep_window=True)

    def _scroll_delta(self, event) -> int:
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

    def on_chart_scroll(self, event):
        if event is None:
            return
        inaxes = getattr(event, "inaxes", None)
        if inaxes is not None and inaxes not in (self.price_ax, self.volume_ax):
            return
        if self.chart_history is None or getattr(self.chart_history, "empty", True):
            return
        total = len(self.chart_history)
        window = max(15, min(int(self.chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        if max_start <= 0:
            return

        delta = self._scroll_delta(event)
        if delta == 0:
            return
        self._move_chart_window(delta)

    def on_chart_mousewheel(self, event):
        self.on_chart_scroll(event)

    def on_chart_click(self, event):
        if event is None:
            return
        if event.inaxes not in (self.price_ax, self.volume_ax):
            return
        if getattr(event, "button", None) not in (1, None):
            return
        self.chart_click_target_date = ""
        if self.chart_dates and event.xdata is not None:
            try:
                idx = int(round(float(event.xdata)))
                idx = max(0, min(idx, len(self.chart_dates) - 1))
                self.chart_click_target_date = str(self.chart_dates[idx] or "").strip()
            except (TypeError, ValueError):
                self.chart_click_target_date = ""
        if event.x is None:
            self.chart_dragging = False
            return
        self.chart_dragging = True
        self.chart_drag_moved = False
        self.chart_drag_start_x = float(event.x)
        self.chart_drag_start_window = int(self.chart_window_start)

    def on_chart_drag_motion(self, event):
        if not self.chart_dragging:
            return
        if event is None or getattr(event, "inaxes", None) not in (self.price_ax, self.volume_ax):
            return
        if self.chart_history is None or getattr(self.chart_history, "empty", True):
            return
        if event.x is None:
            return
        total = len(self.chart_history)
        window = max(15, min(int(self.chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        if max_start <= 0:
            return
        pixel_per_bar = max(3.0, self.canvas.get_tk_widget().winfo_width() / max(1, window))
        bars = int(round((float(event.x) - self.chart_drag_start_x) / pixel_per_bar))
        new_start = max(0, min(self.chart_drag_start_window - bars, max_start))
        if new_start == self.chart_window_start:
            return
        self.chart_drag_moved = True
        self.chart_window_start = new_start
        self._draw_chart(self.chart_history, self.chart_analysis, keep_window=True)

    def on_chart_drag_release(self, event):
        if not self.chart_dragging:
            return
        self.chart_dragging = False
        if self.chart_drag_moved:
            self.chart_drag_moved = False
            return
        code = str(self.current_code or self.request_code or "").strip().zfill(6)
        if not code:
            return
        self.app.intraday.open_view_with_offset(code, day_offset=0, target_trade_date=self.chart_click_target_date)
        self.chart_drag_moved = False
