"""分时 Tab：matplotlib 1min K 线 + 成交量 + 价格分布。

包含：
- ttk.Frame 容器（self.frame）
- matplotlib Figure / 三个 Axes（self.fig / price_ax / volume_ax / dist_ax）
- 导航按钮（前一天/后一天）
- 标题 / 交易日 StringVar
- 加载/渲染各方法（_load / _apply / _draw_* / _resolve_*）
- 状态字段（request_code / loading_code / day_offset / available_dates / payload_cache）

跨 tab 引用通过 self.app.xxx 访问（如 self.app.notebook / status_var / _ui / _post_to_ui / stock_filter）。
全局数据源偏好 self.app.intraday_source_var 保留在 App 上。
"""
from __future__ import annotations

import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk

import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter

from src.utils.cancel_token import CancelToken

if TYPE_CHECKING:
    from src.gui.app import StockMonitorApp


class IntradayTab:
    """分时 tab：1min K 线 + 成交量 + 价格分布矩阵。"""

    # GUI 层 LRU 缓存上限（原 StockMonitorApp._INTRADAY_CACHE_MAX）
    _INTRADAY_CACHE_MAX = 30
    # 实时请求 TTL；指定历史日（day_offset<0 或 target_trade_date 非空）则数据不变长缓存
    _INTRADAY_LIVE_TTL_SEC = 30.0

    def __init__(self, app: "StockMonitorApp", notebook: ttk.Notebook) -> None:
        self.app = app
        # 状态字段（原 self._intraday_xxx）
        self.request_code: str = ""
        self.loading_code: str = ""
        self.request_offset: int = 0
        self.loading_offset: int = 0
        self.request_target_date: str = ""
        self.loading_target_date: str = ""
        self.day_offset: int = 0
        self.available_dates: List[str] = []
        self.selected_date: str = ""
        self.payload_cache: "OrderedDict[Tuple[str, int, str], Tuple[float, Dict[str, Any], bool]]" = OrderedDict()
        self._build(notebook)

    def _build(self, notebook: ttk.Notebook) -> None:
        """构建 widget。从原 setup_intraday_tab 整体迁移。"""
        self.frame = ttk.Frame(notebook, padding="5")
        notebook.add(self.frame, text="分时")

        info = ttk.Frame(self.frame)
        info.pack(fill=tk.X, pady=(0, 6))
        self.title_var = tk.StringVar(value="分时图（点击 K 线打开）")
        ttk.Label(info, textvariable=self.title_var).pack(side=tk.LEFT)
        self.day_var = tk.StringVar(value="交易日: -")
        ttk.Label(info, textvariable=self.day_var).pack(side=tk.LEFT, padx=(12, 8))
        self.prev_btn = ttk.Button(info, text="前一天", command=lambda: self.navigate_day(-1), state=tk.DISABLED)
        self.prev_btn.pack(side=tk.RIGHT, padx=(6, 0))
        self.next_btn = ttk.Button(info, text="后一天", command=lambda: self.navigate_day(1), state=tk.DISABLED)
        self.next_btn.pack(side=tk.RIGHT)

        chart_frame = ttk.LabelFrame(self.frame, text="分时走势", padding="5")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.fig = Figure(figsize=(11, 6.8), dpi=100)
        gs = self.fig.add_gridspec(
            2,
            2,
            width_ratios=[4.5, 1.2],
            height_ratios=[3.0, 1.2],
            wspace=0.36,
            hspace=0.14,
        )
        self.price_ax = self.fig.add_subplot(gs[0, 0])
        self.volume_ax = self.fig.add_subplot(gs[1, 0], sharex=self.price_ax)
        self.dist_ax = self.fig.add_subplot(gs[:, 1])
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self._draw_loading("点击详情页 K 线打开分时")

    # ======================== 公开入口（被其他 tab 调用） ========================

    def open_view(self, stock_code: str):
        self.open_view_with_offset(stock_code, day_offset=0)

    def navigate_day(self, delta: int):
        code = str(self.request_code or self.app._current_detail_code or "").strip().zfill(6)
        if not code:
            return
        try:
            step = int(delta)
        except (TypeError, ValueError):
            return
        if step == 0:
            return
        if not self.available_dates:
            self.open_view_with_offset(code, day_offset=self.day_offset + step)
            return
        try:
            current_idx = self.available_dates.index(self.selected_date)
        except ValueError:
            current_idx = len(self.available_dates) - 1
        target_idx = max(0, min(current_idx + step, len(self.available_dates) - 1))
        target_date = self.available_dates[target_idx]
        self.open_view_with_offset(code, day_offset=0, target_trade_date=target_date)

    def _refresh_nav_buttons(self):
        has_code = bool(str(self.request_code or "").strip())
        has_dates = len(self.available_dates) > 0
        can_prev = has_code and has_dates and (self.day_offset > -(len(self.available_dates) - 1))
        can_next = has_code and has_dates and (self.day_offset < 0)
        self.prev_btn.config(state=(tk.NORMAL if can_prev else tk.DISABLED))
        self.next_btn.config(state=(tk.NORMAL if can_next else tk.DISABLED))

    def open_view_with_offset(self, stock_code: str, day_offset: int = 0, target_trade_date: str = ""):
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return
        last_code = str(self.request_code or "").strip().zfill(6)
        if last_code and last_code != code:
            self.available_dates = []
            self.selected_date = ""

        try:
            requested_offset = int(day_offset)
        except (TypeError, ValueError):
            requested_offset = 0
        if requested_offset > 0:
            requested_offset = 0
        if self.available_dates:
            requested_offset = max(requested_offset, -(len(self.available_dates) - 1))

        self.request_code = code
        self.request_offset = requested_offset
        self.day_offset = requested_offset
        self.title_var.set(f"分时图 - {code}")
        self.app._set_top_header_for_code(code)
        self.day_var.set("交易日: 加载中...")
        self._refresh_nav_buttons()
        self._draw_loading(f"正在加载 {code} 分时...")
        self.app.notebook.select(self.frame)

        normalized_target_date = str(target_trade_date or "").strip()
        self.request_target_date = normalized_target_date

        # ---- GUI 层 LRU 缓存命中：历史日期永久有效，实时请求 30s TTL ----
        cache_key = (code, requested_offset, normalized_target_date)
        cached = self.payload_cache.get(cache_key)
        if cached is not None:
            ts, payload, is_live = cached
            age = time.time() - ts
            fresh = (not is_live) or (age < self._INTRADAY_LIVE_TTL_SEC)
            if fresh:
                self.payload_cache.move_to_end(cache_key)
                try:
                    self._apply_if_current(
                        code, requested_offset, normalized_target_date, payload,
                    )
                    return
                except Exception as e:
                    self.app._log(f"渲染内存缓存分时失败，回退到完整加载: {e}")

        if (
            self.loading_code == code
            and self.loading_offset == requested_offset
            and self.loading_target_date == normalized_target_date
        ):
            return
        self.loading_code = code
        self.loading_offset = requested_offset
        self.loading_target_date = normalized_target_date
        self.app._start_background_job(
            self._load,
            name=f"intraday-{code}",
            args=(code, requested_offset, normalized_target_date),
        )

    # ======================== 数据加载 & 应用 ========================

    def _load(self, stock_code: str, day_offset: int, target_trade_date: str, cancel_token: CancelToken):
        try:
            if cancel_token.is_cancelled():
                return
            payload = self.app.stock_filter.get_stock_intraday(
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
                    self.payload_cache[cache_key] = (time.time(), payload, is_live)
                    self.payload_cache.move_to_end(cache_key)
                    while len(self.payload_cache) > self._INTRADAY_CACHE_MAX:
                        self.payload_cache.popitem(last=False)
                except Exception:
                    pass
                self._apply_if_current(stock_code, day_offset, target_trade_date, payload)

            self.app._post_to_ui(_apply)
        except Exception as e:
            self.app._post_to_ui(lambda: self._draw_error(stock_code, f"分时加载失败: {e}"))
            self.app._post_to_ui(lambda: self.app._log(f"分时加载失败 {stock_code}: {e}"))
        finally:
            self.app._post_to_ui(lambda: self._finish_status(stock_code, day_offset))

    def _apply_if_current(self, stock_code: str, day_offset: int, target_trade_date: str, payload: Dict[str, Any]) -> None:
        code = str(stock_code).strip().zfill(6)
        if (
            code != self.request_code
            or int(day_offset) != int(self.request_offset)
            or str(target_trade_date or "").strip() != self.request_target_date
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
        self.available_dates = available_trade_dates
        self.day_offset = applied_day_offset
        self.request_offset = applied_day_offset
        self.selected_date = selected_trade_date
        self.day_var.set(f"交易日: {selected_trade_date or '-'}")
        # 调试日志：检查分时数据内容和竞价点
        try:
            if intraday_df is not None and not intraday_df.empty:
                times = pd.to_datetime(intraday_df["time"], errors="coerce")
                time_strs = [t.strftime("%H:%M") for t in times if not pd.isna(t)]
                first_label = time_strs[0] if time_strs else "-"
                last_label = time_strs[-1] if time_strs else "-"
                has_auction = isinstance(auction_snapshot, dict) and auction_snapshot.get("price") is not None
                self.app._log(
                    f"【分时调试】{code} 数据共 {len(intraday_df)} 行，区间 {first_label}~{last_label}，竞价标记: {'是' if has_auction else '否'}"
                )
                if has_auction:
                    self.app._log(
                        f"   竞价数据: 时间={auction_snapshot.get('time')}, 价格={auction_snapshot.get('price')}, 成交量={auction_snapshot.get('volume')}"
                    )
            else:
                self.app._log(f"【分时调试】{code} 分时数据为空")
        except Exception as e:
            self.app._log(f"【分时调试】记录日志出错: {e}")

        self._refresh_nav_buttons()
        self._draw_chart(code, intraday_df, prev_close=prev_close, auction_snapshot=auction_snapshot)

    def _finish_status(self, stock_code: str, day_offset: int) -> None:
        code = str(stock_code).strip().zfill(6)
        if code == self.loading_code and int(day_offset) == int(self.loading_offset):
            self.loading_code = ""
            self.loading_offset = 0
            self.loading_target_date = ""

    # ======================== 绘图：loading / error ========================

    def _draw_loading(self, message: str):
        self.price_ax.clear()
        self.volume_ax.clear()
        self.dist_ax.clear()
        self.price_ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=13)
        self.volume_ax.text(0.5, 0.5, "请稍候", ha="center", va="center", fontsize=11)
        self.dist_ax.text(0.5, 0.5, "等待分时数据", ha="center", va="center", fontsize=11)
        self.price_ax.set_axis_off()
        self.volume_ax.set_axis_off()
        self.dist_ax.set_axis_off()
        self.canvas.draw()

    def _draw_error(self, stock_code: str, message: str):
        code = str(stock_code).strip().zfill(6)
        self.title_var.set(f"分时图 - {code}")
        self.app._set_top_header_for_code(code)
        if not self.selected_date:
            self.day_var.set("交易日: -")
        self.price_ax.clear()
        self.volume_ax.clear()
        self.dist_ax.clear()
        self.price_ax.text(0.5, 0.5, "分时数据加载失败", ha="center", va="center", fontsize=13, color="#b22222")
        self.volume_ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=10)
        self.dist_ax.text(0.5, 0.5, "无分布数据", ha="center", va="center", fontsize=10)
        self.price_ax.set_axis_off()
        self.volume_ax.set_axis_off()
        self.dist_ax.set_axis_off()
        self._refresh_nav_buttons()
        self.canvas.draw()

    # ======================== 数据 / 标尺 辅助 ========================

    def _resolve_base_price(self, close_series, prev_close: Optional[float]) -> float:
        if prev_close is not None and pd.notna(prev_close) and float(prev_close) > 0:
            return float(prev_close)
        first_close = pd.to_numeric(close_series, errors="coerce").dropna()
        if not first_close.empty:
            return max(float(first_close.iloc[0]), 1.0)
        return 1.0

    def _resolve_average_price(self, df, close_series, volume_series):
        avg_price = pd.to_numeric(df.get("avg_price"), errors="coerce")
        if not avg_price.isna().all():
            return avg_price
        cumulative_volume = volume_series.cumsum()
        if (cumulative_volume > 0).any():
            weighted_amount = (close_series.ffill().fillna(0) * volume_series).cumsum()
            return pd.to_numeric(weighted_amount / cumulative_volume.replace(0, pd.NA), errors="coerce")
        return close_series.expanding(min_periods=1).mean()

    def _normalize_auction_snapshot(self, auction_snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
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

    def _build_tick_positions(self, time_labels: List[str], x: List[int], auction_context: Dict[str, Any]):
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

    # ======================== 绘图：三个 panel ========================

    def _draw_price_panel(
        self,
        x: List[int],
        pct_close,
        pct_avg,
        pct_ma5,
        base_price: float,
        first_close,
        auction_context: Dict[str, Any],
    ) -> None:
        self.price_ax.plot(x, pct_close, color="#2f6fd6", linewidth=1.4, label="分时")
        self.price_ax.plot(x, pct_avg, color="#f08a24", linewidth=1.3, label="均价线")
        self.price_ax.plot(x, pct_ma5, color="#7b52ab", linewidth=1.0, linestyle="--", alpha=0.85, label="MA5")
        self.price_ax.axhline(0.0, color="#888888", linewidth=0.9, linestyle="--", alpha=0.85, label="昨收")
        self.price_ax.grid(True, alpha=0.25)
        self.price_ax.set_ylabel("涨\n跌\n幅\n(%)", rotation=0, labelpad=14, va="center")
        self.price_ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:.1f}%"))

        valid_pct_parts = [pct_close, pct_avg, pct_ma5]
        if auction_context["has_auction"] and auction_context["price"] is not None:
            valid_pct_parts.append(pd.Series([(auction_context["price"] / base_price - 1.0) * 100.0]))
        valid_pct = pd.concat(valid_pct_parts, ignore_index=True)
        valid_pct = pd.to_numeric(valid_pct, errors="coerce").dropna()
        if valid_pct.empty or first_close.empty:
            self.price_ax.set_ylim(-2.0, 2.0)
        else:
            # 同花顺风格：围绕 0%（昨收）对称，上下等幅
            max_abs = float(valid_pct.abs().max())
            pad = max(max_abs * 0.08, 0.1)
            m = min(max_abs + pad, 35.0)
            if m < 1.0:
                m = 1.0
            self.price_ax.set_ylim(-m, m)

        secax = self.price_ax.secondary_yaxis(
            "right",
            functions=(
                lambda y: base_price * (1.0 + y / 100.0),
                lambda p: (p / base_price - 1.0) * 100.0,
            ),
        )
        secax.set_ylabel("价\n格\n(元)", rotation=0, labelpad=12, va="center")
        secax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:.2f}"))

        self.price_ax.set_title(
            "分时走势（含竞价标记）" if auction_context["has_auction"] else "分时走势",
            pad=10,
        )
        self.price_ax.legend(
            loc="lower left",
            bbox_to_anchor=(0.0, 1.01),
            ncol=4,
            fontsize=8,
            framealpha=0.9,
            borderaxespad=0.0,
            columnspacing=1.0,
            handlelength=1.8,
        )

        if auction_context["has_auction"] and auction_context["x"] is not None and auction_context["price"] is not None:
            q_pct = (auction_context["price"] / base_price - 1.0) * 100.0
            self.price_ax.axvline(auction_context["x"] + 0.1, color="#888888", linewidth=0.9, alpha=0.7, linestyle=":")
            self.price_ax.scatter([auction_context["x"]], [q_pct], s=26, color="#555555", zorder=5, label="_nolegend_")
            first_intraday_pct = pd.to_numeric(pd.Series([pct_close.iloc[0]]), errors="coerce").iloc[0] if len(pct_close) else None
            if first_intraday_pct is not None and pd.notna(first_intraday_pct):
                self.price_ax.plot(
                    [auction_context["x"], 0],
                    [q_pct, float(first_intraday_pct)],
                    color="#777777",
                    linewidth=0.9,
                    linestyle=":",
                )
            self.price_ax.text(
                auction_context["x"],
                self.price_ax.get_ylim()[1],
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
                parts.append(f"量 {self.app._format_volume(auction_context['volume'])}")
            elif auction_context["amount"] is not None:
                parts.append(f"额 {self.app._format_amount(auction_context['amount'])}")
            auction_info_text = "竞价: " + " / ".join(parts)
        self.price_ax.text(
            0.995,
            0.92,
            auction_info_text,
            transform=self.price_ax.transAxes,
            ha="right",
            va="top",
            fontsize=8,
            color="#555555",
            bbox=dict(boxstyle="round,pad=0.2", fc="#f2f2f2", ec="#c9c9c9", alpha=0.9),
        )

    def _draw_volume_panel(
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
        self.volume_ax.bar(x, volume_series, width=0.65, color=colors, alpha=0.85)
        self.volume_ax.grid(True, alpha=0.2)
        self.volume_ax.set_ylabel("成\n交\n量", rotation=0, labelpad=10, va="center")
        self.volume_ax.set_xlabel("时间")
        if auction_context["has_auction"] and auction_context["x"] is not None:
            self.volume_ax.axvline(auction_context["x"] + 0.1, color="#888888", linewidth=0.9, alpha=0.65, linestyle=":")

        self.price_ax.set_xticks(tick_positions)
        self.price_ax.set_xticklabels([])
        self.price_ax.tick_params(axis="x", which="both", length=0)

        self.volume_ax.set_xticks(tick_positions)
        self.volume_ax.set_xticklabels(tick_labels, rotation=45, ha="right", fontsize=8)
        if x:
            left_limit = -1.0 if auction_context["has_auction"] else -0.5
            right_limit = x[-1] + 0.5
            self.price_ax.set_xlim(left_limit, right_limit)
            self.volume_ax.set_xlim(left_limit, right_limit)

    def _draw_distribution_panel(self, close_series, volume_series) -> None:
        dist_df = pd.DataFrame({"price": close_series, "volume": volume_series}).dropna(subset=["price"])
        dist_df["volume"] = pd.to_numeric(dist_df["volume"], errors="coerce").fillna(0)
        dist_df = dist_df[dist_df["volume"] > 0]

        if dist_df.empty:
            self.dist_ax.text(0.5, 0.5, "暂无成交分布数据", ha="center", va="center", fontsize=11)
            self.dist_ax.set_axis_off()
            return

        dist_df["price"] = dist_df["price"].round(2)
        grouped = dist_df.groupby("price", as_index=False)["volume"].sum()
        total_volume = float(grouped["volume"].sum())
        if total_volume <= 0:
            self.dist_ax.text(0.5, 0.5, "暂无成交分布数据", ha="center", va="center", fontsize=11)
            self.dist_ax.set_axis_off()
            return

        grouped["ratio"] = grouped["volume"] / total_volume
        grouped = grouped.sort_values("ratio", ascending=False).reset_index(drop=True).head(min(12, len(grouped)))
        y = list(range(len(grouped)))
        ratios_pct = (grouped["ratio"] * 100.0).tolist()
        labels = [f"{p:.2f}" for p in grouped["price"].tolist()]
        self.dist_ax.barh(y, ratios_pct, color="#5b7bd5", alpha=0.9)
        self.dist_ax.set_yticks(y)
        self.dist_ax.set_yticklabels(labels, fontsize=9)
        self.dist_ax.invert_yaxis()
        self.dist_ax.yaxis.tick_right()
        self.dist_ax.tick_params(axis="y", labelright=True, labelleft=False, pad=4)
        self.dist_ax.set_xlabel("占比(%)")
        self.dist_ax.set_ylabel("价\n位\n(元)", rotation=0, labelpad=14, va="center")
        self.dist_ax.yaxis.set_label_position("right")
        self.dist_ax.set_title("成交价格分布")
        self.dist_ax.grid(True, axis="x", alpha=0.2)

        for yi, value in zip(y, ratios_pct):
            text_x = max(value - 0.35, 0.12)
            text_color = "white" if value >= 3.0 else "#222222"
            self.dist_ax.text(
                text_x,
                yi,
                f"{value:.2f}%",
                va="center",
                ha="right",
                fontsize=8,
                color=text_color,
            )

    def _draw_chart(
        self,
        stock_code: str,
        intraday_df,
        prev_close: Optional[float] = None,
        auction_snapshot: Optional[Dict[str, Any]] = None,
    ):
        code = str(stock_code).strip().zfill(6)
        self.title_var.set(f"分时图 - {code}")
        self.app._set_top_header_for_code(code)
        self.price_ax.clear()
        self.volume_ax.clear()
        self.dist_ax.clear()
        self.price_ax.set_axis_on()
        self.volume_ax.set_axis_on()
        self.dist_ax.set_axis_on()

        if intraday_df is None or getattr(intraday_df, "empty", True):
            self._draw_error(code, "暂无分时数据")
            return

        df = intraday_df.copy().reset_index(drop=True)
        close_series = pd.to_numeric(df.get("close"), errors="coerce")
        open_series = pd.to_numeric(df.get("open"), errors="coerce")
        volume_series = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0)
        times = pd.to_datetime(df.get("time"), errors="coerce")

        if close_series.isna().all() or times.isna().all():
            self._draw_error(code, "分时数据无有效价格")
            return

        base_price = self._resolve_base_price(close_series, prev_close)
        first_close = close_series.dropna()
        avg_price = self._resolve_average_price(df, close_series, volume_series)
        ma5 = close_series.rolling(window=5, min_periods=1).mean()
        auction_context = self._normalize_auction_snapshot(auction_snapshot)
        x = list(range(len(df)))
        pct_close = (close_series / base_price - 1.0) * 100.0
        pct_avg = (avg_price / base_price - 1.0) * 100.0
        pct_ma5 = (ma5 / base_price - 1.0) * 100.0
        time_labels = [t.strftime("%H:%M") if not pd.isna(t) else "" for t in times]
        self._draw_price_panel(
            x,
            pct_close,
            pct_avg,
            pct_ma5,
            base_price,
            first_close,
            auction_context,
        )
        tick_positions, tick_labels = self._build_tick_positions(time_labels, x, auction_context)
        self._draw_volume_panel(
            x,
            open_series,
            close_series,
            volume_series,
            tick_positions,
            tick_labels,
            auction_context,
        )
        self._draw_distribution_panel(close_series, volume_series)
        self.fig.tight_layout(rect=[0.02, 0.06, 0.985, 0.965], h_pad=1.2, w_pad=0.9)
        self.canvas.draw()
