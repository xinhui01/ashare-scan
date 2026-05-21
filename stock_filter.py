from __future__ import annotations

import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, TimeoutError as FutureTimeoutError
from typing import List, Dict, Any, Optional, Callable, Tuple
import threading

import pandas as pd

from scan_models import FilterSettings, HistoryRequestPlan
from src.models.analysis_models import HistoryAnalysisConfig
from src.services.history_analysis_service import HistoryAnalysisService
from src.utils.cancel_token import CancelToken, coerce_should_stop
from stock_data import StockDataFetcher, DaemonThreadPoolExecutor
from stock_logger import get_logger
from stock_store import (
    load_limit_up_stock_meta,
    save_last_limit_up_prediction,
    save_limit_up_prediction_record,
)

logger = get_logger(__name__)

# 模块级 K 线历史形态 helper 已迁移到 src/services/scoring/helpers.py
# 保留 re-export 以兼容 `from stock_filter import _count_historical_*` 老调用方
from src.services.scoring.helpers import (
    _count_historical_continuation,
    _count_historical_followthrough,
    _count_historical_wrap,
)
from src.services.scoring import shared as _scoring_shared
from src.services.scoring import classifiers as _scoring_classifiers
from src.services.scoring import profile as _scoring_profile
from src.services.scoring import cont as _scoring_cont
from src.services.scoring import first as _scoring_first
from src.services.scoring import fresh as _scoring_fresh
from src.services.scoring import wrap as _scoring_wrap
from src.services.scoring import trend as _scoring_trend


class StockFilter:
    def __init__(self):
        self.fetcher = StockDataFetcher()
        self._log: Optional[Callable[[str], None]] = None
        self.apply_settings(FilterSettings())

    def set_log_callback(self, cb: Optional[Callable[[str], None]]) -> None:
        self._log = cb
        self.fetcher.set_log_callback(cb)

    def set_history_source_preference(self, source: str) -> None:
        self.fetcher.set_default_history_source(source)

    def set_intraday_source_preference(self, source: str) -> None:
        self.fetcher.set_default_intraday_source(source)

    def set_fund_flow_source_preference(self, source: str) -> None:
        self.fetcher.set_default_fund_flow_source(source)

    def set_limit_up_reason_source_preference(self, source: str) -> None:
        self.fetcher.set_default_limit_up_reason_source(source)

    def _log_runtime_diagnostics(self, stage: str) -> None:
        if not self._log:
            return
        diag = self.fetcher.get_runtime_diagnostics()
        self._log(
            f"【诊断/{stage}】历史并发上限={diag.get('history_concurrency_limit')}，"
            f"最小请求间隔={diag.get('history_min_interval_sec')}s，"
            f"镜像缓存={diag.get('cached_mirror_count')}，"
            f"冷却中={'是' if diag.get('history_request_blocked') else '否'}"
        )
        self._log(
            f"【诊断/{stage}】缓存命中={diag.get('cache_hits')}，网络请求={diag.get('network_requests')}，"
            f"成功={diag.get('network_success')}，失败={diag.get('network_failures')}，"
            f"缓存回退={diag.get('fallback_cache_returns')}，限流事件={diag.get('rate_limit_events')}，"
            f"冷却跳过={diag.get('cooldown_skips')}"
        )

    def _resolve_stock_identity(self, universe: Optional[pd.DataFrame], stock_code: str) -> Dict[str, str]:
        code = str(stock_code or "").strip().zfill(6)
        cached_meta = load_limit_up_stock_meta(code) or {}
        if universe is None or universe.empty or not code:
            return {
                "name": str(cached_meta.get("name", "") or ""),
                "board": "",
                "exchange": "",
                "industry": str(cached_meta.get("industry", "") or ""),
                "last_limit_up_trade_date": str(cached_meta.get("last_limit_up_trade_date", "") or ""),
            }
        try:
            match = universe[universe["code"].astype(str).str.zfill(6) == code]
        except Exception:
            match = pd.DataFrame()
        if match.empty:
            return {
                "name": str(cached_meta.get("name", "") or ""),
                "board": "",
                "exchange": "",
                "industry": str(cached_meta.get("industry", "") or ""),
                "last_limit_up_trade_date": str(cached_meta.get("last_limit_up_trade_date", "") or ""),
            }
        row = match.iloc[0]
        return {
            "name": str(row.get("name", "") or "") or str(cached_meta.get("name", "") or ""),
            "board": str(row.get("board", "") or ""),
            "exchange": str(row.get("exchange", "") or ""),
            "industry": str(cached_meta.get("industry", "") or ""),
            "last_limit_up_trade_date": str(cached_meta.get("last_limit_up_trade_date", "") or ""),
        }

    def _enrich_analysis_with_history_snapshot(
        self,
        analysis: Dict[str, Any],
        history: Optional[pd.DataFrame],
    ) -> None:
        if history is not None and not history.empty:
            latest_row = history.iloc[-1]
            analysis["latest_volume"] = latest_row.get("volume")
            analysis["latest_amount"] = latest_row.get("amount")
            analysis["quote_time"] = str(latest_row.get("date", "") or "")
            return
        analysis["latest_volume"] = None
        analysis["latest_amount"] = None
        analysis["quote_time"] = ""

    def _enrich_analysis_with_fund_flow(
        self,
        analysis: Dict[str, Any],
        fund_flow_df: Optional[pd.DataFrame],
    ) -> None:
        if fund_flow_df is not None and not fund_flow_df.empty:
            latest_flow = fund_flow_df.iloc[-1]
            analysis["flow_date"] = str(latest_flow.get("date", "") or "")
            analysis["main_force_amount"] = latest_flow.get("main_force_amount")
            analysis["big_order_amount"] = latest_flow.get("big_order_amount")
            analysis["super_big_order_amount"] = latest_flow.get("super_big_order_amount")
            analysis["main_force_ratio"] = latest_flow.get("main_force_ratio")
            analysis["big_order_ratio"] = latest_flow.get("big_order_ratio")
            analysis["super_big_order_ratio"] = latest_flow.get("super_big_order_ratio")
            analysis["fund_flow_history"] = fund_flow_df.to_dict("records")
            return
        analysis["flow_date"] = ""
        analysis["main_force_amount"] = None
        analysis["big_order_amount"] = None
        analysis["super_big_order_amount"] = None
        analysis["main_force_ratio"] = None
        analysis["big_order_ratio"] = None
        analysis["super_big_order_ratio"] = None
        analysis["fund_flow_history"] = []

    def _enrich_analysis_with_indicators(
        self,
        analysis: Dict[str, Any],
        history: Optional[pd.DataFrame],
    ) -> None:
        """在 analysis 字典中追加 MACD/KDJ/RSI/BOLL 最新值。"""
        if history is None or history.empty or "close" not in history.columns:
            analysis["macd_dif"] = None
            analysis["macd_dea"] = None
            analysis["macd_bar"] = None
            analysis["kdj_k"] = None
            analysis["kdj_d"] = None
            analysis["kdj_j"] = None
            analysis["rsi_6"] = None
            analysis["rsi_12"] = None
            analysis["boll_upper"] = None
            analysis["boll_mid"] = None
            analysis["boll_lower"] = None
            return
        try:
            from stock_indicators import calc_macd, calc_kdj, calc_rsi, calc_boll
            close = pd.to_numeric(history["close"], errors="coerce")
            m = calc_macd(close)
            analysis["macd_dif"] = round(float(m["dif"].iloc[-1]), 3) if not pd.isna(m["dif"].iloc[-1]) else None
            analysis["macd_dea"] = round(float(m["dea"].iloc[-1]), 3) if not pd.isna(m["dea"].iloc[-1]) else None
            analysis["macd_bar"] = round(float(m["macd"].iloc[-1]), 3) if not pd.isna(m["macd"].iloc[-1]) else None

            if all(c in history.columns for c in ("high", "low")):
                k = calc_kdj(history["high"], history["low"], close)
                analysis["kdj_k"] = round(float(k["k"].iloc[-1]), 2) if not pd.isna(k["k"].iloc[-1]) else None
                analysis["kdj_d"] = round(float(k["d"].iloc[-1]), 2) if not pd.isna(k["d"].iloc[-1]) else None
                analysis["kdj_j"] = round(float(k["j"].iloc[-1]), 2) if not pd.isna(k["j"].iloc[-1]) else None
            else:
                analysis["kdj_k"] = analysis["kdj_d"] = analysis["kdj_j"] = None

            r = calc_rsi(close, periods=(6, 12))
            analysis["rsi_6"] = round(float(r["rsi_6"].iloc[-1]), 2) if not pd.isna(r["rsi_6"].iloc[-1]) else None
            analysis["rsi_12"] = round(float(r["rsi_12"].iloc[-1]), 2) if not pd.isna(r["rsi_12"].iloc[-1]) else None

            b = calc_boll(close)
            analysis["boll_upper"] = round(float(b["upper"].iloc[-1]), 2) if not pd.isna(b["upper"].iloc[-1]) else None
            analysis["boll_mid"] = round(float(b["mid"].iloc[-1]), 2) if not pd.isna(b["mid"].iloc[-1]) else None
            analysis["boll_lower"] = round(float(b["lower"].iloc[-1]), 2) if not pd.isna(b["lower"].iloc[-1]) else None
        except Exception as exc:
            logger.debug("技术指标计算失败: %s", exc)

    def _build_stock_detail_payload(
        self,
        stock_code: str,
        stock_identity: Dict[str, str],
        history: Optional[pd.DataFrame],
        analysis: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "code": str(stock_code).strip().zfill(6),
            "name": str(stock_identity.get("name", "") or ""),
            "board": str(stock_identity.get("board", "") or ""),
            "exchange": str(stock_identity.get("exchange", "") or ""),
            "industry": str(stock_identity.get("industry", "") or ""),
            "last_limit_up_trade_date": str(stock_identity.get("last_limit_up_trade_date", "") or ""),
            "history": history,
            "analysis": analysis,
        }

    def get_settings(self) -> FilterSettings:
        return FilterSettings(
            trend_days=int(self.trend_days),
            ma_period=int(self.ma_period),
            limit_up_lookback_days=int(self.limit_up_lookback_days),
            volume_lookback_days=int(self.volume_lookback_days),
            volume_expand_enabled=bool(self.volume_expand_enabled),
            volume_expand_factor=float(self.volume_expand_factor),
            require_limit_up_within_days=bool(self.require_limit_up_within_days),
            strong_ft_enabled=bool(self.strong_ft_enabled),
            strong_ft_max_pullback_pct=float(self.strong_ft_max_pullback_pct),
            strong_ft_max_volume_ratio=float(self.strong_ft_max_volume_ratio),
            strong_ft_min_hold_days=int(self.strong_ft_min_hold_days),
        )

    def apply_settings(self, settings: FilterSettings) -> None:
        self.trend_days = max(1, int(settings.trend_days))
        self.ma_period = max(1, int(settings.ma_period))
        self.limit_up_lookback_days = max(1, int(settings.limit_up_lookback_days))
        self.volume_lookback_days = max(1, int(settings.volume_lookback_days))
        self.volume_expand_enabled = bool(settings.volume_expand_enabled)
        self.volume_expand_factor = max(1.0, float(settings.volume_expand_factor))
        self.require_limit_up_within_days = bool(settings.require_limit_up_within_days)
        self.strong_ft_enabled = bool(settings.strong_ft_enabled)
        self.strong_ft_max_pullback_pct = max(0.0, float(settings.strong_ft_max_pullback_pct))
        self.strong_ft_max_volume_ratio = max(0.0, float(settings.strong_ft_max_volume_ratio))
        self.strong_ft_min_hold_days = max(0, int(settings.strong_ft_min_hold_days))

    _timeout_pool: Optional[ThreadPoolExecutor] = None
    _timeout_pool_lock = threading.Lock()

    @classmethod
    def _get_timeout_pool(cls) -> ThreadPoolExecutor:
        if cls._timeout_pool is None:
            with cls._timeout_pool_lock:
                if cls._timeout_pool is None:
                    cls._timeout_pool = DaemonThreadPoolExecutor(
                        max_workers=4, thread_name_prefix="timeout"
                    )
        return cls._timeout_pool

    def _call_with_timeout(
        self,
        task: Callable[[], Any],
        timeout_sec: float,
        fallback: Any = None,
        task_name: str = "任务",
        cancel_token: Optional[CancelToken] = None,
    ) -> Any:
        # 已取消：直接跳过，连调度都不做
        if cancel_token is not None and cancel_token.is_cancelled():
            return fallback
        pool = self._get_timeout_pool()
        future = pool.submit(task)
        deadline = time.time() + max(0.5, float(timeout_sec))
        try:
            # 用短轮询等待，这样取消信号到来时最多等一个 poll 间隔
            if cancel_token is None:
                return future.result(timeout=max(0.5, float(timeout_sec)))
            poll = 0.2
            while True:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise FutureTimeoutError()
                if cancel_token.is_cancelled():
                    future.cancel()
                    return fallback
                try:
                    return future.result(timeout=min(poll, remaining))
                except FutureTimeoutError:
                    continue
        except FutureTimeoutError:
            future.cancel()
            if self._log:
                self._log(f"{task_name} 超时（>{timeout_sec:.0f}s），已跳过。")
            return fallback
        except Exception as exc:
            if self._log:
                self._log(f"{task_name} 失败: {exc}")
            return fallback

    def check_close_above_ma(
        self, history_data: pd.DataFrame, streak_days: int, ma_period: int
    ) -> bool:
        return self._build_analysis_service().check_close_above_ma(
            history_data,
            streak_days=streak_days,
            ma_period=ma_period,
        )

    def _resolve_analysis_config(
        self,
        *,
        streak_days: Optional[int] = None,
        ma_period: Optional[int] = None,
        limit_up_lookback_days: Optional[int] = None,
        volume_lookback_days: Optional[int] = None,
        volume_expand_enabled: Optional[bool] = None,
        volume_expand_factor: Optional[float] = None,
    ) -> HistoryAnalysisConfig:
        return HistoryAnalysisConfig.from_filter_settings(
            self.get_settings(),
            trend_days=streak_days,
            ma_period=ma_period,
            limit_up_lookback_days=limit_up_lookback_days,
            volume_lookback_days=volume_lookback_days,
            volume_expand_enabled=volume_expand_enabled,
            volume_expand_factor=volume_expand_factor,
        )

    def _build_analysis_service(
        self,
        *,
        streak_days: Optional[int] = None,
        ma_period: Optional[int] = None,
        limit_up_lookback_days: Optional[int] = None,
        volume_lookback_days: Optional[int] = None,
        volume_expand_enabled: Optional[bool] = None,
        volume_expand_factor: Optional[float] = None,
    ) -> HistoryAnalysisService:
        config = self._resolve_analysis_config(
            streak_days=streak_days,
            ma_period=ma_period,
            limit_up_lookback_days=limit_up_lookback_days,
            volume_lookback_days=volume_lookback_days,
            volume_expand_enabled=volume_expand_enabled,
            volume_expand_factor=volume_expand_factor,
        )
        return HistoryAnalysisService(config)

    def _limit_up_threshold(self, board: str = "", stock_name: str = "") -> float:
        return self._build_analysis_service().limit_up_threshold(
            board=board,
            stock_name=stock_name,
        )

    @staticmethod
    def _calculate_limit_up_streak(mask: pd.Series) -> int:
        """计算从最新交易日往前数的连续涨停天数。"""
        streak = 0
        for flag in reversed(mask.tolist()):
            if bool(flag):
                streak += 1
            else:
                break
        return streak

    def _calculate_trade_score(
        self,
        result: Dict[str, Any],
        streak_days: int,
        ma_period: int,
        volume_enabled: bool,
    ) -> tuple[int, str]:
        return self._build_analysis_service(
            streak_days=streak_days,
            ma_period=ma_period,
            volume_expand_enabled=volume_enabled,
        ).calculate_trade_score(result)

    def analyze_history(
        self,
        history_data: pd.DataFrame,
        streak_days: Optional[int] = None,
        ma_period: Optional[int] = None,
        limit_up_lookback_days: Optional[int] = None,
        volume_lookback_days: Optional[int] = None,
        volume_expand_enabled: Optional[bool] = None,
        volume_expand_factor: Optional[float] = None,
        board: str = "",
        stock_name: str = "",
        stock_code: str = "",
    ) -> Dict[str, Any]:
        return self._build_analysis_service(
            streak_days=streak_days,
            ma_period=ma_period,
            limit_up_lookback_days=limit_up_lookback_days,
            volume_lookback_days=volume_lookback_days,
            volume_expand_enabled=volume_expand_enabled,
            volume_expand_factor=volume_expand_factor,
        ).analyze_history(
            history_data,
            board=board,
            stock_name=stock_name,
            stock_code=stock_code,
        )

    def _build_filter_result_shell(
        self,
        stock_code: str,
        stock_name: str,
        board: str,
        exchange: str,
    ) -> Dict[str, Any]:
        result = {
            "code": str(stock_code).strip().zfill(6),
            "name": stock_name or "",
            "passed": False,
            "reasons": [],
            "data": {},
        }
        if board:
            result["data"]["board"] = board
        if exchange:
            result["data"]["exchange"] = exchange
        return result

    def _resolve_filter_history_days(self) -> int:
        return max(
            14,
            self.trend_days + self.ma_period + 4,
            self.limit_up_lookback_days + self.ma_period + 4,
            self.volume_lookback_days + 4,
        )

    def _attach_filter_analysis(
        self,
        result: Dict[str, Any],
        history_data: pd.DataFrame,
        stock_code: str,
        stock_name: str,
        board: str,
    ) -> Dict[str, Any]:
        analysis = self.analyze_history(
            history_data,
            self.trend_days,
            self.ma_period,
            self.limit_up_lookback_days,
            self.volume_lookback_days,
            self.volume_expand_enabled,
            self.volume_expand_factor,
            board=board,
            stock_name=stock_name,
            stock_code=stock_code,
        )
        result["data"]["analysis"] = analysis
        result["data"]["history_tail"] = history_data.tail(max(self.trend_days, self.limit_up_lookback_days)).copy()
        return analysis

    def _apply_limit_up_requirement_failure(
        self,
        result: Dict[str, Any],
        analysis: Dict[str, Any],
    ) -> bool:
        if not self.require_limit_up_within_days or analysis.get("limit_up_within_days"):
            return False
        analysis["summary"] = (
            f"{analysis['summary']}；未命中过去{self.limit_up_lookback_days}个交易日涨停条件"
            if analysis.get("summary")
            else f"未命中过去{self.limit_up_lookback_days}个交易日涨停条件"
        )
        result["reasons"].append(analysis["summary"])
        return True

    def _apply_strong_followthrough_failure(
        self,
        result: Dict[str, Any],
        analysis: Dict[str, Any],
    ) -> bool:
        """当开启"承接强势"过滤时，未命中形态的股票直接淘汰。"""
        if not getattr(self, "strong_ft_enabled", False):
            return False
        ft = analysis.get("strong_followthrough") or {}
        if ft.get("has_strong_followthrough"):
            return False
        reason = self._build_strong_ft_failure_reason(ft)
        analysis["summary"] = (
            f"{analysis['summary']}；{reason}" if analysis.get("summary") else reason
        )
        result["reasons"].append(reason)
        return True

    def _build_strong_ft_failure_reason(self, ft: Dict[str, Any]) -> str:
        """把 followthrough 结果翻译成人类友好的失败原因。"""
        if ft.get("limit_up_is_today"):
            return f"{ft.get('limit_up_date')} 刚涨停，次日走势还未出现，无法判断承接"
        if not ft.get("limit_up_date"):
            return f"近{self.limit_up_lookback_days}日未找到可承接的涨停日"
        parts = [f"{ft['limit_up_date']} 涨停后"]
        if not ft.get("is_pullback_day"):
            parts.append("次日未回落（未形成承接形态）")
        if not ft.get("pullback_within_limit"):
            parts.append(
                f"回撤过深（{ft.get('pullback_pct', 0):.1f}% > {self.strong_ft_max_pullback_pct:.1f}%）"
            )
        if not ft.get("volume_shrunk"):
            parts.append(
                f"未缩量（次日量比 {ft.get('pullback_volume_ratio', 0):.0%} > {self.strong_ft_max_volume_ratio:.0%}）"
            )
        if not ft.get("holds_above_pullback_low"):
            parts.append("后续跌破回落日最低价")
        elif ft.get("hold_days", 0) < ft.get("min_hold_days", 0):
            parts.append(f"站稳天数不足（{ft.get('hold_days', 0)} < {ft.get('min_hold_days', 0)}）")
        return "；".join(parts)

    def _finalize_filter_result(
        self,
        result: Dict[str, Any],
        analysis: Dict[str, Any],
    ) -> Dict[str, Any]:
        result["passed"] = bool(analysis.get("passed"))
        result["reasons"].append(analysis["summary"])
        return result

    def filter_stock(
        self,
        stock_code: str,
        stock_name: str = "",
        board: str = "",
        exchange: str = "",
        history_mirror: Optional[str] = None,
        mirror_pool: Optional[List[str]] = None,
        history_plan: Optional[HistoryRequestPlan] = None,
    ) -> Dict[str, Any]:
        result = self._build_filter_result_shell(stock_code, stock_name, board, exchange)
        history_data = self.fetcher.get_history_data(
            stock_code,
            days=self._resolve_filter_history_days(),
            preferred_mirror=history_mirror,
            mirror_pool=mirror_pool,
            request_plan=history_plan,
        )
        result["data"]["history"] = history_data
        if history_data is None or history_data.empty:
            result["reasons"].append("无法获取历史数据")
            return result

        analysis = self._attach_filter_analysis(result, history_data, stock_code, stock_name, board)
        if self._apply_limit_up_requirement_failure(result, analysis):
            return result
        if self._apply_strong_followthrough_failure(result, analysis):
            return result
        return self._finalize_filter_result(result, analysis)

    def _result_sort_key(self, item: Dict[str, Any]):
        analysis = (item.get("data", {}) or {}).get("analysis") or {}
        five_day_return = analysis.get("five_day_return")
        volume_expand_ratio = analysis.get("volume_expand_ratio")
        latest_change_pct = analysis.get("latest_change_pct")
        return (
            five_day_return if five_day_return is not None else float("-inf"),
            volume_expand_ratio if volume_expand_ratio is not None else float("-inf"),
            1 if analysis.get("limit_up_within_days") else 0,
            latest_change_pct if latest_change_pct is not None else float("-inf"),
            str(item.get("code", "")),
        )

    def _filter_scan_universe(
        self,
        all_stocks: pd.DataFrame,
        allowed_boards: Optional[List[str]],
        allowed_exchanges: Optional[List[str]],
        log: Optional[Callable[[str], None]] = None,
    ) -> pd.DataFrame:
        filtered = all_stocks
        if allowed_boards and "board" in filtered.columns:
            allowed_board_set = {str(x).strip() for x in allowed_boards if str(x).strip()}
            if allowed_board_set:
                before = len(filtered)
                filtered = filtered[filtered["board"].astype(str).isin(allowed_board_set)]
                if log:
                    log(f"板块过滤：保留 {len(filtered)}/{before} 只，目标板块 {', '.join(sorted(allowed_board_set))}")

        if allowed_exchanges and "exchange" in filtered.columns:
            allowed_exchange_set = {str(x).strip() for x in allowed_exchanges if str(x).strip()}
            if allowed_exchange_set:
                before = len(filtered)
                filtered = filtered[filtered["exchange"].astype(str).isin(allowed_exchange_set)]
                if log:
                    log(f"交易所过滤：保留 {len(filtered)}/{before} 只，目标交易所 {', '.join(sorted(allowed_exchange_set))}")
        return filtered

    def _limit_scan_subset(self, all_stocks: pd.DataFrame, max_stocks: int) -> pd.DataFrame:
        if max_stocks and max_stocks > 0:
            return all_stocks.head(max_stocks).reset_index(drop=True)
        return all_stocks.reset_index(drop=True)

    def _resolve_scan_workers(self, max_workers: Optional[int]) -> Tuple[int, int]:
        if max_workers is None:
            try:
                max_workers = int(os.environ.get("ASHARE_SCAN_SCAN_WORKERS", "3").strip() or "3")
            except ValueError:
                max_workers = 3
        requested_workers = max(1, min(int(max_workers), 16))
        history_workers = max(1, int(self.fetcher.history_request_concurrency_limit()))
        return requested_workers, min(requested_workers, history_workers)

    def _build_scan_history_plan(self, history_source: str, local_history_only: bool) -> HistoryRequestPlan:
        if local_history_only:
            return HistoryRequestPlan(
                mode="cache_only",
                provider_sequence=("local-cache",),
                mirror_urls=(),
                reason="scan-local-cache-only",
            )
        return self.fetcher.build_history_request_plan(
            source=history_source,
            force_refresh=False,
        )

    @staticmethod
    def _build_local_cache_history_plan(reason: str = "local-cache-only") -> HistoryRequestPlan:
        return HistoryRequestPlan(
            mode="cache_only",
            provider_sequence=("local-cache",),
            mirror_urls=(),
            reason=reason,
        )

    def _assign_scan_jobs(
        self,
        subset: pd.DataFrame,
        available_mirrors: List[str],
    ) -> Tuple[List[Tuple[Dict[str, Any], Optional[str]]], Dict[str, int]]:
        rows = subset.to_dict("records")
        random.Random(int(time.time())).shuffle(rows)
        assigned_jobs: List[Tuple[Dict[str, Any], Optional[str]]] = []
        mirror_counts: Dict[str, int] = {}
        for idx, row in enumerate(rows):
            mirror = available_mirrors[idx % len(available_mirrors)] if available_mirrors else None
            if mirror:
                mirror_counts[mirror] = mirror_counts.get(mirror, 0) + 1
            assigned_jobs.append((row, mirror))
        return assigned_jobs, mirror_counts

    def _log_scan_history_context(
        self,
        log: Optional[Callable[[str], None]],
        coverage: Dict[str, Any],
        history_plan: HistoryRequestPlan,
        local_history_only: bool,
    ) -> None:
        if not log:
            return
        log(
            f"历史缓存覆盖率：{coverage.get('covered_count', 0)}/{coverage.get('universe_count', 0)} "
            f"({coverage.get('coverage_ratio', 0.0) * 100:.1f}%)，最新交易日 {coverage.get('latest_trade_date') or '-'}"
        )
        log(f"历史数据源策略：{'/'.join(history_plan.provider_sequence)}")
        if history_plan.cache_only:
            if history_plan.reason and history_plan.reason != "scan-local-cache-only":
                log(f"历史接口镜像当前不可用，最近探测失败示例：{history_plan.reason}")
            if history_plan.reason == "scan-local-cache-only":
                log("本轮扫描使用本地历史缓存，不发起公网历史请求；未命中缓存的股票会被跳过。")
            else:
                log("历史接口暂不可用，本轮改为缓存优先扫描；未命中本地缓存的股票会被跳过。")
            return
        if not history_plan.mirror_urls and history_plan.provider_sequence and history_plan.provider_sequence[0] != "eastmoney":
            log("当前扫描已切换到非东方财富历史源。")

    def _log_scan_execution_context(
        self,
        log: Optional[Callable[[str], None]],
        total_universe: int,
        total: int,
        workers: int,
        requested_workers: int,
        local_history_only: bool,
        available_mirrors: List[str],
        mirror_counts: Dict[str, int],
    ) -> None:
        if not log:
            return
        log(f"【阶段 2/3】股票池 {total_universe} 只，本次扫描 {total} 只，最近{self.trend_days}日收盘 > MA{self.ma_period}，并发 {workers} 线程。")
        if requested_workers != workers:
            log(f"并发保护已生效：你请求 {requested_workers} 线程，但历史接口当前只允许 {workers} 个并发，以降低东方财富限流风险。")
        log("说明：扫描阶段只拉历史日线，不拉实时、资金流或内外盘。")
        if local_history_only:
            log("说明：扫描阶段默认只读本地缓存；首次或缓存不足时请先执行“更新历史缓存”。")
        else:
            log("说明：历史请求优先使用所选数据源；若为自动模式，会在东财失败后切换到腾讯/新浪。")
        if available_mirrors:
            mirror_summary = "，".join(
                f"{mirror.split('//', 1)[-1].split('/', 1)[0]}={mirror_counts.get(mirror, 0)}"
                for mirror in available_mirrors
            )
            log(f"历史镜像分区：{mirror_summary}")
        else:
            log("历史镜像分区：cache-only")

    def _submit_scan_tasks(
        self,
        executor: ThreadPoolExecutor,
        assigned_jobs: List[Tuple[Dict[str, Any], Optional[str]]],
        available_mirrors: List[str],
        history_plan: HistoryRequestPlan,
    ) -> Dict[Any, Tuple[str, str, str, str, Optional[str]]]:
        future_to_meta = {}
        for row, mirror in assigned_jobs:
            code = str(row["code"]).strip().zfill(6)
            name = str(row.get("name", "") or "")
            board = str(row.get("board", "") or "")
            exchange = str(row.get("exchange", "") or "")
            future = executor.submit(
                self.filter_stock,
                code,
                name,
                board,
                exchange,
                mirror,
                available_mirrors,
                history_plan,
            )
            future_to_meta[future] = (code, name, board, exchange, mirror)
        return future_to_meta

    def _pending_scan_sample_text(
        self,
        pending,
        future_to_meta: Dict[Any, Tuple[str, str, str, str, Optional[str]]],
    ) -> str:
        sample = [
            (
                f"{future_to_meta[f][0]}@{future_to_meta[f][4].split('//', 1)[-1].split('/', 1)[0]}"
                if future_to_meta[f][4]
                else f"{future_to_meta[f][0]}@cache-only"
            )
            for f in list(pending)[:3]
        ]
        return "、".join(sample) if sample else "-"

    def _scan_mirror_label(self, mirror: Optional[str]) -> str:
        if not mirror:
            return "cache-only"
        return mirror.split("//", 1)[-1].split("/", 1)[0]

    def _should_stop_scan(
        self,
        should_stop: Optional[Callable[[], bool]],
        log: Optional[Callable[[str], None]],
    ) -> bool:
        if not should_stop or not should_stop():
            return False
        if log:
            log("收到停止信号，正在取消未完成任务...")
        return True

    def _log_pending_scan_wait(
        self,
        log: Optional[Callable[[str], None]],
        pending,
        future_to_meta: Dict[Any, Tuple[str, str, str, str, Optional[str]]],
        completed: int,
        total: int,
        results: List[Dict[str, Any]],
        started_at: float,
        last_report: float,
    ) -> float:
        now = time.time()
        if not log or now - last_report < 10:
            return last_report
        elapsed = now - started_at
        sample_text = self._pending_scan_sample_text(pending, future_to_meta)
        log(
            f"进度 {completed}/{total}，命中 {len(results)} 只，已用时 {elapsed:.1f}s，"
            f"仍在等待历史数据返回，示例代码 {sample_text}"
        )
        return now

    def _build_scan_error_result(
        self,
        code: str,
        name: str,
        board: str,
        exchange: str,
        mirror: Optional[str],
        error: Exception,
        log: Optional[Callable[[str], None]],
    ) -> Dict[str, Any]:
        if log:
            log(f"  {code} {name} 检测异常[{self._scan_mirror_label(mirror)}]: {error}")
        return {
            "code": code,
            "name": name,
            "passed": False,
            "reasons": [str(error)],
            "data": {"board": board, "exchange": exchange},
        }

    def _resolve_scan_future_result(
        self,
        fut,
        future_to_meta: Dict[Any, Tuple[str, str, str, str, Optional[str]]],
        log: Optional[Callable[[str], None]],
    ) -> Tuple[str, str, str, str, Optional[str], Dict[str, Any]]:
        code, name, board, exchange, mirror = future_to_meta[fut]
        try:
            filter_result = fut.result()
        except Exception as exc:
            filter_result = self._build_scan_error_result(code, name, board, exchange, mirror, exc, log)
        return code, name, board, exchange, mirror, filter_result

    def _log_scan_pass_result(
        self,
        log: Optional[Callable[[str], None]],
        completed: int,
        total: int,
        code: str,
        name: str,
        filter_result: Dict[str, Any],
    ) -> None:
        if not log or not filter_result.get("passed"):
            return
        analysis = filter_result.get("data", {}).get("analysis") or {}
        log(
            f"  通过 {completed}/{total} {code} {name} "
            f"最新收盘 {analysis.get('latest_close', 0):.2f} / MA{self.ma_period} {analysis.get('latest_ma', 0):.2f}"
        )

    def _append_scan_hit(
        self,
        results: List[Dict[str, Any]],
        filter_result: Dict[str, Any],
        name: str,
        board: str,
        exchange: str,
    ) -> None:
        if not filter_result.get("passed"):
            return
        filter_result["name"] = name
        filter_result.setdefault("data", {})
        filter_result["data"].setdefault("board", board)
        filter_result["data"].setdefault("exchange", exchange)
        results.append(filter_result)

    def _notify_scan_progress(
        self,
        progress_callback,
        completed: int,
        total: int,
        code: str,
        name: str,
    ) -> None:
        if not progress_callback:
            return
        try:
            progress_callback(completed, total, code, name)
        except StopIteration:
            raise

    def _maybe_log_scan_progress(
        self,
        log: Optional[Callable[[str], None]],
        completed: int,
        total: int,
        results: List[Dict[str, Any]],
        started_at: float,
        last_report: float,
        report_every: int,
        code: str,
        name: str,
        mirror: Optional[str],
    ) -> float:
        now = time.time()
        if not log or (completed % report_every != 0 and now - last_report < 10):
            return last_report
        elapsed = now - started_at
        log(
            f"进度 {completed}/{total}，命中 {len(results)} 只，已用时 {elapsed:.1f}s，"
            f"当前 {code} {name} @ {self._scan_mirror_label(mirror)}"
        )
        return now

    def scan_all_stocks(
        self,
        max_stocks: int = 0,
        progress_callback=None,
        max_workers: Optional[int] = None,
        history_source: str = "auto",
        local_history_only: bool = True,
        should_stop: Optional[Callable[[], bool]] = None,
        refresh_universe: bool = False,
        allowed_boards: Optional[List[str]] = None,
        allowed_exchanges: Optional[List[str]] = None,
        cancel_token: Optional[CancelToken] = None,
    ) -> List[Dict[str, Any]]:
        # 兼容旧接口：将 should_stop 回调与 CancelToken 合并成同一个谓词
        should_stop = coerce_should_stop(cancel_token, should_stop)
        log = self._log
        t0 = time.time()
        if log:
            log("【阶段 1/3】加载股票池...")
        self._log_runtime_diagnostics("扫描前")

        all_stocks = self.fetcher.get_all_stocks(force_refresh=refresh_universe)
        if all_stocks.empty:
            if log:
                log("股票池为空，扫描终止。")
            return []

        total_universe = len(all_stocks)
        all_stocks = self._filter_scan_universe(all_stocks, allowed_boards, allowed_exchanges, log=log)
        subset = self._limit_scan_subset(all_stocks, max_stocks)
        total = len(subset)
        requested_workers, workers = self._resolve_scan_workers(max_workers)

        coverage = self.fetcher.get_history_cache_summary()
        history_plan = self._build_scan_history_plan(history_source, local_history_only)
        available_mirrors = list(history_plan.mirror_urls)
        self._log_scan_history_context(log, coverage, history_plan, local_history_only)
        assigned_jobs, mirror_counts = self._assign_scan_jobs(subset, available_mirrors)
        self._log_scan_execution_context(
            log,
            total_universe,
            total,
            workers,
            requested_workers,
            local_history_only,
            available_mirrors,
            mirror_counts,
        )

        results: List[Dict[str, Any]] = []
        completed = 0
        last_report = time.time()
        report_every = 25

        # 提交前再检查一次：用户可能在加载股票池阶段就点了停止
        if self._should_stop_scan(should_stop, log):
            return results

        executor = DaemonThreadPoolExecutor(max_workers=workers)
        try:
            future_to_meta = self._submit_scan_tasks(executor, assigned_jobs, available_mirrors, history_plan)
            if log:
                log("【阶段 3/3】开始逐只拉取历史日线并计算结果...")

            pending = set(future_to_meta)
            while pending:
                # 更短的轮询周期，让取消信号更快生效；同时在每轮起点主动检查一次
                if self._should_stop_scan(should_stop, log):
                    for fut in pending:
                        fut.cancel()
                    break
                done, pending = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
                if not done:
                    if self._should_stop_scan(should_stop, log):
                        for fut in pending:
                            fut.cancel()
                        break
                    last_report = self._log_pending_scan_wait(
                        log,
                        pending,
                        future_to_meta,
                        completed,
                        total,
                        results,
                        t0,
                        last_report,
                    )
                    continue

                for fut in done:
                    if self._should_stop_scan(should_stop, log):
                        for p in pending:
                            p.cancel()
                        pending.clear()
                        break
                    code, name, board, exchange, mirror, filter_result = self._resolve_scan_future_result(
                        fut,
                        future_to_meta,
                        log,
                    )
                    completed += 1
                    self._log_scan_pass_result(log, completed, total, code, name, filter_result)
                    self._append_scan_hit(results, filter_result, name, board, exchange)
                    self._notify_scan_progress(progress_callback, completed, total, code, name)
                    last_report = self._maybe_log_scan_progress(
                        log,
                        completed,
                        total,
                        results,
                        t0,
                        last_report,
                        report_every,
                        code,
                        name,
                        mirror,
                    )
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        results.sort(key=self._result_sort_key, reverse=True)

        if log:
            elapsed = time.time() - t0
            log(f"【完成】扫描结束，命中 {len(results)} 只，用时 {elapsed:.1f}s。")
        self._log_runtime_diagnostics("扫描后")

        return results

    def get_stock_detail_quick(self, stock_code: str) -> Dict[str, Any]:
        code = str(stock_code).strip().zfill(6)
        history_days = max(80, self.trend_days + self.limit_up_lookback_days + self.ma_period + 20)
        history = self._call_with_timeout(
            lambda: self.fetcher.get_history_data(code, days=history_days),
            timeout_sec=15.0,
            fallback=None,
            task_name=f"详情历史 {code}",
        )
        analysis = self.analyze_history(
            history,
            self.trend_days,
            self.ma_period,
            self.limit_up_lookback_days,
            self.volume_lookback_days,
            self.volume_expand_enabled,
            self.volume_expand_factor,
            stock_code=stock_code,
        )
        self._enrich_analysis_with_history_snapshot(analysis, history)
        return self._build_stock_detail_payload(
            code,
            {"name": "", "board": "", "exchange": ""},
            history,
            analysis,
        )

    def get_stock_detail(
        self,
        stock_code: str,
        preloaded_history: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        code = str(stock_code).strip().zfill(6)
        history_days = max(80, self.trend_days + self.limit_up_lookback_days + self.ma_period + 20)

        # ---- 并行获取：历史 / 股票池同时发起 ----
        from concurrent.futures import ThreadPoolExecutor, as_completed
        history = preloaded_history
        universe = None
        fund_flow_df = None

        tasks = {}
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="detail") as pool:
            if history is None:
                tasks["history"] = pool.submit(
                    self._call_with_timeout,
                    lambda: self.fetcher.get_history_data(code, days=history_days),
                    15.0, None, f"详情历史 {code}",
                )
            tasks["universe"] = pool.submit(
                self._call_with_timeout,
                lambda: self.fetcher.get_all_stocks(),
                8.0, None, f"详情股票池 {code}",
            )
            for key, fut in tasks.items():
                try:
                    result = fut.result()
                    if key == "history":
                        history = result
                    elif key == "universe":
                        universe = result
                except Exception as exc:
                    logger.debug("预取数据 %s 异常: %s", key, exc)

        stock_identity = self._resolve_stock_identity(universe, code)
        analysis = self.analyze_history(
            history,
            self.trend_days,
            self.ma_period,
            self.limit_up_lookback_days,
            self.volume_lookback_days,
            self.volume_expand_enabled,
            self.volume_expand_factor,
            board=stock_identity["board"],
            stock_name=stock_identity["name"],
            stock_code=stock_code,
        )
        self._enrich_analysis_with_history_snapshot(analysis, history)
        self._enrich_analysis_with_fund_flow(analysis, fund_flow_df)
        self._enrich_analysis_with_indicators(analysis, history)
        return self._build_stock_detail_payload(code, stock_identity, history, analysis)

    def get_stock_detail_history(self, stock_code: str, days: int) -> Optional[pd.DataFrame]:
        code = str(stock_code).strip().zfill(6)
        history_days = max(60, int(days))
        return self._call_with_timeout(
            lambda: self.fetcher.get_history_data(code, days=history_days),
            timeout_sec=15.0,
            fallback=None,
            task_name=f"补充详情历史 {code}",
        )

    # ================= 涨停技术形态分类 =================

    def classify_limit_up_pattern(
        self,
        stock_code: str,
        board: str = "",
        stock_name: str = "",
    ) -> Dict[str, Any]:
        """涨停形态分类（thin delegate → scoring/classifiers.py）。"""
        return _scoring_classifiers.classify_limit_up_pattern(
            self.fetcher,
            stock_code,
            board=board,
            stock_name=stock_name,
            log_fn=self._log,
            limit_up_threshold_fn=self._limit_up_threshold,
            call_with_timeout_fn=self._call_with_timeout,
        )

    def _prefetch_history_for_pool(
        self,
        codes: List[str],
        days: int = 65,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        cache_only: bool = False,
    ) -> None:
        """批量预取涨停池股票历史数据（thin delegate → scoring/classifiers.py）。"""
        return _scoring_classifiers.prefetch_history_for_pool(
            self.fetcher,
            codes,
            days,
            progress_callback,
            cache_only,
            log_fn=self._log,
        )

    def classify_limit_up_pool(
        self,
        pool_records: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> List[Dict[str, Any]]:
        """涨停池批量分类（thin delegate → scoring/classifiers.py）。"""
        return _scoring_classifiers.classify_limit_up_pool(
            self.fetcher,
            pool_records,
            progress_callback,
            log_fn=self._log,
            limit_up_threshold_fn=self._limit_up_threshold,
            call_with_timeout_fn=self._call_with_timeout,
        )

    def _resolve_intraday_prev_close(
        self,
        history_df: Optional[pd.DataFrame],
        selected_trade_date: str,
    ) -> Optional[float]:
        if history_df is None or history_df.empty or "close" not in history_df.columns:
            return None

        df = history_df.copy()
        if "date" in df.columns:
            df["date"] = df["date"].astype(str).str.strip()
        else:
            df["date"] = ""
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"]).sort_values("date").reset_index(drop=True)
        if df.empty:
            return None

        target_date = str(selected_trade_date or "").strip()
        if target_date:
            previous_rows = df[df["date"] < target_date]
            if not previous_rows.empty:
                return float(previous_rows.iloc[-1]["close"])

        if len(df) >= 2:
            return float(df.iloc[-2]["close"])
        return float(df.iloc[-1]["close"])

    def get_stock_intraday(
        self,
        stock_code: str,
        day_offset: int = 0,
        target_trade_date: str = "",
    ) -> Dict[str, Any]:
        code = str(stock_code).strip().zfill(6)

        # ---- 并行获取：分时数据 + 历史(昨收)同时发起 ----
        from concurrent.futures import ThreadPoolExecutor
        intraday_payload = {}
        history_df = None

        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="intraday") as pool:
            fut_intraday = pool.submit(
                self._call_with_timeout,
                lambda: self.fetcher.get_intraday_data(
                    code, day_offset=day_offset,
                    target_trade_date=target_trade_date, include_meta=True,
                ),
                12.0, {}, f"分时 {code}",
            )
            fut_history = pool.submit(
                self._call_with_timeout,
                lambda: self.fetcher.get_history_data(code, days=20),
                6.0, None, f"分时昨收 {code}",
            )
            try:
                intraday_payload = fut_intraday.result() or {}
            except Exception as exc:
                logger.debug("分时数据获取失败 %s: %s", code, exc)
                intraday_payload = {}
            try:
                history_df = fut_history.result()
            except Exception as exc:
                logger.debug("历史数据获取失败 %s: %s", code, exc)
                history_df = None

        intraday_df = None
        selected_trade_date = ""
        available_trade_dates: List[str] = []
        applied_day_offset = 0
        auction_snapshot = None
        if isinstance(intraday_payload, dict):
            intraday_df = intraday_payload.get("intraday")
            selected_trade_date = str(intraday_payload.get("selected_trade_date") or "")
            available_trade_dates = [str(d) for d in (intraday_payload.get("available_trade_dates") or [])]
            raw_auction = intraday_payload.get("auction")
            if isinstance(raw_auction, dict):
                auction_snapshot = raw_auction
            try:
                applied_day_offset = int(intraday_payload.get("applied_day_offset") or 0)
            except (TypeError, ValueError):
                applied_day_offset = 0

        prev_close = self._resolve_intraday_prev_close(history_df, selected_trade_date)
        return {
            "code": code,
            "intraday": intraday_df,
            "prev_close": prev_close,
            "selected_trade_date": selected_trade_date,
            "available_trade_dates": available_trade_dates,
            "applied_day_offset": applied_day_offset,
            "auction": auction_snapshot,
        }

    # ================= 涨停预测 =================

    def _extract_pre_limit_up_features(
        self,
        code: str,
        limit_up_date_idx: int,
        df: pd.DataFrame,
        close: pd.Series,
        volume: pd.Series,
        amount: pd.Series,
        change_pct: pd.Series,
    ) -> Optional[Dict[str, Any]]:
        """单股涨停前 T-1 日特征提取（thin delegate → scoring/profile.py）。"""
        return _scoring_profile.extract_pre_limit_up_features(
            self.fetcher,
            code,
            limit_up_date_idx,
            df,
            close,
            volume,
            amount,
            change_pct,
            log_fn=self._log,
        )

    def analyze_pre_limit_up_profile(
        self,
        lookback_days: int = 5,
        trade_date: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, Any]:
        """回溯涨停股 T-1 日特征画像（thin delegate → scoring/profile.py）。"""
        return _scoring_profile.analyze_pre_limit_up_profile(
            self.fetcher,
            lookback_days,
            trade_date,
            progress_callback,
            log_fn=self._log,
            prefetch_history_for_pool_fn=self._prefetch_history_for_pool,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
        )

    @staticmethod
    def _aggregate_profile(samples: List[Dict[str, Any]]) -> Dict[str, Any]:
        """聚合特征样本分布（thin delegate → scoring/profile.py）。"""
        return _scoring_profile.aggregate_profile(samples)

    def predict_limit_up_candidates(
        self,
        trade_date: str,
        lookback_days: int = 5,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        historical_mode: bool = False,
    ) -> Dict[str, Any]:
        """基于涨停对比 + 二波接力数据预测明日涨停候选。

        步骤：
        1. 回看最近 N 日涨停对比，统计昨日首板晋级率等环境数据
        2. 保留涨停候选：对今日涨停股按封板质量 + 近期晋级环境评分
        3. 二波接力候选：近期涨停过 + 今日已启动 + 收盘强势的接力候选

        返回字段沿用旧结构，便于 GUI 直接复用：
            profile: 兼容旧 UI，现固定为空
            continuation_candidates: 保留涨停/连板候选
            first_board_candidates: 二波接力候选（字段名沿用 first_board_*）
            hot_industries: 今日涨停行业分布
            summary: 文字摘要

        `historical_mode=True`：回测模式。spot_df 从 history 表合成（"as of 收盘"）
        而非实时快照；龙虎榜 / 北向 / 板块强度等实时指标全部置空。涨停池仍用
        get_limit_up_pool（其本身有 SQLite 缓存）。仅在需要"对任意历史日期回放
        预测"的批量回测场景下使用。
        """
        # 阶段1：回看最近 N 日涨停对比环境
        if self._log:
            self._log(f"涨停预测：阶段1 - 统计最近 {lookback_days} 日涨停对比环境...")
        if progress_callback:
            progress_callback(0, 1, "统计最近涨停对比环境...")

        profile: Dict[str, Any] = {}
        feature_samples: List[Dict[str, Any]] = []
        compare_context = self._build_compare_market_context(trade_date, lookback_days)

        # 阶段2：获取今日涨停池 + 全市场行情
        if self._log:
            self._log(f"涨停预测：阶段2 - 获取 {trade_date} 涨停池 + 全市场行情...")
        if progress_callback:
            progress_callback(0, 1, "获取今日涨停池...")

        # 并行获取涨停池和全市场行情快照
        today_pool_df: Optional[pd.DataFrame] = None
        spot_df: Optional[pd.DataFrame] = None
        zt_codes: set = set()

        def _fetch_pool():
            nonlocal today_pool_df
            today_pool_df = self.fetcher.get_limit_up_pool(trade_date)

        if historical_mode:
            # 历史模式：spot_df 从 history 表合成（"as of 收盘"），不走实时网络
            import stock_store as _stock_store
            try:
                spot_df = _stock_store.load_spot_snapshot_at(trade_date)
                if self._log:
                    cnt = 0 if spot_df is None else len(spot_df)
                    self._log(f"涨停预测[历史模式]：从 history 合成 spot 快照 {cnt} 行")
            except Exception as e:
                if self._log:
                    self._log(f"涨停预测[历史模式]：合成 spot 失败: {e}")
                spot_df = None
            # 涨停池仍然走 get_limit_up_pool —— 本地 SQLite 命中即可，未命中才联网
            try:
                _fetch_pool()
            except Exception as e:
                if self._log:
                    self._log(f"涨停预测[历史模式]：获取涨停池失败: {e}")
        else:
            def _fetch_spot():
                nonlocal spot_df
                spot_df = self._fetch_spot_snapshot()

            # 使用线程池并行获取两个数据源。这里不能用 `with`，否则退出上下文时会 wait=True，
            # 即使 result(timeout=...) 超时了，仍然会继续等待后台任务跑完。
            executor = DaemonThreadPoolExecutor(max_workers=2, thread_name_prefix="stage2")
            try:
                future_pool = executor.submit(_fetch_pool)
                future_spot = executor.submit(_fetch_spot)

                try:
                    # 涨停池最多 15 秒（底层 _ashare_request_with_retry 有 20s deadline 兜底）
                    future_pool.result(timeout=15.0)
                except FutureTimeoutError as e:
                    if self._log:
                        self._log(f"涨停预测：获取涨停池超时 (get_limit_up_pool): {e}")
                except Exception as e:
                    if self._log:
                        self._log(f"涨停预测：获取涨停池失败 (get_limit_up_pool): {e}")

                try:
                    # 全市场行情：东财约5秒，新浪约30秒
                    from stock_data import _eastmoney_circuit_breaker_open
                    _spot_timeout = 45.0 if _eastmoney_circuit_breaker_open() else 20.0
                    future_spot.result(timeout=_spot_timeout)
                except FutureTimeoutError as e:
                    if self._log:
                        self._log(f"涨停预测：获取全市场行情超时 (5000+只股票): {e}")
                        self._log("涨停预测：将跳过首板候选筛选，继续执行连板延续分析")
                except Exception as e:
                    if self._log:
                        self._log(f"涨停预测：获取全市场行情失败: {e}")
                        self._log("涨停预测：将跳过首板候选筛选，继续执行连板延续分析")
            finally:
                executor.shutdown(wait=False, cancel_futures=True)

        if today_pool_df is None or today_pool_df.empty:
            non_trading = False
            try:
                from datetime import datetime as _dt2
                from src.utils.trade_calendar import _get_trade_calendar, _is_trading_day
                parsed = _dt2.strptime(str(trade_date).strip(), "%Y%m%d").date()
                non_trading = not _is_trading_day(parsed, _get_trade_calendar())
            except Exception:
                pass
            summary = (
                f"{trade_date} 非交易日（周末/节假日），无涨停池数据"
                if non_trading
                else f"{trade_date} 未获取到涨停池数据"
            )
            result = {
                "trade_date": trade_date,
                "profile": profile,
                "profile_samples": feature_samples,
                "continuation_candidates": [],
                "first_board_candidates": [],
                "fresh_first_board_candidates": [],
                "broken_board_wrap_candidates": [],
                "trend_limit_up_candidates": [],
                "hot_industries": {},
                "summary": summary,
            }
            try:
                save_last_limit_up_prediction(result)
            except Exception:
                pass
            try:
                save_limit_up_prediction_record(result)
            except Exception:
                pass
            return result

        all_pool_records = self._parse_full_pool(today_pool_df)
        if self._log:
            self._log(f"涨停预测：解析涨停池完成，共 {len(all_pool_records)} 只")
        hot_industries = self._count_pool_industries(today_pool_df)
        if self._log:
            self._log(f"涨停预测：统计热门行业完成，共 {len(hot_industries)} 个行业")

        if not today_pool_df.empty and "代码" in today_pool_df.columns:
            zt_codes = set(today_pool_df["代码"].astype(str).str.strip().str.zfill(6))
            if self._log:
                self._log(f"涨停预测：提取涨停股代码 {len(zt_codes)} 只")

        # 阶段2.5：尝试读取已缓存的 AI 题材聚类，作为预测打分的题材热度维度
        # 仅使用缓存，不主动触发 LLM 调用；缓存缺失则跳过题材加分
        try:
            from llm_theme_clustering import load_cached_themes
            themes_payload = load_cached_themes(trade_date) or {}
        except Exception as exc:
            logger.debug("加载题材聚类缓存失败: %s", exc)
            themes_payload = {}

        themes = themes_payload.get("themes") or []
        code_industry_map: Dict[str, str] = {
            r["code"]: r.get("industry", "") for r in all_pool_records
        }
        code_theme_map: Dict[str, str] = {}
        theme_size_map: Dict[str, int] = {}
        industry_theme_heat: Dict[str, int] = {}
        for theme in themes:
            try:
                name = str(theme.get("name", "")).strip()
                codes_in_theme = [str(c).strip().zfill(6) for c in (theme.get("codes") or [])]
            except Exception:
                continue
            if not name or len(codes_in_theme) < 2:
                continue
            size = len(codes_in_theme)
            theme_size_map[name] = size
            inds_in_theme: set = set()
            for c in codes_in_theme:
                code_theme_map[c] = name
                ind = code_industry_map.get(c) or ""
                if ind:
                    inds_in_theme.add(ind)
            for ind in inds_in_theme:
                if industry_theme_heat.get(ind, 0) < size:
                    industry_theme_heat[ind] = size

        # 把题材信息塞进 compare_context，所有 scorer 共用
        compare_context["industry_theme_heat"] = industry_theme_heat
        compare_context["code_theme_map"] = code_theme_map
        compare_context["theme_size_map"] = theme_size_map
        if self._log and themes:
            self._log(f"涨停预测：加载题材聚类缓存 {len(themes)} 个题材，"
                      f"覆盖 {len(code_theme_map)} 只涨停股 / {len(industry_theme_heat)} 个行业")
        elif self._log:
            self._log("涨停预测：未找到题材聚类缓存（如需题材加分，请先在涨停对比 tab 跑一次 AI 题材聚类）")

        # 阶段2.6：加载龙虎榜 + 北向资金（失败不影响预测）
        # 历史模式：这些都是"当前时刻"的指标，对历史日期没意义，全部置空
        if historical_mode:
            if self._log:
                self._log("涨停预测[历史模式]：跳过龙虎榜 / 北向 / 板块强度（实时指标）")
            lhb_map = {}
            northbound_map = {}
            board_strength = {}
        else:
            if self._log:
                self._log("涨停预测：正在加载龙虎榜 / 北向资金...")
            try:
                lhb_map = self._load_lhb_for_date(trade_date)
            except Exception as exc:
                logger.debug("龙虎榜加载异常: %s", exc)
                lhb_map = {}
            try:
                northbound_map = self._load_northbound_accumulation()
            except Exception as exc:
                logger.debug("北向资金加载异常: %s", exc)
                northbound_map = {}

            try:
                board_strength = self._load_industry_board_strength()
            except Exception as exc:
                logger.debug("板块涨跌幅加载异常: %s", exc)
                board_strength = {}

        compare_context["lhb_map"] = lhb_map
        compare_context["northbound_map"] = northbound_map
        compare_context["board_strength"] = board_strength
        if self._log:
            top_boards = sorted(board_strength.items(), key=lambda x: -x[1])[:5]
            board_summary = "、".join(f"{k}({v:.1f}%)" for k, v in top_boards)
            self._log(
                f"涨停预测：龙虎榜 {len(lhb_map)} 只 / 北向 3 日榜 {len(northbound_map)} 只 / "
                f"板块强弱榜 TOP5 {board_summary}"
            )

        # 市场情绪评分（供二波接力等评分调节，冰点情绪下降权）
        try:
            from src.services.market_sentiment_service import analyze_market_sentiment
            sent = analyze_market_sentiment(
                trade_date, fetch_external=True, log=self._log,
            )
            compare_context["sentiment_score"] = int(sent.get("score", 50))
            compare_context["sentiment_label"] = (
                (sent.get("position_suggest") or {}).get("label", "")
            )
            if self._log:
                self._log(
                    f"涨停预测：市场情绪 {compare_context['sentiment_score']}/100"
                    f" → {compare_context['sentiment_label']}"
                )
        except Exception as exc:
            logger.debug("接入市场情绪评分失败: %s", exc)
            compare_context["sentiment_score"] = 50

        # 龙头身份预算：同板块今日最高板数 + 持有该板数的代码集合
        # 让 cont 评分识别"板块独苗高板龙头"vs"高位跟风票"
        industry_max_boards: Dict[str, int] = {}
        industry_top_codes: Dict[str, set] = {}
        for r in all_pool_records:
            ind = str(r.get("industry") or "").strip()
            if not ind:
                continue
            try:
                b = int(r.get("consecutive_boards") or 1)
            except (TypeError, ValueError):
                b = 1
            cur_max = industry_max_boards.get(ind, 0)
            code = str(r.get("code") or "").strip()
            if b > cur_max:
                industry_max_boards[ind] = b
                industry_top_codes[ind] = {code}
            elif b == cur_max:
                industry_top_codes.setdefault(ind, set()).add(code)
        compare_context["industry_max_boards"] = industry_max_boards
        compare_context["industry_top_codes"] = industry_top_codes

        # 题材阶段映射（来自 concept_hype 服务）：code → 该票所在题材的最佳阶段
        # 萌芽 > 主升 > 末期 > 退潮 优先级，让连板股能识别自己处在主线还是末班车
        try:
            from src.services.concept_hype_service import analyze_concept_hype
            hype = analyze_concept_hype(trade_date, lookback=10, log=self._log)
            phase_priority = {"萌芽": 4, "主升": 3, "末期": 2, "退潮": 1}
            code_to_phase: Dict[str, str] = {}
            for c in hype.get("concepts") or []:
                phase = str(c.get("phase") or "")
                for m in c.get("members") or []:
                    code = str(m.get("code") or "").strip()
                    if not code:
                        continue
                    existing = code_to_phase.get(code)
                    if (not existing or
                            phase_priority.get(phase, 0) > phase_priority.get(existing, 0)):
                        code_to_phase[code] = phase
            compare_context["code_to_concept_phase"] = code_to_phase
            if self._log:
                self._log(
                    f"涨停预测：题材阶段映射 {len(code_to_phase)} 只票，"
                    f"萌芽 {sum(1 for v in code_to_phase.values() if v == '萌芽')} / "
                    f"主升 {sum(1 for v in code_to_phase.values() if v == '主升')} / "
                    f"末期 {sum(1 for v in code_to_phase.values() if v == '末期')} / "
                    f"退潮 {sum(1 for v in code_to_phase.values() if v == '退潮')}"
                )
        except Exception as exc:
            logger.debug("接入题材阶段映射失败: %s", exc)
            compare_context["code_to_concept_phase"] = {}

        # 阶段3：统一预取所有需要的历史数据（一次搞定）
        if self._log:
            self._log("涨停预测：阶段3 - 统一预取历史数据...")

        # 收集所有需要历史数据的股票代码
        pool_codes = [r["code"] for r in all_pool_records]
        candidate_codes: List[str] = []
        if spot_df is not None and not spot_df.empty:
            if self._log:
                self._log(f"涨停预测：开始筛选强势股（全市场 {len(spot_df)} 只）...")
            strong = self._filter_strong_stocks(spot_df, zt_codes)
            if self._log:
                self._log(f"涨停预测：筛选强势股完成，共 {len(strong)} 只")
            pullback = self._filter_ma5_pullback_stocks(spot_df, zt_codes)
            if self._log:
                self._log(f"涨停预测：筛选回踩MA5完成，共 {len(pullback)} 只")
            seen = set()
            for rec in strong + pullback:
                if rec["code"] not in seen:
                    seen.add(rec["code"])
                    candidate_codes.append(rec["code"])
        else:
            if self._log:
                self._log("涨停预测：无全市场行情，跳过强势股筛选（首板候选将不可用）")

        all_codes = list(set(pool_codes + candidate_codes))
        if self._log:
            self._log(f"涨停预测：统一预取 {len(all_codes)} 只股票历史数据"
                      f"（涨停池{len(pool_codes)} + 候选{len(candidate_codes)}）")
        # 只使用本地缓存，不发起网络请求
        self._prefetch_history_for_pool(all_codes, days=65, progress_callback=progress_callback, cache_only=True)
        if self._log:
            self._log("涨停预测：阶段3完成 - 历史数据预取结束")

        # 阶段4：保留涨停 / 连板延续候选评分
        if self._log:
            self._log(f"涨停预测：阶段4 - 分析 {len(all_pool_records)} 只涨停股的保留涨停潜力...")

        continuation_candidates = []
        for idx, rec in enumerate(all_pool_records):
            score_info = self._score_continuation_by_compare(rec, hot_industries, compare_context)
            # 门槛 40→50：30 天数据显示 0-49 段命中仅 15.2%（n=277），50+ 段才有
            # 22.5% 区分度，进一步过滤减少 false positive
            if score_info["score"] >= 50:
                continuation_candidates.append(score_info)
            if progress_callback:
                progress_callback(idx + 1, len(all_pool_records),
                                  f"保留涨停分析 {rec['code']} {rec.get('name', '')}")
        continuation_candidates.sort(key=lambda x: -x["score"])

        # 阶段5：二波接力候选（历史数据 + 行情都已缓存）
        if self._log:
            self._log("涨停预测：阶段5 - 识别二波接力候选...")

        first_board_candidates = self._scan_followthrough_candidates_cached(
            hot_industries, spot_df, zt_codes, compare_context, progress_callback, lookback_days=lookback_days,
        )

        # 阶段6：首板涨停候选（最近 N 日未涨停、今日量价启动）
        if self._log:
            self._log("涨停预测：阶段6 - 识别首板涨停候选...")
        fresh_first_board_candidates = self._scan_fresh_first_board_candidates_cached(
            spot_df, zt_codes, hot_industries, compare_context, progress_callback,
        )

        # 阶段7：断板反包候选（近期涨停被打掉，今日逼近反包）
        if self._log:
            self._log("涨停预测：阶段7 - 识别断板反包候选...")
        broken_board_wrap_candidates = self._scan_broken_board_wrap_candidates_cached(
            spot_df, zt_codes, hot_industries, compare_context, progress_callback,
        )

        # 阶段8：趋势涨停候选（多头排列、稳健上行）
        if self._log:
            self._log("涨停预测：阶段8 - 识别趋势涨停候选...")
        trend_limit_up_candidates = self._scan_trend_limit_up_candidates_cached(
            spot_df, zt_codes, hot_industries, compare_context, progress_callback,
        )

        # 摘要
        summary_lines = [
            f"预测日期：基于 {trade_date} 数据预测次日涨停候选",
            f"环境样本：最近 {compare_context.get('pair_count', 0)} 组首板晋级对比",
            f"今日涨停总数：{len(all_pool_records)} 只",
            f"保留涨停候选：{len(continuation_candidates)} 只（得分>=40）",
            f"二波接力候选：{len(first_board_candidates)} 只（得分>=50）",
            f"首板涨停候选：{len(fresh_first_board_candidates)} 只（5日未涨停，得分>=45）",
            f"反包/承接候选：{len(broken_board_wrap_candidates)} 只（近期涨停被打掉反包 / 不破前涨停价承接，得分>=50）",
            f"趋势涨停候选：{len(trend_limit_up_candidates)} 只（多头排列稳健上行，得分>=65）",
        ]
        latest_cont_rate = compare_context.get("latest_continuation_rate")
        avg_cont_rate = compare_context.get("avg_continuation_rate")
        if latest_cont_rate is not None:
            summary_lines.append(f"昨日首板最新晋级率：{latest_cont_rate:.1f}%")
        if avg_cont_rate is not None:
            summary_lines.append(f"近{compare_context.get('pair_count', 0)}组平均晋级率：{avg_cont_rate:.1f}%")
        if hot_industries:
            top3 = sorted(hot_industries.items(), key=lambda x: -x[1])[:3]
            summary_lines.append(f"热门行业：{'、'.join(f'{k}({v})' for k, v in top3)}")
        if theme_size_map:
            top_themes = sorted(theme_size_map.items(), key=lambda x: -x[1])[:3]
            summary_lines.append(
                f"AI 题材聚类：{'、'.join(f'{k}({v}只)' for k, v in top_themes)}"
            )
        if lhb_map:
            net_buys = [(c, v.get("net_buy", 0)) for c, v in lhb_map.items() if v.get("net_buy", 0) > 0]
            net_buys.sort(key=lambda x: -x[1])
            if net_buys:
                top_lhb = net_buys[:3]
                summary_lines.append(
                    f"龙虎榜净买 TOP3：{'、'.join(f'{c}({v/1e8:.2f}亿)' for c, v in top_lhb)}"
                )
        if northbound_map:
            top_nb = sorted(northbound_map.items(), key=lambda x: -x[1])[:3]
            summary_lines.append(
                f"北向 3 日加仓 TOP3：{'、'.join(f'{c}({v/1e4:.2f}亿)' for c, v in top_nb if v > 0)}"
            )
        if board_strength:
            top_boards = sorted(board_strength.items(), key=lambda x: -x[1])[:5]
            summary_lines.append(
                f"强势板块 TOP5：{'、'.join(f'{k}({v:+.1f}%)' for k, v in top_boards)}"
            )

        result = {
            "trade_date": trade_date,
            "profile": profile,
            "profile_samples": feature_samples,
            "continuation_candidates": continuation_candidates,
            "first_board_candidates": first_board_candidates,
            "fresh_first_board_candidates": fresh_first_board_candidates,
            "broken_board_wrap_candidates": broken_board_wrap_candidates,
            "trend_limit_up_candidates": trend_limit_up_candidates,
            "hot_industries": hot_industries,
            "compare_context": compare_context,
            "summary": "\n".join(summary_lines),
        }
        try:
            save_last_limit_up_prediction(result)
        except Exception:
            pass
        try:
            save_limit_up_prediction_record(result)
        except Exception:
            pass
        return result

    def _build_compare_market_context(
        self,
        trade_date: str,
        lookback_days: int,
    ) -> Dict[str, Any]:
        """从最近几组涨停对比中提炼市场环境。"""
        window_days = max(2, int(lookback_days or 2) + 1)
        trade_dates = self.fetcher._recent_trade_dates(trade_date, window_days)
        pair_stats: List[Dict[str, Any]] = []

        for idx in range(1, len(trade_dates)):
            prev_date = trade_dates[idx - 1]
            cur_date = trade_dates[idx]
            try:
                compare = self.fetcher.compare_limit_up_pools(cur_date, prev_date)
            except Exception as exc:
                logger.debug("涨停预测获取涨停对比 %s/%s 失败: %s", cur_date, prev_date, exc)
                continue

            yesterday_first = compare.get("yesterday_first", []) or []
            continued = compare.get("continued_codes", []) or []
            lost = compare.get("lost_codes", []) or []
            first_count = len(yesterday_first)
            rate = round(len(continued) / first_count * 100, 1) if first_count else None
            pair_stats.append({
                "today_date": cur_date,
                "yesterday_date": prev_date,
                "yesterday_first_count": first_count,
                "continued_count": len(continued),
                "lost_count": len(lost),
                "continuation_rate": rate,
                "today_first_count": len(compare.get("today_first", []) or []),
            })

        valid_rates = [item["continuation_rate"] for item in pair_stats if item.get("continuation_rate") is not None]
        avg_rate = round(sum(valid_rates) / len(valid_rates), 1) if valid_rates else None
        latest_rate = pair_stats[-1]["continuation_rate"] if pair_stats else None
        latest_first_count = pair_stats[-1]["today_first_count"] if pair_stats else 0

        return {
            "trade_dates": trade_dates,
            "pair_stats": pair_stats,
            "pair_count": len(pair_stats),
            "avg_continuation_rate": avg_rate,
            "latest_continuation_rate": latest_rate,
            "latest_first_count": latest_first_count,
        }

    def _parse_full_pool(self, pool_df: pd.DataFrame) -> List[Dict[str, Any]]:
        return _scoring_shared.parse_full_pool(pool_df)

    @staticmethod
    def _count_pool_industries(pool_df: pd.DataFrame) -> Dict[str, int]:
        return _scoring_shared.count_pool_industries(pool_df)

    def _theme_bonus(
        self,
        code: str,
        industry: str,
        compare_context: Dict[str, Any],
    ) -> Tuple[float, Optional[str]]:
        return _scoring_shared.theme_bonus(code, industry, compare_context)

    def _capital_flow_bonus(
        self,
        code: str,
        compare_context: Dict[str, Any],
        *,
        industry: str = "",
        boards: int = 0,
    ) -> Tuple[float, List[str]]:
        return _scoring_shared.capital_flow_bonus(
            code, compare_context, industry=industry, boards=boards,
        )

    def _vol_ratio_with_baseline(
        self,
        volume: pd.Series,
        t: int,
    ) -> Tuple[Optional[float], Optional[float]]:
        return _scoring_shared.vol_ratio_with_baseline(volume, t)

    def _score_continuation(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
    ) -> Dict[str, Any]:
        """对涨停股进行连板延续评分（thin delegate → scoring/cont.py）。"""
        return _scoring_cont.score_continuation(
            rec, hot_industries,
            fetcher=self.fetcher,
            log_fn=self._log,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
            limit_up_threshold_pct_fn=self._limit_up_threshold_pct,
        )

    def _score_continuation_by_compare(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
        compare_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """结合最近涨停对比环境评分（thin delegate → scoring/cont.py）。"""
        return _scoring_cont.score_continuation_by_compare(
            rec, hot_industries, compare_context,
            fetcher=self.fetcher,
            log_fn=self._log,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
            limit_up_threshold_pct_fn=self._limit_up_threshold_pct,
            classify_limit_up_pattern_fn=self.classify_limit_up_pattern,
        )

    def _scan_followthrough_candidates_cached(
        self,
        hot_industries: Dict[str, int],
        spot_df: Optional[pd.DataFrame],
        zt_codes: set,
        compare_context: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        *,
        lookback_days: int = 5,
    ) -> List[Dict[str, Any]]:
        """从今日强势股中识别二波接力候选（thin delegate → scoring/first.py）。"""
        return _scoring_first.scan_followthrough_candidates_cached(
            hot_industries,
            spot_df,
            zt_codes,
            compare_context,
            progress_callback,
            fetcher=self.fetcher,
            lookback_days=lookback_days,
            log_fn=self._log,
            limit_up_threshold_pct_fn=self._limit_up_threshold_pct,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
            filter_strong_stocks_fn=self._filter_strong_stocks,
        )

    def _score_followthrough_candidate(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
        compare_context: Dict[str, Any],
        *,
        lookback_days: int = 5,
    ) -> Optional[Dict[str, Any]]:
        """二波接力候选评分（thin delegate → scoring/first.py）。"""
        return _scoring_first.score_followthrough_candidate(
            rec,
            hot_industries,
            compare_context,
            fetcher=self.fetcher,
            lookback_days=lookback_days,
            log_fn=self._log,
            limit_up_threshold_pct_fn=self._limit_up_threshold_pct,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
        )

    @staticmethod
    def _limit_up_threshold_pct(code: str) -> float:
        """A股各板块涨停阈值（百分比）。ST/退市单独处理，本预测已排除 ST。"""
        c = (code or "").strip()
        if c.startswith(("30", "68")):
            return 19.5
        if c.startswith(("43", "83", "87", "88", "92")):
            return 29.5
        return 9.5

    def _scan_fresh_first_board_candidates_cached(
        self,
        spot_df: Optional[pd.DataFrame],
        zt_codes: set,
        hot_industries: Dict[str, int],
        compare_context: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        *,
        cooldown_days: int = 5,
    ) -> List[Dict[str, Any]]:
        """从全市场强势股中识别"首板涨停"候选（thin delegate → scoring/fresh.py）。"""
        return _scoring_fresh.scan_fresh_first_board_candidates_cached(
            spot_df,
            zt_codes,
            hot_industries,
            compare_context,
            progress_callback,
            fetcher=self.fetcher,
            cooldown_days=cooldown_days,
            log_fn=self._log,
            limit_up_threshold_pct_fn=self._limit_up_threshold_pct,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
            filter_strong_stocks_fn=self._filter_strong_stocks,
        )

    def _score_fresh_first_board(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
        compare_context: Dict[str, Any],
        *,
        cooldown_days: int = 5,
    ) -> Optional[Dict[str, Any]]:
        """对"近期未涨停、今日量价启动"的强势股评分（thin delegate → scoring/fresh.py）。"""
        return _scoring_fresh.score_fresh_first_board(
            rec,
            hot_industries,
            compare_context,
            fetcher=self.fetcher,
            cooldown_days=cooldown_days,
            log_fn=self._log,
            limit_up_threshold_pct_fn=self._limit_up_threshold_pct,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
        )

    def _scan_broken_board_wrap_candidates_cached(
        self,
        spot_df: Optional[pd.DataFrame],
        zt_codes: set,
        hot_industries: Dict[str, int],
        compare_context: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        *,
        lookback_days: int = 5,
        drop_threshold_pct: float = -3.0,
    ) -> List[Dict[str, Any]]:
        """识别"断板反包"候选（thin delegate → scoring/wrap.py）。"""
        return _scoring_wrap.scan_broken_board_wrap_candidates_cached(
            spot_df,
            zt_codes,
            hot_industries,
            compare_context,
            progress_callback,
            fetcher=self.fetcher,
            lookback_days=lookback_days,
            drop_threshold_pct=drop_threshold_pct,
            log_fn=self._log,
            limit_up_threshold_pct_fn=self._limit_up_threshold_pct,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
            filter_strong_stocks_fn=self._filter_strong_stocks,
            filter_ma5_pullback_stocks_fn=self._filter_ma5_pullback_stocks,
        )

    def _score_broken_board_wrap(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
        compare_context: Dict[str, Any],
        *,
        lookback_days: int = 5,
        drop_threshold_pct: float = -3.0,
    ) -> Optional[Dict[str, Any]]:
        """对断板反包 / 强势承接候选评分（thin delegate → scoring/wrap.py）。"""
        return _scoring_wrap.score_broken_board_wrap(
            rec,
            hot_industries,
            compare_context,
            fetcher=self.fetcher,
            lookback_days=lookback_days,
            drop_threshold_pct=drop_threshold_pct,
            log_fn=self._log,
            limit_up_threshold_pct_fn=self._limit_up_threshold_pct,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
        )

    def _scan_trend_limit_up_candidates_cached(
        self,
        spot_df: Optional[pd.DataFrame],
        zt_codes: set,
        hot_industries: Dict[str, int],
        compare_context: Dict[str, Any],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> List[Dict[str, Any]]:
        """识别"趋势涨停"候选（thin delegate → scoring/trend.py）。"""
        return _scoring_trend.scan_trend_limit_up_candidates_cached(
            spot_df,
            zt_codes,
            hot_industries,
            compare_context,
            progress_callback,
            fetcher=self.fetcher,
            log_fn=self._log,
            limit_up_threshold_pct_fn=self._limit_up_threshold_pct,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
            filter_strong_stocks_fn=self._filter_strong_stocks,
            filter_ma5_pullback_stocks_fn=self._filter_ma5_pullback_stocks,
        )

    def _score_trend_limit_up(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
        compare_context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """对趋势涨停候选评分（thin delegate → scoring/trend.py）。"""
        return _scoring_trend.score_trend_limit_up(
            rec,
            hot_industries,
            compare_context,
            fetcher=self.fetcher,
            log_fn=self._log,
            limit_up_threshold_pct_fn=self._limit_up_threshold_pct,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
        )

    def _scan_first_board_candidates_cached(
        self,
        today_pool_df: pd.DataFrame,
        hot_industries: Dict[str, int],
        profile: Dict[str, Any],
        spot_df: Optional[pd.DataFrame],
        zt_codes: set,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> List[Dict[str, Any]]:
        """用画像匹配候选股（行情和历史数据均已提前缓存）。"""
        if spot_df is None or spot_df.empty:
            return []

        strong_stocks = self._filter_strong_stocks(spot_df, zt_codes)
        ma5_pullback_stocks = self._filter_ma5_pullback_stocks(spot_df, zt_codes)

        seen_codes = set()
        merged: List[Dict[str, Any]] = []
        for rec in strong_stocks:
            if rec["code"] not in seen_codes:
                seen_codes.add(rec["code"])
                merged.append(rec)
        for rec in ma5_pullback_stocks:
            if rec["code"] not in seen_codes:
                seen_codes.add(rec["code"])
                merged.append(rec)

        if not merged:
            return []

        if self._log:
            self._log(f"涨停预测：强势股 {len(strong_stocks)} 只 + 回踩MA5 {len(ma5_pullback_stocks)} 只，"
                      f"合并去重后 {len(merged)} 只")

        # 历史数据已在阶段3统一预取，这里直接评分
        candidates = []
        total = len(merged)
        for idx, rec in enumerate(merged):
            score_info = self._score_first_board_by_profile(rec, hot_industries, profile)
            if score_info["score"] >= 50:
                candidates.append(score_info)
            if progress_callback:
                progress_callback(idx + 1, total, f"首板匹配 {rec['code']} {rec.get('name', '')}")

        candidates.sort(key=lambda x: -x["score"])
        return candidates[:50]

    @staticmethod
    def _parse_lhb_jiedu(jiedu: str) -> Dict[str, Any]:
        """解析龙虎榜「解读」字段。

        返回:
          institution_buy: 机构买入家数
          institution_sell: 机构卖出家数
          main_t_trade: 是否"主力做T"
          hot_money_region: 命中的知名游资地区（西藏/宁波/上海/江苏/深圳/广东/浙江），无则 None
          ordinary_seats_only: 是否只是"普通席位"（弱信号）
          top1_dominant: 是否"买一主买"（单一席位主导）
          success_rate: 历史接力成功率 %（None=无）
          is_buy_dominant: 整体偏买入还是卖出
        """
        info: Dict[str, Any] = {
            "institution_buy": 0,
            "institution_sell": 0,
            "main_t_trade": False,
            "hot_money_region": None,
            "ordinary_seats_only": False,
            "top1_dominant": False,
            "success_rate": None,
            "is_buy_dominant": False,
        }
        if not jiedu:
            return info
        import re
        m = re.search(r"(\d+)家机构买入", jiedu)
        if m:
            info["institution_buy"] = int(m.group(1))
            info["is_buy_dominant"] = True
        m = re.search(r"(\d+)家机构卖出", jiedu)
        if m:
            info["institution_sell"] = int(m.group(1))
        if "主力做T" in jiedu or "营业部接力T接" in jiedu:
            info["main_t_trade"] = True
            if "买入" in jiedu or "T接" in jiedu:
                info["is_buy_dominant"] = True
        for region in ("西藏", "宁波", "上海", "江苏", "深圳", "广东", "浙江", "北京"):
            if region in jiedu and "买入" in jiedu and "卖出" not in jiedu.split(region, 1)[1][:20]:
                info["hot_money_region"] = region
                info["is_buy_dominant"] = True
                break
        if "普通席位" in jiedu:
            info["ordinary_seats_only"] = True
        if "买一主买" in jiedu:
            info["top1_dominant"] = True
            info["is_buy_dominant"] = True
        m = re.search(r"成功率(\d+(?:\.\d+)?)%", jiedu)
        if m:
            try:
                info["success_rate"] = float(m.group(1))
            except ValueError:
                pass
        return info

    def _load_lhb_for_date(self, trade_date: str) -> Dict[str, Dict[str, Any]]:
        """加载指定交易日的龙虎榜数据。

        返回 dict: code → {
            "net_buy": 净买入额,
            "buy": 买入额,
            "sell": 卖出额,
            "reason": 上榜原因,
            "jiedu": 解读原文,
            "jiedu_parsed": 解析后的 dict（机构家数/游资地区/成功率等）
        }
        网络失败/无数据返回空 dict（不阻塞预测）。

        缓存键: stock_filter_lhb_<trade_date>
        """
        from stock_store import load_app_config, save_app_config
        cache_key = f"stock_filter_lhb_{str(trade_date).strip()}"
        cached = load_app_config(cache_key, default=None)
        # 旧缓存里没有 jiedu_parsed，需要重建一次
        if isinstance(cached, dict) and cached:
            sample = next(iter(cached.values())) if cached else None
            if isinstance(sample, dict) and "jiedu_parsed" in sample:
                return cached  # type: ignore[return-value]

        try:
            import akshare as ak
            from stock_data import _retry_ak_call
            df = _retry_ak_call(
                ak.stock_lhb_detail_em,
                start_date=str(trade_date).strip(),
                end_date=str(trade_date).strip(),
            )
        except Exception as exc:
            if self._log:
                self._log(f"涨停预测：龙虎榜拉取失败 {exc}")
            return {}

        if df is None or df.empty:
            return {}

        result: Dict[str, Dict[str, Any]] = {}
        for _, row in df.iterrows():
            try:
                code = str(row.get("代码", "")).strip().zfill(6)
                if not code or len(code) != 6:
                    continue
                net = float(row.get("龙虎榜净买额") or 0)
                buy = float(row.get("龙虎榜买入额") or 0)
                sell = float(row.get("龙虎榜卖出额") or 0)
                reason = str(row.get("上榜原因") or "").strip()
                jiedu = str(row.get("解读") or "").strip()
                # 同一天可能多次上榜，累加金额并保留首次解读
                if code in result:
                    result[code]["net_buy"] += net
                    result[code]["buy"] += buy
                    result[code]["sell"] += sell
                else:
                    result[code] = {
                        "net_buy": net,
                        "buy": buy,
                        "sell": sell,
                        "reason": reason,
                        "jiedu": jiedu,
                        "jiedu_parsed": self._parse_lhb_jiedu(jiedu),
                    }
            except (TypeError, ValueError):
                continue

        if result:
            try:
                save_app_config(cache_key, result)
            except Exception:
                pass
        return result

    def _load_industry_board_strength(self) -> Dict[str, float]:
        """加载东财行业板块涨跌幅，识别强势板块。

        返回 dict: 行业名 → 当日涨跌幅 %
        """
        from stock_store import load_app_config, save_app_config
        from datetime import datetime as _dt
        today_key = _dt.now().strftime("%Y%m%d_%H")  # 小时级缓存（盘中变化）
        cache_key = f"stock_filter_board_strength_{today_key}"
        cached = load_app_config(cache_key, default=None)
        if isinstance(cached, dict) and cached:
            return cached  # type: ignore[return-value]

        try:
            import akshare as ak
            from stock_data import _retry_ak_call
            df = _retry_ak_call(ak.stock_board_industry_name_em)
        except Exception as exc:
            if self._log:
                self._log(f"涨停预测：板块涨跌幅拉取失败 {exc}")
            return {}

        if df is None or df.empty:
            return {}

        result: Dict[str, float] = {}
        for _, row in df.iterrows():
            try:
                name = str(row.get("板块名称", "")).strip()
                chg = float(row.get("涨跌幅") or 0)
                if name:
                    result[name] = chg
            except (TypeError, ValueError):
                continue

        if result:
            try:
                save_app_config(cache_key, result)
            except Exception:
                pass
        return result

    def _load_northbound_accumulation(self) -> Dict[str, float]:
        """加载北向资金 3 日加仓榜（沪股通 + 深股通合并）。

        返回 dict: code → 3 日持股市值变化（万元）
        正值表示北向加仓，负值表示北向减仓。

        北向数据 T+1 才公布，预测时只能取最近一个交易日的快照。
        当日缓存 24h，避免重复拉取。
        """
        from stock_store import load_app_config, save_app_config
        from datetime import datetime as _dt
        today_key = _dt.now().strftime("%Y%m%d")
        cache_key = f"stock_filter_northbound_{today_key}"
        cached = load_app_config(cache_key, default=None)
        if isinstance(cached, dict) and cached:
            return cached  # type: ignore[return-value]

        result: Dict[str, float] = {}
        try:
            import akshare as ak
            from stock_data import _retry_ak_call
            for market in ("沪股通", "深股通"):
                try:
                    df = _retry_ak_call(
                        ak.stock_hsgt_hold_stock_em,
                        market=market,
                        indicator="3日排行",
                    )
                except Exception as exc:
                    if self._log:
                        self._log(f"涨停预测：北向 {market} 拉取失败 {exc}")
                    continue
                if df is None or df.empty:
                    continue
                # 列名形如 "3日增持估计-市值"（万元，正=北向加仓，负=减仓）
                value_col = next(
                    (c for c in df.columns
                     if "增持" in str(c) and "市值" in str(c) and "增幅" not in str(c)),
                    None,
                )
                if value_col is None:
                    # 兼容旧版列名
                    value_col = next(
                        (c for c in df.columns if "市值变化" in str(c) and "3日" in str(c)),
                        None,
                    )
                if value_col is None:
                    continue
                for _, row in df.iterrows():
                    try:
                        code = str(row.get("代码", "")).strip().zfill(6)
                        if not code or len(code) != 6:
                            continue
                        change = float(row.get(value_col) or 0)
                        # 同一只票如果在两个市场都有持股不会重复（沪/深分离），累加保险
                        result[code] = result.get(code, 0.0) + change
                    except (TypeError, ValueError):
                        continue
        except Exception as exc:
            if self._log:
                self._log(f"涨停预测：北向资金加载失败 {exc}")
            return {}

        if result:
            try:
                save_app_config(cache_key, result)
            except Exception:
                pass
        return result

    def _fetch_spot_snapshot(self) -> Optional[pd.DataFrame]:
        """获取全市场实时行情快照（只调一次 API）。
        优先东财，东财熔断时自动回退到新浪。
        """
        import akshare as ak
        from stock_data import _retry_ak_call, _eastmoney_circuit_breaker_open
        # 东财可用时优先东财
        if not _eastmoney_circuit_breaker_open():
            try:
                if self._log:
                    self._log("涨停预测：正在获取全市场实时行情快照（东财）...")
                return _retry_ak_call(ak.stock_zh_a_spot_em)
            except Exception as e:
                if self._log:
                    self._log(f"涨停预测：东财实时行情失败: {e}，尝试新浪备选...")
        # 新浪备选
        try:
            if self._log:
                self._log("涨停预测：正在获取全市场实时行情快照（新浪，约30s）...")
            df = _retry_ak_call(ak.stock_zh_a_spot)
            if df is not None and not df.empty:
                # 新浪代码带交易所前缀（如 sh600000），去掉前缀统一为纯数字
                if "代码" in df.columns:
                    df["代码"] = df["代码"].astype(str).str.replace(r"^(sh|sz|bj)", "", regex=True).str.strip().str.zfill(6)
                return df
        except Exception as e2:
            if self._log:
                self._log(f"涨停预测：新浪实时行情也失败: {e2}")
        return None

    @staticmethod
    def _parse_spot_record(row, exclude_codes: set) -> Optional[Dict[str, Any]]:
        """从实时行情行中解析基础记录，返回 None 表示需跳过。"""
        code = str(row.get("代码", "")).strip().zfill(6)
        if code in exclude_codes:
            return None
        name = str(row.get("名称", ""))
        if "ST" in name.upper():
            return None
        close = float(row["最新价"]) if pd.notna(row.get("最新价")) else None
        if close is None or close <= 0:
            return None
        change_pct = float(row["涨跌幅"]) if pd.notna(row.get("涨跌幅")) else None
        amount_val = float(row["成交额"]) if pd.notna(row.get("成交额")) else None
        if amount_val is not None and amount_val < 5000_0000:
            return None
        volume_val = float(row["成交量"]) if pd.notna(row.get("成交量")) else None
        turnover = float(row["换手率"]) if pd.notna(row.get("换手率")) else None
        industry = str(
            row.get("所属行业", row.get("行业", row.get("板块", ""))) or ""
        ).strip()
        return {
            "code": code, "name": name, "change_pct": change_pct,
            "close": close, "volume": volume_val, "amount": amount_val,
            "turnover": turnover, "industry": industry,
        }

    def _filter_strong_stocks(
        self, spot_df: pd.DataFrame, exclude_codes: set
    ) -> List[Dict[str, Any]]:
        """从行情快照中筛选涨幅 3%~9.95% 的强势股（含擦边没封板的 9.x% 票）。

        历史 K 线已统一从本地缓存读取，无需再做 top-N 截断。
        """
        records = []
        for _, row in spot_df.iterrows():
            rec = self._parse_spot_record(row, exclude_codes)
            if rec is None:
                continue
            chg = rec.get("change_pct")
            if chg is None or chg < 3.0 or chg >= 9.95:
                continue
            records.append(rec)
        records.sort(key=lambda x: -(x.get("change_pct") or 0))
        return records

    def _filter_ma5_pullback_stocks(
        self, spot_df: pd.DataFrame, exclude_codes: set
    ) -> List[Dict[str, Any]]:
        """从行情快照中筛选涨跌幅 -5%~+3% 的回踩MA5候选（用于反包/承接候选）。

        历史 K 线已统一从本地缓存读取，无需再做 top-N 截断。
        """
        records = []
        for _, row in spot_df.iterrows():
            rec = self._parse_spot_record(row, exclude_codes)
            if rec is None:
                continue
            chg = rec.get("change_pct")
            if chg is None or chg < -5.0 or chg >= 3.0:
                continue
            records.append(rec)
        records.sort(key=lambda x: -(x.get("amount") or 0))
        return records

    def _score_first_board_by_profile(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
        profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        """用涨停前兆画像对强势股打分。

        核心思路：把当前股票的特征和画像中涨停股 T-1 日特征对比，
        越接近画像中位数/均值的，得分越高。
        """
        code = rec["code"]
        name = rec.get("name", "")
        score = 0.0
        reasons: List[str] = []
        change_pct = rec.get("change_pct", 0)
        turnover = rec.get("turnover")

        # 当日涨幅
        if change_pct is not None:
            if change_pct >= 8:
                score += 18
                reasons.append(f"涨{change_pct:.1f}%接近涨停+18")
            elif change_pct >= 6:
                score += 12
                reasons.append(f"涨{change_pct:.1f}%+12")
            elif change_pct >= 3:
                score += 6
                reasons.append(f"涨{change_pct:.1f}%+6")

        # 获取历史数据计算特征（已预取到缓存，直接读取）
        try:
            # 只使用本地缓存，不发起网络请求
            history = self.fetcher.get_history_data(
                code, days=120, force_refresh=False,
                request_plan=self._build_local_cache_history_plan(reason="predict-first-board-cache-only"),
            )
        except Exception as exc:
            logger.debug("预测首板获取历史 %s 失败: %s", code, exc)
            history = None

        industry = ""
        vol_ratio = None
        position_60d = None
        trend_10d = None
        ma_bullish = False

        if history is not None and not history.empty and len(history) >= 10:
            df = history.sort_values("date").reset_index(drop=True)
            close = pd.to_numeric(df["close"], errors="coerce")
            volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
            amount = pd.to_numeric(df.get("amount"), errors="coerce") if "amount" in df.columns else pd.Series(dtype=float)
            latest_close = float(close.iloc[-1]) if not pd.isna(close.iloc[-1]) else None
            t = len(df) - 1  # 当前最新一行

            ma5 = close.rolling(5, min_periods=5).mean()
            ma10 = close.rolling(10, min_periods=10).mean()
            ma20 = close.rolling(20, min_periods=20).mean()
            ma5_val = float(ma5.iloc[t]) if not pd.isna(ma5.iloc[t]) else None
            ma10_val = float(ma10.iloc[t]) if not pd.isna(ma10.iloc[t]) else None
            ma20_val = float(ma20.iloc[t]) if not pd.isna(ma20.iloc[t]) else None

            # --- 量比匹配 ---
            if len(volume) >= 6 and not pd.isna(volume.iloc[t]):
                vol_window = volume.iloc[max(0, t - 5):t].dropna()
                if not vol_window.empty and float(vol_window.mean()) > 0:
                    vol_ratio = round(float(volume.iloc[t]) / float(vol_window.mean()), 2)
                    p = profile.get("vol_ratio_t1", {})
                    p_med = p.get("median")
                    p_p25 = p.get("p25")
                    p_p75 = p.get("p75")
                    if p_med is not None and p_p25 is not None and p_p75 is not None:
                        if p_p25 <= vol_ratio <= p_p75:
                            score += 15
                            reasons.append(f"量比{vol_ratio:.1f}x吻合画像[{p_p25:.1f}~{p_p75:.1f}]+15")
                        elif vol_ratio >= p_med * 0.6:
                            score += 8
                            reasons.append(f"量比{vol_ratio:.1f}x接近画像+8")
                    elif vol_ratio >= 1.5:
                        score += 8
                        reasons.append(f"放量{vol_ratio:.1f}x+8")

            # --- 额比匹配 ---
            if len(amount) >= 6 and not pd.isna(amount.iloc[t]):
                amt_window = amount.iloc[max(0, t - 5):t].dropna()
                if not amt_window.empty and float(amt_window.mean()) > 0:
                    amt_ratio = round(float(amount.iloc[t]) / float(amt_window.mean()), 2)
                    p = profile.get("amt_ratio_t1", {})
                    p_med = p.get("median")
                    if p_med is not None and amt_ratio >= p_med * 0.8:
                        score += 5
                        reasons.append(f"额比{amt_ratio:.1f}x匹配+5")

            # --- 均线匹配 ---
            if ma5_val is not None and ma10_val is not None and ma20_val is not None:
                if ma5_val > ma10_val > ma20_val:
                    ma_bullish = True
                    p_bull = profile.get("ma_bullish", {})
                    if p_bull.get("ratio", 0) >= 50:
                        score += 10
                        reasons.append(f"多头排列(画像{p_bull['ratio']:.0f}%)+10")
                    else:
                        score += 5
                        reasons.append("多头排列+5")

            # 站上MA5
            if latest_close is not None and ma5_val is not None and latest_close > ma5_val:
                p_above = profile.get("above_ma5", {})
                if p_above.get("ratio", 0) >= 60:
                    score += 5
                    reasons.append(f"站上MA5(画像{p_above['ratio']:.0f}%)+5")

            # --- MA5 距离匹配 ---
            if latest_close and ma5_val and ma5_val > 0:
                dist_ma5 = round((latest_close / ma5_val - 1) * 100, 2)
                p = profile.get("dist_ma5_pct", {})
                p_p25 = p.get("p25")
                p_p75 = p.get("p75")
                if p_p25 is not None and p_p75 is not None:
                    if p_p25 <= dist_ma5 <= p_p75:
                        score += 5
                        reasons.append(f"距MA5 {dist_ma5:+.1f}%吻合+5")

            # --- 回踩MA5检测 ---
            # 收盘接近或略低于MA5（-3%~+1%），且前几日曾站上MA5
            if latest_close and ma5_val and ma5_val > 0:
                dist_ma5_now = (latest_close / ma5_val - 1) * 100
                if -3.0 <= dist_ma5_now <= 1.0:
                    was_above_ma5 = False
                    for lb in range(2, min(6, t + 1)):
                        idx_b = t - lb
                        if idx_b >= 0 and not pd.isna(close.iloc[idx_b]) and not pd.isna(ma5.iloc[idx_b]):
                            if float(close.iloc[idx_b]) > float(ma5.iloc[idx_b]) * 1.01:
                                was_above_ma5 = True
                                break
                    if was_above_ma5:
                        # 回踩MA5，这是涨停前常见形态
                        p_pb = profile.get("ma5_pullback", {})
                        pb_ratio = p_pb.get("ratio", 0)
                        if pb_ratio >= 20:
                            score += 15
                            reasons.append(f"回踩MA5(画像{pb_ratio:.0f}%)+15")
                        else:
                            score += 10
                            reasons.append(f"回踩MA5(距{dist_ma5_now:+.1f}%)+10")

            # --- 60日位置匹配 ---
            if len(close) >= 20 and latest_close is not None:
                window = close.tail(min(60, len(close))).dropna()
                if len(window) >= 10:
                    position_60d = round(float((window < latest_close).sum()) / len(window) * 100, 1)
                    p = profile.get("position_60d", {})
                    p_med = p.get("median")
                    p_p25 = p.get("p25")
                    p_p75 = p.get("p75")
                    if p_med is not None and p_p25 is not None and p_p75 is not None:
                        if p_p25 <= position_60d <= p_p75:
                            score += 8
                            reasons.append(f"位置{position_60d:.0f}%吻合画像[{p_p25:.0f}~{p_p75:.0f}]+8")
                        elif position_60d < 30:
                            score += 5
                            reasons.append(f"低位{position_60d:.0f}%+5")

            # --- 10日趋势 ---
            if t >= 10 and not pd.isna(close.iloc[t - 10]) and close.iloc[t - 10] > 0:
                trend_10d = round((float(close.iloc[t]) / float(close.iloc[t - 10]) - 1) * 100, 1)

            # --- 缩量蓄势匹配 ---
            if len(volume) >= 6:
                vol_3 = volume.iloc[max(0, t - 3):t].dropna()
                vol_5 = volume.iloc[max(0, t - 5):t].dropna()
                if not vol_3.empty and not vol_5.empty and float(vol_5.mean()) > 0:
                    shrink = round(float(vol_3.mean()) / float(vol_5.mean()), 2)
                    p = profile.get("shrink_ratio_t1", {})
                    p_med = p.get("median")
                    if p_med is not None and shrink <= p_med and vol_ratio is not None and vol_ratio >= 1.5:
                        score += 10
                        reasons.append(f"缩量蓄势后放量(缩{shrink:.2f}/量比{vol_ratio:.1f}x)+10")

        # 板块热度
        if industry and hot_industries.get(industry, 0) >= 3:
            score += 10
            reasons.append(f"热门板块({hot_industries[industry]}只)+10")
        elif industry and hot_industries.get(industry, 0) >= 2:
            score += 5
            reasons.append(f"板块有{hot_industries[industry]}只+5")

        # 换手率
        if turnover is not None:
            p = profile.get("turnover_t1", {})
            p_p25 = p.get("p25")
            p_p75 = p.get("p75")
            if p_p25 is not None and p_p75 is not None:
                if p_p25 <= turnover <= p_p75:
                    score += 5
                    reasons.append(f"换手{turnover:.1f}%吻合画像+5")
            elif 3 <= turnover <= 20:
                score += 3
                reasons.append(f"换手{turnover:.1f}%适中+3")
            if turnover > 40:
                score -= 5
                reasons.append(f"换手{turnover:.1f}%过高-5")

        final_score = max(0, min(100, int(round(score))))
        return {
            "code": code,
            "name": name,
            "industry": industry,
            "close": rec.get("close"),
            "change_pct": change_pct,
            "turnover": turnover,
            "vol_ratio": vol_ratio,
            "position_60d": position_60d,
            "trend_10d": trend_10d,
            "ma_bullish": ma_bullish,
            "score": final_score,
            "reasons": " / ".join(reasons[:8]),
            "predict_type": "首板候选",
        }
