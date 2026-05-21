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
from src.services.scoring import first_board as _scoring_first_board
from src.services.scoring import predict as _scoring_predict
from src.services.scanning import orchestrator as _scanning


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
        """股票池板块 / 交易所过滤（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.filter_scan_universe(all_stocks, allowed_boards, allowed_exchanges, log=log)

    def _limit_scan_subset(self, all_stocks: pd.DataFrame, max_stocks: int) -> pd.DataFrame:
        """裁剪扫描子集（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.limit_scan_subset(all_stocks, max_stocks)

    def _resolve_scan_workers(self, max_workers: Optional[int]) -> Tuple[int, int]:
        """解析并发线程数（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.resolve_scan_workers(max_workers, fetcher=self.fetcher)

    def _build_scan_history_plan(self, history_source: str, local_history_only: bool) -> HistoryRequestPlan:
        """构建扫描历史请求计划（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.build_scan_history_plan(
            history_source,
            local_history_only,
            fetcher=self.fetcher,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
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
        """随机分配扫描任务到镜像（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.assign_scan_jobs(subset, available_mirrors)

    def _log_scan_history_context(
        self,
        log: Optional[Callable[[str], None]],
        coverage: Dict[str, Any],
        history_plan: HistoryRequestPlan,
        local_history_only: bool,
    ) -> None:
        """打印扫描历史上下文日志（thin delegate -> scanning/orchestrator.py）。"""
        _scanning.log_scan_history_context(log, coverage, history_plan, local_history_only)

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
        """打印扫描执行上下文日志（thin delegate -> scanning/orchestrator.py）。"""
        _scanning.log_scan_execution_context(
            log,
            total_universe,
            total,
            workers,
            requested_workers,
            local_history_only,
            available_mirrors,
            mirror_counts,
            trend_days=self.trend_days,
            ma_period=self.ma_period,
        )

    def _submit_scan_tasks(
        self,
        executor: ThreadPoolExecutor,
        assigned_jobs: List[Tuple[Dict[str, Any], Optional[str]]],
        available_mirrors: List[str],
        history_plan: HistoryRequestPlan,
    ) -> Dict[Any, Tuple[str, str, str, str, Optional[str]]]:
        """向线程池提交扫描任务（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.submit_scan_tasks(
            executor,
            assigned_jobs,
            available_mirrors,
            history_plan,
            filter_stock_fn=self.filter_stock,
        )

    def _pending_scan_sample_text(
        self,
        pending,
        future_to_meta: Dict[Any, Tuple[str, str, str, str, Optional[str]]],
    ) -> str:
        """生成待处理任务的样例代码文本（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.pending_scan_sample_text(pending, future_to_meta)

    def _scan_mirror_label(self, mirror: Optional[str]) -> str:
        """生成镜像简短标签（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.scan_mirror_label(mirror)

    def _should_stop_scan(
        self,
        should_stop: Optional[Callable[[], bool]],
        log: Optional[Callable[[str], None]],
    ) -> bool:
        """判断是否应停止扫描（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.should_stop_scan(should_stop, log)

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
        """打印扫描等待日志（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.log_pending_scan_wait(
            log,
            pending,
            future_to_meta,
            completed,
            total,
            results,
            started_at,
            last_report,
        )

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
        """构造扫描异常结果（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.build_scan_error_result(code, name, board, exchange, mirror, error, log)

    def _resolve_scan_future_result(
        self,
        fut,
        future_to_meta: Dict[Any, Tuple[str, str, str, str, Optional[str]]],
        log: Optional[Callable[[str], None]],
    ) -> Tuple[str, str, str, str, Optional[str], Dict[str, Any]]:
        """解析单个扫描 future 结果（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.resolve_scan_future_result(fut, future_to_meta, log)

    def _log_scan_pass_result(
        self,
        log: Optional[Callable[[str], None]],
        completed: int,
        total: int,
        code: str,
        name: str,
        filter_result: Dict[str, Any],
    ) -> None:
        """打印通过日志（thin delegate -> scanning/orchestrator.py）。"""
        _scanning.log_scan_pass_result(
            log, completed, total, code, name, filter_result, ma_period=self.ma_period
        )

    def _append_scan_hit(
        self,
        results: List[Dict[str, Any]],
        filter_result: Dict[str, Any],
        name: str,
        board: str,
        exchange: str,
    ) -> None:
        """追加扫描命中结果（thin delegate -> scanning/orchestrator.py）。"""
        _scanning.append_scan_hit(results, filter_result, name, board, exchange)

    def _notify_scan_progress(
        self,
        progress_callback,
        completed: int,
        total: int,
        code: str,
        name: str,
    ) -> None:
        """触发扫描进度回调（thin delegate -> scanning/orchestrator.py）。"""
        _scanning.notify_scan_progress(progress_callback, completed, total, code, name)

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
        """节流地打印扫描进度（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.maybe_log_scan_progress(
            log,
            completed,
            total,
            results,
            started_at,
            last_report,
            report_every,
            code,
            name,
            mirror,
        )

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
        """扫描所有股票（thin delegate -> scanning/orchestrator.py）。"""
        return _scanning.scan_all_stocks(
            max_stocks=max_stocks,
            progress_callback=progress_callback,
            max_workers=max_workers,
            history_source=history_source,
            local_history_only=local_history_only,
            should_stop=should_stop,
            refresh_universe=refresh_universe,
            allowed_boards=allowed_boards,
            allowed_exchanges=allowed_exchanges,
            cancel_token=cancel_token,
            fetcher=self.fetcher,
            log_fn=self._log,
            trend_days=self.trend_days,
            ma_period=self.ma_period,
            filter_stock_fn=self.filter_stock,
            result_sort_key_fn=self._result_sort_key,
            log_runtime_diagnostics_fn=self._log_runtime_diagnostics,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
        )

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
        """基于涨停对比 + 二波接力数据预测明日涨停候选（thin delegate -> scoring/predict.py）。"""
        return _scoring_predict.predict_limit_up_candidates(
            trade_date,
            lookback_days,
            progress_callback,
            historical_mode,
            fetcher=self.fetcher,
            log_fn=self._log,
            limit_up_threshold_pct_fn=self._limit_up_threshold_pct,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
            classify_limit_up_pattern_fn=self.classify_limit_up_pattern,
        )

    def _build_compare_market_context(
        self,
        trade_date: str,
        lookback_days: int,
    ) -> Dict[str, Any]:
        """从最近几组涨停对比中提炼市场环境（thin delegate -> scoring/predict.py）。"""
        return _scoring_predict.build_compare_market_context(
            trade_date, lookback_days, fetcher=self.fetcher,
        )

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
        """用画像匹配候选股（thin delegate -> scoring/first_board.py）。"""
        return _scoring_first_board.scan_first_board_candidates_cached(
            today_pool_df,
            hot_industries,
            profile,
            spot_df,
            zt_codes,
            progress_callback,
            fetcher=self.fetcher,
            log_fn=self._log,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
        )

    @staticmethod
    def _parse_lhb_jiedu(jiedu: str) -> Dict[str, Any]:
        """解析龙虎榜「解读」字段（thin delegate -> scoring/first_board.py）。"""
        return _scoring_first_board.parse_lhb_jiedu(jiedu)

    def _load_lhb_for_date(self, trade_date: str) -> Dict[str, Dict[str, Any]]:
        """加载指定交易日的龙虎榜数据（thin delegate -> scoring/first_board.py）。"""
        return _scoring_first_board.load_lhb_for_date(trade_date, log_fn=self._log)

    def _load_industry_board_strength(self) -> Dict[str, float]:
        """加载东财行业板块涨跌幅（thin delegate -> scoring/first_board.py）。"""
        return _scoring_first_board.load_industry_board_strength(log_fn=self._log)

    def _load_northbound_accumulation(self) -> Dict[str, float]:
        """加载北向资金 3 日加仓榜（thin delegate -> scoring/first_board.py）。"""
        return _scoring_first_board.load_northbound_accumulation(log_fn=self._log)

    def _fetch_spot_snapshot(self) -> Optional[pd.DataFrame]:
        """获取全市场实时行情快照（thin delegate -> scoring/first_board.py）。"""
        return _scoring_first_board.fetch_spot_snapshot(log_fn=self._log)

    @staticmethod
    def _parse_spot_record(row, exclude_codes: set) -> Optional[Dict[str, Any]]:
        """从实时行情行中解析基础记录（thin delegate -> scoring/first_board.py）。"""
        return _scoring_first_board.parse_spot_record(row, exclude_codes)

    def _filter_strong_stocks(
        self, spot_df: pd.DataFrame, exclude_codes: set
    ) -> List[Dict[str, Any]]:
        """从行情快照筛选 +3%~+9.95% 强势股（thin delegate -> scoring/first_board.py）。"""
        return _scoring_first_board.filter_strong_stocks(spot_df, exclude_codes)

    def _filter_ma5_pullback_stocks(
        self, spot_df: pd.DataFrame, exclude_codes: set
    ) -> List[Dict[str, Any]]:
        """从行情快照筛选 -5%~+3% 回踩MA5 候选（thin delegate -> scoring/first_board.py）。"""
        return _scoring_first_board.filter_ma5_pullback_stocks(spot_df, exclude_codes)

    def _score_first_board_by_profile(
        self,
        rec: Dict[str, Any],
        hot_industries: Dict[str, int],
        profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        """用涨停前兆画像对强势股打分（thin delegate -> scoring/first_board.py）。"""
        return _scoring_first_board.score_first_board_by_profile(
            rec,
            hot_industries,
            profile,
            fetcher=self.fetcher,
            log_fn=self._log,
            build_local_cache_history_plan_fn=self._build_local_cache_history_plan,
        )
