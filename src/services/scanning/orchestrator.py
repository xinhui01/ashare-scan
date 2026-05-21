"""扫描编排（scan orchestrator）。

19 个模块级函数（参数注入模式），覆盖 stock_filter.scan_all_stocks 的全部协调逻辑：

筹备阶段：
- filter_scan_universe / limit_scan_subset / resolve_scan_workers
- build_scan_history_plan / assign_scan_jobs

日志阶段：
- log_scan_history_context / log_scan_execution_context
- log_pending_scan_wait / log_scan_pass_result / maybe_log_scan_progress

执行阶段：
- submit_scan_tasks / should_stop_scan / pending_scan_sample_text / scan_mirror_label
- resolve_scan_future_result / build_scan_error_result / append_scan_hit / notify_scan_progress

主入口：
- scan_all_stocks

依赖：StockDataFetcher（fetcher 参数）+ 注入的 stock_filter 公开方法
（filter_stock_fn、result_sort_key_fn、log_runtime_diagnostics_fn、
build_local_cache_history_plan_fn）。
"""
from __future__ import annotations

import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from scan_models import HistoryRequestPlan
from src.utils.cancel_token import CancelToken, coerce_should_stop


def filter_scan_universe(
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


def limit_scan_subset(all_stocks: pd.DataFrame, max_stocks: int) -> pd.DataFrame:
    if max_stocks and max_stocks > 0:
        return all_stocks.head(max_stocks).reset_index(drop=True)
    return all_stocks.reset_index(drop=True)


def resolve_scan_workers(
    max_workers: Optional[int],
    *,
    fetcher,
) -> Tuple[int, int]:
    if max_workers is None:
        try:
            max_workers = int(os.environ.get("ASHARE_SCAN_SCAN_WORKERS", "3").strip() or "3")
        except ValueError:
            max_workers = 3
    requested_workers = max(1, min(int(max_workers), 16))
    history_workers = max(1, int(fetcher.history_request_concurrency_limit()))
    return requested_workers, min(requested_workers, history_workers)


def build_scan_history_plan(
    history_source: str,
    local_history_only: bool,
    *,
    fetcher,
    build_local_cache_history_plan_fn: Optional[Callable[..., HistoryRequestPlan]] = None,
) -> HistoryRequestPlan:
    if local_history_only:
        if build_local_cache_history_plan_fn is not None:
            return build_local_cache_history_plan_fn(reason="scan-local-cache-only")
        return HistoryRequestPlan(
            mode="cache_only",
            provider_sequence=("local-cache",),
            mirror_urls=(),
            reason="scan-local-cache-only",
        )
    return fetcher.build_history_request_plan(
        source=history_source,
        force_refresh=False,
    )


def assign_scan_jobs(
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


def log_scan_history_context(
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


def log_scan_execution_context(
    log: Optional[Callable[[str], None]],
    total_universe: int,
    total: int,
    workers: int,
    requested_workers: int,
    local_history_only: bool,
    available_mirrors: List[str],
    mirror_counts: Dict[str, int],
    *,
    trend_days: int,
    ma_period: int,
) -> None:
    if not log:
        return
    log(f"【阶段 2/3】股票池 {total_universe} 只，本次扫描 {total} 只，最近{trend_days}日收盘 > MA{ma_period}，并发 {workers} 线程。")
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


def submit_scan_tasks(
    executor: ThreadPoolExecutor,
    assigned_jobs: List[Tuple[Dict[str, Any], Optional[str]]],
    available_mirrors: List[str],
    history_plan: HistoryRequestPlan,
    *,
    filter_stock_fn: Callable[..., Dict[str, Any]],
) -> Dict[Any, Tuple[str, str, str, str, Optional[str]]]:
    future_to_meta = {}
    for row, mirror in assigned_jobs:
        code = str(row["code"]).strip().zfill(6)
        name = str(row.get("name", "") or "")
        board = str(row.get("board", "") or "")
        exchange = str(row.get("exchange", "") or "")
        future = executor.submit(
            filter_stock_fn,
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


def pending_scan_sample_text(
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


def scan_mirror_label(mirror: Optional[str]) -> str:
    if not mirror:
        return "cache-only"
    return mirror.split("//", 1)[-1].split("/", 1)[0]


def should_stop_scan(
    should_stop: Optional[Callable[[], bool]],
    log: Optional[Callable[[str], None]],
) -> bool:
    if not should_stop or not should_stop():
        return False
    if log:
        log("收到停止信号，正在取消未完成任务...")
    return True


def log_pending_scan_wait(
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
    sample_text = pending_scan_sample_text(pending, future_to_meta)
    log(
        f"进度 {completed}/{total}，命中 {len(results)} 只，已用时 {elapsed:.1f}s，"
        f"仍在等待历史数据返回，示例代码 {sample_text}"
    )
    return now


def build_scan_error_result(
    code: str,
    name: str,
    board: str,
    exchange: str,
    mirror: Optional[str],
    error: Exception,
    log: Optional[Callable[[str], None]],
) -> Dict[str, Any]:
    if log:
        log(f"  {code} {name} 检测异常[{scan_mirror_label(mirror)}]: {error}")
    return {
        "code": code,
        "name": name,
        "passed": False,
        "reasons": [str(error)],
        "data": {"board": board, "exchange": exchange},
    }


def resolve_scan_future_result(
    fut,
    future_to_meta: Dict[Any, Tuple[str, str, str, str, Optional[str]]],
    log: Optional[Callable[[str], None]],
) -> Tuple[str, str, str, str, Optional[str], Dict[str, Any]]:
    code, name, board, exchange, mirror = future_to_meta[fut]
    try:
        filter_result = fut.result()
    except Exception as exc:
        filter_result = build_scan_error_result(code, name, board, exchange, mirror, exc, log)
    return code, name, board, exchange, mirror, filter_result


def log_scan_pass_result(
    log: Optional[Callable[[str], None]],
    completed: int,
    total: int,
    code: str,
    name: str,
    filter_result: Dict[str, Any],
    *,
    ma_period: int,
) -> None:
    if not log or not filter_result.get("passed"):
        return
    analysis = filter_result.get("data", {}).get("analysis") or {}
    log(
        f"  通过 {completed}/{total} {code} {name} "
        f"最新收盘 {analysis.get('latest_close', 0):.2f} / MA{ma_period} {analysis.get('latest_ma', 0):.2f}"
    )


def append_scan_hit(
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


def notify_scan_progress(
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


def maybe_log_scan_progress(
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
        f"当前 {code} {name} @ {scan_mirror_label(mirror)}"
    )
    return now


def scan_all_stocks(
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
    *,
    fetcher,
    log_fn: Optional[Callable[[str], None]] = None,
    trend_days: int,
    ma_period: int,
    filter_stock_fn: Callable[..., Dict[str, Any]],
    result_sort_key_fn: Callable[[Dict[str, Any]], Any],
    log_runtime_diagnostics_fn: Optional[Callable[[str], None]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., HistoryRequestPlan]] = None,
) -> List[Dict[str, Any]]:
    """扫描股票池主编排（迁自 StockFilter.scan_all_stocks；行为零变化）。"""
    # 这里 import 是为了避免顶层 import 循环（stock_data → stock_filter 老链路）
    from stock_data import DaemonThreadPoolExecutor

    # 兼容旧接口：将 should_stop 回调与 CancelToken 合并成同一个谓词
    should_stop = coerce_should_stop(cancel_token, should_stop)
    log = log_fn
    t0 = time.time()
    if log:
        log("【阶段 1/3】加载股票池...")
    if log_runtime_diagnostics_fn is not None:
        log_runtime_diagnostics_fn("扫描前")

    all_stocks = fetcher.get_all_stocks(force_refresh=refresh_universe)
    if all_stocks.empty:
        if log:
            log("股票池为空，扫描终止。")
        return []

    total_universe = len(all_stocks)
    all_stocks = filter_scan_universe(all_stocks, allowed_boards, allowed_exchanges, log=log)
    subset = limit_scan_subset(all_stocks, max_stocks)
    total = len(subset)
    requested_workers, workers = resolve_scan_workers(max_workers, fetcher=fetcher)

    coverage = fetcher.get_history_cache_summary()
    history_plan = build_scan_history_plan(
        history_source,
        local_history_only,
        fetcher=fetcher,
        build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
    )
    available_mirrors = list(history_plan.mirror_urls)
    log_scan_history_context(log, coverage, history_plan, local_history_only)
    assigned_jobs, mirror_counts = assign_scan_jobs(subset, available_mirrors)
    log_scan_execution_context(
        log,
        total_universe,
        total,
        workers,
        requested_workers,
        local_history_only,
        available_mirrors,
        mirror_counts,
        trend_days=trend_days,
        ma_period=ma_period,
    )

    results: List[Dict[str, Any]] = []
    completed = 0
    last_report = time.time()
    report_every = 25

    # 提交前再检查一次：用户可能在加载股票池阶段就点了停止
    if should_stop_scan(should_stop, log):
        return results

    executor = DaemonThreadPoolExecutor(max_workers=workers)
    try:
        future_to_meta = submit_scan_tasks(
            executor,
            assigned_jobs,
            available_mirrors,
            history_plan,
            filter_stock_fn=filter_stock_fn,
        )
        if log:
            log("【阶段 3/3】开始逐只拉取历史日线并计算结果...")

        pending = set(future_to_meta)
        while pending:
            # 更短的轮询周期，让取消信号更快生效；同时在每轮起点主动检查一次
            if should_stop_scan(should_stop, log):
                for fut in pending:
                    fut.cancel()
                break
            done, pending = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
            if not done:
                if should_stop_scan(should_stop, log):
                    for fut in pending:
                        fut.cancel()
                    break
                last_report = log_pending_scan_wait(
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
                if should_stop_scan(should_stop, log):
                    for p in pending:
                        p.cancel()
                    pending.clear()
                    break
                code, name, board, exchange, mirror, filter_result = resolve_scan_future_result(
                    fut,
                    future_to_meta,
                    log,
                )
                completed += 1
                log_scan_pass_result(
                    log, completed, total, code, name, filter_result, ma_period=ma_period
                )
                append_scan_hit(results, filter_result, name, board, exchange)
                notify_scan_progress(progress_callback, completed, total, code, name)
                last_report = maybe_log_scan_progress(
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

    results.sort(key=result_sort_key_fn, reverse=True)

    if log:
        elapsed = time.time() - t0
        log(f"【完成】扫描结束，命中 {len(results)} 只，用时 {elapsed:.1f}s。")
    if log_runtime_diagnostics_fn is not None:
        log_runtime_diagnostics_fn("扫描后")

    return results
