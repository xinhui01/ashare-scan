"""涨停预测主编排（predict）。

2 个模块级函数（参数注入模式）：
- predict_limit_up_candidates: 主编排，整合所有 scorer 模块，输出涨停候选预测结果
- build_compare_market_context: 从最近几组涨停对比中提炼市场环境

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn / build_local_cache_history_plan_fn。
内部直接调用 scoring 包内的各 scorer 模块（cont / first / fresh / wrap / first_board /
classifiers / shared）。
"""
from __future__ import annotations

import logging
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.services.scoring import classifiers as _classifiers
from src.services.scoring import cont as _cont
from src.services.scoring import first as _first
from src.services.scoring import first_board as _first_board
from src.services.scoring import fresh as _fresh
from src.services.scoring import shared as _shared
from src.services.scoring import wrap as _wrap

logger = logging.getLogger(__name__)


@dataclass
class _AsOfHistoryFetcher:
    """历史回放专用 fetcher 代理：所有日线读取都截断到 as-of 当天。"""
    base_fetcher: Any
    as_of_trade_date: str

    def __getattr__(self, name: str) -> Any:
        return getattr(self.base_fetcher, name)

    def get_history_data(
        self,
        stock_code: str,
        days: int = 10,
        force_refresh: bool = False,
        preferred_mirror: Optional[str] = None,
        mirror_pool: Optional[List[str]] = None,
        request_plan: Optional[Any] = None,
    ):
        requested_days = int(days or 0)
        effective_days = max(requested_days, 120)

        def _fetch(use_days: int):
            try:
                return self.base_fetcher.get_history_data(
                    stock_code,
                    days=use_days,
                    force_refresh=force_refresh,
                    preferred_mirror=preferred_mirror,
                    mirror_pool=mirror_pool,
                    request_plan=request_plan,
                    as_of_trade_date=self.as_of_trade_date,
                )
            except TypeError:
                return self.base_fetcher.get_history_data(
                    stock_code,
                    days=use_days,
                    force_refresh=force_refresh,
                    preferred_mirror=preferred_mirror,
                    mirror_pool=mirror_pool,
                    request_plan=request_plan,
                )

        df = _fetch(effective_days)
        # store_facade.load_history 在 cache 行数 < min_rows 时整体返回 None，
        # 新股 / 短历史票会因此被误判为"零 K 线"。这里用调用方实际请求的 days
        # 再试一次，让短历史票也能命中缓存。
        if (df is None or df.empty) and requested_days and requested_days < effective_days:
            df = _fetch(requested_days)
        if df is None or df.empty or "date" not in df.columns:
            return df

        as_of = str(self.as_of_trade_date or "").strip()
        if len(as_of) == 8 and as_of.isdigit():
            as_of = f"{as_of[:4]}-{as_of[4:6]}-{as_of[6:8]}"

        date_col = (
            df["date"].astype(str).str.replace("/", "-", regex=False).str.replace(".", "-", regex=False)
        )
        trimmed = df.loc[date_col <= as_of].copy()
        if trimmed.empty:
            return trimmed
        return trimmed.tail(days).reset_index(drop=True)


def build_compare_market_context(
    trade_date: str,
    lookback_days: int,
    *,
    fetcher,
) -> Dict[str, Any]:
    """从最近几组涨停对比中提炼市场环境。

    迁自 StockFilter._build_compare_market_context；行为零变化。
    """
    window_days = max(2, int(lookback_days or 2) + 1)
    trade_dates = fetcher._recent_trade_dates(trade_date, window_days)
    pair_stats: List[Dict[str, Any]] = []

    for idx in range(1, len(trade_dates)):
        prev_date = trade_dates[idx - 1]
        cur_date = trade_dates[idx]
        try:
            compare = fetcher.compare_limit_up_pools(cur_date, prev_date)
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


def _compute_timing_hint(trade_date: str, historical_mode: bool) -> str:
    """预测时机提示：盘中 / 盘后 / 历史模式 reason 数据完整度差异。

    复盘网（涨停 reason 主源）当天数据通常盘后 16:00+ 才发布完整。盘中跑预测
    时 reason 字段会走概念兜底（[xxx / yyy]），不是真实涨停原因。提示用户
    "什么时候跑预测最准"。
    """
    try:
        now = datetime.now()
        today_key = now.strftime("%Y%m%d")
    except Exception:
        return ""
    td = str(trade_date or "").strip()
    if not td:
        return ""

    if historical_mode or td < today_key:
        return (
            "历史模式：复盘网应已有完整 reason 数据；行情走本地缓存合成，结果稳定。"
        )

    if td == today_key:
        hour = now.hour
        if hour < 15:
            return (
                f"盘中预测（{now.strftime('%H:%M')}）：复盘网 reason 数据通常 "
                f"16:00 后才更新完整，当前 reason 字段将走概念标签兜底（不是真实涨停原因）。"
                f"建议盘后 16:30+ 重跑一次获取真实 reason。"
            )
        if hour < 16:
            return (
                f"刚收盘（{now.strftime('%H:%M')}）：复盘网正在更新中，部分 reason "
                f"可能仍为空 / 概念兜底。16:30 后重跑可拿到完整数据。"
            )
        return (
            f"盘后预测（{now.strftime('%H:%M')}）：复盘网 reason 数据应已稳定，预测精度最佳。"
        )

    return f"未来日期（{td}）：暂无数据，请检查 trade_date 设置。"


def _derive_board_strength_from_spot(spot_df: Optional[pd.DataFrame]) -> Dict[str, float]:
    """历史模式无 akshare 行业实时接口，从合成 spot 按 "所属行业" 聚合平均涨跌幅。

    覆盖范围受 limit_up_stock_meta 行业字段限制（只覆盖曾涨停过的票），
    无行业字段的票自动跳过；返回 dict: 行业名 → 平均涨跌幅 %。
    """
    if spot_df is None or not isinstance(spot_df, pd.DataFrame) or spot_df.empty:
        return {}
    if "所属行业" not in spot_df.columns or "涨跌幅" not in spot_df.columns:
        return {}
    df = spot_df[["所属行业", "涨跌幅"]].copy()
    df["所属行业"] = df["所属行业"].astype(str).str.strip()
    df = df[df["所属行业"] != ""]
    df["涨跌幅"] = pd.to_numeric(df["涨跌幅"], errors="coerce")
    df = df.dropna(subset=["涨跌幅"])
    if df.empty:
        return {}
    agg = df.groupby("所属行业")["涨跌幅"].mean()
    return {str(k): float(round(v, 2)) for k, v in agg.items()}


def _is_new_stock_history(history: pd.DataFrame, today: datetime) -> bool:
    """K 线行数不足 10 但最早 K 线日期在最近 30 个自然日内 → 视为新股。

    用 history 自己最早日期判定，不依赖外部上市日期接口，避免每只票多一次网络。
    """
    if history is None or history.empty or "date" not in history.columns:
        return False
    try:
        earliest = str(history["date"].astype(str).iloc[0]).strip()
    except Exception:
        return False
    if not earliest:
        return False
    norm = earliest.replace("/", "-").replace(".", "-")
    if len(norm) == 8 and norm.isdigit():
        norm = f"{norm[:4]}-{norm[4:6]}-{norm[6:8]}"
    try:
        earliest_dt = datetime.strptime(norm[:10], "%Y-%m-%d")
    except Exception:
        return False
    return (today - earliest_dt).days <= 30


def _check_prerequisites(
    *,
    historical_mode: bool,
    pool_source: str,
    concept_themes_count: int,
    board_strength: Dict[str, Any],
    sentiment_degraded: bool,
    zt_codes: set,
    fetcher,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> List[str]:
    """硬校验所有预测必备数据是否就位。返回缺失项列表（空列表 = 通过）。

    用户原则：宁可不出预测，也别用兜底数据骗用户。任一项缺失，predict 直接
    返回"中止结果"，并把这份清单显示给用户去逐项修复。
    """
    missing: List[str] = []

    # 1. 涨停池来源必须可信
    trusted_sources = {"cache_db", "eastmoney", "cache_memory"}
    if pool_source not in trusted_sources:
        missing.append(
            f"❌ 涨停池数据源不可信（当前 = {pool_source!r}）→ "
            f"请检查东财涨停池接口 / 网络 / 清缓存重试"
        )

    # 2. 概念炒作分析必须识别出题材
    if concept_themes_count <= 0:
        missing.append(
            "❌ 概念炒作分析未识别出题材 → "
            "可能原因：最近 10 个交易日的涨停池数据不足 / 概念库未刷新。"
            "请到「概念炒作」tab 检查是否能看到题材列表，"
            "若题材列表为空请先补足涨停池历史 + 刷新概念库"
        )

    # 3. 板块强度（仅实时模式必备；历史模式从合成 spot 兜底）
    if not historical_mode and not board_strength:
        missing.append(
            "❌ 板块强度（行业涨跌榜）数据缺失 → 请检查东财板块接口"
        )

    # 4. 市场情绪不得降级
    if sentiment_degraded:
        missing.append(
            "❌ 市场情绪数据降级（跌停池 / 上证指数未完整拉到）→ "
            "请检查网络 / akshare 接口，或点「刷新（强制重拉外部数据）」重试"
        )

    # 5. 所有涨停股个股历史 K 线 ≥ 10 行（cont scorer 必备）
    # 新股豁免：若 K 线不足 10 行但最早一根 K 线在最近 30 个自然日内，视为新股
    # （cont scorer 已自带 len(history) >= 10 守卫，新股进流程会自动降级评分而不会崩）
    missing_kline: List[str] = []
    new_stock_skipped: List[str] = []
    if zt_codes:
        try:
            request_plan = (
                build_local_cache_history_plan_fn(
                    reason="predict-prereq-check-cache-only",
                )
                if build_local_cache_history_plan_fn is not None
                else None
            )
        except Exception:
            request_plan = None
        today = datetime.now()
        for code in sorted(zt_codes):
            # 关键：这里只校验 ≥ 10 行；days 要直接传 10，否则 store_facade.load_history
            # 在 cache_only 模式下会因 min_rows 不达标而整体返回 None，把 60 行历史的票
            # 也误判成"K 线缺失"。
            try:
                history = fetcher.get_history_data(
                    code, days=10, force_refresh=False,
                    request_plan=request_plan,
                )
            except Exception:
                history = None
            if history is None or history.empty:
                missing_kline.append(code)
                continue
            if len(history) >= 10:
                continue
            if _is_new_stock_history(history, today):
                new_stock_skipped.append(code)
            else:
                missing_kline.append(code)
    if missing_kline:
        preview = "、".join(missing_kline[:10])
        tail = f" 等 {len(missing_kline)} 只" if len(missing_kline) > 10 else ""
        missing.append(
            f"❌ 个股历史 K 线不足 10 行: {preview}{tail} → "
            f"请在「K 线缓存」tab 用「批量补历史」更新这些票"
        )
    if new_stock_skipped and log_fn:
        preview = "、".join(new_stock_skipped[:10])
        tail = f" 等 {len(new_stock_skipped)} 只" if len(new_stock_skipped) > 10 else ""
        log_fn(f"涨停预测：识别到新股（K 线 < 10 行但最早 K 线在 30 日内），不阻断预测：{preview}{tail}")

    return missing


def predict_limit_up_candidates(
    trade_date: str,
    lookback_days: int = 5,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    historical_mode: bool = False,
    *,
    fetcher,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    classify_limit_up_pattern_fn: Optional[Callable[..., Dict[str, Any]]] = None,
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
    而非实时快照；板块强度从合成 spot 按行业聚合涨跌幅。涨停池仍用
    get_limit_up_pool（其本身有 SQLite 缓存）。仅在需要"对任意历史日期回放
    预测"的批量回测场景下使用。

    迁自 StockFilter.predict_limit_up_candidates；行为零变化。
    """
    # 这里 import 是为了避免顶层 import 循环（stock_store / llm_theme_clustering 等老模块）
    from stock_store import (
        save_last_limit_up_prediction,
        save_limit_up_prediction_record,
    )
    from stock_data import DaemonThreadPoolExecutor

    # ===== auto-promote historical_mode =====
    # 若 trade_date 早于今日，强制切到历史模式。原因：实时 spot / 板块强度
    # 等接口都不带 date 参数，永远返回"当前时刻"，跨天调用会拉到错误
    # 日期的盘中数据，导致两台机器（或同一台机器不同时间）跑同一历史日期
    # 结果不一致。auto-promote 后历史 spot 从本地 history 表合成，结果稳定。
    if not historical_mode and trade_date:
        today_key = datetime.now().strftime("%Y%m%d")
        td_digits = str(trade_date).strip().replace("-", "").replace("/", "")
        if td_digits and td_digits.isdigit() and len(td_digits) == 8 \
                and td_digits < today_key:
            historical_mode = True
            if log_fn:
                log_fn(
                    f"涨停预测：trade_date={trade_date} ≠ 今日({today_key})，"
                    f"自动切到历史模式（spot 从本地 history 合成，板块强度走"
                    f"合成 spot 兜底，保证两台机器结果一致）"
                )
    if progress_callback:
        progress_callback(0, 1, "统计最近涨停对比环境...")

    # 数据健康度收集：每个数据源在哪个分支被读到、是否降级，全部记一笔
    # 用户看 UI 时能直接判断"这次预测哪些维度是真数据、哪些是 fallback"
    data_quality: Dict[str, Any] = {
        "historical_mode": bool(historical_mode),
        "trade_date": trade_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "spot": {"rows": 0, "industry_missing": 0, "source": "none"},
        "limit_up_pool": {"rows": 0, "source": "none"},
        "themes": {"loaded": False, "themes": 0, "covered_codes": 0},
        "board_strength": {"loaded": False, "rows": 0},
        "sentiment": {"loaded": False, "score": None, "degraded": False},
        "warnings": [],
        "timing_hint": _compute_timing_hint(trade_date, historical_mode),
    }
    if data_quality["timing_hint"] and log_fn:
        log_fn(f"涨停预测：{data_quality['timing_hint']}")

    profile: Dict[str, Any] = {}
    feature_samples: List[Dict[str, Any]] = []
    compare_context = build_compare_market_context(trade_date, lookback_days, fetcher=fetcher)
    scoring_fetcher = (
        _AsOfHistoryFetcher(fetcher, trade_date)
        if historical_mode
        else fetcher
    )

    # 阶段2：获取今日涨停池 + 全市场行情
    if log_fn:
        log_fn(f"涨停预测：阶段2 - 获取 {trade_date} 涨停池 + 全市场行情...")
    if progress_callback:
        progress_callback(0, 1, "获取今日涨停池...")

    # 并行获取涨停池和全市场行情快照
    today_pool_df: Optional[pd.DataFrame] = None
    spot_df: Optional[pd.DataFrame] = None
    zt_codes: set = set()

    def _fetch_pool():
        nonlocal today_pool_df
        today_pool_df = fetcher.get_limit_up_pool(trade_date)

    if historical_mode:
        # 历史模式：spot_df 从 history 表合成（"as of 收盘"），不走实时网络
        import stock_store as _stock_store
        try:
            spot_df = _stock_store.load_spot_snapshot_at(trade_date)
            cnt = 0 if spot_df is None else len(spot_df)
            data_quality["spot"]["source"] = "local_history"
            data_quality["spot"]["rows"] = int(cnt)
            if spot_df is not None and "所属行业" in spot_df.columns:
                missing = int((spot_df["所属行业"].fillna("") == "").sum())
                data_quality["spot"]["industry_missing"] = missing
            if cnt == 0:
                data_quality["warnings"].append(
                    f"历史 spot 为空：本地 history 表无 {trade_date} 数据，"
                    f"首板候选将无法筛选（请在线时打开软件触发当日 K 线下载）"
                )
            if log_fn:
                log_fn(f"涨停预测[历史模式]：从 history 合成 spot 快照 {cnt} 行")
        except Exception as e:
            if log_fn:
                log_fn(f"涨停预测[历史模式]：合成 spot 失败: {e}")
            spot_df = None
            data_quality["warnings"].append(f"历史 spot 合成异常: {e}")
        # 涨停池仍然走 get_limit_up_pool —— 本地 SQLite 命中即可，未命中才联网
        try:
            _fetch_pool()
        except Exception as e:
            if log_fn:
                log_fn(f"涨停预测[历史模式]：获取涨停池失败: {e}")
            data_quality["warnings"].append(f"涨停池拉取失败: {e}")
    else:
        def _fetch_spot():
            nonlocal spot_df
            spot_df = _first_board.fetch_spot_snapshot(log_fn=log_fn)

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
                if log_fn:
                    log_fn(f"涨停预测：获取涨停池超时 (get_limit_up_pool): {e}")
            except Exception as e:
                if log_fn:
                    log_fn(f"涨停预测：获取涨停池失败 (get_limit_up_pool): {e}")

            try:
                # 全市场行情上限 60s：东财快路径 5s 内 return；
                # 东财 RST/熔断时，需要给"东财重试 ~15s + 新浪 ~30s"留足时间，否则
                # 主线程会在新浪刚开始时就超时放弃，导致首板候选筛选每次都被跳过。
                future_spot.result(timeout=60.0)
            except FutureTimeoutError as e:
                if log_fn:
                    log_fn(f"涨停预测：获取全市场行情超时 (5000+只股票): {e}")
                    log_fn("涨停预测：将跳过首板候选筛选，继续执行连板延续分析")
            except Exception as e:
                if log_fn:
                    log_fn(f"涨停预测：获取全市场行情失败: {e}")
                    log_fn("涨停预测：将跳过首板候选筛选，继续执行连板延续分析")
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        # 记一笔实时 spot 行数（实时模式下行业字段由源接口直接给出）
        if spot_df is not None:
            data_quality["spot"]["source"] = "realtime"
            data_quality["spot"]["rows"] = int(len(spot_df))
            if "所属行业" in spot_df.columns:
                data_quality["spot"]["industry_missing"] = int(
                    (spot_df["所属行业"].fillna("") == "").sum()
                )

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
        data_quality["warnings"].append(summary)
        result = {
            "trade_date": trade_date,
            "profile": profile,
            "profile_samples": feature_samples,
            "continuation_candidates": [],
            "first_board_candidates": [],
            "fresh_first_board_candidates": [],
            "broken_board_wrap_candidates": [],
            "hot_industries": {},
            "summary": summary,
            "data_quality": data_quality,
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

    # 涨停池就绪 → 记录来源 + 行数
    data_quality["limit_up_pool"]["rows"] = int(len(today_pool_df))
    try:
        # fetcher 内部维护了 _last_pool_source[date]，标识 cache_memory / cache_db /
        # eastmoney / spot_fallback / empty，便于 UI 显示"涨停池数据来自哪里"。
        td_norm = fetcher._normalize_trade_date(trade_date)
        data_quality["limit_up_pool"]["source"] = (
            getattr(fetcher, "_last_pool_source", {}).get(td_norm, "unknown")
        )
    except Exception:
        data_quality["limit_up_pool"]["source"] = "unknown"

    all_pool_records = _shared.parse_full_pool(today_pool_df)
    if log_fn:
        log_fn(f"涨停预测：解析涨停池完成，共 {len(all_pool_records)} 只")
    hot_industries = _shared.count_pool_industries(today_pool_df)
    if log_fn:
        log_fn(f"涨停预测：统计热门行业完成，共 {len(hot_industries)} 个行业")

    if not today_pool_df.empty and "代码" in today_pool_df.columns:
        zt_codes = set(today_pool_df["代码"].astype(str).str.strip().str.zfill(6))
        if log_fn:
            log_fn(f"涨停预测：提取涨停股代码 {len(zt_codes)} 只")

    # 阶段2.5：概念炒作分析（concept_hype 服务）
    # 这是题材维度的【唯一入口】：内部已合并三源（涨停池行业 + 概念库反查 +
    # LLM 题材缓存若有），同时给出题材阶段（萌芽/主升/末期/退潮）。
    # 不再单独调 llm_theme_clustering（涨停对比 tab 已下线，那条入口已 dead）。
    try:
        from src.services.concept_hype_service import analyze_concept_hype
        hype = analyze_concept_hype(trade_date, lookback=10, log=log_fn)
    except Exception as exc:
        logger.debug("加载概念炒作分析失败: %s", exc)
        hype = {}

    concepts = hype.get("concepts") or []
    code_industry_map: Dict[str, str] = {
        r["code"]: r.get("industry", "") for r in all_pool_records
    }
    code_theme_map: Dict[str, str] = {}        # code → 该票最具代表性的题材名
    theme_size_map: Dict[str, int] = {}        # 题材名 → 成员数
    industry_theme_heat: Dict[str, int] = {}   # 行业 → 关联到的最大题材规模
    code_to_phase: Dict[str, str] = {}         # code → 该票所在题材的最佳阶段
    phase_priority = {"萌芽": 4, "主升": 3, "末期": 2, "退潮": 1}

    for c in concepts:
        try:
            name = str(c.get("name") or "").strip()
            phase = str(c.get("phase") or "")
            members = c.get("members") or []
            codes_in_theme = [
                str(m.get("code") or "").strip().zfill(6)
                for m in members if m.get("code")
            ]
            codes_in_theme = [x for x in codes_in_theme if x]
        except Exception:
            continue
        if not name or len(codes_in_theme) < 2:
            continue
        size = len(codes_in_theme)
        # 题材规模取最大命中（同名题材跨多 source 时合并）
        if theme_size_map.get(name, 0) < size:
            theme_size_map[name] = size
        inds_in_theme: set = set()
        for code in codes_in_theme:
            # code → theme：选 size 较大的题材作为该票代表
            existing = code_theme_map.get(code)
            if not existing or theme_size_map.get(existing, 0) < size:
                code_theme_map[code] = name
            # code → phase：萌芽/主升 优先
            existing_phase = code_to_phase.get(code)
            if (not existing_phase
                    or phase_priority.get(phase, 0)
                    > phase_priority.get(existing_phase, 0)):
                code_to_phase[code] = phase
            ind = code_industry_map.get(code) or ""
            if ind:
                inds_in_theme.add(ind)
        for ind in inds_in_theme:
            if industry_theme_heat.get(ind, 0) < size:
                industry_theme_heat[ind] = size

    # 把题材信息塞进 compare_context，所有 scorer 共用
    compare_context["industry_theme_heat"] = industry_theme_heat
    compare_context["code_theme_map"] = code_theme_map
    compare_context["theme_size_map"] = theme_size_map
    compare_context["code_to_concept_phase"] = code_to_phase

    data_quality["themes"]["loaded"] = bool(concepts)
    data_quality["themes"]["themes"] = len(concepts)
    data_quality["themes"]["covered_codes"] = len(code_theme_map)
    data_quality["themes"]["source"] = "concept_hype"

    if log_fn:
        if concepts:
            log_fn(
                f"涨停预测：概念炒作识别 {len(concepts)} 个题材，"
                f"覆盖 {len(code_theme_map)} 只涨停股 / "
                f"{len(industry_theme_heat)} 个行业 / "
                f"阶段映射 {len(code_to_phase)} 只 "
                f"(萌芽 {sum(1 for v in code_to_phase.values() if v == '萌芽')} / "
                f"主升 {sum(1 for v in code_to_phase.values() if v == '主升')} / "
                f"末期 {sum(1 for v in code_to_phase.values() if v == '末期')} / "
                f"退潮 {sum(1 for v in code_to_phase.values() if v == '退潮')})"
            )
        else:
            log_fn(
                "涨停预测：概念炒作分析未识别出题材"
                "（最近 10 日 limit_up_pool 数据可能不足）"
            )

    # 阶段2.6：加载板块强度（失败不影响预测）
    # 板块强度 fallback 链：
    #   - 历史模式：东财历史日 K → 同花顺历史日 K → 合成 spot 按行业聚合
    #   - 实时模式：东财行业列表 → 同花顺当日日 K → 合成 spot 按行业聚合
    #   THS 命名跟东财不一致，下游 lookup miss 一部分，但聊胜于无
    if log_fn:
        log_fn("涨停预测：正在加载板块强度...")

    if historical_mode:
        try:
            board_strength = _first_board.load_industry_board_strength_for_date(
                trade_date, log_fn=log_fn,
            )
        except Exception as exc:
            logger.debug("历史行业板块强度（东财）拉取异常: %s", exc)
            board_strength = {}
        if not board_strength:
            try:
                board_strength = (
                    _first_board.load_industry_board_strength_for_date_ths(
                        trade_date, log_fn=log_fn,
                    )
                )
                if board_strength and log_fn:
                    log_fn(
                        f"涨停预测[历史模式]：东财死 → 同花顺行业 K 线 "
                        f"{len(board_strength)} 个板块（命名跟 EM 不一致，部分 lookup 会 miss）"
                    )
            except Exception as exc:
                logger.debug("历史行业板块强度（同花顺）拉取异常: %s", exc)
        if not board_strength:
            board_strength = _derive_board_strength_from_spot(spot_df)
            if log_fn:
                log_fn(
                    f"涨停预测[历史模式]：东财 + THS 均空 → "
                    f"合成 spot 兜底聚合 {len(board_strength)} 个板块"
                )
        elif log_fn and not board_strength.get("__source_already_logged__"):
            log_fn(
                f"涨停预测[历史模式]：板块强度共 {len(board_strength)} 个板块"
            )
    else:
        try:
            board_strength = _first_board.load_industry_board_strength(log_fn=log_fn)
        except Exception as exc:
            logger.debug("板块涨跌幅（东财）拉取异常: %s", exc)
            board_strength = {}
        if not board_strength:
            # 实时 THS：用今日做 trade_date
            from datetime import datetime as _dt_now
            today_key = _dt_now.now().strftime("%Y%m%d")
            try:
                board_strength = (
                    _first_board.load_industry_board_strength_for_date_ths(
                        today_key, log_fn=log_fn,
                    )
                )
                if board_strength and log_fn:
                    log_fn(
                        f"涨停预测：东财板块强度空 → 同花顺当日 K "
                        f"{len(board_strength)} 个板块（命名跟 EM 不一致，部分 lookup 会 miss）"
                    )
            except Exception as exc:
                logger.debug("板块涨跌幅（同花顺）拉取异常: %s", exc)
        if not board_strength:
            board_strength = _derive_board_strength_from_spot(spot_df)
            if log_fn:
                log_fn(
                    f"涨停预测：东财 + THS 均空 → "
                    f"合成 spot 兜底聚合 {len(board_strength)} 个板块"
                )

    data_quality["board_strength"]["loaded"] = bool(board_strength)
    data_quality["board_strength"]["rows"] = len(board_strength)

    compare_context["board_strength"] = board_strength
    if log_fn:
        top_boards = sorted(board_strength.items(), key=lambda x: -x[1])[:5]
        board_summary = "、".join(f"{k}({v:.1f}%)" for k, v in top_boards)
        log_fn(f"涨停预测：板块强弱榜 TOP5 {board_summary}")

    # 市场情绪评分（供二波接力等评分调节，冰点情绪下降权）
    try:
        from src.services.market_sentiment_service import analyze_market_sentiment
        sent = analyze_market_sentiment(
            trade_date, fetch_external=True, log=log_fn,
        )
        compare_context["sentiment_score"] = int(sent.get("score", 50))
        compare_context["sentiment_label"] = (
            (sent.get("position_suggest") or {}).get("label", "")
        )
        data_quality["sentiment"]["loaded"] = True
        data_quality["sentiment"]["score"] = compare_context["sentiment_score"]
        data_quality["sentiment"]["label"] = compare_context.get("sentiment_label", "")
        # 检查 sentiment 自身是否降级（跌停 / 上证拉失败时 raw.external.ok=False）
        sent_external = ((sent.get("raw") or {}).get("external") or {})
        if not sent_external.get("ok", True):
            data_quality["sentiment"]["degraded"] = True
            data_quality["warnings"].append(
                "市场情绪外部数据降级：跌停池/上证指数未完整拉到"
            )
        if log_fn:
            log_fn(
                f"涨停预测：市场情绪 {compare_context['sentiment_score']}/100"
                f" → {compare_context['sentiment_label']}"
            )
    except Exception as exc:
        logger.debug("接入市场情绪评分失败: %s", exc)
        compare_context["sentiment_score"] = 50
        data_quality["sentiment"]["degraded"] = True
        data_quality["warnings"].append(f"市场情绪评分失败: {exc}")

    # ============== 预测前置硬校验 ==============
    # 所有必备数据必须就位才允许预测。任一项缺失 → 直接中止 + 列出待修复清单。
    # 原则：宁可不出预测，也不让用户被兜底数据骗。
    prereq_missing = _check_prerequisites(
        historical_mode=historical_mode,
        pool_source=data_quality["limit_up_pool"].get("source", "unknown"),
        concept_themes_count=int(data_quality["themes"].get("themes") or 0),
        board_strength=board_strength,
        sentiment_degraded=bool(data_quality["sentiment"].get("degraded")),
        zt_codes=zt_codes,
        fetcher=scoring_fetcher,
        build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        log_fn=log_fn,
    )
    if prereq_missing:
        data_quality["blocked"] = True
        data_quality["missing"] = prereq_missing
        if log_fn:
            log_fn(f"涨停预测：前置校验失败，中止预测（{len(prereq_missing)} 项待修复）")
            for m in prereq_missing:
                log_fn(f"  {m}")
        summary = (
            f"❌ 预测中止 — {trade_date} 数据未就位（{len(prereq_missing)} 项待修复）\n\n"
            + "\n\n".join(prereq_missing)
            + "\n\n修复后请重新点「开始预测」。"
        )
        result = {
            "trade_date": trade_date,
            "profile": profile,
            "profile_samples": feature_samples,
            "continuation_candidates": [],
            "first_board_candidates": [],
            "fresh_first_board_candidates": [],
            "broken_board_wrap_candidates": [],
            "hot_industries": {},
            "compare_context": compare_context,
            "summary": summary,
            "data_quality": data_quality,
        }
        try:
            save_last_limit_up_prediction(result)
        except Exception:
            pass
        # 注意：中止结果不写入 prediction_record 历史表，避免污染历史
        return result

    # 龙头身份预算：同板块今日最高板数 + 持有该板数的代码集合
    # 让 cont 评分识别"板块独苗高板龙头"vs"高位跟风票"
    industry_max_boards: Dict[str, int] = {}
    industry_top_codes: Dict[str, set] = {}
    market_max_boards = 0
    market_top_codes: set = set()
    for r in all_pool_records:
        ind = str(r.get("industry") or "").strip()
        if not ind:
            continue
        try:
            b = int(r.get("consecutive_boards") or 1)
        except (TypeError, ValueError):
            b = 1
        code = str(r.get("code") or "").strip()
        if b > market_max_boards:
            market_max_boards = b
            market_top_codes = {code} if code else set()
        elif b == market_max_boards and code:
            market_top_codes.add(code)
        cur_max = industry_max_boards.get(ind, 0)
        if b > cur_max:
            industry_max_boards[ind] = b
            industry_top_codes[ind] = {code}
        elif b == cur_max:
            industry_top_codes.setdefault(ind, set()).add(code)
    compare_context["industry_max_boards"] = industry_max_boards
    compare_context["industry_top_codes"] = industry_top_codes
    compare_context["market_max_boards"] = market_max_boards
    compare_context["market_top_codes"] = market_top_codes

    # 题材阶段映射已在阶段 2.5（concept_hype）一并完成，无需重复调用。

    # 阶段3：统一预取所有需要的历史数据（一次搞定）
    if log_fn:
        log_fn("涨停预测：阶段3 - 统一预取历史数据...")

    # 收集所有需要历史数据的股票代码
    pool_codes = [r["code"] for r in all_pool_records]
    candidate_codes: List[str] = []
    if spot_df is not None and not spot_df.empty:
        if log_fn:
            log_fn(f"涨停预测：开始筛选强势股（全市场 {len(spot_df)} 只）...")
        strong = _first_board.filter_strong_stocks(spot_df, zt_codes)
        if log_fn:
            log_fn(f"涨停预测：筛选强势股完成，共 {len(strong)} 只")
        pullback = _first_board.filter_ma5_pullback_stocks(spot_df, zt_codes)
        if log_fn:
            log_fn(f"涨停预测：筛选回踩MA5完成，共 {len(pullback)} 只")
        seen = set()
        for rec in strong + pullback:
            if rec["code"] not in seen:
                seen.add(rec["code"])
                candidate_codes.append(rec["code"])
    else:
        if log_fn:
            log_fn("涨停预测：无全市场行情，跳过强势股筛选（首板候选将不可用）")

    all_codes = list(set(pool_codes + candidate_codes))
    if log_fn:
        log_fn(f"涨停预测：统一预取 {len(all_codes)} 只股票历史数据"
               f"（涨停池{len(pool_codes)} + 候选{len(candidate_codes)}）")
    # 只使用本地缓存，不发起网络请求
    _classifiers.prefetch_history_for_pool(
        scoring_fetcher, all_codes, 65, progress_callback, True, log_fn=log_fn,
    )
    if log_fn:
        log_fn("涨停预测：阶段3完成 - 历史数据预取结束")

    # 阶段4：保留涨停 / 连板延续候选评分
    if log_fn:
        log_fn(f"涨停预测：阶段4 - 分析 {len(all_pool_records)} 只涨停股的保留涨停潜力...")

    continuation_candidates = []
    for idx, rec in enumerate(all_pool_records):
        classify_fn = classify_limit_up_pattern_fn
        if historical_mode:
            classify_fn = (
                lambda stock_code, stock_name="", board="":
                _classifiers.classify_limit_up_pattern(
                    scoring_fetcher,
                    stock_code,
                    stock_name=stock_name,
                    board=board,
                    log_fn=log_fn,
                )
            )
        score_info = _cont.score_continuation_by_compare(
            rec, hot_industries, compare_context,
            fetcher=scoring_fetcher,
            log_fn=log_fn,
            build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
            limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
            classify_limit_up_pattern_fn=classify_fn,
        )
        # 门槛 40→50：30 天数据显示 0-49 段命中仅 15.2%（n=277），50+ 段才有
        # 22.5% 区分度，进一步过滤减少 false positive
        if score_info["score"] >= 50:
            continuation_candidates.append(score_info)
        if progress_callback:
            progress_callback(idx + 1, len(all_pool_records),
                              f"保留涨停分析 {rec['code']} {rec.get('name', '')}")
    continuation_candidates.sort(key=lambda x: -x["score"])

    # 阶段5：二波接力候选（历史数据 + 行情都已缓存）
    if log_fn:
        log_fn("涨停预测：阶段5 - 识别二波接力候选...")

    first_board_candidates = _first.scan_followthrough_candidates_cached(
        hot_industries, spot_df, zt_codes, compare_context, progress_callback,
        fetcher=scoring_fetcher,
        lookback_days=lookback_days,
        log_fn=log_fn,
        limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
        build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        filter_strong_stocks_fn=_first_board.filter_strong_stocks,
    )

    # 阶段6：首板涨停候选（最近 N 日未涨停、今日量价启动）
    if log_fn:
        log_fn("涨停预测：阶段6 - 识别首板涨停候选...")
    fresh_first_board_candidates = _fresh.scan_fresh_first_board_candidates_cached(
        spot_df, zt_codes, hot_industries, compare_context, progress_callback,
        fetcher=scoring_fetcher,
        log_fn=log_fn,
        limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
        build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        filter_strong_stocks_fn=_first_board.filter_strong_stocks,
    )

    # 阶段7：断板反包候选（近期涨停被打掉，今日逼近反包）
    if log_fn:
        log_fn("涨停预测：阶段7 - 识别断板反包候选...")
    broken_board_wrap_candidates = _wrap.scan_broken_board_wrap_candidates_cached(
        spot_df, zt_codes, hot_industries, compare_context, progress_callback,
        fetcher=scoring_fetcher,
        log_fn=log_fn,
        limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
        build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        filter_wrap_candidate_stocks_fn=_first_board.filter_wrap_candidate_stocks,
    )

    # 摘要
    summary_lines = [
        f"预测日期：基于 {trade_date} 数据预测次日涨停候选",
        f"环境样本：最近 {compare_context.get('pair_count', 0)} 组首板晋级对比",
        f"今日涨停总数：{len(all_pool_records)} 只",
        f"保留涨停候选：{len(continuation_candidates)} 只（得分>=40）",
        f"二波接力候选：{len(first_board_candidates)} 只（得分>=50）",
        f"首板涨停候选：{len(fresh_first_board_candidates)} 只（5日未涨停，得分>=45）",
        f"反包候选：{len(broken_board_wrap_candidates)} 只（≥2 板涨停被打掉，T0 在 -10.5%~+3% 区间，得分>=70）",
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
        "hot_industries": hot_industries,
        "compare_context": compare_context,
        "summary": "\n".join(summary_lines),
        "data_quality": data_quality,
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
