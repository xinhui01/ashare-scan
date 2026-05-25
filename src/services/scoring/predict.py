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
        try:
            df = self.base_fetcher.get_history_data(
                stock_code,
                days=max(int(days or 0), 120),
                force_refresh=force_refresh,
                preferred_mirror=preferred_mirror,
                mirror_pool=mirror_pool,
                request_plan=request_plan,
                as_of_trade_date=self.as_of_trade_date,
            )
        except TypeError:
            df = self.base_fetcher.get_history_data(
                stock_code,
                days=max(int(days or 0), 120),
                force_refresh=force_refresh,
                preferred_mirror=preferred_mirror,
                mirror_pool=mirror_pool,
                request_plan=request_plan,
            )
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
    而非实时快照；龙虎榜 / 北向 / 板块强度等实时指标全部置空。涨停池仍用
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

    # 阶段1：回看最近 N 日涨停对比环境
    if log_fn:
        log_fn(f"涨停预测：阶段1 - 统计最近 {lookback_days} 日涨停对比环境...")
    if progress_callback:
        progress_callback(0, 1, "统计最近涨停对比环境...")

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
            if log_fn:
                cnt = 0 if spot_df is None else len(spot_df)
                log_fn(f"涨停预测[历史模式]：从 history 合成 spot 快照 {cnt} 行")
        except Exception as e:
            if log_fn:
                log_fn(f"涨停预测[历史模式]：合成 spot 失败: {e}")
            spot_df = None
        # 涨停池仍然走 get_limit_up_pool —— 本地 SQLite 命中即可，未命中才联网
        try:
            _fetch_pool()
        except Exception as e:
            if log_fn:
                log_fn(f"涨停预测[历史模式]：获取涨停池失败: {e}")
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
    if log_fn and themes:
        log_fn(f"涨停预测：加载题材聚类缓存 {len(themes)} 个题材，"
               f"覆盖 {len(code_theme_map)} 只涨停股 / {len(industry_theme_heat)} 个行业")
    elif log_fn:
        log_fn("涨停预测：未找到题材聚类缓存（如需题材加分，请先在涨停对比 tab 跑一次 AI 题材聚类）")

    # 阶段2.6：加载龙虎榜 + 板块强度（失败不影响预测）
    # 北向逐日明细自 2024-08-17 起停止披露，永久置空。
    # 历史模式：实时指标对历史日期没意义，全部置空
    northbound_map: Dict[str, float] = {}
    if historical_mode:
        if log_fn:
            log_fn("涨停预测[历史模式]：跳过龙虎榜 / 板块强度（实时指标）")
        lhb_map = {}
        board_strength = {}
    else:
        if log_fn:
            log_fn("涨停预测：正在加载龙虎榜 / 板块强度...")
        try:
            lhb_map = _first_board.load_lhb_for_date(trade_date, log_fn=log_fn)
        except Exception as exc:
            logger.debug("龙虎榜加载异常: %s", exc)
            lhb_map = {}

        try:
            board_strength = _first_board.load_industry_board_strength(log_fn=log_fn)
        except Exception as exc:
            logger.debug("板块涨跌幅加载异常: %s", exc)
            board_strength = {}

    compare_context["lhb_map"] = lhb_map
    compare_context["northbound_map"] = northbound_map
    compare_context["board_strength"] = board_strength
    if log_fn:
        top_boards = sorted(board_strength.items(), key=lambda x: -x[1])[:5]
        board_summary = "、".join(f"{k}({v:.1f}%)" for k, v in top_boards)
        log_fn(
            f"涨停预测：龙虎榜 {len(lhb_map)} 只 / 板块强弱榜 TOP5 {board_summary}"
        )

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
        if log_fn:
            log_fn(
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

    # 题材阶段映射（来自 concept_hype 服务）：code → 该票所在题材的最佳阶段
    # 萌芽 > 主升 > 末期 > 退潮 优先级，让连板股能识别自己处在主线还是末班车
    try:
        from src.services.concept_hype_service import analyze_concept_hype
        hype = analyze_concept_hype(trade_date, lookback=10, log=log_fn)
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
        if log_fn:
            log_fn(
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
        f"反包候选：{len(broken_board_wrap_candidates)} 只（≥2 板涨停被打掉，T0 在 -10.5%~+3% 区间，得分>=75）",
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
