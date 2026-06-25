"""市场情绪综合评分 + 仓位建议。

7 个维度的本地规则评分（baseline 50，clip 到 0-100），输出明确的仓位建议
（空仓 / 2 成 / 半仓 / 7 成 / 满仓）。

数据源：
1. limit_up_pool (SQLite)            — 涨停数 / 炸板 / 连板高度 / 4+板数量
2. limit_up_pool[T-1] vs [T]         — 昨日涨停今日继续涨停率（赚钱效应）
3. ak.stock_zt_pool_dtgc_em          — 跌停池（按日，需要联网）
4. ak.stock_zh_index_daily_em        — 上证/深成指日 K（需要联网）

联网拉到的数据按 trade_date 缓存到 app_config，同一天只拉一次。
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import stock_data  # 触发 SSL/proxy 网络补丁
import stock_store
from stock_logger import get_logger
from src.utils.trade_calendar import _get_trade_calendar, _previous_trading_day

logger = get_logger(__name__)

CACHE_KEY_PREFIX = "market_sentiment_external_"
_POOL_FETCHER: Optional["stock_data.StockDataFetcher"] = None

SEMICONDUCTOR_FOCUS_NAME = "芯片/半导体"
SEMICONDUCTOR_FOCUS_KEYWORDS = (
    "半导体",
    "芯片",
    "集成电路",
    "先进封装",
    "封装",
    "存储",
    "光刻",
    "晶圆",
    "硅片",
)


# ============== 工具 ==============

def _normalize_date(s: Any) -> str:
    raw = str(s or "").strip().replace("-", "").replace("/", "")
    return raw if len(raw) == 8 and raw.isdigit() else ""


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _parse_date_key(date_key: str):
    try:
        return datetime.strptime(date_key, "%Y%m%d").date()
    except ValueError:
        return None


def _get_pool_fetcher() -> "stock_data.StockDataFetcher":
    global _POOL_FETCHER
    if _POOL_FETCHER is None:
        _POOL_FETCHER = stock_data.StockDataFetcher()
    return _POOL_FETCHER


# ============== 信号计算 ==============

def _signal_lu_count_vs_avg(today: int, avg5: float) -> Tuple[int, str]:
    """涨停数 vs 5 日均值。"""
    if avg5 <= 0:
        return 0, "无 5 日均值数据"
    ratio = today / avg5 - 1.0  # +0.3 = +30%
    delta = int(round(ratio * 30))
    delta = max(-15, min(15, delta))
    pct = ratio * 100
    arrow = "↑" if pct >= 0 else "↓"
    return delta, f"vs 5 日均值 {avg5:.0f}，{arrow}{abs(pct):.0f}%"


def _signal_break_rate(broken: int, total: int) -> Tuple[int, str]:
    """炸板率。低 = 承接力强。"""
    if total <= 0:
        return 0, "无涨停数据"
    rate = broken / total
    if rate < 0.30:
        delta, note = 10, "承接力强"
    elif rate < 0.45:
        delta, note = 5, "承接力良好"
    elif rate < 0.55:
        delta, note = 0, "承接力中性"
    elif rate < 0.70:
        delta, note = -5, "承接力偏弱"
    else:
        delta, note = -10, "承接力差"
    return delta, f"{rate*100:.0f}% · {note}"


def _signal_max_boards(max_b: int) -> Tuple[int, str]:
    """最高连板。直接反映赚钱效应天花板。"""
    if max_b >= 7:
        return 15, f"{max_b} 板天花板，赚钱效应强"
    if max_b >= 6:
        return 10, f"{max_b} 板，龙头活跃"
    if max_b >= 5:
        return 5, f"{max_b} 板，尚有龙头"
    if max_b >= 4:
        return 0, f"{max_b} 板，中规中矩"
    if max_b >= 3:
        return -5, f"{max_b} 板，龙头稀薄"
    return -10, f"{max_b} 板，无高度"


def _signal_high_board_count(n4plus: int) -> Tuple[int, str]:
    """4+ 板数量。反映高度题材厚度。"""
    if n4plus >= 5:
        return 10, f"{n4plus} 只高度股，题材厚"
    if n4plus >= 3:
        return 5, f"{n4plus} 只高度股，尚可"
    if n4plus >= 1:
        return 0, f"{n4plus} 只高度股，偏薄"
    return -5, "无高度股"


def _signal_continuation(yesterday_lu: int, today_continued: int) -> Tuple[int, str]:
    """昨日涨停股今日继续涨停率（晋级率 = 赚钱效应核心指标）。"""
    if yesterday_lu <= 0:
        return 0, "昨日无涨停"
    rate = today_continued / yesterday_lu
    if rate >= 0.40:
        delta, note = 15, "赚钱效应极强"
    elif rate >= 0.30:
        delta, note = 10, "赚钱效应较强"
    elif rate >= 0.20:
        delta, note = 5, "赚钱效应一般"
    elif rate >= 0.10:
        delta, note = -5, "赚钱效应偏弱"
    else:
        delta, note = -15, "赚钱效应崩塌"
    return delta, f"昨日涨停 {yesterday_lu} 只，今日继续 {today_continued} 只 ({rate*100:.0f}%) · {note}"


def _signal_index(idx_pct: Optional[float], name: str = "上证") -> Tuple[int, str]:
    """大盘指数涨跌幅。系统性风险。"""
    if idx_pct is None:
        return 0, f"{name}指数无数据"
    if idx_pct > 1.0:
        delta, note = 10, "强势"
    elif idx_pct > 0.3:
        delta, note = 5, "偏强"
    elif idx_pct > -0.3:
        delta, note = 0, "震荡"
    elif idx_pct > -1.0:
        delta, note = -5, "偏弱"
    else:
        delta, note = -10, "明显下跌"
    sign = "+" if idx_pct >= 0 else ""
    return delta, f"{name} {sign}{idx_pct:.2f}% · {note}"


def _format_index_detail(name: str, pct: Optional[float]) -> str:
    if pct is None:
        return f"{name} -"
    sign = "+" if pct >= 0 else ""
    return f"{name} {sign}{pct:.2f}%"


def _composite_index_pct(sh_pct: Optional[float], sz_pct: Optional[float]) -> Optional[float]:
    values = [v for v in (sh_pct, sz_pct) if v is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _signal_down_limit(n_dt: int) -> Tuple[int, str]:
    """跌停数。极端恐慌指标。"""
    if n_dt < 5:
        return 0, "极少跌停"
    if n_dt < 15:
        return -3, f"{n_dt} 只，正常出清"
    if n_dt < 30:
        return -7, f"{n_dt} 只，恐慌出现"
    return -10, f"{n_dt} 只，恐慌蔓延"


# ============== 数据加载 ==============

def _industry_aggregates(df) -> Dict[str, Any]:
    """从涨停池 df 提取行业分布相关聚合（HHI = 板块集中度）。"""
    if df is None or df.empty or "所属行业" not in df.columns:
        return {"top_industries": [], "industry_count": 0, "hhi": 0.0}
    industries = df["所属行业"].astype(str).tolist()
    total = len(industries)
    if total == 0:
        return {"top_industries": [], "industry_count": 0, "hhi": 0.0}
    cnt = Counter(industries)
    hhi = sum((n / total) ** 2 for n in cnt.values())
    return {
        "top_industries": cnt.most_common(5),
        "industry_count": len(cnt),
        "hhi": round(hhi, 4),
    }


def _load_pool_aggregates(date_key: str) -> Optional[Dict[str, Any]]:
    """从 limit_up_pool 聚合涨停数 / 炸板 / 最高板 / 4+板 / 行业分布等。"""
    df = stock_store.load_limit_up_pool(date_key)
    if df is None or df.empty:
        return None
    boards = df["连板数"].astype(int) if "连板数" in df.columns else None
    breaks = df["炸板次数"].astype(int) if "炸板次数" in df.columns else None
    codes = (
        df["代码"].astype(str).str.zfill(6).tolist()
        if "代码" in df.columns
        else []
    )
    ind_agg = _industry_aggregates(df)

    first_board_industries: List[str] = []
    if boards is not None and "所属行业" in df.columns:
        mask = boards == 1
        first_board_industries = df.loc[mask, "所属行业"].astype(str).tolist()

    return {
        "lu_count": len(df),
        "broken_count": int((breaks > 0).sum()) if breaks is not None else 0,
        "broken_total_times": int(breaks.sum()) if breaks is not None else 0,
        "max_boards": int(boards.max()) if boards is not None and len(boards) else 0,
        "high_board_count_4plus": int((boards >= 4).sum()) if boards is not None else 0,
        "first_board_count": int((boards == 1).sum()) if boards is not None else 0,
        "first_board_industries": first_board_industries,
        "codes": codes,
        "top_industries": ind_agg["top_industries"],
        "industry_count": ind_agg["industry_count"],
        "hhi": ind_agg["hhi"],
    }


def _avg_lu_count_5d(end_date: str) -> Tuple[float, List[str], Dict[str, int]]:
    """end_date 之前最近 5 个真实交易日的平均涨停数。

    返回 (平均值, 参与平均的日期顺序, {日期: 当日涨停数}).
    """
    prior = _previous_trade_dates(end_date, 5)
    if not prior:
        # 交易日历不可用时，退回旧逻辑：按已缓存日期近似。
        all_dates = stock_store.list_limit_up_pool_trade_dates() or []
        prior = [d for d in all_dates if d < end_date][-5:]
    if not prior:
        return 0.0, [], {}
    counts_map: Dict[str, int] = {}
    for d in prior:
        df = stock_store.load_limit_up_pool(d)
        if df is not None and not df.empty:
            counts_map[d] = len(df)
    if not counts_map:
        return 0.0, prior, {}
    return sum(counts_map.values()) / len(counts_map), prior, counts_map


def _continuation_today_from_yesterday(
    yesterday_codes: List[str],
    today_codes: List[str],
) -> Tuple[int, int]:
    """返回 (yesterday_lu_count, today_continued_count)."""
    if not yesterday_codes:
        return 0, 0
    today_set = set(today_codes)
    continued = sum(1 for c in yesterday_codes if c in today_set)
    return len(yesterday_codes), continued


def _previous_pool_date(end_date: str) -> str:
    prior = _previous_trade_dates(end_date, 1)
    if prior:
        return prior[0]
    all_dates = stock_store.list_limit_up_pool_trade_dates() or []
    cached_prior = [d for d in all_dates if d < end_date]
    return cached_prior[-1] if cached_prior else ""


def _previous_trade_dates(end_date: str, count: int) -> List[str]:
    """返回 end_date 之前最近 count 个真实交易日（不含 end_date）。"""
    target = _parse_date_key(end_date)
    if target is None or count <= 0:
        return []
    cal = _get_trade_calendar()
    out: List[str] = []
    cursor = target
    seen = set()
    for _ in range(max(count * 8, 20)):
        prev = _previous_trading_day(cursor, cal)
        key = prev.strftime("%Y%m%d")
        if key in seen:
            break
        out.append(key)
        seen.add(key)
        cursor = prev
        if len(out) >= count:
            break
    return out


def _required_pool_dates(end_date: str) -> List[str]:
    """市场情绪计算依赖的涨停池日期：当日 + 前 5 个交易日。"""
    dates = [end_date]
    dates.extend(_previous_trade_dates(end_date, 5))
    deduped: List[str] = []
    seen = set()
    for d in dates:
        if d and d not in seen:
            deduped.append(d)
            seen.add(d)
    return deduped


def _ensure_pool_dates_ready(
    date_keys: List[str],
    *,
    log: Callable[[str], None],
) -> List[str]:
    """缺哪个交易日就补哪个；返回仍然缺失的日期。"""
    missing = [d for d in date_keys if d and _load_pool_aggregates(d) is None]
    if not missing:
        return []

    fetcher = _get_pool_fetcher()
    try:
        fetcher.set_log_callback(log)
    except Exception:
        pass

    unresolved: List[str] = []
    for d in missing:
        try:
            log(f"市场情绪依赖缺失，正在补齐涨停池 {d} ...")
            df = fetcher.get_limit_up_pool(d)
            if df is None or df.empty:
                unresolved.append(d)
                log(f"  涨停池 {d} 补齐失败或为空")
                continue
            agg = _load_pool_aggregates(d)
            if agg is None:
                unresolved.append(d)
                log(f"  涨停池 {d} 补齐后仍不可用")
        except Exception as exc:
            unresolved.append(d)
            log(f"  涨停池 {d} 补齐失败: {exc}")
    return unresolved


def _compute_pct_from_index_df(df, date_key: str) -> Optional[float]:
    """通用：在指数日 K DataFrame 里找 date_key 那行，算 (close/prev_close - 1)*100。

    适配三种 akshare 指数源：东财(stock_zh_index_daily_em)、新浪(stock_zh_index_daily)、
    腾讯(stock_zh_index_daily_tx)，它们都有 date + close 字段。
    """
    if df is None or df.empty:
        return None
    work = df.copy()
    work["date_key"] = work["date"].astype(str).str.replace("-", "")
    target = work[work["date_key"] == date_key]
    if target.empty or len(work) < 2:
        return None
    idx = work.index[work["date_key"] == date_key][0]
    pos = work.index.get_loc(idx)
    if pos <= 0:
        return None
    today_close = _safe_float(work.iloc[pos]["close"])
    prev_close = _safe_float(work.iloc[pos - 1]["close"])
    if prev_close <= 0:
        return None
    return (today_close / prev_close - 1.0) * 100


def _fetch_index_pct(
    date_key: str,
    *,
    symbol: str,
    name: str,
    log: Callable[[str], None],
) -> Optional[float]:
    """拉指数当日涨跌幅。当日(盘中)优先实时快照；历史日走 东财→新浪→腾讯 日线级联。"""
    import time
    import akshare as ak
    from datetime import datetime as _dt, timedelta

    # 0. 当前交易日(盘中)：日线接口要么抽风(东财 push2his)、要么没有今天(新浪/腾讯日线)，
    #    故优先用新浪实时快照 stock_zh_index_spot_sina（host=hq.sinajs.cn，不碰 push2his）
    #    直接取当日涨跌幅，支撑“实时情绪”。仅 date_key==今天 时走这条；历史日跳过。
    if date_key == _dt.now().strftime("%Y%m%d"):
        try:
            spot = ak.stock_zh_index_spot_sina()
            if spot is not None and not spot.empty and "代码" in spot.columns:
                row = spot[spot["代码"].astype(str) == symbol]
                if not row.empty:
                    try:
                        pct = float(row.iloc[0].get("涨跌幅"))
                    except (TypeError, ValueError):
                        pct = None
                    if pct is not None and pct == pct:  # 过滤 None / NaN
                        log(f"  {name}(新浪实时快照) 当日 {pct:+.2f}%")
                        return pct
        except Exception as exc:
            log(f"  {name}(新浪实时快照)拉取失败: {exc}")

    # 1. 东财日线：历史日首选；盘中也带实时 K 但 push2his 间歇抽风，故对瞬时抖动
    #    （ProxyError/RemoteDisconnected）重试几次兜底。
    end_dt = _dt.strptime(date_key, "%Y%m%d")
    start_dt = end_dt - timedelta(days=10)
    for _attempt in range(3):
        try:
            df = ak.stock_zh_index_daily_em(
                symbol=symbol,
                start_date=start_dt.strftime("%Y%m%d"),
                end_date=date_key,
            )
            pct = _compute_pct_from_index_df(df, date_key)
            if pct is not None:
                return pct
            break  # 连上了但当天无此行（历史日缺数据）→ 不重试，转兜底源
        except Exception as exc:
            log(f"  {name}(东财)拉取失败(第 {_attempt + 1}/3 次): {exc}")
            if _attempt < 2:
                time.sleep(0.6)

    # 2. 新浪：拉全量历史，毫秒级解析
    try:
        df = ak.stock_zh_index_daily(symbol=symbol)
        pct = _compute_pct_from_index_df(df, date_key)
        if pct is not None:
            log(f"  {name}(东财失败，新浪兜底成功)")
            return pct
    except Exception as exc:
        log(f"  {name}(新浪)拉取失败: {exc}")

    # 3. 腾讯：拉全量历史，逐分块下载较慢但稳
    try:
        df = ak.stock_zh_index_daily_tx(symbol=symbol)
        pct = _compute_pct_from_index_df(df, date_key)
        if pct is not None:
            log(f"  {name}(东财/新浪失败，腾讯兜底成功)")
            return pct
    except Exception as exc:
        log(f"  {name}(腾讯)拉取失败: {exc}")

    return None


def _fetch_sh_index_pct(date_key: str, *, log: Callable[[str], None]) -> Optional[float]:
    return _fetch_index_pct(date_key, symbol="sh000001", name="上证指数", log=log)


def _fetch_sz_index_pct(date_key: str, *, log: Callable[[str], None]) -> Optional[float]:
    return _fetch_index_pct(date_key, symbol="sz399001", name="深成指", log=log)


def _fetch_external(date_key: str, *, log: Callable[[str], None]) -> Dict[str, Any]:
    """拉跌停数 + 上证/深成指涨跌幅，缓存按日。"""
    cache_key = f"{CACHE_KEY_PREFIX}{date_key}"
    cached = stock_store.load_app_config(cache_key, default=None)
    if isinstance(cached, dict) and cached.get("ok"):
        # 二次校验：旧版可能写过 ok=True 但字段是 None 的"半成功"缓存。
        # 检测到这种情况就忽略缓存，强制重拉一次。
        if (cached.get("down_limit_count") is not None
                and cached.get("sh_index_pct") is not None
                and cached.get("sz_index_pct") is not None
                and cached.get("index_composite_pct") is not None):
            return cached
        log(f"  外部数据缓存字段缺失（旧版半成功记录），忽略缓存重拉")

    out: Dict[str, Any] = {
        "date_key": date_key,
        "down_limit_count": None,
        "sh_index_pct": None,
        "sz_index_pct": None,
        "index_composite_pct": None,
    }

    # 触发网络补丁（确保 SSL bypass 生效）
    _ = stock_data  # noqa: F841

    try:
        import akshare as ak
        df = ak.stock_zt_pool_dtgc_em(date=date_key)
        out["down_limit_count"] = 0 if df is None else len(df)
    except Exception as exc:
        log(f"  跌停池拉取失败: {exc}")

    out["sh_index_pct"] = _fetch_sh_index_pct(date_key, log=log)
    out["sz_index_pct"] = _fetch_sz_index_pct(date_key, log=log)
    out["index_composite_pct"] = _composite_index_pct(
        out.get("sh_index_pct"),
        out.get("sz_index_pct"),
    )

    out["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 只在外部字段都拿到时才缓存为"完整成功"。
    # 半失败状态不写缓存，让下次调用继续重试，避免 SSL/网络瞬时抖动导致
    # 大盘/跌停数据永久 None。
    all_fetched = (
        out.get("down_limit_count") is not None
        and out.get("sh_index_pct") is not None
        and out.get("sz_index_pct") is not None
        and out.get("index_composite_pct") is not None
    )
    out["ok"] = all_fetched
    if all_fetched:
        try:
            stock_store.save_app_config(cache_key, out)
        except Exception:
            logger.exception("保存 sentiment external 缓存失败")
    else:
        log(
            f"  外部数据未完整拿到（跌停={out.get('down_limit_count')}, "
            f"上证pct={out.get('sh_index_pct')}, 深成指pct={out.get('sz_index_pct')}），"
            f"本次不缓存，下次重试"
        )
    return out


# ============== 题材轮动 + 市场状态分类 ==============

def _compute_rotation_metrics(
    today_agg: Optional[Dict[str, Any]],
    yest_agg: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """计算题材轮动指标（不进 score，仅用于状态分类）。

    rotation_score 范围 ~[-50, +70]：正 = 偏轮动（主线断档/新方向涌现），
    负 = 偏延续（主线维持）。
    """
    default = {
        "main_line": None,
        "main_line_status": "unknown",  # continued / weakened / broken / unknown
        "industry_overlap_rate": 0.0,
        "hhi_today": (today_agg or {}).get("hhi", 0.0),
        "hhi_yesterday": (yest_agg or {}).get("hhi", 0.0),
        "rotation_score": 0,
        "today_top_industries": (today_agg or {}).get("top_industries", [])[:5],
        "yesterday_top_industries": (yest_agg or {}).get("top_industries", [])[:5],
        "new_industries": [],
    }
    if not today_agg or not yest_agg:
        return default

    today_top_list = [n for n, _ in today_agg.get("top_industries", [])]
    yest_top_list = [n for n, _ in yest_agg.get("top_industries", [])]
    main_line = yest_top_list[0] if yest_top_list else None
    today_top3 = today_top_list[:3]

    if main_line is None:
        status = "unknown"
    elif main_line in today_top3:
        status = "continued"
    elif main_line in today_top_list:
        status = "weakened"
    else:
        status = "broken"

    today_set = set(today_top_list)
    yest_set = set(yest_top_list)
    overlap = (
        len(today_set & yest_set) / max(1, len(today_set))
        if today_set else 0.0
    )
    new_industries = sorted(today_set - yest_set)

    score = 0
    score += {"broken": 40, "weakened": 20, "continued": -20, "unknown": 0}[status]
    if overlap < 0.3:
        score += 30
    elif overlap < 0.5:
        score += 10
    else:
        score -= 10
    # HHI 大幅下降 = 板块从集中变分散 = 更轮动
    hhi_t = today_agg.get("hhi", 0.0)
    hhi_y = yest_agg.get("hhi", 0.0)
    if hhi_y > 0:
        if hhi_t < hhi_y * 0.7:
            score += 10
        elif hhi_t > hhi_y * 1.3:
            score -= 10

    return {
        "main_line": main_line,
        "main_line_status": status,
        "industry_overlap_rate": round(overlap, 3),
        "hhi_today": hhi_t,
        "hhi_yesterday": hhi_y,
        "rotation_score": score,
        "today_top_industries": today_agg.get("top_industries", [])[:5],
        "yesterday_top_industries": yest_agg.get("top_industries", [])[:5],
        "new_industries": new_industries,
    }


def _infer_local_focus_from_concepts(concepts: List[Dict[str, Any]]) -> Dict[str, Any]:
    hits: List[Dict[str, Any]] = []
    for raw in concepts or []:
        if not isinstance(raw, dict):
            continue
        source = str(raw.get("source") or "").strip()
        if source == "行业":
            continue
        name = str(raw.get("name") or "").strip()
        if not name or not any(keyword in name for keyword in SEMICONDUCTOR_FOCUS_KEYWORDS):
            continue
        count = _safe_int(raw.get("today_count"), 0)
        if count < 2:
            continue
        hits.append({
            "name": name,
            "today_count": count,
            "phase": str(raw.get("phase") or "").strip(),
        })

    if len(hits) < 2 and sum(_safe_int(x.get("today_count"), 0) for x in hits) < 4:
        return {}

    hits.sort(key=lambda item: (-_safe_int(item.get("today_count"), 0), str(item.get("name") or "")))
    evidence = "、".join(f"{item['name']}({item['today_count']}只)" for item in hits[:3])
    return {
        "name": SEMICONDUCTOR_FOCUS_NAME,
        "reason": f"细题材证据：{evidence}",
        "evidence": hits[:5],
    }


def _load_local_focus(end: str, log: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    try:
        from src.services.concept_hype_service import analyze_concept_hype
        hype = analyze_concept_hype(end, log=log)
    except Exception as exc:
        logger.debug("加载局部强方向题材失败: %s", exc)
        return {}
    return _infer_local_focus_from_concepts(hype.get("concepts") or [])


def _apply_local_focus_to_market_state(
    market_state: Dict[str, Any],
    local_focus: Dict[str, Any],
) -> Dict[str, Any]:
    if not local_focus:
        return market_state
    out = dict(market_state)
    strategy = dict(out.get("strategy") or {})
    notes = str(strategy.get("notes") or "").strip()
    focus_note = (
        f"局部强方向：{local_focus['name']}（{local_focus['reason']}）；"
        "优先看该方向内二波/趋势核心，首板只做补涨确认。"
    )
    strategy["notes"] = f"{notes} {focus_note}".strip()
    out["strategy"] = strategy
    out["local_focus"] = local_focus
    return out


# 状态 → 推荐打法的核心规则表。
# pools 对应 scoring 模块的池子键: cont(连板) / first(首板) / fresh(新晋) / wrap(反包) / first_board(打首板)
#
# ⚠ 回测警示（2026-05-28，36 天样本 2026-04-08 ~ 2026-05-28）：
# 当前映射 **与回测结论矛盾**，已识别但暂未修正（等更长样本/实盘验证）。
#
# 1) 反包(炸过板) 全状态制霸：每个 state 下都是最优策略
#    累计 +15.51%，日均 +0.41%；vs adaptive -0.87% / 接力(2+) -12.60%
# 2) 接力日 T+1 是危险日：4 种策略全亏 -1.13% ~ -2.49%
#    "接力日 → 连板接力 + 仓位 0.8" 这条规则跟数据完全相反
# 3) 接力(2+) 全局垫底：胜率 35% 看着高，但平均涨幅是负的（多数小亏 + 少数大涨）
#
# 建议修正方向（未应用）：
# - 接力日: "连板接力+高度龙头(0.8)"  → "减仓兑现/避免追高(0.4)"
# - 轮动日: "首板新题材(0.6)"          → "反包炸板+新方向首板(0.6)"，反包优先
# - 退潮日: "反包/低吸(0.3)"           → 维持
# - 冰点日: "空仓+试探(0.1)"           → 维持
# - 过渡日: "首板谨慎接力(0.5)"        → "反包优先，控仓(0.4)"
#
# 局限：样本仅 36 天，可能赶上特定行情风格；反包(炸过板) 的 broken_count 字段
# 需分钟数据，无法用 derived_pool_service 派生数据扩展验证。要修正前请先用
# 累积更多真实涨停池数据后重跑 market_state_backtest_service。
_STATE_STRATEGIES = {
    "接力日": {
        "color": "#2e7d32",
        "strategy": {
            "label": "连板接力 + 高度龙头",
            "pools": ["cont", "first"],
            "position_cap": 0.8,
            "notes": "重点 2-4 板加速 + 龙头补涨；首板优先选主线方向；避开人气退潮的孤独高位。",
        },
    },
    "轮动日": {
        "color": "#1565c0",
        "strategy": {
            "label": "首板新题材 / 避开老主线",
            "pools": ["fresh", "first_board"],
            "position_cap": 0.6,
            "notes": "重点：今日新冒头行业（昨日 top3 外）的首板；老主线龙头的二次接力胜率低。",
        },
    },
    "退潮日": {
        "color": "#d84315",
        "strategy": {
            "label": "反包/低吸为主，轻仓",
            "pools": ["wrap", "fresh"],
            "position_cap": 0.3,
            "notes": "不打高位接力；优先昨日炸板/前期强势股回踩反包；新首板谨慎，控制单股仓位。",
        },
    },
    "冰点日": {
        "color": "#b71c1c",
        "strategy": {
            "label": "空仓观望 / 极少试探超跌反包",
            "pools": ["wrap"],
            "position_cap": 0.1,
            "notes": "原则空仓；仅可少量试探跌停股反包，单只 ≤2%。等情绪修复出明确赚钱效应再加仓。",
        },
    },
    "过渡日": {
        "color": "#f9a825",
        "strategy": {
            "label": "首板为主，谨慎接力",
            "pools": ["first", "fresh"],
            "position_cap": 0.5,
            "notes": "状态模糊，控仓为先；优先首板低位股，少量参与 2 板接力测试方向。",
        },
    },
}


def _classify_market_state(
    *,
    score: int,
    today_agg: Dict[str, Any],
    rotation: Dict[str, Any],
    yest_lu: int,
    today_continued: int,
) -> Dict[str, Any]:
    """基于综合分 + 高度结构 + 轮动指标判定当日市场状态。"""
    lu = int(today_agg.get("lu_count", 0))
    max_b = int(today_agg.get("max_boards", 0))
    n4 = int(today_agg.get("high_board_count_4plus", 0))
    cont_rate = (today_continued / yest_lu) if yest_lu else 0.0
    rot = int(rotation.get("rotation_score", 0))
    main_status = rotation.get("main_line_status", "unknown")

    # 冰点：宽度+高度同时极弱（不依赖 score，否则会误把"低评分但涨停几十只"的退潮日归冰点）
    if lu < 15 or (lu < 25 and max_b <= 2):
        label = "冰点日"
        reason = f"涨停 {lu} 只 + 最高 {max_b} 板，赚钱效应崩塌"
        confidence = 0.9
    elif max_b <= 3 and n4 == 0 and cont_rate < 0.20:
        label = "退潮日"
        reason = (
            f"最高 {max_b} 板 + 无 4+ 板 + 晋级 {cont_rate*100:.0f}%，"
            "高度梯队断档，赚钱效应退潮"
        )
        confidence = 0.85
    elif (
        max_b >= 5 and n4 >= 2
        and (cont_rate >= 0.20 or score >= 80)
        and rot < 30
    ):
        label = "接力日"
        reason = (
            f"最高 {max_b} 板 + 4+ 板 {n4} 只 + 晋级 {cont_rate*100:.0f}% + "
            f"主线 {main_status}，溢价模式"
        )
        confidence = 0.85
    elif lu >= 25 and main_status in ("broken", "weakened") and rot >= 30:
        label = "轮动日"
        reason = (
            f"主线 {main_status} + 轮动分 {rot:+d} + 涨停 {lu} 只，"
            "新方向涌现"
        )
        confidence = 0.75
    else:
        label = "过渡日"
        reason = (
            f"未触发明确接力/轮动/退潮规则：涨停 {lu} 只 · 最高 {max_b} 板 · "
            f"主线 {main_status} · 轮动 {rot:+d}"
        )
        confidence = 0.6

    spec = _STATE_STRATEGIES[label]
    return {
        "label": label,
        "color": spec["color"],
        "confidence": confidence,
        "reason": reason,
        "strategy": spec["strategy"],
    }


# ============== 仓位映射 ==============

def _position_advice(score: int) -> Dict[str, Any]:
    if score >= 80:
        return {"label": "满仓", "ratio": 1.0, "color": "#2e7d32"}
    if score >= 65:
        return {"label": "7 成", "ratio": 0.7, "color": "#558b2f"}
    if score >= 50:
        return {"label": "半仓", "ratio": 0.5, "color": "#f9a825"}
    if score >= 35:
        return {"label": "3-4 成", "ratio": 0.35, "color": "#ef6c00"}
    if score >= 20:
        return {"label": "2 成 (试探)", "ratio": 0.2, "color": "#d84315"}
    return {"label": "空仓 / 防御", "ratio": 0.0, "color": "#b71c1c"}


# ============== 对外主入口 ==============

def analyze_market_sentiment(
    end_date: Optional[str] = None,
    *,
    fetch_external: bool = True,
    include_previous: bool = True,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """计算市场情绪综合分 + 仓位建议。

    Args:
        end_date: 目标交易日（YYYYMMDD），默认取 limit_up_pool 最新一天
        fetch_external: True 时联网拉跌停池 / 上证指数；False 时只用本地涨停池数据
        include_previous: True 时附带昨日完整情绪（previous 字段），用于复盘对比
        log: 日志回调

    Returns 详见模块顶部 docstring。
    """
    def _l(msg: str) -> None:
        if log is not None:
            try:
                log(msg)
            except Exception:
                pass
        logger.info(msg)

    requested_end = _normalize_date(end_date or "")
    end = requested_end
    all_dates = stock_store.list_limit_up_pool_trade_dates() or []
    if not all_dates:
        return {
            "trade_date": "",
            "score": 50,
            "position_suggest": _position_advice(50),
            "signals": [],
            "summary": "本地无 limit_up_pool 缓存，无法判断情绪。",
            "raw": {},
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    if not end:
        end = all_dates[-1]
    elif end not in all_dates:
        required_dates = _required_pool_dates(end)
        missing_dates = _ensure_pool_dates_ready(required_dates, log=_l)
        all_dates = stock_store.list_limit_up_pool_trade_dates() or []
        if end not in all_dates:
            return {
                "trade_date": end,
                "score": 50,
                "position_suggest": _position_advice(50),
                "signals": [],
                "summary": (
                    f"{end} 情绪依赖数据不完整，缺少涨停池: "
                    + "、".join(missing_dates or [end])
                    + "。已尝试自动补齐，但仍未成功，请稍后重试。"
                ),
                "raw": {
                    "required_pool_dates": required_dates,
                    "missing_pool_dates": missing_dates or [end],
                },
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

    required_dates = _required_pool_dates(end)
    missing_dates = _ensure_pool_dates_ready(required_dates, log=_l)
    if missing_dates:
        return {
            "trade_date": end,
            "score": 50,
            "position_suggest": _position_advice(50),
            "signals": [],
            "summary": (
                f"{end} 情绪依赖数据不完整，缺少涨停池: "
                + "、".join(missing_dates)
                + "。已尝试自动补齐，但仍未成功，请稍后重试。"
            ),
            "raw": {
                "required_pool_dates": required_dates,
                "missing_pool_dates": missing_dates,
            },
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    today_agg = _load_pool_aggregates(end)
    if not today_agg:
        return {
            "trade_date": end,
            "score": 50,
            "position_suggest": _position_advice(50),
            "signals": [],
            "summary": f"{end} 无涨停池数据，无法判断情绪。",
            "raw": {},
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    avg5, prior_dates, prior_counts = _avg_lu_count_5d(end)
    yest_date = _previous_pool_date(end)
    yest_agg = _load_pool_aggregates(yest_date) if yest_date else None
    yest_codes = (yest_agg or {}).get("codes", [])
    today_codes = today_agg.get("codes", [])
    yest_lu, today_continued = _continuation_today_from_yesterday(yest_codes, today_codes)

    external = (
        _fetch_external(end, log=_l) if fetch_external else
        {
            "down_limit_count": None,
            "sh_index_pct": None,
            "sz_index_pct": None,
            "index_composite_pct": None,
        }
    )

    # 计算 7 个信号
    signals: List[Dict[str, Any]] = []
    score = 50

    delta, note = _signal_lu_count_vs_avg(today_agg["lu_count"], avg5)
    score += delta
    signals.append({"name": "涨停数", "value": str(today_agg["lu_count"]),
                    "delta": delta, "note": note})

    delta, note = _signal_break_rate(today_agg["broken_count"], today_agg["lu_count"])
    score += delta
    signals.append({"name": "炸板率",
                    "value": f"{today_agg['broken_count']}/{today_agg['lu_count']}",
                    "delta": delta, "note": note})

    delta, note = _signal_max_boards(today_agg["max_boards"])
    score += delta
    signals.append({"name": "最高连板", "value": f"{today_agg['max_boards']}板",
                    "delta": delta, "note": note})

    delta, note = _signal_high_board_count(today_agg["high_board_count_4plus"])
    score += delta
    signals.append({"name": "4+ 板", "value": str(today_agg["high_board_count_4plus"]),
                    "delta": delta, "note": note})

    delta, note = _signal_continuation(yest_lu, today_continued)
    score += delta
    signals.append({"name": "晋级率",
                    "value": (f"{today_continued}/{yest_lu}" if yest_lu else "-"),
                    "delta": delta, "note": note})

    index_pct = external.get("index_composite_pct")
    if index_pct is None:
        index_pct = _composite_index_pct(
            external.get("sh_index_pct"),
            external.get("sz_index_pct"),
        )
    delta, note = _signal_index(index_pct, "大盘")
    if index_pct is not None:
        note = (
            f"{note}（{_format_index_detail('上证', external.get('sh_index_pct'))}，"
            f"{_format_index_detail('深成指', external.get('sz_index_pct'))}）"
        )
    score += delta
    signals.append({"name": "大盘",
                    "value": (f"{index_pct:+.2f}%"
                              if index_pct is not None else "-"),
                    "delta": delta, "note": note})

    delta, note = _signal_down_limit(_safe_int(external.get("down_limit_count"), 0))
    score += delta
    signals.append({"name": "跌停数",
                    "value": (str(external.get("down_limit_count"))
                              if external.get("down_limit_count") is not None else "-"),
                    "delta": delta, "note": note})

    score = max(0, min(100, score))
    advice = _position_advice(score)

    rotation = _compute_rotation_metrics(today_agg, yest_agg)
    market_state = _classify_market_state(
        score=score,
        today_agg=today_agg,
        rotation=rotation,
        yest_lu=yest_lu,
        today_continued=today_continued,
    )
    local_focus = _load_local_focus(end, log=_l)
    market_state = _apply_local_focus_to_market_state(market_state, local_focus)

    # 一句话总结
    parts: List[str] = []
    parts.append(
        f"涨停 {today_agg['lu_count']} 只"
        + (f"（vs 5 日均值 {avg5:.0f}）" if avg5 > 0 else "")
    )
    parts.append(f"最高 {today_agg['max_boards']} 板")
    if yest_lu:
        rate = today_continued / yest_lu * 100
        parts.append(f"晋级 {rate:.0f}%")
    if index_pct is not None:
        parts.append(f"大盘 {index_pct:+.2f}%")
    if external.get("down_limit_count") is not None:
        parts.append(f"跌停 {external['down_limit_count']} 只")
    summary = (
        "；".join(parts)
        + f"。综合 {score} 分 → 建议 {advice['label']}。"
        + f" 状态：{market_state['label']} → {market_state['strategy']['label']}。"
    )
    if local_focus:
        summary += f" 局部强方向：{local_focus['name']}（{local_focus['reason']}）。"

    _l(
        f"市场情绪 {end}: 综合 {score}/100 → {advice['label']} | "
        f"状态 {market_state['label']} → {market_state['strategy']['label']}"
    )

    # 昨日完整情绪（复盘对比用）：递归算一次 T-1，include_previous=False 防止无限递归。
    # 涨停池数据在本地、外部数据按日缓存，正常情况下这一步不产生额外联网。
    previous: Optional[Dict[str, Any]] = None
    if include_previous and yest_date:
        try:
            prev_full = analyze_market_sentiment(
                yest_date,
                fetch_external=fetch_external,
                include_previous=False,
                log=log,
            )
            if prev_full.get("market_state"):
                previous = {
                    "trade_date": prev_full.get("trade_date"),
                    "score": prev_full.get("score"),
                    "position_suggest": prev_full.get("position_suggest"),
                    "market_state": prev_full.get("market_state"),
                    "signals": prev_full.get("signals"),
                    "summary": prev_full.get("summary"),
                }
        except Exception:
            logger.exception("计算昨日情绪失败（不影响当日结果）")

    return {
        "trade_date": end,
        "score": score,
        "position_suggest": advice,
        "signals": signals,
        "summary": summary,
        "market_state": market_state,
        "previous": previous,
        "raw": {
            "today": today_agg,
            "yesterday_date": yest_date,
            "yesterday_lu": yest_lu,
            "today_continued": today_continued,
            "avg5": avg5,
            "prior_dates": prior_dates,
            "prior_counts": prior_counts,
            "required_pool_dates": required_dates,
            "external": external,
            "rotation": rotation,
            "local_focus": local_focus,
        },
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
