"""市场情绪综合评分 + 仓位建议。

7 个维度的本地规则评分（baseline 50，clip 到 0-100），输出明确的仓位建议
（空仓 / 2 成 / 半仓 / 7 成 / 满仓）。

数据源：
1. limit_up_pool (SQLite)            — 涨停数 / 炸板 / 连板高度 / 4+板数量
2. limit_up_pool[T-1] vs [T]         — 昨日涨停今日继续涨停率（赚钱效应）
3. ak.stock_zt_pool_dtgc_em          — 跌停池（按日，需要联网）
4. ak.stock_zh_index_daily_em        — 上证/深证指数日 K（需要联网）

联网拉到的数据按 trade_date 缓存到 app_config，同一天只拉一次。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import stock_data  # 触发 SSL/proxy 网络补丁
import stock_store
from stock_logger import get_logger
from src.utils.trade_calendar import _get_trade_calendar, _previous_trading_day

logger = get_logger(__name__)

CACHE_KEY_PREFIX = "market_sentiment_external_"
_POOL_FETCHER: Optional["stock_data.StockDataFetcher"] = None


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

def _load_pool_aggregates(date_key: str) -> Optional[Dict[str, Any]]:
    """从 limit_up_pool 聚合涨停数 / 炸板 / 最高板 / 4+板等。"""
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
    return {
        "lu_count": len(df),
        "broken_count": int((breaks > 0).sum()) if breaks is not None else 0,
        "broken_total_times": int(breaks.sum()) if breaks is not None else 0,
        "max_boards": int(boards.max()) if boards is not None and len(boards) else 0,
        "high_board_count_4plus": int((boards >= 4).sum()) if boards is not None else 0,
        "codes": codes,
    }


def _avg_lu_count_5d(end_date: str) -> Tuple[float, List[str]]:
    """end_date 之前最近 5 个真实交易日的平均涨停数。"""
    prior = _previous_trade_dates(end_date, 5)
    if not prior:
        # 交易日历不可用时，退回旧逻辑：按已缓存日期近似。
        all_dates = stock_store.list_limit_up_pool_trade_dates() or []
        prior = [d for d in all_dates if d < end_date][-5:]
    if not prior:
        return 0.0, []
    counts: List[int] = []
    for d in prior:
        df = stock_store.load_limit_up_pool(d)
        if df is not None and not df.empty:
            counts.append(len(df))
    if not counts:
        return 0.0, prior
    return sum(counts) / len(counts), prior


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


def _fetch_external(date_key: str, *, log: Callable[[str], None]) -> Dict[str, Any]:
    """拉跌停数 + 上证指数涨跌幅，缓存按日。"""
    cache_key = f"{CACHE_KEY_PREFIX}{date_key}"
    cached = stock_store.load_app_config(cache_key, default=None)
    if isinstance(cached, dict) and cached.get("ok"):
        return cached

    out: Dict[str, Any] = {"date_key": date_key, "down_limit_count": None, "sh_index_pct": None}

    # 触发网络补丁（确保 SSL bypass 生效）
    _ = stock_data  # noqa: F841

    try:
        import akshare as ak
        df = ak.stock_zt_pool_dtgc_em(date=date_key)
        out["down_limit_count"] = 0 if df is None else len(df)
    except Exception as exc:
        log(f"  跌停池拉取失败: {exc}")

    try:
        import akshare as ak
        # 拉前后各几天的 K，找到 date_key 对应那一行
        from datetime import datetime as _dt, timedelta
        end_dt = _dt.strptime(date_key, "%Y%m%d")
        start_dt = end_dt - timedelta(days=10)
        df = ak.stock_zh_index_daily_em(
            symbol="sh000001",
            start_date=start_dt.strftime("%Y%m%d"),
            end_date=date_key,
        )
        if df is not None and not df.empty:
            df = df.copy()
            df["date_key"] = df["date"].astype(str).str.replace("-", "")
            target = df[df["date_key"] == date_key]
            if not target.empty and len(df) >= 2:
                today_close = _safe_float(target.iloc[0]["close"])
                # 找前一行的 close
                idx = df.index[df["date_key"] == date_key][0]
                pos = df.index.get_loc(idx)
                if pos > 0:
                    prev_close = _safe_float(df.iloc[pos - 1]["close"])
                    if prev_close > 0:
                        out["sh_index_pct"] = (today_close / prev_close - 1.0) * 100
    except Exception as exc:
        log(f"  上证指数拉取失败: {exc}")

    out["ok"] = True
    out["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        stock_store.save_app_config(cache_key, out)
    except Exception:
        logger.exception("保存 sentiment external 缓存失败")
    return out


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
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """计算市场情绪综合分 + 仓位建议。

    Args:
        end_date: 目标交易日（YYYYMMDD），默认取 limit_up_pool 最新一天
        fetch_external: True 时联网拉跌停池 / 上证指数；False 时只用本地涨停池数据
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

    end = _normalize_date(end_date or "")
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
    if not end or end not in all_dates:
        end = all_dates[-1]

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

    avg5, prior_dates = _avg_lu_count_5d(end)
    yest_date = _previous_pool_date(end)
    yest_agg = _load_pool_aggregates(yest_date) if yest_date else None
    yest_codes = (yest_agg or {}).get("codes", [])
    today_codes = today_agg.get("codes", [])
    yest_lu, today_continued = _continuation_today_from_yesterday(yest_codes, today_codes)

    external = (
        _fetch_external(end, log=_l) if fetch_external else
        {"down_limit_count": None, "sh_index_pct": None}
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

    delta, note = _signal_index(external.get("sh_index_pct"), "上证")
    score += delta
    signals.append({"name": "大盘",
                    "value": (f"{external.get('sh_index_pct'):+.2f}%"
                              if external.get("sh_index_pct") is not None else "-"),
                    "delta": delta, "note": note})

    delta, note = _signal_down_limit(_safe_int(external.get("down_limit_count"), 0))
    score += delta
    signals.append({"name": "跌停数",
                    "value": (str(external.get("down_limit_count"))
                              if external.get("down_limit_count") is not None else "-"),
                    "delta": delta, "note": note})

    score = max(0, min(100, score))
    advice = _position_advice(score)

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
    if external.get("sh_index_pct") is not None:
        parts.append(f"上证 {external['sh_index_pct']:+.2f}%")
    if external.get("down_limit_count") is not None:
        parts.append(f"跌停 {external['down_limit_count']} 只")
    summary = "；".join(parts) + f"。综合 {score} 分 → 建议 {advice['label']}。"

    _l(f"市场情绪 {end}: 综合 {score}/100 → {advice['label']}")

    return {
        "trade_date": end,
        "score": score,
        "position_suggest": advice,
        "signals": signals,
        "summary": summary,
        "raw": {
            "today": today_agg,
            "yesterday_date": yest_date,
            "yesterday_lu": yest_lu,
            "today_continued": today_continued,
            "avg5": avg5,
            "prior_dates": prior_dates,
            "required_pool_dates": required_dates,
            "external": external,
        },
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
