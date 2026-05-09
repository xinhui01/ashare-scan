"""涨停预测准确率回填与查询服务。

输入：`limit_up_predictions` 表里已保存的预测结果（按 trade_date 索引）。
输出：在 `limit_up_prediction_accuracy` 表中写入每个候选在 T+1 的实际表现，
并提供查询接口（聚合命中率、单日明细、对比展示）。

命中口径：
- hit_strict：T+1 收盘 = 涨停（按代码前缀推断 10% / 20% / 5%）且非一字板
- hit_loose ：T+1「按开盘价买入、收盘价卖出」收益 ≥ 5%，或者达到涨停（含 hit_strict）
              —— 用于"保留涨停"以外的预测类别；比单纯 t1_pct ≥ 5% 更贴合实盘
- hit_buyable：非一字（open!=high 或 涨幅可买），非停牌；用于剔除"理论命中但买不到"的样本

成功标准（GUI / 统计层面会按类别区分）：
- 保留涨停（cont 及子类 cont_1to2/.../cont_5plus）：必须 hit_strict（次日继续涨停）
- 其他类别（first/fresh/wrap/trend）：hit_loose（开盘买、收盘 ≥ 5% 或涨停）

类别名称（与 stock_filter.predict_limit_up_candidates 输出一致）：
    cont   = continuation_candidates
    first  = first_board_candidates
    fresh  = fresh_first_board_candidates
    wrap   = broken_board_wrap_candidates
    trend  = trend_limit_up_candidates
"""

from __future__ import annotations

from datetime import datetime
from collections import Counter
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

import stock_store
from stock_logger import get_logger
from src.utils.trade_calendar import (
    _get_trade_calendar,
    _is_trading_day,
)

logger = get_logger(__name__)


CATEGORY_KEYS: Dict[str, str] = {
    "cont": "continuation_candidates",
    "first": "first_board_candidates",
    "fresh": "fresh_first_board_candidates",
    "wrap": "broken_board_wrap_candidates",
    "trend": "trend_limit_up_candidates",
}

# 保留涨停按当前连板数细分（仅追加，不替换 cont 总类）
# - 1板今日涨停 → 预测明日 2 连板 = "1进2"
# - 2板今日涨停 → 预测明日 3 连板 = "2进3"
# - 依此类推，5 板及以上合并为 "5进6+"
CONT_SUB_CATEGORY_KEYS: List[str] = [
    "cont_1to2", "cont_2to3", "cont_3to4", "cont_4to5", "cont_5plus",
]

CATEGORY_LABELS: Dict[str, str] = {
    "cont": "保留涨停",
    "first": "二波接力",
    "fresh": "首板涨停",
    "wrap": "反包/承接",
    "trend": "趋势涨停",
    "cont_1to2": "1进2",
    "cont_2to3": "2进3",
    "cont_3to4": "3进4",
    "cont_4to5": "4进5",
    "cont_5plus": "5进6+",
}


def _is_cont_category(category: Any) -> bool:
    """保留涨停的主类别 + 1进2/2进3/.../5进6+ 子类别。"""
    cat = str(category or "")
    return cat == "cont" or cat in CONT_SUB_CATEGORY_KEYS


def _row_is_hit(row: Dict[str, Any]) -> bool:
    """按类别选取成功口径：cont 走 hit_strict（必须涨停），其它类别走 hit_loose。"""
    if not int(row.get("hit_buyable") or 0):
        return False
    if _is_cont_category(row.get("category")):
        return bool(int(row.get("hit_strict") or 0))
    return bool(int(row.get("hit_loose") or 0))


def _cont_sub_category(boards: Any) -> str:
    """按当前连板数派生子类别 key（用于 1进2/2进3 等命中率拆分）。"""
    try:
        b = int(boards)
    except (TypeError, ValueError):
        b = 1
    if b <= 1:
        return "cont_1to2"
    if b == 2:
        return "cont_2to3"
    if b == 3:
        return "cont_3to4"
    if b == 4:
        return "cont_4to5"
    return "cont_5plus"


def _normalize_date_yyyymmdd(value: str) -> str:
    text = str(value or "").strip().replace("-", "").replace("/", "")
    return text


def _to_dash_date(value: str) -> str:
    """YYYYMMDD → YYYY-MM-DD（history 表是中划线格式）。"""
    text = _normalize_date_yyyymmdd(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _next_trading_day_yyyymmdd(trade_date: str) -> Optional[str]:
    """返回 trade_date 之后的下一个交易日 YYYYMMDD。"""
    text = _normalize_date_yyyymmdd(trade_date)
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        d = datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None
    cal = _get_trade_calendar()
    from datetime import timedelta
    for i in range(1, 31):
        cand = d + timedelta(days=i)
        if _is_trading_day(cand, cal):
            return cand.strftime("%Y%m%d")
    return None


def _infer_verify_date_from_history(
    history_cache: Dict[str, Optional[pd.DataFrame]],
    trade_date_dash: str,
) -> Optional[str]:
    """从本地 history 缓存推断 trade_date 之后的真实下一个交易日。

    主要用于交易日历不可用时的节假日兜底：优先取"最常见的下一个日期"，
    并在并列时取更早的那个日期。
    """
    next_dates: List[str] = []
    for df in history_cache.values():
        if df is None or df.empty or "date" not in df.columns:
            continue
        try:
            dates = df["date"].astype(str).str.strip()
        except Exception:
            continue
        later = dates[dates > trade_date_dash]
        if later.empty:
            continue
        next_dates.append(str(later.iloc[0]).strip())

    if not next_dates:
        return None

    counter = Counter(next_dates)
    return sorted(counter.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _limit_up_threshold(code: str, name: str = "") -> float:
    """根据代码前缀和股票名称推断涨停阈值（百分比）。

    标准涨停常因价格四舍五入触及 9.93%~10.00%，因此预留 0.3% 容忍。
    """
    code = str(code or "").strip().zfill(6)
    name = str(name or "")
    is_st = "ST" in name.upper()
    if is_st:
        return 4.7  # ST: 5%
    # 创业板 / 科创板：20%
    if code.startswith("30") or code.startswith("68"):
        return 19.5
    # 北交所：30%
    if code.startswith("4") or code.startswith("8"):
        return 29.5
    # 主板：10%
    return 9.7


def _is_one_word(open_: Optional[float], high: Optional[float],
                 low: Optional[float], close: Optional[float]) -> bool:
    """判定一字板：开高低收完全相等，且涨幅触顶。"""
    vals = [open_, high, low, close]
    if any(v is None for v in vals):
        return False
    try:
        return abs(open_ - high) < 1e-6 and abs(open_ - low) < 1e-6 and abs(open_ - close) < 1e-6
    except TypeError:
        return False


def _evaluate_candidate(
    code: str,
    name: str,
    history_df: Optional[pd.DataFrame],
    trade_date_dash: str,
    verify_date_dash: str,
) -> Optional[Dict[str, Any]]:
    """从 history_df 中提取 T 与 T+1 的价格信息，返回评估字段；缺数据返回 None。"""
    if history_df is None or history_df.empty:
        return None
    try:
        df = history_df.copy()
        df["date"] = df["date"].astype(str).str.strip()
    except Exception:
        return None

    t_row = df[df["date"] == trade_date_dash]
    t1_row = df[df["date"] == verify_date_dash]

    t_close = float(t_row["close"].iloc[0]) if not t_row.empty and pd.notna(t_row["close"].iloc[0]) else None

    if t1_row.empty:
        # T+1 K 线缺失：视为停牌或暂未到日期
        return {
            "t_close": t_close,
            "t1_open": None, "t1_high": None, "t1_low": None,
            "t1_close": None, "t1_pct": None, "t1_open_close_pct": None,
            "t1_limit_up": False, "t1_one_word": False, "t1_suspended": True,
            "hit_strict": False, "hit_loose": False, "hit_buyable": False,
        }

    def _f(col: str) -> Optional[float]:
        v = t1_row[col].iloc[0] if col in t1_row.columns else None
        return float(v) if v is not None and pd.notna(v) else None

    t1_open = _f("open")
    t1_high = _f("high")
    t1_low = _f("low")
    t1_close = _f("close")
    t1_pct = _f("change_pct")

    # 开盘买入、收盘卖出的真实涨幅 —— 实盘可达盈亏的口径
    if t1_open is not None and t1_close is not None and t1_open > 0:
        t1_open_close_pct: Optional[float] = (t1_close - t1_open) / t1_open * 100.0
    else:
        t1_open_close_pct = None

    threshold = _limit_up_threshold(code, name)
    is_lu = (t1_pct is not None and t1_pct >= threshold)
    one_word = is_lu and _is_one_word(t1_open, t1_high, t1_low, t1_close)
    buyable = not one_word  # 一字板买不到
    hit_strict = bool(is_lu and buyable)
    # 新口径：开盘买、收盘 ≥ 5% 或涨停（且可买）；保留涨停类别另在统计层强制走 hit_strict
    hit_loose = bool(
        buyable and (
            hit_strict
            or (t1_open_close_pct is not None and t1_open_close_pct >= 5.0)
        )
    )

    return {
        "t_close": t_close,
        "t1_open": t1_open,
        "t1_high": t1_high,
        "t1_low": t1_low,
        "t1_close": t1_close,
        "t1_pct": t1_pct,
        "t1_open_close_pct": t1_open_close_pct,
        "t1_limit_up": is_lu,
        "t1_one_word": one_word,
        "t1_suspended": False,
        "hit_strict": hit_strict,
        "hit_loose": hit_loose,
        "hit_buyable": buyable,
    }


def evaluate(trade_date: str) -> Dict[str, Any]:
    """评估某一日的预测结果，回填到 limit_up_prediction_accuracy。

    返回 {trade_date, verify_date, written, skipped, reason}。
    若 verify_date 尚未到达或无 K 线数据，返回 reason='not_ready'。
    """
    td = _normalize_date_yyyymmdd(trade_date)
    if not td:
        return {"trade_date": trade_date, "written": 0, "reason": "invalid_date"}

    payload = stock_store.load_limit_up_prediction_by_date(td)
    if not payload:
        return {"trade_date": td, "written": 0, "reason": "no_prediction"}

    verify_date = _next_trading_day_yyyymmdd(td)
    if not verify_date:
        return {"trade_date": td, "written": 0, "reason": "no_next_trade_day"}

    today_str = datetime.now().strftime("%Y%m%d")
    if verify_date > today_str:
        return {"trade_date": td, "verify_date": verify_date,
                "written": 0, "reason": "not_ready"}

    trade_date_dash = _to_dash_date(td)
    verify_date_dash = _to_dash_date(verify_date)

    history_cache: Dict[str, Optional[pd.DataFrame]] = {}

    def _build_records(target_verify_date: str) -> Tuple[List[Dict[str, Any]], bool]:
        records: List[Dict[str, Any]] = []
        seen: set = set()  # (code, category) 去重
        has_any_t1_data = False
        target_verify_date_dash = _to_dash_date(target_verify_date)

        for cat_key, payload_key in CATEGORY_KEYS.items():
            for cand in payload.get(payload_key, []) or []:
                if not isinstance(cand, dict):
                    continue
                code = str(cand.get("code") or "").strip().zfill(6)
                if not code:
                    continue
                key = (code, cat_key)
                if key in seen:
                    continue
                seen.add(key)

                if code not in history_cache:
                    try:
                        history_cache[code] = stock_store.load_history(code)
                    except Exception:
                        history_cache[code] = None
                df = history_cache[code]

                evaluation = _evaluate_candidate(
                    code=code,
                    name=str(cand.get("name") or ""),
                    history_df=df,
                    trade_date_dash=trade_date_dash,
                    verify_date_dash=target_verify_date_dash,
                )
                if evaluation is None:
                    # 完全无 K 线 → 标记为停牌、命中=0
                    evaluation = {
                        "t_close": None, "t1_open": None, "t1_high": None,
                        "t1_low": None, "t1_close": None, "t1_pct": None,
                        "t1_open_close_pct": None,
                        "t1_limit_up": False, "t1_one_word": False,
                        "t1_suspended": True,
                        "hit_strict": False, "hit_loose": False, "hit_buyable": False,
                    }
                elif not evaluation.get("t1_suspended"):
                    has_any_t1_data = True

                record = {
                    "trade_date": td,
                    "verify_date": target_verify_date,
                    "code": code,
                    "category": cat_key,
                    "name": str(cand.get("name") or ""),
                    "industry": str(cand.get("industry") or ""),
                    "predicted_score": int(cand.get("score") or 0),
                    "predicted_type": str(cand.get("predict_type") or ""),
                    **evaluation,
                }
                records.append(record)

                # 保留涨停额外按连板数派生子类别（cont_1to2/cont_2to3/...）
                # 主类别 cont 仍然写入，子类别用于拆分命中率统计
                if cat_key == "cont":
                    sub_cat = _cont_sub_category(cand.get("consecutive_boards"))
                    sub_key = (code, sub_cat)
                    if sub_key not in seen:
                        seen.add(sub_key)
                        sub_record = dict(record)
                        sub_record["category"] = sub_cat
                        records.append(sub_record)
        return records, has_any_t1_data

    records, has_any_t1_data = _build_records(verify_date)

    if records and not has_any_t1_data:
        inferred_verify_date_dash = _infer_verify_date_from_history(history_cache, trade_date_dash)
        inferred_verify_date = _normalize_date_yyyymmdd(inferred_verify_date_dash or "")
        if inferred_verify_date and inferred_verify_date != verify_date:
            alt_records, alt_has_any_t1_data = _build_records(inferred_verify_date)
            if alt_has_any_t1_data:
                verify_date = inferred_verify_date
                records = alt_records
                has_any_t1_data = True

    # 没有任何候选拿到 T+1 K 线 → 视为本地缓存还没同步，
    # 不写入坏记录，避免 evaluate_all_pending 把这一天误标为"已评估"后永久卡住
    if records and not has_any_t1_data:
        return {
            "trade_date": td,
            "verify_date": verify_date,
            "written": 0,
            "candidates": len(records),
            "reason": "no_verify_data",
        }

    written = stock_store.save_prediction_accuracy_records(records)
    return {
        "trade_date": td,
        "verify_date": verify_date,
        "written": written,
        "candidates": len(records),
        "reason": "ok",
    }


def evaluate_all_pending(
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    force: bool = False,
) -> Dict[str, Any]:
    """扫描所有 limit_up_predictions，对 T+1 已就绪且未写入或被强制刷新的日期回填。

    `force=True` 时强制重新评估所有日期（覆盖旧记录）。
    """
    pred_dates = stock_store.list_limit_up_prediction_dates()
    if not pred_dates:
        return {"evaluated": [], "skipped": [], "total": 0}
    if force:
        evaluated_set: set = set()
    else:
        # 已评估口径：accuracy 表里同时存在 cont 子类别记录的日期。
        # 这样旧版本只写了 cont 主类别的日期，下次刷新时会被自动重跑以补全 1进2/2进3 等子类别。
        evaluated_set = set(stock_store.list_prediction_accuracy_dates_with_cont_subcat())

    pending: List[str] = []
    for d in pred_dates:
        td = _normalize_date_yyyymmdd(d)
        if force or td not in evaluated_set:
            pending.append(td)

    pending.sort()  # 早 → 晚
    total = len(pending)
    evaluated: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for idx, td in enumerate(pending, start=1):
        if progress_cb:
            try:
                progress_cb(idx, total, td)
            except Exception:
                pass
        try:
            res = evaluate(td)
        except Exception as exc:
            logger.exception("回填准确率失败 trade_date=%s", td)
            skipped.append({"trade_date": td, "reason": f"error: {exc}"})
            continue
        if res.get("reason") == "ok" and res.get("written", 0) > 0:
            evaluated.append(res)
        else:
            skipped.append(res)
    return {"evaluated": evaluated, "skipped": skipped, "total": total}


def query_compare(trade_date: str) -> Dict[str, Any]:
    """构造对比弹窗的数据结构。

    返回 {
        trade_date, verify_date,
        candidates: [{...单条预测含 result字段...}],   # 按 score 降序
        actual_lu: [{code, name, industry, ...}],     # T+1 实际涨停名单
        stats: {predicted, hit, hit_rate, missed, ...}
    }
    """
    td = _normalize_date_yyyymmdd(trade_date)
    rows = stock_store.load_prediction_accuracy_by_date(td)
    if not rows:
        return {
            "trade_date": td, "verify_date": "",
            "candidates": [], "actual_lu": [],
            "stats": {"predicted": 0, "hit": 0, "missed": 0, "hit_rate": 0.0,
                      "buyable": 0, "actual_count": 0, "missed_predict": 0},
        }
    verify_date = next((r["verify_date"] for r in rows if r.get("verify_date")), "")

    # 候选去重时按 (code) 合并多类别；cont 子类别仅用于命中率拆分，不参与对比展示
    by_code: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        cat = str(r.get("category") or "")
        if cat in CONT_SUB_CATEGORY_KEYS:
            continue
        code = r.get("code") or ""
        cur = by_code.get(code)
        cat_label = CATEGORY_LABELS.get(cat, cat)
        if cur is None:
            cur = {
                "code": code,
                "name": r.get("name") or "",
                "industry": r.get("industry") or "",
                "categories": [cat_label],
                "category_keys": [cat],
                "max_score": int(r.get("predicted_score") or 0),
                "t1_pct": r.get("t1_pct"),
                "t1_open_close_pct": r.get("t1_open_close_pct"),
                "t1_close": r.get("t1_close"),
                "t1_open": r.get("t1_open"),
                "hit_strict": int(r.get("hit_strict") or 0),
                "hit_loose": int(r.get("hit_loose") or 0),
                "hit_buyable": int(r.get("hit_buyable") or 0),
                "t1_one_word": int(r.get("t1_one_word") or 0),
                "t1_suspended": int(r.get("t1_suspended") or 0),
                "t1_limit_up": int(r.get("t1_limit_up") or 0),
            }
            by_code[code] = cur
        else:
            if cat_label not in cur["categories"]:
                cur["categories"].append(cat_label)
            if cat not in cur["category_keys"]:
                cur["category_keys"].append(cat)
            cur["max_score"] = max(cur["max_score"], int(r.get("predicted_score") or 0))

    candidates = sorted(by_code.values(), key=lambda x: -x["max_score"])

    # 实际涨停名单：从 limit_up_compares 取 T+1 today_first
    actual_lu: List[Dict[str, Any]] = []
    if verify_date:
        compare_payload = stock_store.load_limit_up_compare_by_date(verify_date)
        if compare_payload:
            for entry in compare_payload.get("today_first", []) or []:
                if isinstance(entry, dict):
                    actual_lu.append(entry)

    actual_codes = {str(e.get("code") or "").zfill(6) for e in actual_lu}
    candidate_codes = set(by_code.keys())
    missed_predict_codes = actual_codes - candidate_codes  # 实际涨停但没预测出来

    predicted = len(candidates)
    buyable_cands = [c for c in candidates if c["hit_buyable"]]
    buyable = len(buyable_cands)

    def _candidate_is_hit(c: Dict[str, Any]) -> bool:
        # 涨停始终算命中（任何类别都满足）
        if int(c.get("hit_strict") or 0):
            return True
        # 非 cont 类别走 hit_loose（开盘买、收盘 ≥ 5%）
        cat_keys = c.get("category_keys") or []
        if any(not _is_cont_category(k) for k in cat_keys):
            return bool(int(c.get("hit_loose") or 0))
        return False

    hit = sum(1 for c in buyable_cands if _candidate_is_hit(c))
    hit_rate = (hit / buyable * 100.0) if buyable else 0.0

    return {
        "trade_date": td,
        "verify_date": verify_date,
        "candidates": candidates,
        "actual_lu": actual_lu,
        "actual_codes": list(actual_codes),
        "missed_predict_codes": list(missed_predict_codes),
        "stats": {
            "predicted": predicted,
            "buyable": buyable,
            "hit": hit,
            "missed": buyable - hit,
            "hit_rate": hit_rate,
            "actual_count": len(actual_lu),
            "missed_predict": len(missed_predict_codes),
        },
    }


def query_category_stats(lookback_dates: int = 20) -> Dict[str, Dict[str, Any]]:
    """返回主类别 + cont 子类别在最近 N 个交易日的命中率统计。

    返回 key：cont/first/fresh/wrap/trend + cont_1to2/cont_2to3/.../cont_5plus
    """
    out: Dict[str, Dict[str, Any]] = {}
    all_cats: List[str] = list(CATEGORY_KEYS.keys()) + list(CONT_SUB_CATEGORY_KEYS)
    for cat in all_cats:
        out[cat] = stock_store.query_prediction_accuracy_stats(
            category=cat, lookback_dates=lookback_dates,
        )
    return out


def query_category_stats_yesterday() -> Dict[str, Dict[str, Any]]:
    """返回主类别 + cont 子类别在「最近一个已评估交易日」的命中率统计。

    实现：每个 category 调用 query_prediction_accuracy_stats(lookback_dates=1)；
    其内部按 trade_date DESC LIMIT 1 取该类别下最新一日，所以不同类别的"昨日"
    可能对应不同 trade_date（少见，但 fresh/wrap 等可能在某天没有任何候选）。
    """
    out: Dict[str, Dict[str, Any]] = {}
    all_cats: List[str] = list(CATEGORY_KEYS.keys()) + list(CONT_SUB_CATEGORY_KEYS)
    for cat in all_cats:
        out[cat] = stock_store.query_prediction_accuracy_stats(
            category=cat, lookback_dates=1,
        )
    return out


def query_overall_stats(lookback_dates: int = 20) -> Dict[str, Any]:
    return stock_store.query_prediction_accuracy_stats(
        category=None, lookback_dates=lookback_dates,
    )


def get_per_code_results(trade_date: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """返回 {(code, category): row_dict}，供 GUI 在 Treeview 里逐行标注命中。"""
    rows = stock_store.load_prediction_accuracy_by_date(trade_date)
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        key = (str(r.get("code") or "").zfill(6), str(r.get("category") or ""))
        out[key] = r
    return out


# ============== 策略分析（分数段 / 行业 / 失败归因） ==============

SCORE_BUCKETS: List[Tuple[str, int, int]] = [
    ("0-49", 0, 49),
    ("50-59", 50, 59),
    ("60-69", 60, 69),
    ("70-79", 70, 79),
    ("80-100", 80, 100),
]


def _load_recent_rows(
    category: Optional[str],
    lookback_dates: int,
) -> List[Dict[str, Any]]:
    """取最近 N 个 trade_date 的所有 accuracy 行（可按 category 过滤）。"""
    all_dates = stock_store.list_prediction_accuracy_dates()
    if not all_dates:
        return []
    recent = all_dates[: max(1, int(lookback_dates))]
    out: List[Dict[str, Any]] = []
    for td in recent:
        rows = stock_store.load_prediction_accuracy_by_date(td)
        for r in rows:
            cat = str(r.get("category") or "")
            if category:
                if cat != category:
                    continue
            else:
                # 全类别聚合时跳过 cont 子类别，避免和 cont 主类别重复计数
                if cat in CONT_SUB_CATEGORY_KEYS:
                    continue
            out.append(r)
    return out


def query_score_bucket_stats(
    category: Optional[str] = None,
    lookback_dates: int = 20,
) -> List[Dict[str, Any]]:
    """按预测分分桶统计命中率。

    返回 [{label, lo, hi, total, buyable, hit, rate, avg_pct}] 5 个桶。
    """
    rows = _load_recent_rows(category, lookback_dates)
    out: List[Dict[str, Any]] = []
    for label, lo, hi in SCORE_BUCKETS:
        bucket = [r for r in rows if lo <= int(r.get("predicted_score") or 0) <= hi]
        buyable = [r for r in bucket if int(r.get("hit_buyable") or 0)]
        hit = sum(1 for r in buyable if _row_is_hit(r))
        # avg_pct 用"开盘买、收盘卖"口径，保留涨停类别仍可参考但回退到 t1_pct
        pcts = [
            float(r["t1_open_close_pct"]) if r.get("t1_open_close_pct") is not None
            else float(r["t1_pct"])
            for r in buyable
            if (r.get("t1_open_close_pct") is not None or r.get("t1_pct") is not None)
        ]
        avg_pct = (sum(pcts) / len(pcts)) if pcts else 0.0
        out.append({
            "label": label,
            "lo": lo, "hi": hi,
            "total": len(bucket),
            "buyable": len(buyable),
            "hit": hit,
            "rate": (hit / len(buyable) * 100.0) if buyable else 0.0,
            "avg_pct": avg_pct,
        })
    return out


def query_industry_stats(
    lookback_dates: int = 20,
    min_samples: int = 3,
) -> List[Dict[str, Any]]:
    """按行业统计命中率，按命中率降序排列。

    `min_samples`：可买入样本数小于该值的行业不展示，避免小样本噪声。
    """
    rows = _load_recent_rows(None, lookback_dates)
    by_ind: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        ind = (r.get("industry") or "").strip() or "未分类"
        by_ind.setdefault(ind, []).append(r)

    out: List[Dict[str, Any]] = []
    for ind, lst in by_ind.items():
        buyable = [r for r in lst if int(r.get("hit_buyable") or 0)]
        if len(buyable) < min_samples:
            continue
        hit = sum(1 for r in buyable if _row_is_hit(r))
        pcts = [
            float(r["t1_open_close_pct"]) if r.get("t1_open_close_pct") is not None
            else float(r["t1_pct"])
            for r in buyable
            if (r.get("t1_open_close_pct") is not None or r.get("t1_pct") is not None)
        ]
        avg_pct = (sum(pcts) / len(pcts)) if pcts else 0.0
        out.append({
            "industry": ind,
            "total": len(lst),
            "buyable": len(buyable),
            "hit": hit,
            "rate": (hit / len(buyable) * 100.0) if buyable else 0.0,
            "avg_pct": avg_pct,
        })
    out.sort(key=lambda x: (-x["rate"], -x["buyable"]))
    return out


# 失败归因标签
FAILURE_REASONS: List[str] = [
    "冲高回落", "低开低走", "弱势震荡", "大跌/跌停", "其他",
]


def classify_failure(row: Dict[str, Any]) -> Optional[str]:
    """根据 T+1 OHLC 给未命中的可买入候选打失败标签。

    命中 / 一字 / 停牌 → 返回 None（不参与归因）。
    """
    if not int(row.get("hit_buyable") or 0):
        return None
    if _row_is_hit(row):
        return None
    t_close = row.get("t_close")
    t1_open = row.get("t1_open")
    t1_high = row.get("t1_high")
    t1_close = row.get("t1_close")
    t1_pct = row.get("t1_pct")
    if t1_pct is None or t1_open is None or t1_close is None or t_close is None:
        return "其他"

    # 1) 大跌：跌幅 ≥ 5%
    if t1_pct <= -5:
        return "大跌/跌停"

    # 2) 冲高回落：盘中冲高 ≥ +3% 但收盘 ≤ 昨收
    if t1_high is not None and t_close > 0:
        intraday_high_pct = (t1_high - t_close) / t_close * 100.0
        if intraday_high_pct >= 3 and t1_close <= t_close:
            return "冲高回落"

    # 3) 低开低走：低开 ≥ 1% 且 收 ≤ 开
    if t_close > 0:
        open_pct = (t1_open - t_close) / t_close * 100.0
        if open_pct <= -1 and t1_close <= t1_open:
            return "低开低走"

    # 4) 弱势震荡：涨跌幅在 [-2%, +2%] 之间
    if -2 <= t1_pct <= 2:
        return "弱势震荡"

    return "其他"


def query_failure_reasons(
    category: Optional[str] = None,
    lookback_dates: int = 20,
) -> Dict[str, Any]:
    """统计未命中候选的失败模式分布。

    返回 {total_miss, by_reason: [{reason, count, ratio, avg_pct}], by_industry_top: [...]}。
    """
    rows = _load_recent_rows(category, lookback_dates)
    miss_rows: List[Tuple[str, Dict[str, Any]]] = []
    for r in rows:
        reason = classify_failure(r)
        if reason is None:
            continue
        miss_rows.append((reason, r))

    total_miss = len(miss_rows)
    by_reason_count: Dict[str, List[float]] = {}
    by_reason_ind: Dict[str, Dict[str, int]] = {}
    for reason, r in miss_rows:
        # 失败 bucket 的 avg_pct 使用"开盘买、收盘卖"口径（实盘可达盈亏）
        if r.get("t1_open_close_pct") is not None:
            pct_val = float(r["t1_open_close_pct"])
        elif r.get("t1_pct") is not None:
            pct_val = float(r["t1_pct"])
        else:
            pct_val = 0.0
        by_reason_count.setdefault(reason, []).append(pct_val)
        ind = (r.get("industry") or "").strip() or "未分类"
        by_reason_ind.setdefault(reason, {}).setdefault(ind, 0)
        by_reason_ind[reason][ind] += 1

    out_reasons: List[Dict[str, Any]] = []
    for reason in FAILURE_REASONS:
        pcts = by_reason_count.get(reason, [])
        cnt = len(pcts)
        if cnt == 0:
            out_reasons.append({
                "reason": reason, "count": 0, "ratio": 0.0,
                "avg_pct": 0.0, "top_industries": [],
            })
            continue
        avg_pct = sum(pcts) / cnt
        ind_dict = by_reason_ind.get(reason, {})
        top_inds = sorted(ind_dict.items(), key=lambda x: -x[1])[:3]
        out_reasons.append({
            "reason": reason,
            "count": cnt,
            "ratio": (cnt / total_miss * 100.0) if total_miss else 0.0,
            "avg_pct": avg_pct,
            "top_industries": [{"industry": k, "count": v} for k, v in top_inds],
        })

    return {
        "total_miss": total_miss,
        "by_reason": out_reasons,
    }
