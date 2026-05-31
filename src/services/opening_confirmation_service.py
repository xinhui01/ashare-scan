"""Opening auction buy-point confirmation for existing prediction candidates."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd


_CATEGORY_LABELS = {
    "cont": "保留涨停",
    "first": "二波接力",
    "fresh": "首板涨停",
    "wrap": "反包",
}
_STATUS_ORDER = {"可买": 0, "观察": 1, "放弃": 2, "风险过高": 3}


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if pd.notna(out) else None


def _limit_up_threshold_pct(code: str) -> float:
    c = str(code or "").strip().zfill(6)
    if c.startswith(("300", "301", "688")):
        return 20.0
    if c.startswith(("8", "9")):
        return 30.0
    return 10.0


def _status_rank(status: str) -> int:
    return _STATUS_ORDER.get(str(status or ""), 9)


def _iter_candidates(candidate_lists: Dict[str, List[Dict[str, Any]]]) -> Iterable[Tuple[str, Dict[str, Any]]]:
    for category, records in (candidate_lists or {}).items():
        for rec in records or []:
            if isinstance(rec, dict):
                yield str(category or ""), rec


def _first_open_price(payload: Any) -> Optional[float]:
    if isinstance(payload, dict):
        df = payload.get("intraday")
    else:
        df = payload
    if df is None or getattr(df, "empty", True):
        return None
    if "time" in df.columns:
        work = df.copy()
        work["time"] = pd.to_datetime(work["time"], errors="coerce")
        work = work.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
        if not work.empty:
            hhmm = work["time"].dt.strftime("%H:%M")
            after_open = work.loc[hhmm >= "09:30"]
            if not after_open.empty:
                row = after_open.iloc[0]
                return _safe_float(row.get("open")) or _safe_float(row.get("close"))
    row = df.iloc[0]
    return _safe_float(row.get("open")) or _safe_float(row.get("close"))


def _category_gap_window(category: str, rec: Dict[str, Any], limit_pct: float) -> Tuple[float, float, float]:
    """Return (min_buy_gap, max_buy_gap, high_risk_gap)."""
    if category == "fresh":
        return 0.3, min(5.5, limit_pct - 2.0), limit_pct - 1.0
    if category == "wrap":
        return 0.0, min(5.8, limit_pct - 2.0), limit_pct - 1.0
    if category == "first":
        return -0.3, min(5.0, limit_pct - 2.5), limit_pct - 1.2
    try:
        boards = int(rec.get("consecutive_boards") or rec.get("boards") or 1)
    except (TypeError, ValueError):
        boards = 1
    if boards >= 3:
        return 1.0, min(4.8, limit_pct - 2.8), limit_pct - 1.5
    return 0.0, min(5.2, limit_pct - 2.2), limit_pct - 1.2


def evaluate_candidate_opening(
    rec: Dict[str, Any],
    *,
    category: str,
    auction: Optional[Dict[str, Any]] = None,
    open_price: Optional[float] = None,
) -> Dict[str, Any]:
    code = str(rec.get("code") or "").strip().zfill(6)
    prev_close = _safe_float(rec.get("close"))
    score = _safe_float(rec.get("calibrated_score")) or _safe_float(rec.get("score")) or 0.0
    limit_pct = _limit_up_threshold_pct(code)

    auction_price = _safe_float((auction or {}).get("price")) if auction else None
    auction_amount = _safe_float((auction or {}).get("amount")) if auction else None
    auction_gap = None
    if prev_close and prev_close > 0 and auction_price:
        auction_gap = (auction_price / prev_close - 1.0) * 100.0
    open_gap = None
    if prev_close and prev_close > 0 and open_price:
        open_gap = (open_price / prev_close - 1.0) * 100.0

    status = "观察"
    reasons: List[str] = []
    if auction_gap is None:
        reasons.append("缺竞价价格")
        return {
            "status": status,
            "label": status,
            "category": _CATEGORY_LABELS.get(category, category),
            "auction_price": auction_price,
            "auction_gap_pct": None,
            "auction_amount": auction_amount,
            "open_price": open_price,
            "open_gap_pct": open_gap,
            "score": int(round(score)),
            "reason": " / ".join(reasons),
        }

    min_gap, max_gap, high_risk_gap = _category_gap_window(category, rec, limit_pct)
    if auction_gap >= high_risk_gap:
        status = "风险过高"
        reasons.append(f"接近涨停/一字风险 {auction_gap:.1f}%")
    elif auction_gap <= -3.0:
        status = "放弃"
        reasons.append(f"低开过多 {auction_gap:.1f}%")
    elif auction_gap < min_gap:
        status = "观察"
        reasons.append(f"竞价偏弱 {auction_gap:.1f}%")
    elif auction_gap > max_gap:
        status = "观察"
        reasons.append(f"高开偏多 {auction_gap:.1f}%")
    else:
        amount_ok = auction_amount is None or auction_amount >= 10_000_000
        if score >= 60 and amount_ok:
            status = "可买"
            reasons.append(f"竞价强度匹配 {auction_gap:.1f}%")
        elif score < 60:
            status = "观察"
            reasons.append(f"分数不足 {int(round(score))}")
        else:
            status = "观察"
            reasons.append("竞价成交额偏小")

    if auction_amount is not None:
        if auction_amount >= 50_000_000:
            reasons.append(f"竞价额{auction_amount / 100_000_000:.2f}亿")
        elif auction_amount < 5_000_000 and status == "可买":
            status = "观察"
            reasons.append("竞价额不足")

    if open_gap is not None:
        if status == "可买" and open_gap <= min(auction_gap - 1.5, -1.0):
            status = "观察"
            reasons.append(f"开盘转弱 {open_gap:.1f}%")
        elif status in ("观察", "可买") and open_gap <= -3.0:
            status = "放弃"
            reasons.append(f"开盘低于预期 {open_gap:.1f}%")
        else:
            reasons.append(f"开盘{open_gap:+.1f}%")

    return {
        "status": status,
        "label": status,
        "category": _CATEGORY_LABELS.get(category, category),
        "auction_price": auction_price,
        "auction_gap_pct": auction_gap,
        "auction_amount": auction_amount,
        "open_price": open_price,
        "open_gap_pct": open_gap,
        "score": int(round(score)),
        "reason": " / ".join(reasons),
    }


def _should_fetch_intraday(now: Optional[datetime]) -> bool:
    current = now or datetime.now()
    return (current.hour, current.minute) >= (9, 30)


def confirm_candidate_lists(
    candidate_lists: Dict[str, List[Dict[str, Any]]],
    *,
    fetcher: Any,
    now: Optional[datetime] = None,
    max_workers: int = 2,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    grouped: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
    for category, rec in _iter_candidates(candidate_lists):
        code = str(rec.get("code") or "").strip().zfill(6)
        if code:
            grouped.setdefault(code, []).append((category, rec))

    codes = list(grouped.keys())
    total = len(codes)
    if not codes:
        return {
            "total": 0,
            "confirmed": 0,
            "status_counts": {},
            "generated_at": (now or datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
        }

    fetch_intraday = _should_fetch_intraday(now)
    snapshots: Dict[str, Tuple[Optional[Dict[str, Any]], Optional[float]]] = {}

    def _fetch(code: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[float]]:
        auction = None
        open_price = None
        try:
            auction = fetcher.get_auction_snapshot(code)
        except Exception as exc:  # noqa: BLE001
            if log_fn:
                log_fn(f"竞价确认 {code} 竞价获取失败: {exc}")
        if fetch_intraday:
            try:
                payload = fetcher.get_intraday_data(code, include_meta=True)
                open_price = _first_open_price(payload)
            except Exception as exc:  # noqa: BLE001
                if log_fn:
                    log_fn(f"竞价确认 {code} 开盘分时获取失败: {exc}")
        return code, auction, open_price

    done = 0
    workers = max(1, min(int(max_workers or 1), 4))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="opening-confirm") as executor:
        futures = [executor.submit(_fetch, code) for code in codes]
        for fut in as_completed(futures):
            code, auction, open_price = fut.result()
            snapshots[code] = (auction, open_price)
            done += 1
            if progress_callback:
                progress_callback(done, total, code)

    status_counts: Dict[str, int] = {}
    for code, entries in grouped.items():
        auction, open_price = snapshots.get(code, (None, None))
        for category, rec in entries:
            confirmation = evaluate_candidate_opening(
                rec,
                category=category,
                auction=auction,
                open_price=open_price,
            )
            rec["opening_confirmation"] = confirmation
            status = confirmation.get("status", "观察")
            status_counts[status] = status_counts.get(status, 0) + 1

    for _category, records in (candidate_lists or {}).items():
        records.sort(
            key=lambda rec: (
                _status_rank(((rec.get("opening_confirmation") or {}).get("status"))),
                -int((rec.get("opening_confirmation") or {}).get("score") or rec.get("score") or 0),
            )
        )

    return {
        "total": total,
        "confirmed": sum(status_counts.values()),
        "status_counts": status_counts,
        "generated_at": (now or datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
        "fetched_intraday": fetch_intraday,
    }
