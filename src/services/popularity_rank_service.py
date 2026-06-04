"""Eastmoney stock popularity rank helpers.

The public Eastmoney rank list only exposes the current market-wide Top 100.
For historical replay we use the per-stock rank history endpoint and look up
the selected trade date after candidates have already been generated.
"""
from __future__ import annotations

import logging
import re
from concurrent.futures import as_completed
from functools import lru_cache
from typing import Any, Callable, Dict, Iterable, List, Optional

import requests

from src.utils.daemon_executor import DaemonThreadPoolExecutor

logger = logging.getLogger(__name__)

EASTMONEY_STOCK_RANK_HISTORY_URL = "https://emappdata.eastmoney.com/stockrank/getHisList"
EASTMONEY_STOCK_RANK_SOURCE = "eastmoney_stockrank_history"


def normalize_trade_date(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def eastmoney_stock_rank_symbol(code: Any) -> str:
    c = re.sub(r"\D", "", str(code or "")).zfill(6)[-6:]
    if len(c) != 6:
        return ""
    if c.startswith(("6", "5", "9")):
        return f"SH{c}"
    if c.startswith(("0", "2", "3")):
        return f"SZ{c}"
    if c.startswith(("4", "8")):
        return f"BJ{c}"
    return ""


def _rank_bonus(rank: Optional[int]) -> int:
    if rank is None or rank <= 0:
        return 0
    if rank <= 20:
        return 8
    if rank <= 50:
        return 6
    if rank <= 100:
        return 4
    if rank <= 200:
        return 2
    return 0


def _row_value(row: Any, keys: Iterable[str], index: int) -> Any:
    if isinstance(row, dict):
        for key in keys:
            if key in row:
                return row.get(key)
        return None
    if isinstance(row, (list, tuple)) and len(row) > index:
        return row[index]
    return None


def _parse_rank_rows(data: Any, symbol: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(data, list):
        return rows
    for item in data:
        date_raw = _row_value(item, ("时间", "date", "time", "calcTime", "rankDate", "日期"), 0)
        rank_raw = _row_value(item, ("排名", "rank", "rk", "ranking", "hotRank"), 1)
        trade_date = normalize_trade_date(date_raw)
        try:
            rank = int(float(str(rank_raw).strip()))
        except (TypeError, ValueError):
            continue
        if not trade_date or rank <= 0:
            continue
        rows.append({
            "trade_date": trade_date,
            "rank": rank,
            "symbol": symbol,
            "source": EASTMONEY_STOCK_RANK_SOURCE,
        })
    return rows


@lru_cache(maxsize=4096)
def fetch_eastmoney_rank_history(symbol: str) -> List[Dict[str, Any]]:
    payload = {
        "appId": "appId01",
        "globalId": "786e4c21-70dc-435a-93bb-38",
        "marketType": "",
        "srcSecurityCode": symbol,
        "yearType": "5",
    }
    resp = requests.post(EASTMONEY_STOCK_RANK_HISTORY_URL, json=payload, timeout=(3, 8))
    resp.raise_for_status()
    data_json = resp.json()
    return _parse_rank_rows(data_json.get("data"), symbol)


def get_stock_popularity_rank(
    code: Any,
    trade_date: Any,
    *,
    fetch_history_func: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
) -> Optional[Dict[str, Any]]:
    target = normalize_trade_date(trade_date)
    symbol = eastmoney_stock_rank_symbol(code)
    if not target or not symbol:
        return None

    fetcher = fetch_history_func or fetch_eastmoney_rank_history
    rows = fetcher(symbol)
    if not rows:
        return None
    for row in rows:
        if normalize_trade_date(row.get("trade_date")) == target:
            rank = row.get("rank")
            try:
                rank_int = int(rank)
            except (TypeError, ValueError):
                return None
            return {
                "trade_date": target,
                "rank": rank_int,
                "symbol": symbol,
                "source": row.get("source") or EASTMONEY_STOCK_RANK_SOURCE,
            }
    return None


def enrich_wrap_candidates_with_popularity(
    candidates: List[Dict[str, Any]],
    trade_date: Any,
    *,
    fetch_history_func: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    max_workers: int = 4,
) -> Dict[str, int]:
    """Mutate wrap candidates with historical per-stock popularity rank."""
    if not candidates:
        return {"total": 0, "hit": 0, "missing": 0, "bonus": 0}

    target = normalize_trade_date(trade_date)
    if not target:
        return {"total": len(candidates), "hit": 0, "missing": len(candidates), "bonus": 0}

    total = len(candidates)
    workers = max(1, min(int(max_workers or 1), total))
    hit = 0
    bonus_count = 0

    def _fetch_one(rec: Dict[str, Any]) -> tuple[str, Optional[Dict[str, Any]]]:
        code = str(rec.get("code") or "").strip().zfill(6)
        try:
            return code, get_stock_popularity_rank(
                code, target, fetch_history_func=fetch_history_func,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("人气历史排名获取失败 %s %s: %s", target, code, exc)
            return code, None

    by_code = {str(rec.get("code") or "").strip().zfill(6): rec for rec in candidates}
    with DaemonThreadPoolExecutor(max_workers=workers, thread_name_prefix="pop-rank") as executor:
        futures = [executor.submit(_fetch_one, rec) for rec in candidates]
        for fut in as_completed(futures):
            code, rank_info = fut.result()
            rec = by_code.get(code)
            if rec is None:
                continue
            if not rank_info:
                rec.setdefault("popularity_rank", None)
                rec.setdefault("popularity_bonus", 0)
                continue

            rank = int(rank_info["rank"])
            bonus = _rank_bonus(rank)
            rec["popularity_rank"] = rank
            rec["popularity_trade_date"] = target
            rec["popularity_source"] = rank_info.get("source") or EASTMONEY_STOCK_RANK_SOURCE
            rec["popularity_bonus"] = bonus
            hit += 1
            if bonus > 0:
                old_score = int(rec.get("score") or 0)
                rec["score"] = max(0, min(100, old_score + bonus))
                reason = f"人气{rank}名+{bonus}"
                existing = str(rec.get("reasons") or "").strip()
                rec["reasons"] = f"{existing} / {reason}" if existing else reason
                bonus_count += 1

    candidates.sort(
        key=lambda item: (
            -int(item.get("score") or 0),
            int(item.get("popularity_rank") or 999999),
            str(item.get("code") or ""),
        )
    )
    stats = {
        "total": total,
        "hit": hit,
        "missing": max(total - hit, 0),
        "bonus": bonus_count,
    }
    if log_fn:
        log_fn(
            f"涨停预测：反包候选人气排名补齐 {stats['hit']}/{stats['total']}，"
            f"加分 {stats['bonus']} 只"
        )
    return stats
