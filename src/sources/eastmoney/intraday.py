"""东方财富 trends2 接口：盘前竞价快照 + 当日 1 分钟分时图。

akshare 的 ``stock_zh_a_hist_min_em`` 在接口字段数变动时会抛 "Length mismatch"，
本模块直连 trends2 并按实际列数截断列名，规避该脆弱点。
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from src.network.headers import random_eastmoney_headers
from src.sources._common import first_existing_column


def fetch_auction_snapshot(
    stock_code: str,
    logger: Optional[Callable[[str], None]] = None,
) -> Optional[Dict[str, Any]]:
    """抓取东方财富盘前竞价快照（09:25 集合竞价撮合）。"""
    # 延迟导入避免与 stock_data 循环
    from stock_data import _use_bypass_proxy, _use_insecure_ssl

    market_code = 1 if str(stock_code).startswith("6") else 0
    url = "https://push2.eastmoney.com/api/qt/stock/trends2/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ndays": "1",
        "iscr": "1",
        "iscca": "1",
        "secid": f"{market_code}.{stock_code}",
    }
    import requests
    try:
        req_kw = {
            "url": url,
            "params": params,
            "timeout": 8.0,
            "headers": random_eastmoney_headers(),
        }
        if _use_insecure_ssl():
            req_kw["verify"] = False
        with requests.Session() as session:
            if _use_bypass_proxy():
                session.trust_env = False
            r = session.get(**req_kw)
            r.raise_for_status()
            data_json = r.json()
    except Exception as exc:
        if logger:
            logger(f"竞价数据网络请求失败 {stock_code}: {exc}")
        return None
    if not data_json or not data_json.get("data"):
        return None

    trends = (data_json.get("data") or {}).get("trends") or []
    if not trends:
        return None
    temp_df = pd.DataFrame([str(item).split(",") for item in trends])
    if temp_df.empty:
        return None

    available_cols = ["时间", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "均价"]
    temp_df.columns = available_cols[:len(temp_df.columns)]
    if "时间" not in temp_df.columns:
        return None

    temp_df["时间"] = pd.to_datetime(temp_df["时间"], errors="coerce")
    temp_df = temp_df.dropna(subset=["时间"]).sort_values("时间").reset_index(drop=True)
    if temp_df.empty:
        return None

    numeric_cols = [c for c in temp_df.columns if c != "时间"]
    for col in numeric_cols:
        temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")

    auction_rows = temp_df[temp_df["时间"].dt.strftime("%H:%M") == "09:25"].reset_index(drop=True)
    if auction_rows.empty:
        return None

    row = auction_rows.iloc[-1]
    price_candidates = [
        row.get("收盘"),
        row.get("开盘"),
        row.get("均价"),
        row.get("最高"),
        row.get("最低"),
    ]
    auction_price = next(
        (
            float(value)
            for value in price_candidates
            if pd.notna(value) and float(value) > 0
        ),
        None,
    )
    if auction_price is None:
        return None

    amount = row.get("成交额")
    volume = row.get("成交量")
    avg_price = row.get("均价")
    return {
        "trade_date": row["时间"].date().isoformat(),
        "time": row["时间"],
        "price": auction_price,
        "open": float(row["开盘"]) if pd.notna(row.get("开盘")) else None,
        "high": float(row["最高"]) if pd.notna(row.get("最高")) else None,
        "low": float(row["最低"]) if pd.notna(row.get("最低")) else None,
        "avg_price": float(avg_price) if pd.notna(avg_price) and float(avg_price) > 0 else None,
        "volume": float(volume) if pd.notna(volume) and float(volume) > 0 else None,
        "amount": float(amount) if pd.notna(amount) and float(amount) > 0 else None,
    }


def fetch_intraday_1min(
    stock_code: str,
    ndays: int = 5,
    logger: Optional[Callable[[str], None]] = None,
) -> "pd.DataFrame":
    """直连东方财富 trends2 获取 1 分钟分时，容忍可变字段数。"""
    # 延迟导入：_ashare_request_with_retry 依赖 akshare patch + 节流，仍在 stock_data 中
    from stock_data import _ashare_request_with_retry

    market_code = 1 if str(stock_code).startswith("6") else 0
    url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "ndays": str(max(1, int(ndays or 1))),
        "iscr": "0",
        "secid": f"{market_code}.{stock_code}",
    }
    try:
        response = _ashare_request_with_retry(url, params=params, timeout=15)
        data_json = response.json()
    except Exception as exc:
        if logger:
            logger(f"分时行情(东财直连) {stock_code} 请求失败: {exc}")
        raise

    trends = (data_json.get("data") or {}).get("trends") or []
    if not trends:
        if logger:
            logger(f"分时行情(东财直连) {stock_code} 无 trends 数据")
        return pd.DataFrame()

    rows = [str(item).split(",") for item in trends]
    temp_df = pd.DataFrame(rows)
    if temp_df.empty:
        return pd.DataFrame()

    canonical_cols = ["时间", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "均价"]
    actual_col_count = len(temp_df.columns)
    if actual_col_count >= len(canonical_cols):
        temp_df = temp_df.iloc[:, : len(canonical_cols)]
        temp_df.columns = canonical_cols
    else:
        temp_df.columns = canonical_cols[:actual_col_count]
        for col in canonical_cols[actual_col_count:]:
            temp_df[col] = pd.NA

    if "时间" not in temp_df.columns:
        if logger:
            logger(
                f"分时行情(东财直连) {stock_code} 缺少时间列，实际 {actual_col_count} 列"
            )
        return pd.DataFrame()

    for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "均价"]:
        if col in temp_df.columns:
            temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")
    temp_df["时间"] = pd.to_datetime(temp_df["时间"], errors="coerce").astype(str)
    return temp_df


def empty_meta_payload(
    selected_trade_date: str = "",
    available_trade_dates: Optional[List[str]] = None,
    applied_day_offset: int = 0,
    auction_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "intraday": None,
        "selected_trade_date": str(selected_trade_date or "").strip(),
        "available_trade_dates": [str(item).strip() for item in (available_trade_dates or []) if str(item).strip()],
        "applied_day_offset": int(applied_day_offset),
        "auction": auction_snapshot if isinstance(auction_snapshot, dict) else None,
    }


def normalize_source_frame(
    raw_frame: "pd.DataFrame",
    stock_code: str,
    logger: Optional[Callable[[str], None]] = None,
) -> "pd.DataFrame":
    if raw_frame is None or getattr(raw_frame, "empty", True):
        return pd.DataFrame()

    source_columns = [str(col) for col in raw_frame.columns.tolist()]
    rename_map: Dict[str, str] = {}
    time_col = first_existing_column(source_columns, ["时间", "日期时间", "datetime", "time", "day"])
    open_col = first_existing_column(source_columns, ["开盘", "open"])
    close_col = first_existing_column(source_columns, ["收盘", "close", "最新价"])
    high_col = first_existing_column(source_columns, ["最高", "high"])
    low_col = first_existing_column(source_columns, ["最低", "low"])
    volume_col = first_existing_column(source_columns, ["成交量", "volume"])
    amount_col = first_existing_column(source_columns, ["成交额", "amount"])
    avg_price_col = first_existing_column(source_columns, ["均价", "avg_price"])

    for src, dst in [
        (time_col, "time"),
        (open_col, "open"),
        (close_col, "close"),
        (high_col, "high"),
        (low_col, "low"),
        (volume_col, "volume"),
        (amount_col, "amount"),
        (avg_price_col, "avg_price"),
    ]:
        if src:
            rename_map[src] = dst

    normalized = raw_frame.rename(columns=rename_map).copy()
    if "time" not in normalized.columns:
        if logger:
            logger(f"分时行情 {stock_code} 缺少时间列，返回列: {', '.join(source_columns)}")
        return pd.DataFrame()

    normalized["time"] = pd.to_datetime(normalized["time"], errors="coerce")
    normalized = normalized.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    if normalized.empty:
        return pd.DataFrame()

    for col in ["open", "close", "high", "low", "volume", "amount", "avg_price"]:
        if col in normalized.columns:
            normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
        else:
            normalized[col] = None

    # ---- 过滤竞价时段数据，避免与竞价标记重叠 ----
    # 东方财富分时接口可能返回 09:25~09:29 的竞价撮合数据，
    # 这些数据会和独立获取的竞价快照在图表上产生时间重叠。
    # 正式连续竞价从 09:30 开始，午盘从 13:00 开始；
    # 仅保留 [09:30, 11:30] ∪ [13:00, 15:00] 的有效交易分钟。
    hhmm = normalized["time"].dt.strftime("%H:%M")
    in_morning = (hhmm >= "09:30") & (hhmm <= "11:30")
    in_afternoon = (hhmm >= "13:00") & (hhmm <= "15:00")
    before_filter_len = len(normalized)
    normalized = normalized[in_morning | in_afternoon].reset_index(drop=True)
    if normalized.empty:
        return pd.DataFrame()
    filtered_count = before_filter_len - len(normalized)
    if filtered_count > 0 and logger:
        logger(f"分时行情 {stock_code} 过滤 {filtered_count} 条非交易时段数据（竞价/午休）")

    return normalized[["time", "open", "close", "high", "low", "volume", "amount", "avg_price"]]


def resolve_trade_dates(df: "pd.DataFrame") -> List[str]:
    if df is None or df.empty or "time" not in df.columns:
        return []
    return sorted({d.isoformat() for d in df["time"].dt.date.dropna().tolist()})


def select_trade_date(
    trade_dates: List[str],
    day_offset: int = 0,
    target_trade_date: str = "",
) -> Tuple[str, int]:
    if not trade_dates:
        return "", 0

    normalized_target = str(target_trade_date or "").strip()
    if normalized_target:
        selected_trade_date = ""
        if normalized_target in trade_dates:
            selected_trade_date = normalized_target
        else:
            for candidate in reversed(trade_dates):
                if candidate <= normalized_target:
                    selected_trade_date = candidate
                    break
            if not selected_trade_date:
                selected_trade_date = trade_dates[0]
        selected_index = trade_dates.index(selected_trade_date)
        return selected_trade_date, selected_index - (len(trade_dates) - 1)

    try:
        request_offset = int(day_offset)
    except (TypeError, ValueError):
        request_offset = 0
    max_back = len(trade_dates) - 1
    applied_offset = max(-max_back, min(request_offset, 0))
    selected_index = len(trade_dates) - 1 + applied_offset
    return trade_dates[selected_index], applied_offset


def slice_frame_by_trade_date(df: "pd.DataFrame", selected_trade_date: str) -> "pd.DataFrame":
    if df is None or df.empty or "time" not in df.columns or not selected_trade_date:
        return pd.DataFrame()
    target_date = pd.to_datetime(selected_trade_date, errors="coerce").date()
    return df[df["time"].dt.date == target_date].reset_index(drop=True)
