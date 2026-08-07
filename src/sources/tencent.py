"""腾讯证券历史 K 线源 + 实时竞价快照兜底。

历史 K 线主路径用自建直连（带镜像轮换 + UA 随机化），失败回退到 akshare 的
``stock_zh_a_hist_tx``。竞价快照走 ``qt.gtimg.cn`` 实时行情，用于东财 push2
集群被网络出口拦截时（盘前 09:25 窗口）的兜底。
"""
from __future__ import annotations

import random
import re
import time
from datetime import datetime
from datetime import time as dtime
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.network.headers import USER_AGENT_POOL
from src.network.host_health import filter_healthy_urls, mark_failed, mark_ok
from src.sources._common import market_prefixed_code, normalize_history_frame


HISTORY_MIRRORS = [
    "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get",
    "https://web.ifzqgtimg.cn/appstock/app/fqkline/get",
]

_REALTIME_QUOTE_URL = "https://qt.gtimg.cn/q="

# qt.gtimg.cn 实时快照的 ~ 分隔字段位（A 股）
_QT_FIELD_PRICE = 3
_QT_FIELD_OPEN = 5
_QT_FIELD_VOLUME_HAND = 6
_QT_FIELD_TIME = 30
_QT_FIELD_HIGH = 33
_QT_FIELD_LOW = 34
_QT_FIELD_AMOUNT_WAN = 37


def _quote_float(fields: List[str], index: int) -> Optional[float]:
    try:
        value = float(str(fields[index]).strip())
    except (IndexError, TypeError, ValueError):
        return None
    return value if value > 0 else None


def _parse_quote_time(raw: str) -> Optional[datetime]:
    digits = re.sub(r"\D", "", str(raw or ""))
    if len(digits) < 12:
        return None
    digits = digits[:14].ljust(14, "0")
    try:
        return datetime.strptime(digits, "%Y%m%d%H%M%S")
    except ValueError:
        return None


def fetch_auction_snapshot(
    stock_code: str,
    logger: Optional[Callable[[str], None]] = None,
) -> Optional[Dict[str, Any]]:
    """从腾讯实时行情推导 09:25 集合竞价快照（东财被拦时的兜底）。

    09:25 撮合完成到 09:30 连续竞价开始前，qt.gtimg.cn 的"当前价"就是
    竞价撮合价、累计成交额就是竞价成交额。只有行情自身时间戳落在
    [09:25, 09:30) 窗口内才返回快照；盘中/收盘后当前价已不是竞价价，
    返回 None 让上层继续回退（新浪分时）。
    """
    import requests
    from stock_data import _use_bypass_proxy, _use_insecure_ssl

    code = str(stock_code or "").strip()
    if not code:
        return None
    code = code.zfill(6)
    req_kw = {
        "url": _REALTIME_QUOTE_URL + market_prefixed_code(code),
        "timeout": (5, 10),
        "headers": {
            "User-Agent": random.choice(USER_AGENT_POOL),
            "Referer": "https://gu.qq.com/",
        },
    }
    if _use_insecure_ssl():
        req_kw["verify"] = False
    with requests.Session() as session:
        if _use_bypass_proxy():
            session.trust_env = False
        resp = session.get(**req_kw)

    if resp.status_code != 200:
        if logger:
            logger(f"竞价行情(腾讯) {code} 被拒绝 (HTTP {resp.status_code})，跳过腾讯")
        return None
    text = resp.content.decode("gbk", errors="replace")
    start = text.find('"')
    end = text.rfind('"')
    if start < 0 or end <= start:
        if logger:
            logger(f"竞价行情(腾讯) {code} 返回非预期格式，跳过腾讯")
        return None
    fields = text[start + 1 : end].split("~")
    if len(fields) <= _QT_FIELD_AMOUNT_WAN:
        if logger:
            logger(f"竞价行情(腾讯) {code} 字段不足({len(fields)})，跳过腾讯")
        return None

    price = _quote_float(fields, _QT_FIELD_PRICE)
    ts = _parse_quote_time(fields[_QT_FIELD_TIME])
    if price is None or ts is None:
        return None
    if not (dtime(9, 25) <= ts.time() < dtime(9, 30)):
        if logger:
            logger(f"竞价行情(腾讯) {code} 快照时间 {ts:%H:%M:%S} 不在竞价窗口，忽略")
        return None

    amount_wan = _quote_float(fields, _QT_FIELD_AMOUNT_WAN)
    return {
        "trade_date": ts.date().isoformat(),
        "time": pd.Timestamp(ts),
        "price": price,
        "open": _quote_float(fields, _QT_FIELD_OPEN),
        "high": _quote_float(fields, _QT_FIELD_HIGH),
        "low": _quote_float(fields, _QT_FIELD_LOW),
        "avg_price": None,
        "volume": _quote_float(fields, _QT_FIELD_VOLUME_HAND),
        "amount": amount_wan * 10000.0 if amount_wan is not None else None,
        "source": "tencent",
        "code": code,
    }


def _get_healthy_mirrors() -> List[str]:
    healthy = filter_healthy_urls(HISTORY_MIRRORS)
    return healthy if healthy else list(HISTORY_MIRRORS)


def fetch_hist_direct(
    stock_code: str,
    start_date: str,
    end_date: str,
    log: Optional[Callable[[str], None]] = None,
) -> "pd.DataFrame":
    """直接抓腾讯证券历史日线，带镜像轮换和 UA 随机化。"""
    import requests
    from akshare.utils import demjson

    symbol = market_prefixed_code(stock_code)
    range_start = max(int(start_date[:4]), 2000)
    range_end = int(end_date[:4]) + 1

    mirrors = _get_healthy_mirrors()
    big_df = pd.DataFrame()

    for year in range(range_start, range_end):
        params = {
            "_var": f"kline_day{year}",
            "param": f"{symbol},day,{year}-01-01,{year}-12-31,640,",
            "r": f"0.{random.randint(1000000000, 9999999999)}",
        }
        last_error = None
        for mirror_url in mirrors:
            try:
                time.sleep(random.uniform(0.1, 0.4))
                resp = requests.get(
                    mirror_url,
                    params=params,
                    timeout=(5, 10),
                    headers={
                        "User-Agent": random.choice(USER_AGENT_POOL),
                        "Referer": "https://gu.qq.com/",
                    },
                )
                if resp.status_code != 200:
                    mark_failed(mirror_url)
                    last_error = RuntimeError(f"tencent HTTP {resp.status_code}")
                    continue
                data_text = resp.text
                idx = data_text.find("={")
                if idx < 0:
                    mark_failed(mirror_url)
                    last_error = RuntimeError("tencent: bad response format")
                    continue
                data_json = demjson.decode(data_text[idx + 1:])["data"][symbol]
                if "day" in data_json:
                    temp_df = pd.DataFrame(data_json["day"])
                else:
                    temp_df = pd.DataFrame()
                if not temp_df.empty:
                    big_df = pd.concat([big_df, temp_df], ignore_index=True)
                mark_ok(mirror_url)
                break
            except Exception as e:
                last_error = e
                mark_failed(mirror_url)
                if log:
                    host = re.sub(r"^https?://", "", mirror_url).split("/", 1)[0]
                    log(f"腾讯 {stock_code} 镜像 {host} 年份 {year} 失败: {e}")

    if big_df.empty:
        return pd.DataFrame()

    big_df = big_df.iloc[:, :6]
    big_df.columns = ["date", "open", "close", "high", "low", "volume"]
    for col in ["open", "close", "high", "low", "volume"]:
        big_df[col] = pd.to_numeric(big_df[col], errors="coerce")
    big_df["date"] = pd.to_datetime(big_df["date"], errors="coerce").dt.date.astype(str)
    big_df = big_df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    return normalize_history_frame(big_df)


def fetch_hist_frame(stock_code: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """腾讯历史日线：优先自建直连，失败回退 akshare。"""
    import akshare as ak
    # 延迟导入 stock_data 内的 _retry_ak_call，避免循环 import。
    from stock_data import _retry_ak_call

    try:
        df = fetch_hist_direct(stock_code, start_date, end_date)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    symbol = market_prefixed_code(stock_code)
    df = _retry_ak_call(
        ak.stock_zh_a_hist_tx,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        adjust="",
    )
    return normalize_history_frame(df)
