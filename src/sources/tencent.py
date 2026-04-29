"""腾讯证券历史 K 线源。

主路径用自建直连（带镜像轮换 + UA 随机化），失败回退到 akshare 的 ``stock_zh_a_hist_tx``。
"""
from __future__ import annotations

import random
import re
import time
from typing import Callable, List, Optional

import pandas as pd

from src.network.headers import USER_AGENT_POOL
from src.network.host_health import filter_healthy_urls, mark_failed, mark_ok
from src.sources._common import market_prefixed_code, normalize_history_frame


HISTORY_MIRRORS = [
    "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get",
    "https://web.ifzqgtimg.cn/appstock/app/fqkline/get",
]


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
    big_df.columns = ["date", "open", "close", "high", "low", "amount"]
    for col in ["open", "close", "high", "low", "amount"]:
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
