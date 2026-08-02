"""东方财富个股资金流（自建请求，规避服务端风控 reset）。

背景：akshare 的 ``stock_individual_fund_flow`` 只带一个老版本 Chrome UA，无 Referer / 真实
Cookie，东财服务端经常直接 reset 该连接（``RemoteDisconnected``）。本项目改用：

1. 真实会话 Cookie 预取（先访问东财站点，由服务端下发 ``Set-Cookie``，再用同一个 Session 带真实
   Cookie 请求资金流接口）——比 ``headers.random_eastmoney_cookie`` 的伪造 Cookie 更可能被接受；
2. 完整浏览器请求头（UA / Referer / Accept / sec-ch-ua 等，来自 ``random_eastmoney_headers``）；
3. 多 host 兜底：``push2his`` → ``push2`` → ``push2delay``；
4. 全部直连（``trust_env=False``），不受本机代理/环境干扰。

解析结果严格对齐 akshare ``stock_individual_fund_flow`` 的中文列，下游 ``get_fund_flow_data``
的列映射无需改动。
"""
from __future__ import annotations

import time
from typing import List, Optional

import pandas as pd
import requests

# 多 host 兜底顺序（push2his 为 akshare 原 host，push2 / push2delay 同接口不同入口）
_EM_FUND_FLOW_HOSTS: List[str] = [
    "push2his.eastmoney.com",
    "push2.eastmoney.com",
    "push2delay.eastmoney.com",
]
_EM_FUND_FLOW_PATH = "/api/qt/stock/fflow/daykline/get"

# 与 akshare.stock_individual_fund_flow 完全一致的 15 列（f51..f65 顺序）
_EM_FUND_FLOW_COLUMNS = [
    "日期", "主力净流入-净额", "小单净流入-净额", "中单净流入-净额", "大单净流入-净额",
    "超大单净流入-净额", "主力净流入-净占比", "小单净流入-净占比", "中单净流入-净占比",
    "大单净流入-净占比", "超大单净流入-净占比", "收盘价", "涨跌幅", "-", "-",
]
# 下游 get_fund_flow_data 实际消费的 13 列
_EM_FUND_FLOW_KEEP = [
    "日期", "收盘价", "涨跌幅", "主力净流入-净额", "主力净流入-净占比",
    "超大单净流入-净额", "超大单净流入-净占比", "大单净流入-净额", "大单净流入-净占比",
    "中单净流入-净额", "中单净流入-净占比", "小单净流入-净额", "小单净流入-净占比",
]


def _build_headers() -> dict:
    """完整浏览器头，但移除手动 Cookie，交由 Session 用真实会话 Cookie 管理。"""
    from src.network.headers import random_eastmoney_headers

    h = random_eastmoney_headers()
    h.pop("Cookie", None)  # 用真实会话 Cookie，避免伪造值遮蔽服务端下发的 Set-Cookie
    return h


def _parse_klines_to_df(klines: List[str]) -> pd.DataFrame:
    """把东财返回的 klines（逗号分隔字符串列表）解析为对齐 akshare 的中文列 DataFrame。"""
    df = pd.DataFrame([item.split(",") for item in klines])
    if df.shape[1] != len(_EM_FUND_FLOW_COLUMNS):
        raise ValueError(
            f"东财资金流字段数异常: {df.shape[1]}，期望 {len(_EM_FUND_FLOW_COLUMNS)}"
        )
    df.columns = _EM_FUND_FLOW_COLUMNS
    df = df[_EM_FUND_FLOW_KEEP].copy()
    for col in df.columns:
        if col != "日期":
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.date
    return df


def _request_one_host(session: requests.Session, host: str, params: dict,
                      headers: dict, timeout) -> pd.DataFrame:
    resp = session.get(
        f"https://{host}{_EM_FUND_FLOW_PATH}",
        params=params,
        headers=headers,
        timeout=timeout,
    )
    if resp.status_code != 200 or not resp.text.strip().startswith("{"):
        raise RuntimeError(f"{host} HTTP {resp.status_code}")
    data_json = resp.json()
    klines = (data_json.get("data") or {}).get("klines")
    if not klines:
        raise RuntimeError(f"{host} 返回空 klines（可能限流/无数据）")
    return _parse_klines_to_df(klines)


def fetch_individual_fund_flow(stock: str, market: str = "sh",
                               timeout=(5, 12)) -> pd.DataFrame:
    """自建东财个股资金流请求。

    :param stock: 6 位股票代码
    :param market: sh / sz / bj
    :return: 对齐 akshare 的中文列 DataFrame（含主力/超大单/大单/中单/小单净额与净占比）
    :raises Exception: 所有 host 都失败时抛出最后一个错误
    """
    market_map = {"sh": "1", "sz": "0", "bj": "0"}
    secid = f"{market_map.get(market, '0')}.{stock}"
    params = {
        "lmt": "0",
        "klt": "101",
        "secid": secid,
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "_": int(time.time() * 1000),
    }
    headers = _build_headers()
    last_err: Optional[Exception] = None
    for host in _EM_FUND_FLOW_HOSTS:
        session = requests.Session()
        # 全部直连，忽略环境代理（避免 Clash/公司代理对 eastmoney 断开）
        session.trust_env = False
        session.proxies = {"http": None, "https": None}
        try:
            # 1) 预取真实会话 Cookie：访问东财站点，服务端下发 Set-Cookie
            try:
                session.get(
                    "https://quote.eastmoney.com/",
                    headers=headers,
                    timeout=(5, 10),
                )
            except Exception:
                pass  # 预取失败不致命，仍尝试带已有头请求
            # 2) 带真实 Cookie + 完整头请求资金流
            return _request_one_host(session, host, params, headers, timeout)
        except Exception as e:  # 该 host 失败，尝试下一个
            last_err = e
            continue
    raise last_err or RuntimeError("eastmoney 资金流所有 host 均失败")


def _em_fund_flow_reachable(timeout: float = 8.0) -> bool:
    """启动探针：用真实会话 Cookie + 多 host 探测东财资金流接口是否可达。

    与 ``fetch_individual_fund_flow`` 共用同一套健壮请求逻辑，避免探针用裸请求误判东财"死"。
    """
    params = {
        "lmt": "1",
        "klt": "101",
        "secid": "1.600036",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "_": int(time.time() * 1000),
    }
    headers = _build_headers()
    for host in _EM_FUND_FLOW_HOSTS:
        session = requests.Session()
        session.trust_env = False
        session.proxies = {"http": None, "https": None}
        try:
            try:
                session.get("https://quote.eastmoney.com/", headers=headers, timeout=(5, 10))
            except Exception:
                pass
            df = _request_one_host(session, host, params, headers, (5, timeout))
            if df is not None and not df.empty:
                return True
        except Exception:
            continue
    return False
