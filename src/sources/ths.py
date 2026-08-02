"""同花顺 (THS / 10jqka) 数据源。

- 历史日线：``https://d.10jqka.com.cn/v6/line/{ths_code}/01/{year}.js``（JSONP，按年请求后合并）。
- 个股资金流兜底：``http://data.10jqka.com.cn/funds/ggzjl/`` 榜单，按 code 降序二分翻页定位目标股票。
"""
from __future__ import annotations

import json as _json
import random
import re
import threading
import time
from io import StringIO
from typing import Dict, Optional

import akshare as ak
import pandas as pd
import requests

from src.network.headers import USER_AGENT_POOL
from src.network.host_health import (
    cooldown_remaining,
    mark_failed,
    mark_ok,
    on_cooldown,
)
from src.sources._common import normalize_history_frame

try:
    from akshare.datasets import get_ths_js
    import py_mini_racer

    _THS_HAS_VCODE = True
except Exception:  # pragma: no cover - 缺依赖时退化为无 cookie 访问（仅第 1 页可用）
    get_ths_js = None
    py_mini_racer = None
    _THS_HAS_VCODE = False


_REQUEST_LOCK = threading.Lock()
_NEXT_REQUEST_AT = 0.0
_MIN_INTERVAL = 0.6


def throttle() -> None:
    global _NEXT_REQUEST_AT
    while True:
        with _REQUEST_LOCK:
            now = time.time()
            wait = _NEXT_REQUEST_AT - now
            if wait <= 0:
                _NEXT_REQUEST_AT = now + _MIN_INTERVAL + random.uniform(0.1, 0.4)
                return
        time.sleep(min(wait, 0.5))


def stock_code(code: str) -> str:
    """同花顺用 hs_000001 格式。"""
    c = str(code).strip().zfill(6)
    return f"hs_{c}"


# ---- 个股资金流兜底：按 code 降序二分翻页 ----
# 同花顺榜单没有「按任意 code 查个股资金流」的接口，只有全市场榜单。akshare 的
# stock_fund_flow_individual 用 field/code 算总页数、却用 field/zdf 翻页，排序错位会
# 漏掉 000938 这类股票。这里统一用 field/code 排序，二分定位目标股票所在页。
_THS_BOARD_BASE = "http://data.10jqka.com.cn/funds/ggzjl/field/code/order/desc"
_THS_BOARD_REFERER = "http://data.10jqka.com.cn/funds/hyzjl/"
_THS_VCODE_CACHE: Dict[str, float] = {"v": None, "ts": 0.0}


def _ths_vcode() -> Optional[str]:
    """同花顺反爬 cookie（hexin-v），带 60s 缓存；失败返回 None（第 1 页仍可无 cookie 访问）。"""
    if not _THS_HAS_VCODE:
        return None
    now = time.time()
    if _THS_VCODE_CACHE["v"] is not None and now - _THS_VCODE_CACHE["ts"] < 60.0:
        return _THS_VCODE_CACHE["v"]
    try:
        js = open(get_ths_js("ths.js"), encoding="utf-8").read()
        jr = py_mini_racer.MiniRacer()
        jr.eval(js)
        v = jr.call("v")
        _THS_VCODE_CACHE["v"] = v
        _THS_VCODE_CACHE["ts"] = now
        return v
    except Exception:
        return None


def _ths_board_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENT_POOL),
        "Referer": _THS_BOARD_REFERER,
        "X-Requested-With": "XMLHttpRequest",
    }


def _ths_page_frame(page: int, max_retry: int = 3) -> Optional[pd.DataFrame]:
    """抓取按 code 降序的某一页个股资金流榜单。

    返回 DataFrame（股票代码已补满 6 位）；连续重试仍失败/空表返回 None。
    偶发 401/403 多为 hexin-v 失效，会清空 vcode 缓存并在重试时重算。
    """
    for attempt in range(max_retry):
        try:
            h = _ths_board_headers()
            vc = _ths_vcode()
            if vc:
                h["hexin-v"] = vc
            url = (
                f"{_THS_BOARD_BASE}/page/{page}/ajax/1/free/1/"
                if page > 1
                else f"{_THS_BOARD_BASE}/ajax/1/free/1/"
            )
            resp = requests.get(url, headers=h, timeout=(5, 12), verify=False)
            if resp.status_code != 200:
                if resp.status_code in (401, 403):
                    _THS_VCODE_CACHE["v"] = None  # 可能是 vcode 失效，下次重算
                time.sleep(0.3 + 0.2 * attempt + random.uniform(0.0, 0.3))
                continue
            try:
                d = pd.read_html(StringIO(resp.text))[0]
            except Exception:
                time.sleep(0.3)
                continue
            # pd.read_html 会把 000938 的前导零吞成 938，需从原始 HTML 取真实 6 位代码
            codes = re.findall(r'class="stockCode">(\d{6})<', resp.text)
            if codes and len(codes) == len(d):
                d["股票代码"] = codes
            if "股票代码" not in d.columns or d.empty:
                time.sleep(0.3)
                continue
            return d
        except Exception:
            time.sleep(0.3)
    return None


def _ths_total_pages() -> int:
    try:
        resp = requests.get(
            f"{_THS_BOARD_BASE}/ajax/1/free/1/",
            headers=_ths_board_headers(),
            timeout=(5, 12),
            verify=False,
        )
        m = re.search(r"(\d+)/(\d+)", resp.text)
        if m:
            return max(1, int(m.group(2)))
    except Exception:
        pass
    return 104


def _ths_locate_code_row(code: str) -> pd.DataFrame:
    """二分定位目标股票所在页（榜单按 code 降序），返回该股票单行；找不到返回空 DF。"""
    total = _ths_total_pages()
    lo, hi, cand = 1, total, None
    for _ in range(30):
        mid = (lo + hi) // 2
        d = _ths_page_frame(mid)
        if d is None:
            lo = mid + 1
            continue
        c = d["股票代码"].astype(str).str.extract(r"(\d{6})", expand=False).dropna()
        if c.empty:
            lo = mid + 1
            continue
        first, last = c.iloc[0], c.iloc[-1]
        if code > first:
            hi = mid - 1
        elif code < last:
            lo = mid + 1
        else:
            cand = mid
            break
    if cand is None:
        return pd.DataFrame()
    # 候选页及相邻页各取一次，避免边界/空页导致漏判
    for p in (cand - 2, cand - 1, cand, cand + 1, cand + 2):
        if 1 <= p <= total:
            d = _ths_page_frame(p)
            if d is None:
                continue
            d = d.copy()
            d["股票代码"] = d["股票代码"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("")
            row = d[d["股票代码"] == code]
            if not row.empty:
                return row.reset_index(drop=True)
    return pd.DataFrame()


def fetch_fund_flow_frame(stock_code_in: str) -> "pd.DataFrame":
    """同花顺个股资金流补位。

    用「按 code 降序二分翻页」定位目标股票（修复 akshare 用 zdf 翻页导致 000938 等被漏掉的
    bug）。同花顺榜单仅含流入/流出/净额，无东财那样的主力/大单拆分；这里把「净额」映射到
    主力净额/大单净额以便界面有值（启动时已对用户提示该兜底仅含净额）。
    """
    code = str(stock_code_in or "").strip().zfill(6)
    if not code:
        return pd.DataFrame()

    try:
        row = _ths_locate_code_row(code)
    except Exception:
        row = pd.DataFrame()
    # 首次未命中（多为 vcode 偶发失效）→ 清空 vcode 缓存后重试一次
    if row is None or row.empty:
        try:
            _THS_VCODE_CACHE["v"] = None
            row = _ths_locate_code_row(code)
        except Exception:
            row = pd.DataFrame()
    if row is None or row.empty:
        return pd.DataFrame()

    out = row.copy()
    today_text = pd.Timestamp.now().strftime("%Y-%m-%d")
    if "日期" not in out.columns and "交易日" not in out.columns and "date" not in out.columns:
        out["日期"] = today_text

    # 同花顺榜单列名带「(元)」后缀：净额(元)/流入资金(元)/...
    net_col = None
    for cand in ("净额(元)", "净额"):
        if cand in out.columns:
            net_col = cand
            break
    if net_col is not None:
        if "大单净额" not in out.columns:
            out["大单净额"] = out[net_col]
        if "主力净额" not in out.columns:
            out["主力净额"] = out[net_col]
    if "最新价" in out.columns and "收盘价" not in out.columns:
        out["收盘价"] = out["最新价"]

    return out.reset_index(drop=True)


def fetch_hist_frame(stock_code_in: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """同花顺 CDN 历史日线：JSONP 格式，按年请求后合并筛选。"""
    import requests

    host = "d.10jqka.com.cn"
    if on_cooldown(host):
        remain = cooldown_remaining(host)
        raise RuntimeError(f"ths host on cooldown ({int(remain)}s remaining)")

    ths_code = stock_code(stock_code_in)
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    years = list(range(start_year, end_year + 1))

    all_rows = []
    last_error = None
    for year in years:
        for attempt in range(2):
            try:
                throttle()
                url = f"https://{host}/v6/line/{ths_code}/01/{year}.js"
                resp = requests.get(
                    url,
                    timeout=(5, 12),
                    verify=False,
                    headers={
                        "User-Agent": random.choice(USER_AGENT_POOL),
                        "Referer": "https://www.10jqka.com.cn/",
                    },
                )
                if resp.status_code != 200:
                    last_error = RuntimeError(f"ths HTTP {resp.status_code}")
                    time.sleep(0.5 + random.uniform(0.3, 0.8))
                    continue

                text = resp.text
                lp = text.find("(")
                rp = text.rfind(")")
                if lp < 0 or rp <= lp:
                    last_error = RuntimeError("ths: invalid JSONP response")
                    continue
                data = _json.loads(text[lp + 1 : rp])
                raw = data.get("data", "")
                if not raw:
                    last_error = RuntimeError("ths: empty data field")
                    continue

                # 格式: date,open,high,low,close,volume,amount,turnover_rate,,,flag;...
                for line in raw.split(";"):
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(",")
                    if len(parts) < 7:
                        continue
                    all_rows.append({
                        "date": parts[0],
                        "open": parts[1],
                        "high": parts[2],
                        "low": parts[3],
                        "close": parts[4],
                        "volume": parts[5],
                        "amount": parts[6],
                        "turnover_rate": parts[7] if len(parts) > 7 and parts[7] else None,
                    })
                break
            except Exception as e:
                last_error = e
                time.sleep(1.0 + random.uniform(0.3, 0.8))

    if not all_rows:
        mark_failed(host)
        if last_error is not None:
            raise last_error
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    for col in ("open", "close", "high", "low", "volume", "amount", "turnover_rate"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce").dt.date.astype(str)
    df = df.dropna(subset=["date", "close"])
    df = df[df["close"] > 0]

    sd = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
    ed = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
    df = df[(df["date"] >= sd) & (df["date"] <= ed)]

    mark_ok(host)
    return normalize_history_frame(df)
