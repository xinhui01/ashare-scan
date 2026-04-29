"""HTTP 请求头/cookie 随机化。

东方财富等接口常校验 Referer / User-Agent / Cookie，缺省时易被直接断开连接或限流。
本模块提供 UA / Referer 池和东方财富专用的请求头生成器，所有数据源共用。
"""
from __future__ import annotations

import random
import time
from typing import Dict


USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]

REFERER_POOL = [
    "https://quote.eastmoney.com/",
    "https://data.eastmoney.com/",
    "https://guba.eastmoney.com/",
    "https://so.eastmoney.com/",
    "https://www.eastmoney.com/",
    "https://finance.eastmoney.com/",
]


def random_eastmoney_cookie() -> str:
    """生成伪随机的东方财富 cookie，模拟正常浏览器访问痕迹。"""
    qgqp = "".join(random.choices("0123456789abcdef", k=32))
    em_hq = "js"
    st_pvi = str(random.randint(10000000000, 99999999999))
    st_si = f"{int(time.time() * 1000)}-{random.randint(100000, 999999)}"
    parts = [
        f"qgqp_b_id={qgqp}",
        f"em_hq_fls={em_hq}",
        f"st_pvi={st_pvi}",
        f"st_si={st_si}",
    ]
    if random.random() < 0.6:
        parts.append(f"HAList=a-sz-{random.randint(1, 300):06d}")
    return "; ".join(parts)


def random_eastmoney_headers() -> Dict[str, str]:
    """每次请求生成随机化的请求头，模拟真实浏览器行为。"""
    accept_encodings = [
        "gzip, deflate, br",
        "gzip, deflate",
        "gzip, deflate, br, zstd",
    ]
    sec_ch_ua_pool = [
        '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        '"Chromium";v="124", "Not(A:Brand";v="24", "Google Chrome";v="124"',
        '"Chromium";v="126", "Not(A:Brand";v="8", "Google Chrome";v="126"',
        '"Not)A;Brand";v="99", "Microsoft Edge";v="122", "Chromium";v="122"',
    ]
    headers = {
        "User-Agent": random.choice(USER_AGENT_POOL),
        "Referer": random.choice(REFERER_POOL),
        "Accept": random.choice([
            "application/json, text/plain, */*",
            "*/*",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        ]),
        "Accept-Language": random.choice([
            "zh-CN,zh;q=0.9",
            "zh-CN,zh;q=0.9,en;q=0.8",
            "zh-CN,zh;q=0.8,en;q=0.6",
            "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        ]),
        "Accept-Encoding": random.choice(accept_encodings),
        "Connection": random.choice(["keep-alive", "close"]),
        "sec-ch-ua": random.choice(sec_ch_ua_pool),
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": random.choice(['"Windows"', '"macOS"']),
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }
    if random.random() < 0.5:
        headers["DNT"] = "1"
    if random.random() < 0.3:
        headers["Pragma"] = "no-cache"
        headers["Cache-Control"] = "no-cache"
    headers["Cookie"] = random_eastmoney_cookie()
    return headers
