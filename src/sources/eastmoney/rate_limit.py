"""东方财富限流信号检测：基于状态码 + 文本/JSON 关键字判定。

正常 200 响应也可能是限流页面（带"访问过于频繁"等字样），所以单看 status_code 不够。
"""
from __future__ import annotations

import re
from typing import Any, List


_RATE_LIMIT_TOKENS = (
    "访问过于频繁",
    "访问频繁",
    "请求过于频繁",
    "频繁",
    "forbidden",
    "access denied",
    "too many requests",
    "rate limit",
    "风控",
    "验证码",
)


def looks_like_rate_limit(status_code: int, response_text: str) -> bool:
    if int(status_code or 0) in (403, 418, 429, 451, 503):
        return True
    sample = str(response_text or "")[:400].lower()
    if not sample:
        return False
    return any(token in sample for token in _RATE_LIMIT_TOKENS)


def json_indicates_rate_limit(data_json: Any) -> bool:
    if not isinstance(data_json, dict):
        return False
    texts: List[str] = []
    for key in ("message", "msg", "rc", "rt", "code", "result", "reason"):
        value = data_json.get(key)
        if value is not None:
            texts.append(str(value))
    data_part = data_json.get("data")
    if isinstance(data_part, dict):
        for key in ("message", "msg", "tip", "reason"):
            value = data_part.get(key)
            if value is not None:
                texts.append(str(value))
    return any(looks_like_rate_limit(200, text) for text in texts)


def mirror_host_of(url: str) -> str:
    """从 URL 中提取 host（保留原大小写，与 host_health._normalize_host 区别在于不转小写）。"""
    text = re.sub(r"^https?://", "", str(url or "").strip())
    return text.split("/", 1)[0]
