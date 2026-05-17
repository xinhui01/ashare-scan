"""涨停题材聚类。

把当日涨停股的"涨停原因"喂给大模型，归集成若干题材，
比"行业"字段信息密度更高（同一题材常跨多个行业）。

结果按 trade_date 缓存到 SQLite app_config，键名为 limit_up_themes_<date>。
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from llm_client import (
    DEFAULT_MODEL,
    LlmConfigError,
    LlmRequestError,
    NvidiaNimClient,
)
from stock_logger import get_logger
from stock_store import load_app_config, save_app_config

logger = get_logger(__name__)

CACHE_KEY_PREFIX = "limit_up_themes_"
MAX_STOCKS_PER_CALL = 80


def _cache_key(trade_date: str) -> str:
    return f"{CACHE_KEY_PREFIX}{str(trade_date).strip()}"


def load_cached_themes(trade_date: str) -> Optional[Dict[str, Any]]:
    """读取已缓存的题材聚类结果，未命中返回 None。"""
    payload = load_app_config(_cache_key(trade_date), default=None)
    if isinstance(payload, dict) and payload.get("themes") is not None:
        return payload
    return None


def _build_prompt(stocks: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    lines: List[str] = []
    for s in stocks[:MAX_STOCKS_PER_CALL]:
        code = str(s.get("code") or "").strip().zfill(6)
        name = str(s.get("name") or "").strip()
        industry = str(s.get("industry") or "").strip()
        reason = str(s.get("reason") or "").strip().replace("\n", " ")
        boards = s.get("consecutive_boards") or s.get("boards") or 1
        if not code:
            continue
        lines.append(
            f"{code}|{name}|{industry}|{boards}板|{reason or '-'}"
        )
    sample_block = "\n".join(lines)

    system_msg = (
        "你是 A 股涨停板研究员。任务：把当日涨停股按真实题材分组（不要直接用行业字段，"
        "要优先参考'涨停原因'里的题材/事件词），输出严格 JSON。"
    )
    user_msg = (
        "下面是当日涨停股清单，每行格式为 `代码|名称|行业|连板数|涨停原因`：\n\n"
        f"{sample_block}\n\n"
        "请输出 JSON，结构如下，不要任何额外文字、不要 markdown 代码块：\n"
        "{\n"
        '  "themes": [\n'
        '    {"name": "题材名(不超过8字)", "codes": ["600000", ...], '
        '"core_concept": "题材核心驱动(20字内)", "leaders": ["龙头代码", ...]}\n'
        "  ],\n"
        '  "market_summary": "今日涨停整体逻辑(80字内)"\n'
        "}\n\n"
        "要求：\n"
        "1. 每个题材至少 2 只股票，单只票若题材独立可放入 '其他' 题材。\n"
        "2. 题材名要具体（如'PCB'、'核电'、'机器人减速器'），避免笼统的'科技股'。\n"
        "3. 同一只股票只能归入一个题材。\n"
        "4. 龙头取连板最高 / 涨停理由最贴合题材的 1~3 只。\n"
        "5. 严格输出可被 json.loads 直接解析的 JSON。"
    )
    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    """从 LLM 输出里抽出第一段合法 JSON 对象。

    模型偶尔会包 markdown 代码块或前后加解释文字，做一次容错。
    """
    if not text:
        return None
    # 直接尝试
    try:
        return json.loads(text)
    except ValueError:
        pass
    # 去 markdown fence
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fenced:
        try:
            return json.loads(fenced.group(1))
        except ValueError:
            pass
    # 取第一个 { ... } 块
    brace = re.search(r"\{.*\}", text, re.DOTALL)
    if brace:
        try:
            return json.loads(brace.group(0))
        except ValueError:
            return None
    return None


def cluster_themes(
    stocks: List[Dict[str, Any]],
    *,
    trade_date: str,
    model: str = DEFAULT_MODEL,
    api_key: Optional[str] = None,
    use_cache: bool = True,
) -> Dict[str, Any]:
    """对涨停股做题材聚类。

    Args:
        stocks: 每只股票包含 code/name/industry/reason(入选理由)/consecutive_boards
        trade_date: 用于缓存键
        model: NIM 模型 slug，可在调用方覆盖
        api_key: 显式 API key，None 时回落到 env / app_config
        use_cache: True 时若已有缓存直接返回

    Returns:
        {
          "trade_date": ...,
          "model": ...,
          "themes": [{"name", "codes", "core_concept", "leaders"}],
          "market_summary": ...,
          "saved_at": ...,
          "input_count": ...,
        }

    抛 LlmConfigError / LlmRequestError 由调用方处理。
    """
    td = str(trade_date or "").strip()
    if not td:
        raise ValueError("trade_date 不能为空")

    if use_cache:
        cached = load_cached_themes(td)
        if cached is not None:
            return cached

    if not stocks:
        return {
            "trade_date": td,
            "model": model,
            "themes": [],
            "market_summary": "今日无涨停股",
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "input_count": 0,
        }

    messages = _build_prompt(stocks)
    client = NvidiaNimClient(api_key=api_key)
    raw_text = client.chat(
        messages=messages,
        model=model,
        temperature=0.2,
        max_tokens=2000,
        response_format={"type": "json_object"},
    )

    parsed = _extract_json(raw_text)
    if parsed is None:
        raise LlmRequestError(f"LLM 未返回合法 JSON: {raw_text[:300]}")

    themes_raw = parsed.get("themes") or []
    cleaned: List[Dict[str, Any]] = []
    for t in themes_raw:
        if not isinstance(t, dict):
            continue
        name = str(t.get("name") or "").strip()
        codes = [
            str(c).strip().zfill(6)
            for c in (t.get("codes") or [])
            if str(c).strip()
        ]
        if not name or not codes:
            continue
        cleaned.append({
            "name": name,
            "codes": codes,
            "core_concept": str(t.get("core_concept") or "").strip(),
            "leaders": [
                str(c).strip().zfill(6)
                for c in (t.get("leaders") or [])
                if str(c).strip()
            ],
        })

    result = {
        "trade_date": td,
        "model": model,
        "themes": cleaned,
        "market_summary": str(parsed.get("market_summary") or "").strip(),
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "input_count": len(stocks),
    }
    try:
        save_app_config(_cache_key(td), result)
    except Exception:
        logger.exception("缓存涨停题材聚类失败")
    return result
