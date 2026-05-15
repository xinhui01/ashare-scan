"""AI 每日博弈短报。

把当日规则系统的产出（涨停预测 5 类候选 + 概念炒作主线/萌芽题材）
压缩成结构化 prompt，喂给 NVIDIA NIM，让模型生成 200~300 字的"明日博弈短报"。

定位：辅助叙事，不替代规则决策。规则评分仍是底层真相，AI 只负责把
多维度信号融合成可读的语言化总结，并标注 3-5 个"高确信"重叠候选。

按 (trade_date, payload_hash) 缓存到 app_config，同输入不重复调。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from llm_client import (
    DEFAULT_MODEL,
    LlmConfigError,
    LlmRequestError,
    NvidiaNimClient,
    has_api_key,
)
from stock_logger import get_logger
from stock_store import load_app_config, save_app_config

logger = get_logger(__name__)

CACHE_KEY_PREFIX = "daily_brief_"
TOP_CANDIDATES_PER_CATEGORY = 5
TOP_CONCEPTS = 6
TOP_FRESH = 5

CATEGORY_LABELS: Dict[str, str] = {
    "continuation_candidates": "保留涨停",
    "first_board_candidates": "二波接力",
    "fresh_first_board_candidates": "首板涨停",
    "broken_board_wrap_candidates": "反包/承接",
    "trend_limit_up_candidates": "趋势涨停",
}


# ============== 输入裁剪 ==============

def _trim_predict_candidates(
    predict_result: Optional[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """从 predict_result 摘出每类 top N 候选，只保留打分关键字段。"""
    out: Dict[str, List[Dict[str, Any]]] = {}
    if not predict_result:
        return out
    for cat_key, label in CATEGORY_LABELS.items():
        rows = predict_result.get(cat_key) or []
        if not rows:
            continue
        # rows 已经按分数排序（service 端排好的）
        top = sorted(rows, key=lambda r: -int(r.get("score") or 0))[
            :TOP_CANDIDATES_PER_CATEGORY
        ]
        slim = []
        for r in top:
            slim.append({
                "code": str(r.get("code") or "").strip().zfill(6),
                "name": str(r.get("name") or "").strip(),
                "industry": str(r.get("industry") or "").strip(),
                "boards": int(r.get("consecutive_boards") or 0),
                "change_pct": round(float(r.get("change_pct") or 0.0), 2),
                "score": int(r.get("score") or 0),
                "reasons": str(r.get("reasons") or "")[:120],
            })
        out[label] = slim
    return out


def _trim_news(
    news_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """从 news_feed 结果摘 早餐 + top 电报。"""
    if not news_result:
        return {}
    return {
        "morning_briefing": news_result.get("morning_briefing") or {},
        "telegrams": [
            {"title": t.get("title", ""), "time": t.get("time", "")}
            for t in (news_result.get("telegrams") or [])[:10]
        ],
    }


def _trim_sentiment(
    sentiment_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """从 sentiment 结果摘 score / 仓位 / 7 个 signal。"""
    if not sentiment_result:
        return {}
    return {
        "score": sentiment_result.get("score", 50),
        "position_suggest": sentiment_result.get("position_suggest") or {},
        "summary": sentiment_result.get("summary", ""),
        "signals": [
            {
                "name": s.get("name", ""),
                "value": s.get("value", ""),
                "delta": int(s.get("delta", 0)),
                "note": s.get("note", ""),
            }
            for s in (sentiment_result.get("signals") or [])
        ],
    }


def _trim_concept_hype(
    hype_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """从 concept_hype 结果摘出 top 题材 + 主线 + 萌芽 + 龙头。"""
    if not hype_result:
        return {}
    concepts = hype_result.get("concepts") or []
    top_concepts = []
    for c in concepts[:TOP_CONCEPTS]:
        leaders = [
            {
                "code": m.get("code", ""),
                "name": m.get("name", ""),
                "boards": int(m.get("boards") or 1),
            }
            for m in (c.get("leaders") or [])[:3]
        ]
        top_concepts.append({
            "name": c.get("name", ""),
            "source": c.get("source", ""),
            "phase": c.get("phase", ""),
            "trend": c.get("trend", ""),
            "today_count": int(c.get("today_count") or 0),
            "duration": int(c.get("duration") or 0),
            "score": int(c.get("opportunity_score") or 0),
            "leaders": leaders,
        })
    return {
        "main_line": (hype_result.get("main_line") or {}).get("summary", ""),
        "window": {
            "start": hype_result.get("start_date", ""),
            "end": hype_result.get("end_date", ""),
            "days": len(hype_result.get("trade_dates") or []),
        },
        "stats": hype_result.get("stats") or {},
        "top_concepts": top_concepts,
        "fresh": (hype_result.get("stats") or {}).get("fresh_concepts", [])[
            :TOP_FRESH
        ],
    }


# ============== 缓存键 ==============

def _payload_hash(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:12]


def _cache_key(trade_date: str, payload_hash: str) -> str:
    return f"{CACHE_KEY_PREFIX}{trade_date}_{payload_hash}"


def load_cached_brief(
    trade_date: str, payload_hash: str,
) -> Optional[Dict[str, Any]]:
    return load_app_config(_cache_key(trade_date, payload_hash), default=None)


# ============== Prompt 组装 ==============

def _build_prompt(
    trade_date: str,
    candidates: Dict[str, List[Dict[str, Any]]],
    hype: Dict[str, Any],
    sentiment: Optional[Dict[str, Any]] = None,
    news: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    sys_msg = (
        "你是 A 股短线操盘手助手。任务：基于当日新闻 + 市场情绪 + 涨停候选 + "
        "概念炒作主线，给一份明日博弈短报。"
        "要求：客观、克制、不空喊口号；如果当日新闻里有政策/事件能解释主线题材"
        "或预示明日方向，请明确指出关联；优先标注多维度重叠的高确信票；"
        "明确给出风险点。仓位建议必须严格参考'市场情绪'板块给出的仓位标签，"
        "不可随意上调。不要复读输入数据，要做语言化综合判断。"
    )

    blocks: List[str] = [f"# 基准交易日：{trade_date}", ""]

    # 当日新闻（让 LLM 有"时事感"）
    if news:
        morning = news.get("morning_briefing") or {}
        telegrams = news.get("telegrams") or []
        if morning or telegrams:
            blocks.append("## 当日新闻 / 政策")
            if morning:
                blocks.append(f"早餐摘要 ({morning.get('time', '')})：")
                blocks.append(f"  {morning.get('summary', '')}")
            if telegrams:
                blocks.append("重点电报：")
                for t in telegrams[:10]:
                    blocks.append(f"  - [{t.get('time', '')}] {t.get('title', '')}")
            blocks.append("")

    # 市场情绪（首要上下文，决定仓位基调）
    if sentiment:
        adv = sentiment.get("position_suggest") or {}
        blocks.append("## 市场情绪 / 仓位建议（核心上下文）")
        blocks.append(
            f"综合评分 {sentiment.get('score', 50)}/100 → 建议 {adv.get('label', '-')}"
        )
        blocks.append(f"摘要：{sentiment.get('summary', '')}")
        blocks.append("各维度信号：")
        for s in sentiment.get("signals") or []:
            d = int(s.get("delta", 0))
            sign = "+" if d > 0 else ""
            blocks.append(
                f"  - {s.get('name', '')}: {s.get('value', '')} "
                f"({sign}{d}) — {s.get('note', '')}"
            )
        blocks.append("")

    # 概念炒作板块
    if hype:
        win = hype.get("window") or {}
        stats = hype.get("stats") or {}
        blocks.append("## 概念炒作 / 主线")
        blocks.append(
            f"窗口 {win.get('start', '')} ~ {win.get('end', '')}"
            f"（{win.get('days', 0)} 个交易日）；"
            f"识别题材 {stats.get('total_concepts', 0)}，"
            f"今日活跃 {stats.get('active_concepts', 0)}，"
            f"累计涨停 {stats.get('total_limit_ups', 0)} 次。"
        )
        if hype.get("main_line"):
            blocks.append(f"主线：{hype['main_line']}")
        if hype.get("top_concepts"):
            blocks.append("Top 题材（按机会分降序）：")
            for c in hype["top_concepts"]:
                leader_str = " / ".join(
                    f"{m['name']}({m['boards']}板)" for m in c["leaders"]
                )
                blocks.append(
                    f"  - 【{c['name']}/{c['source']}】机会分 {c['score']}, "
                    f"{c['phase']}/{c['trend']}, 今 {c['today_count']} 只, "
                    f"持续 {c['duration']}d, 龙头：{leader_str}"
                )
        if hype.get("fresh"):
            blocks.append("萌芽题材（≤2 天起爆）：")
            for f in hype["fresh"]:
                blocks.append(
                    f"  - {f['name']} ({f['source']}) 起爆 {f['ignite_date']}, "
                    f"今 {f['today_count']} 只"
                )
        blocks.append("")

    # 涨停预测候选
    if candidates:
        blocks.append("## 涨停预测候选（每类 top）")
        for label, rows in candidates.items():
            if not rows:
                continue
            blocks.append(f"### {label}")
            for r in rows:
                board_part = f", {r['boards']}板" if r["boards"] else ""
                blocks.append(
                    f"  - {r['code']} {r['name']}({r['industry']}{board_part})"
                    f" 分 {r['score']} | {r['reasons']}"
                )
        blocks.append("")

    if not candidates and not hype:
        blocks.append("（无可用数据 — 用户尚未运行涨停预测和概念炒作分析）")

    blocks.append("## 输出要求")
    blocks.append(
        "请用 280 字以内的精炼短报回答，结构如下：\n"
        "1. **盘面综合**（一段话）：今日情绪定调（参考仓位建议）+ 涨停整体特征 + 主线分布\n"
        "2. **仓位建议**（一行）：明确写出『明日建议 X 仓』，必须严格采用『市场情绪』板块给出的仓位标签\n"
        "3. **明日重点**（3-5 个候选）：每个一行，格式 `代码 名称(行业) — 一句逻辑`，"
        "优先选多维度重叠 + 主线/萌芽题材内的票\n"
        "4. **风险点**（一句话）：哪些信号要警惕（如末期题材集中、龙头炸板、跌停数飙升等）\n\n"
        "不要写 JSON、不要 markdown 代码块、直接输出叙述文本。"
    )

    return [
        {"role": "system", "content": sys_msg},
        {"role": "user", "content": "\n".join(blocks)},
    ]


# ============== 对外主入口 ==============

def generate_daily_brief(
    trade_date: str,
    *,
    predict_result: Optional[Dict[str, Any]] = None,
    hype_result: Optional[Dict[str, Any]] = None,
    sentiment_result: Optional[Dict[str, Any]] = None,
    news_result: Optional[Dict[str, Any]] = None,
    model: str = DEFAULT_MODEL,
    api_key: Optional[str] = None,
    use_cache: bool = True,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """生成 AI 博弈短报。

    Returns:
        {
          "trade_date": ..., "model": ..., "brief": "...",
          "from_cache": bool, "generated_at": ..., "payload_hash": ...,
        }

    抛 LlmConfigError / LlmRequestError 由调用方处理。
    """
    def _l(msg: str) -> None:
        if log is not None:
            try:
                log(msg)
            except Exception:
                pass
        logger.info(msg)

    td = str(trade_date or "").strip().replace("-", "")
    if not td:
        td = datetime.now().strftime("%Y%m%d")

    candidates = _trim_predict_candidates(predict_result)
    hype = _trim_concept_hype(hype_result)
    sentiment_slim = _trim_sentiment(sentiment_result)
    news_slim = _trim_news(news_result)
    payload = {
        "candidates": candidates, "hype": hype,
        "sentiment": sentiment_slim, "news": news_slim,
    }
    h = _payload_hash(payload)

    if use_cache:
        cached = load_cached_brief(td, h)
        if cached and cached.get("brief"):
            _l(f"AI 博弈短报：命中缓存 {td}/{h}")
            return {**cached, "from_cache": True}

    if not has_api_key():
        raise LlmConfigError(
            "未配置 NVIDIA_API_KEY。请设置环境变量 NVIDIA_API_KEY 或在应用设置中保存。"
        )

    messages = _build_prompt(td, candidates, hype, sentiment_slim, news_slim)
    _l(f"AI 博弈短报：调用 NIM model={model}")
    client = NvidiaNimClient(api_key=api_key)
    raw = client.chat(
        messages=messages,
        model=model,
        temperature=0.25,
        max_tokens=900,
    )
    brief_text = (raw or "").strip()
    if not brief_text:
        raise LlmRequestError("NIM 返回空内容")

    out = {
        "trade_date": td,
        "model": model,
        "brief": brief_text,
        "payload_hash": h,
        "from_cache": False,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "candidates_count": sum(len(v) for v in candidates.values()),
        "hype_concepts_count": len(hype.get("top_concepts") or []),
    }
    try:
        save_app_config(_cache_key(td, h), out)
    except Exception:
        logger.exception("保存 daily_brief 缓存失败")
    return out
