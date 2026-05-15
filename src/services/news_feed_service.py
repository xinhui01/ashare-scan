"""今日财经新闻聚合，给 AI 博弈短报当上下文。

两个数据源组合：
1. 东方财富财经早餐 (`stock_info_cjzc_em`)
   每日 06:00 一篇宏观摘要，覆盖隔夜外盘 + 重大政策 + 当日关注。

2. 财联社电报 (`stock_info_global_cls('全部')`)
   最新 20 条滚动快讯，覆盖盘后 / 异动 / 临时政策。

两者拼接 + 关键词过滤掉海外/汇率噪音 → 喂给 LLM。
按日期缓存到 app_config，同一天只联网拉一次。
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import stock_data  # 触发 SSL/proxy 网络补丁
from stock_logger import get_logger
from stock_store import load_app_config, save_app_config

logger = get_logger(__name__)

CACHE_KEY_PREFIX = "news_feed_"

# 噪音过滤关键词（标题命中即丢）
DROP_KEYWORDS = (
    "美股", "纳斯达克", "道指", "标普", "美元",
    "微软", "苹果", "特斯拉", "谷歌", "亚马逊", "Meta", "英特尔", "英伟达",
    "比特币", "数字货币", "加密货币", "USDT", "以太",
    "港股", "恒指", "恒生", "暗盘", "新股暗盘",
    "原油", "黄金", "白银", "铜价",
    "外汇", "汇率", "欧元", "日元", "英镑",
    "俄乌", "乌克兰", "基辅", "以色列", "中东", "俄罗斯", "莫斯科", "巴勒斯坦",
)

# 加分关键词（用于排序，标题命中向前提）
BOOST_KEYWORDS = (
    "央行", "证监会", "国务院", "发改委", "财政部", "工信部", "科技部",
    "新规", "政策", "落地", "印发", "出台", "试点",
    "概念", "板块", "题材", "龙头",
    "涨停", "炸板", "异动",
    "AI", "人工智能", "机器人", "光伏", "储能", "锂电",
    "半导体", "芯片", "国产替代", "算力", "数据中心",
    "新能源", "智能驾驶", "氢能",
    "并购", "重组", "回购", "增持",
    "业绩", "扭亏", "暴增",
)


# ============== 数据加载 ==============

def _today_str() -> str:
    return datetime.now().strftime("%Y%m%d")


def _normalize_date(s: Any) -> str:
    raw = str(s or "").strip().replace("-", "").replace("/", "")
    return raw if len(raw) == 8 and raw.isdigit() else _today_str()


def _fetch_morning_briefing(target_date: str) -> Optional[Dict[str, str]]:
    """拉东方财富财经早餐（每日 06:00），匹配目标日期。"""
    try:
        import akshare as ak
        df = ak.stock_info_cjzc_em()
    except Exception as exc:
        logger.warning("拉财经早餐失败: %s", exc)
        return None
    if df is None or df.empty:
        return None
    # 时间格式 "2026-05-15 06:00:11"
    target_with_dash = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    matches = df[df["发布时间"].astype(str).str.startswith(target_with_dash)]
    if matches.empty:
        # 取最近一篇兜底
        matches = df.head(1)
    row = matches.iloc[0]
    return {
        "title": str(row.get("标题") or "").strip(),
        "summary": str(row.get("摘要") or "").strip(),
        "time": str(row.get("发布时间") or "").strip(),
    }


def _fetch_telegrams(target_date: str) -> List[Dict[str, str]]:
    """拉财联社电报最新 20 条，过滤到目标日期。"""
    try:
        import akshare as ak
        df = ak.stock_info_global_cls(symbol="全部")
    except Exception as exc:
        logger.warning("拉财联社电报失败: %s", exc)
        return []
    if df is None or df.empty:
        return []
    out: List[Dict[str, str]] = []
    target_with_dash = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
    for _, row in df.iterrows():
        date_str = str(row.get("发布日期") or "")
        time_str = str(row.get("发布时间") or "")
        title = str(row.get("标题") or "").strip()
        if not title:
            continue
        # 只要目标日期的
        if not date_str.startswith(target_with_dash) and date_str != target_date:
            continue
        out.append({
            "title": title,
            "content": str(row.get("内容") or "").strip()[:200],
            "time": time_str,
        })
    return out


# ============== 过滤 + 排序 ==============

def _is_noise(title: str) -> bool:
    """是否海外 / 商品 / 港股等与 A 股短线无关的噪音。"""
    return any(kw in title for kw in DROP_KEYWORDS)


def _importance_score(title: str) -> int:
    """简单关键词加分，用于排序时把重要新闻顶到前面。"""
    s = 0
    for kw in BOOST_KEYWORDS:
        if kw in title:
            s += 2
    # 公司公告型（"XX：..."）降权 — 个股事件通常不影响整体板块
    if re.match(r"^[一-龥A-Za-z0-9]{2,8}[：:]", title):
        s -= 1
    return s


def _rank_telegrams(items: List[Dict[str, str]], top_n: int = 12) -> List[Dict[str, str]]:
    filtered = [it for it in items if not _is_noise(it["title"])]
    # 按 importance desc + time desc
    filtered.sort(key=lambda it: (-_importance_score(it["title"]), it["time"]),
                  reverse=False)
    # 上面的 sort 在 importance 同分时按 time 升序；我们要 importance desc + time desc，
    # 所以用 lambda 再排一次确保正确
    filtered.sort(key=lambda it: (-_importance_score(it["title"]),
                                   -int(it["time"].replace(":", "") or "0")))
    return filtered[:top_n]


# ============== 对外主入口 ==============

def fetch_today_news(
    target_date: Optional[str] = None,
    *,
    use_cache: bool = True,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """聚合今日财经新闻。

    Returns:
        {
            "date": "20260515",
            "morning_briefing": {"title": ..., "summary": ..., "time": ...} or None,
            "telegrams": [{title, content, time}, ...],
            "fetched_at": ...,
        }
    """
    def _l(msg: str) -> None:
        if log is not None:
            try:
                log(msg)
            except Exception:
                pass
        logger.info(msg)

    td = _normalize_date(target_date or "")
    cache_key = f"{CACHE_KEY_PREFIX}{td}"

    if use_cache:
        cached = load_app_config(cache_key, default=None)
        if isinstance(cached, dict) and cached.get("ok"):
            _l(f"今日新闻：命中缓存 {td}")
            return cached

    _ = stock_data  # 触发 SSL 补丁
    morning = _fetch_morning_briefing(td)
    raw_telegrams = _fetch_telegrams(td)
    ranked = _rank_telegrams(raw_telegrams, top_n=12)
    _l(f"今日新闻：早餐 {'有' if morning else '无'}, 电报 {len(raw_telegrams)} 条 → 排序后取 {len(ranked)}")

    out = {
        "ok": True,
        "date": td,
        "morning_briefing": morning,
        "telegrams": ranked,
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    try:
        save_app_config(cache_key, out)
    except Exception:
        logger.exception("保存 news_feed 缓存失败")
    return out
