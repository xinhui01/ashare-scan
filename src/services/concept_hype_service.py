"""概念炒作识别服务。

把最近 N 个交易日的涨停池，按"概念维度"聚合：
- 哪些概念/行业最近正在被持续炒作（连续 N 日都有涨停）
- 起爆日是哪天，已经持续了多久
- 强弱阶段（萌芽 / 主升 / 末期 / 退潮）
- 主线龙头是谁（连板最高 / 最强势）
- 题材内涨停成员明细（含每只票的涨停日历）

数据源（按可用性级联）：
1. limit_up_pool[`所属行业`]   — 必有，作为兜底主信号
2. stock_concept_tags          — 东财/同花顺概念库反查（用户需先"刷新概念库"才会丰富）
3. limit_up_themes_<date>      — LLM 当日题材聚类缓存（用户需先在涨停对比 tab 跑过）

输出严格不含 LLM 调用，纯本地聚合，毫秒级返回。
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

import stock_store
from llm_theme_clustering import load_cached_themes
from stock_logger import get_logger

logger = get_logger(__name__)


# ============== 阈值 ==============

# 一天内某概念至少 N 只涨停才算"活跃日"
MIN_DAILY_ACTIVE = 2
# 起爆日：当日涨停数 ≥ 此阈值
IGNITE_DAILY_THRESHOLD = 3
# 起爆日 ≥ 前 3 日均值的倍数（防止"持续高位"被错认为新起爆）
IGNITE_RATIO = 2.0
# 列入榜单：累计涨停 ≥ 此值 或 活跃日 ≥ 2
MIN_TOTAL_FOR_LIST = 4
MIN_ACTIVE_DAYS_FOR_LIST = 2
# 主线候选数（呈现 top N）
TOP_LEADERS_PER_CONCEPT = 5
TREND_LABELS = {
    "rising": "上升",
    "flat": "走平",
    "declining": "下降",
}


def compute_opportunity_score(record: Dict[str, Any]) -> int:
    """综合"炒作机会评分"。

    思路：先看机会（阶段/趋势/萌芽窗口），再看强度（活跃度/龙头高度）。
    分数越高越值得关注，末期/退潮的老题材会被自动沉底。

    返回 0~100 的整数。
    """
    phase = str(record.get("phase") or "")
    trend = str(record.get("trend") or "")
    today = int(record.get("today_count") or 0)
    duration = int(record.get("duration") or 0)
    leaders = record.get("leaders") or []
    max_boards = max(
        (int(m.get("boards", 1)) for m in leaders if isinstance(m, dict)),
        default=1,
    )

    score = 0
    score += {"萌芽": 30, "主升": 20, "末期": 5, "退潮": 0}.get(phase, 0)
    score += {"rising": 15, "flat": 0, "declining": -10}.get(trend, 0)
    if today >= 5:
        score += 15
    elif today >= 3:
        score += 10
    elif today >= 1:
        score += 3
    else:
        score -= 5
    if 2 <= duration <= 5:
        score += 10
    elif duration == 1:
        score += 5
    elif duration >= 8:
        score -= 5
    if max_boards >= 5:
        score += 12
    elif max_boards >= 4:
        score += 10
    elif max_boards >= 3:
        score += 5
    return max(0, min(100, score))


# ============== 工具函数 ==============

def trend_label(trend: Any) -> str:
    """把内部趋势枚举转成界面友好的中文。"""
    value = str(trend or "").strip()
    return TREND_LABELS.get(value, value)


def _normalize_date(s: Any) -> str:
    if s is None:
        return ""
    raw = str(s).strip().replace("-", "").replace("/", "")
    return raw if len(raw) == 8 and raw.isdigit() else ""


def _normalize_code(c: Any) -> str:
    return str(c or "").strip().zfill(6)


def _select_window_dates(end_date: str, lookback: int) -> List[str]:
    """从 limit_up_pool 已缓存的交易日里，截取 end_date 及之前的 lookback 天。"""
    end = _normalize_date(end_date)
    all_dates = stock_store.list_limit_up_pool_trade_dates() or []
    if not all_dates:
        return []
    if end:
        candidate = [d for d in all_dates if d <= end]
    else:
        candidate = list(all_dates)
    if not candidate:
        return []
    return candidate[-int(max(1, lookback)):]


def _load_pool_by_date(date_key: str) -> List[Dict[str, Any]]:
    """读取某日涨停池，规范成 [{code, name, industry, boards, change_pct, ...}, ...]。"""
    df = stock_store.load_limit_up_pool(date_key)
    if df is None or df.empty:
        return []
    out: List[Dict[str, Any]] = []
    cols = set(df.columns)
    for _, row in df.iterrows():
        code = _normalize_code(row.get("代码") if "代码" in cols else row.get("code"))
        if len(code) != 6 or not code.isdigit():
            continue
        name = str(row.get("名称") if "名称" in cols else row.get("name") or "").strip()
        industry = str(row.get("所属行业") if "所属行业" in cols else row.get("industry") or "").strip()
        try:
            boards = int(row.get("连板数") if "连板数" in cols else row.get("boards") or 1)
        except (TypeError, ValueError):
            boards = 1
        try:
            chg = float(row.get("涨跌幅") if "涨跌幅" in cols else row.get("change_pct") or 0.0)
        except (TypeError, ValueError):
            chg = 0.0
        try:
            close = float(row.get("最新价") if "最新价" in cols else row.get("close") or 0.0)
        except (TypeError, ValueError):
            close = 0.0
        try:
            turnover = float(row.get("换手率") if "换手率" in cols else row.get("turnover") or 0.0)
        except (TypeError, ValueError):
            turnover = 0.0
        out.append({
            "code": code,
            "name": name or code,
            "industry": industry,
            "boards": boards,
            "change_pct": chg,
            "close": close,
            "turnover": turnover,
        })
    return out


def _build_code_to_concepts(
    all_codes: List[str],
) -> Tuple[Dict[str, List[str]], int]:
    """批量查 stock_concept_tags 反查表，返回 {code: [concept, ...]}。"""
    if not all_codes:
        return {}, 0
    try:
        m = stock_store.lookup_concepts_batch(
            all_codes, per_code_limit=12, sources=None,
        ) or {}
    except Exception:
        logger.exception("批量查 stock_concept_tags 失败")
        return {}, 0
    pairs = sum(len(v) for v in m.values())
    return m, pairs


def _load_llm_theme_per_day(dates: List[str]) -> Dict[str, Dict[str, List[str]]]:
    """逐日加载 LLM 题材缓存，返回 {date: {theme_name: [code, ...]}}。"""
    out: Dict[str, Dict[str, List[str]]] = {}
    for d in dates:
        try:
            payload = load_cached_themes(d) or {}
        except Exception:
            payload = {}
        themes = payload.get("themes") or []
        if not themes:
            continue
        per_day: Dict[str, List[str]] = {}
        for t in themes:
            if not isinstance(t, dict):
                continue
            name = str(t.get("name") or "").strip()
            codes = [_normalize_code(c) for c in (t.get("codes") or [])]
            codes = [c for c in codes if len(c) == 6 and c.isdigit()]
            if not name or not codes:
                continue
            per_day[name] = codes
        if per_day:
            out[d] = per_day
    return out


# ============== 核心聚合 ==============

def _classify_phase(
    daily_counts: List[int],
    duration: int,
) -> Tuple[str, str]:
    """根据日内涨停数序列 + 持续天数，给出 (phase, trend)。

    - phase: 萌芽 / 主升 / 末期 / 退潮
    - trend: rising / flat / declining
    """
    n = len(daily_counts)
    today = daily_counts[-1] if n else 0
    if n >= 4:
        recent = sum(daily_counts[-2:]) / 2.0
        prior = sum(daily_counts[-4:-2]) / 2.0 if daily_counts[-4:-2] else 0.0
    else:
        recent = today
        prior = sum(daily_counts[:-1]) / max(1, n - 1) if n > 1 else 0.0

    if recent > prior + 0.5:
        trend = "rising"
    elif recent < prior - 0.5:
        trend = "declining"
    else:
        trend = "flat"

    # 阶段
    if duration <= 2:
        phase = "萌芽" if today > 0 and trend != "declining" else "退潮"
    elif duration <= 7:
        if trend == "declining" and today < max(daily_counts) * 0.5:
            phase = "末期"
        else:
            phase = "主升"
    else:
        if trend == "declining" or today == 0:
            phase = "退潮"
        else:
            phase = "末期"
    if today == 0 and duration > 0:
        phase = "退潮"
    return phase, trend


def _compute_concept_metrics(
    name: str,
    source: str,
    members_per_day: Dict[str, List[Dict[str, Any]]],
    window_dates: List[str],
) -> Optional[Dict[str, Any]]:
    """聚合单个概念跨日数据，返回标准 record，不达阈值返回 None。"""
    daily_counts: List[int] = []
    daily_count_map: Dict[str, int] = {}
    code_to_member: Dict[str, Dict[str, Any]] = {}
    code_to_lu_dates: Dict[str, List[str]] = defaultdict(list)

    for d in window_dates:
        members_today = members_per_day.get(d) or []
        cnt = len(members_today)
        daily_counts.append(cnt)
        daily_count_map[d] = cnt
        for m in members_today:
            code = m["code"]
            code_to_lu_dates[code].append(d)
            existing = code_to_member.get(code)
            # 取连板数最高 / 涨幅最大的一次代表
            if (
                existing is None
                or int(m.get("boards", 1)) > int(existing.get("boards", 1))
                or (
                    int(m.get("boards", 1)) == int(existing.get("boards", 1))
                    and float(m.get("change_pct", 0)) > float(existing.get("change_pct", 0))
                )
            ):
                code_to_member[code] = dict(m)

    total = sum(daily_counts)
    active_days = sum(1 for c in daily_counts if c >= MIN_DAILY_ACTIVE)
    if total < MIN_TOTAL_FOR_LIST and active_days < MIN_ACTIVE_DAYS_FOR_LIST:
        return None

    today_count = daily_counts[-1] if daily_counts else 0
    peak_count = max(daily_counts) if daily_counts else 0
    peak_idx = daily_counts.index(peak_count) if peak_count else len(daily_counts) - 1
    peak_date = window_dates[peak_idx] if window_dates else ""

    # 起爆日：从前往后第一个满足 (cnt >= IGNITE_DAILY_THRESHOLD) 且
    # >= 前 3 日均值的 IGNITE_RATIO 倍 的日子
    ignite_idx: Optional[int] = None
    for i, c in enumerate(daily_counts):
        if c < IGNITE_DAILY_THRESHOLD:
            continue
        prior_window = daily_counts[max(0, i - 3): i]
        prior_avg = sum(prior_window) / len(prior_window) if prior_window else 0.0
        if c >= max(IGNITE_DAILY_THRESHOLD, prior_avg * IGNITE_RATIO):
            ignite_idx = i
            break
    if ignite_idx is None:
        # 没有显著起爆，找第一个达标活跃日作为起点
        for i, c in enumerate(daily_counts):
            if c >= MIN_DAILY_ACTIVE:
                ignite_idx = i
                break
    if ignite_idx is None:
        return None

    ignite_date = window_dates[ignite_idx]
    duration = len(daily_counts) - ignite_idx  # 起爆日算第 1 天

    phase, trend = _classify_phase(daily_counts, duration)

    # 龙头：按 (boards desc, change_pct desc) 取前 N
    members_sorted = sorted(
        code_to_member.values(),
        key=lambda m: (
            -int(m.get("boards", 1)),
            -float(m.get("change_pct", 0.0)),
        ),
    )
    leaders = members_sorted[:TOP_LEADERS_PER_CONCEPT]
    for m in leaders:
        m_dates = code_to_lu_dates.get(m["code"], [])
        m["limit_up_count"] = len(m_dates)

    # 所有成员（用于详情面板）
    all_members: List[Dict[str, Any]] = []
    for m in members_sorted:
        code = m["code"]
        all_members.append({
            **m,
            "limit_up_dates": list(code_to_lu_dates.get(code, [])),
            "limit_up_count": len(code_to_lu_dates.get(code, [])),
        })

    # 所属行业聚合（仅当 source != industry 时有意义）
    industry_counter: Dict[str, int] = defaultdict(int)
    for m in all_members:
        ind = (m.get("industry") or "").strip()
        if ind:
            industry_counter[ind] += 1
    related_industries = sorted(
        industry_counter.items(), key=lambda x: -x[1],
    )[:5]

    record = {
        "name": name,
        "source": source,
        "total_limit_ups": total,
        "active_days": active_days,
        "today_count": today_count,
        "peak_count": peak_count,
        "peak_date": peak_date,
        "ignite_date": ignite_date,
        "duration": duration,
        "phase": phase,
        "trend": trend,
        "daily_counts": daily_count_map,
        "leaders": leaders,
        "members": all_members,
        "member_count": len(all_members),
        "related_industries": [
            {"name": n, "count": c} for n, c in related_industries
        ],
    }
    record["opportunity_score"] = compute_opportunity_score(record)
    return record


def _build_main_line_summary(
    ranked: List[Dict[str, Any]],
    window_dates: List[str],
) -> Dict[str, Any]:
    if not ranked:
        return {
            "name": "",
            "summary": "近 {} 日涨停池未发现明显的概念集群。".format(len(window_dates)),
        }
    top = ranked[0]
    leaders = top.get("leaders") or []
    leader_str = "、".join(
        f"{m['name']}({m['boards']}板)" for m in leaders[:3]
    ) or "—"
    summary = (
        f"主线题材【{top['name']}】({top['source']})："
        f"近 {len(window_dates)} 日累计 {top['total_limit_ups']} 只涨停，"
        f"已持续 {top['duration']} 天（{top['phase']}），"
        f"龙头：{leader_str}"
    )
    return {
        "name": top["name"],
        "source": top["source"],
        "summary": summary,
        "phase": top["phase"],
        "leaders": leaders[:3],
    }


# ============== 对外主入口 ==============

def analyze_concept_hype(
    end_date: Optional[str] = None,
    *,
    lookback: int = 10,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """识别正在被炒作的概念/行业。

    Args:
        end_date: 结束交易日（YYYYMMDD，默认取 limit_up_pool 最新日）
        lookback: 回看天数（按 limit_up_pool 已缓存的交易日切片）
        log: 可选日志回调（用于在 GUI 状态栏打信息）

    Returns:
        见模块顶部 docstring 注释里的结构。
    """
    def _l(msg: str) -> None:
        if log is not None:
            try:
                log(msg)
            except Exception:
                pass
        logger.info(msg)

    window_dates = _select_window_dates(end_date or "", lookback)
    if not window_dates:
        return {
            "end_date": end_date or "",
            "start_date": "",
            "trade_dates": [],
            "concepts": [],
            "main_line": {"name": "", "summary": "本地无 limit_up_pool 缓存，无法分析。"},
            "stats": {
                "total_concepts": 0, "active_concepts": 0,
                "total_limit_ups": 0, "fresh_concepts": [],
            },
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    actual_end = window_dates[-1]
    actual_start = window_dates[0]
    _l(f"概念炒作分析：窗口 {actual_start}~{actual_end}（{len(window_dates)} 个交易日）")

    # 加载所有窗口日的涨停池
    pool_per_day: Dict[str, List[Dict[str, Any]]] = {}
    all_codes_set: set = set()
    for d in window_dates:
        members = _load_pool_by_date(d)
        pool_per_day[d] = members
        for m in members:
            all_codes_set.add(m["code"])

    total_lu = sum(len(v) for v in pool_per_day.values())
    if not all_codes_set:
        return {
            "end_date": actual_end, "start_date": actual_start,
            "trade_dates": window_dates, "concepts": [],
            "main_line": {"name": "", "summary": "窗口内涨停池均为空。"},
            "stats": {
                "total_concepts": 0, "active_concepts": 0,
                "total_limit_ups": 0, "fresh_concepts": [],
            },
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    _l(f"  累计涨停 {total_lu} 次，去重后 {len(all_codes_set)} 只票")

    # 概念标签反查（增强信号 1）
    code_to_concepts, concept_pairs = _build_code_to_concepts(sorted(all_codes_set))
    _l(f"  概念库反查：{concept_pairs} 对（覆盖 {len(code_to_concepts)} 只票）")

    # LLM 题材缓存（增强信号 2）
    llm_themes_per_day = _load_llm_theme_per_day(window_dates)
    _l(f"  LLM 题材缓存命中 {len(llm_themes_per_day)}/{len(window_dates)} 天")

    # 构建 (concept_label, source) -> {date: [member, ...]} 的逆排
    label_to_day_members: Dict[Tuple[str, str], Dict[str, List[Dict[str, Any]]]] = (
        defaultdict(lambda: defaultdict(list))
    )

    # 1. 按"行业"维度（必有）
    for d, members in pool_per_day.items():
        for m in members:
            ind = (m.get("industry") or "").strip()
            if not ind:
                continue
            label_to_day_members[(ind, "行业")][d].append(m)

    # 2. 按"概念库"维度（来自 stock_concept_tags）
    for d, members in pool_per_day.items():
        for m in members:
            for concept in code_to_concepts.get(m["code"], []) or []:
                concept = concept.strip()
                if not concept:
                    continue
                label_to_day_members[(concept, "概念")][d].append(m)

    # 3. 按"LLM 题材"维度（来自 llm_theme_clustering 缓存）
    for d, themes in llm_themes_per_day.items():
        members_today = pool_per_day.get(d) or []
        code_idx = {m["code"]: m for m in members_today}
        for theme_name, theme_codes in themes.items():
            for c in theme_codes:
                if c in code_idx:
                    label_to_day_members[(theme_name, "LLM题材")][d].append(code_idx[c])

    # 计算每个标签的指标
    ranked: List[Dict[str, Any]] = []
    for (name, source), per_day in label_to_day_members.items():
        rec = _compute_concept_metrics(name, source, per_day, window_dates)
        if rec:
            ranked.append(rec)

    # 排序：机会分 desc → 今涨停 desc → 累计 desc
    ranked.sort(
        key=lambda r: (
            -int(r.get("opportunity_score", 0)),
            -int(r["today_count"]),
            -int(r["total_limit_ups"]),
        ),
    )

    # 萌芽题材（duration<=2 且 today_count>=2）
    fresh = [
        {
            "name": r["name"], "source": r["source"],
            "ignite_date": r["ignite_date"],
            "today_count": r["today_count"],
            "duration": r["duration"],
        }
        for r in ranked
        if r["duration"] <= 2 and r["today_count"] >= MIN_DAILY_ACTIVE
    ][:8]

    main_line = _build_main_line_summary(ranked, window_dates)
    active_count = sum(1 for r in ranked if r["today_count"] > 0)

    _l(f"  识别题材 {len(ranked)} 个，今日活跃 {active_count}，主线：{main_line['name']}")

    return {
        "end_date": actual_end,
        "start_date": actual_start,
        "trade_dates": window_dates,
        "concepts": ranked,
        "main_line": main_line,
        "stats": {
            "total_concepts": len(ranked),
            "active_concepts": active_count,
            "total_limit_ups": total_lu,
            "fresh_concepts": fresh,
        },
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
