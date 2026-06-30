"""跨 scorer 复用的评分调节因子。

6 个无状态函数：
- parse_full_pool: 把涨停池 DataFrame 转 records 列表
- count_pool_industries: 涨停池行业分布
- theme_bonus: AI 题材聚类热度加分
- market_style_bias: 市场状态/轮动风格加减分
- capital_flow_bonus: 板块涨跌幅加分（行业联动）
- vol_ratio_with_baseline: 5/20 日量比双口径计算

设计：纯函数 / 静态方法，无 self.fetcher 依赖。所需上下文（compare_context 等）以参数注入。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def parse_full_pool(pool_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """将涨停池 DataFrame 解析为完整记录列表（包含所有连板数）。"""
    records = []
    if pool_df.empty:
        return records
    for _, row in pool_df.iterrows():
        rec: Dict[str, Any] = {
            "code": str(row.get("代码", "")).strip().zfill(6),
            "name": str(row.get("名称", "")),
            "change_pct": float(row["涨跌幅"]) if pd.notna(row.get("涨跌幅")) else None,
            "close": float(row["最新价"]) if pd.notna(row.get("最新价")) else None,
            "industry": str(row.get("所属行业", "")),
            "amount": float(row["成交额"]) if pd.notna(row.get("成交额")) else None,
            "market_cap": float(row["流通市值"]) if pd.notna(row.get("流通市值")) else None,
            "turnover": float(row["换手率"]) if pd.notna(row.get("换手率")) else None,
            "consecutive_boards": int(row["连板数"]) if pd.notna(row.get("连板数")) else 1,
            "first_board_time": str(row.get("首次封板时间", "")),
            "last_board_time": str(row.get("最后封板时间", "")),
            "break_count": int(row["炸板次数"]) if pd.notna(row.get("炸板次数")) else 0,
            "board_amount": float(row["封板资金"]) if pd.notna(row.get("封板资金")) else None,
        }
        records.append(rec)
    return records


def count_pool_industries(pool_df: pd.DataFrame) -> Dict[str, int]:
    """涨停池按行业计数（≥1 只）。"""
    if pool_df.empty or "所属行业" not in pool_df.columns:
        return {}
    counts = pool_df["所属行业"].astype(str).value_counts().to_dict()
    return {k: int(v) for k, v in counts.items() if k and k.lower() != "nan"}


def theme_bonus(
    code: str,
    industry: str,
    compare_context: Dict[str, Any],
) -> Tuple[float, Optional[str]]:
    """根据 AI 题材聚类缓存返回题材热度加分。

    优先级：
    1. 候选 code 直接命中题材 → 用题材规模
    2. 否则通过行业映射到最热题材 → 用题材规模
    无缓存或不命中返回 (0, None)。
    """
    code_theme_map = compare_context.get("code_theme_map") or {}
    theme_size_map = compare_context.get("theme_size_map") or {}
    industry_theme_heat = compare_context.get("industry_theme_heat") or {}

    theme_name = code_theme_map.get(code)
    size = 0
    direct_hit = False
    if theme_name:
        size = int(theme_size_map.get(theme_name, 0))
        direct_hit = True
    elif industry:
        size = int(industry_theme_heat.get(industry, 0))

    if size >= 6:
        label = f"题材龙头{size}只" if direct_hit else f"题材族群{size}只"
        return 8.0, f"{label}+8"
    if size >= 4:
        label = f"题材{size}只" if direct_hit else f"题材关联{size}只"
        return 5.0, f"{label}+5"
    if size >= 2 and direct_hit:
        return 2.0, f"同题材+2"
    return 0.0, None


def theme_fund_bonus(
    code: str,
    industry: str,
    compare_context: Dict[str, Any],
) -> Tuple[float, List[str]]:
    """题材/板块资金潜伏 + 爆发加分。"""
    code_scores = compare_context.get("code_theme_fund_score") or {}
    industry_scores = compare_context.get("industry_theme_fund_score") or {}
    code_theme_map = compare_context.get("code_theme_map") or {}
    theme_acc_map = compare_context.get("theme_fund_accumulation_map") or {}
    theme_breakout_map = compare_context.get("theme_breakout_map") or {}

    score = 0
    source = ""
    theme_name = str(code_theme_map.get(code) or "").strip()
    if code in code_scores:
        score = int(code_scores.get(code) or 0)
        source = "题材资金"
    elif industry and industry in industry_scores:
        score = int(industry_scores.get(industry) or 0)
        source = "板块资金"

    if score <= 0:
        return 0.0, []

    acc = int(theme_acc_map.get(theme_name, 0)) if theme_name else 0
    burst = int(theme_breakout_map.get(theme_name, 0)) if theme_name else 0
    reasons: List[str] = []
    if score >= 75:
        bonus = 8.0
    elif score >= 60:
        bonus = 6.0
    elif score >= 45:
        bonus = 4.0
    elif score >= 30:
        bonus = 2.0
    else:
        bonus = 0.0
    if bonus:
        detail = f"潜{acc}/爆{burst}" if theme_name else f"{score}"
        reasons.append(f"{source}{detail}+{int(bonus)}")
    return bonus, reasons


def _text_set(values: Any) -> set[str]:
    if isinstance(values, (list, tuple, set)):
        return {str(item).strip() for item in values if str(item).strip()}
    text = str(values or "").strip()
    return {text} if text else set()


def _matches_any_name(value: str, names: set[str]) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return any(text == name or text in name or name in text for name in names if name)


def _retreat_stage_info(compare_context: Dict[str, Any], market_state: Dict[str, Any]) -> Dict[str, Any]:
    stage = compare_context.get("market_retreat_stage")
    if not isinstance(stage, dict):
        stage = market_state.get("retreat_stage")
    return dict(stage) if isinstance(stage, dict) else {}


def _retreat_stage_label(retreat_stage: Dict[str, Any], default: str = "退潮日") -> str:
    return str(retreat_stage.get("label") or default).strip() or default


def market_style_bias(
    category: str,
    code: str,
    industry: str,
    compare_context: Dict[str, Any],
    *,
    boards: int = 0,
) -> Tuple[float, List[str]]:
    """根据市场状态推荐打法做风格加减分。

    市场情绪服务已给出"接力日/轮动日/退潮日/冰点日/过渡日"和轮动明细。
    个股评分不能只看总情绪分，否则轮动日会把老主线接力误当成强势方向。
    """
    if not isinstance(compare_context, dict):
        return 0.0, []

    market_state = compare_context.get("market_state") or {}
    label = str(compare_context.get("market_state_label") or market_state.get("label") or "").strip()
    if not label:
        return 0.0, []

    rotation = (
        compare_context.get("market_rotation")
        or compare_context.get("rotation")
        or (compare_context.get("raw") or {}).get("rotation")
        or {}
    )
    new_names = _text_set(rotation.get("new_industries"))
    theme_name = str((compare_context.get("code_theme_map") or {}).get(code) or "").strip()
    phase = str((compare_context.get("code_to_concept_phase") or {}).get(code) or "").strip()
    is_new_direction = (
        phase == "萌芽"
        or _matches_any_name(industry, new_names)
        or _matches_any_name(theme_name, new_names)
    )
    strong_line = compare_context.get("strong_main_line") or {}
    strong_line_name = ""
    if isinstance(strong_line, dict) and str(strong_line.get("phase") or "").strip() == "主升":
        strong_line_name = str(strong_line.get("name") or "").strip()
    is_confirmed_main_line = bool(
        strong_line_name
        and (
            _matches_any_name(industry, {strong_line_name})
            or _matches_any_name(theme_name, {strong_line_name})
        )
    )

    cat = str(category or "").strip().lower()
    is_cont = cat in {"cont", "continuation"}
    is_first = cat in {"first", "followthrough", "relay"}
    is_fresh = cat in {"fresh", "first_board", "first-board"}
    is_wrap = cat in {"wrap", "broken_board_wrap", "broken-board-wrap"}
    is_trend = cat in {"trend", "trend_limit_up", "trend-limit-up"}

    if label == "轮动日":
        if is_confirmed_main_line:
            if is_fresh:
                return 6.0, [f"轮动日{strong_line_name}主线补涨+6"]
            if is_first:
                return 4.0, [f"轮动日{strong_line_name}主线二波+4"]
            if is_trend:
                return 4.0, [f"轮动日{strong_line_name}主线趋势+4"]
            if is_cont:
                if boards >= 3:
                    return -4.0, [f"轮动日{strong_line_name}高位主线谨慎-4"]
                return 2.0, [f"轮动日{strong_line_name}主线延续+2"]
        if is_fresh:
            if is_new_direction:
                return 10.0, ["轮动日首板新题材+10"]
            return 4.0, ["轮动日首板优先+4"]
        if is_cont:
            if is_new_direction and boards <= 2:
                return -6.0, ["轮动日新方向接力谨慎-6"]
            penalty = -15.0 if boards >= 3 else -12.0
            return penalty, [f"轮动日老主线接力降权{int(penalty)}"]
        if is_first:
            if is_new_direction:
                return -4.0, ["轮动日二波新方向谨慎-4"]
            return -10.0, ["轮动日二波接力降权-10"]
        if is_wrap:
            return 4.0, ["轮动日反包修复+4"]
        if is_trend:
            return -4.0, ["轮动日趋势票降权-4"]

    if label == "接力日":
        if is_cont and boards >= 2:
            return 6.0, ["接力日连板核心+6"]
        if is_first:
            return 4.0, ["接力日二波接力+4"]
        if is_fresh and not is_new_direction:
            return -2.0, ["接力日非新方向首板-2"]

    if label == "过渡日":
        if is_fresh:
            return 3.0, ["过渡日首板试错+3"]
        if is_cont and boards >= 3:
            return -5.0, ["过渡日高位接力谨慎-5"]

    if label == "退潮日":
        retreat_stage = _retreat_stage_info(compare_context, market_state)
        if is_cont or is_first:
            return -15.0, ["退潮日接力降权-15"]
        if is_wrap:
            if retreat_stage.get("allow_wrap"):
                return 4.0, [f"{_retreat_stage_label(retreat_stage)}确认型反包+4"]
            return -12.0, [f"{_retreat_stage_label(retreat_stage)}反包禁做-12"]
        if is_fresh:
            return -4.0, ["退潮日首板控仓-4"]
        if is_trend:
            return -6.0, ["退潮日趋势票降权-6"]

    if label == "冰点日":
        if is_cont or is_first:
            return -20.0, ["冰点日接力回避-20"]
        if is_wrap:
            return 6.0, ["冰点日超跌反包+6"]
        if is_fresh:
            return -8.0, ["冰点日首板试错降权-8"]
        if is_trend:
            return -8.0, ["冰点日趋势票降权-8"]

    return 0.0, []


def relative_strength_bonus(
    code: str,
    history: Optional[pd.DataFrame],
    compare_context: Dict[str, Any],
    *,
    category: str = "",
    boards: int = 0,
) -> Tuple[float, List[str], Dict[str, Any]]:
    """Return score adjustment and display fields for stock/index relative strength.

    Missing index data is intentionally not neutralized into 0. Callers receive
    explicit availability fields so UI/export can show that the factor was skipped.
    """
    from src.services.relative_strength_service import (
        benchmark_symbol_for_stock,
        score_stock_relative_strength,
    )

    benchmark = benchmark_symbol_for_stock(code)
    index_history = (compare_context.get("relative_strength_index_history") or {}).get(benchmark)
    result = score_stock_relative_strength(
        code,
        history if history is not None else pd.DataFrame(),
        index_history,
        category=category,
        boards=boards,
    )
    metrics = dict(result.get("metrics") or {})
    metrics.update({
        "relative_strength_available": bool(result.get("available")),
        "relative_strength_score": result.get("score"),
        "relative_strength_benchmark": result.get("benchmark") or benchmark,
        "relative_strength_note": result.get("warning") or "",
    })
    if not result.get("available"):
        return 0.0, [], metrics

    score = int(result.get("score") or 0)
    if score == 0:
        return 0.0, [], metrics
    first_reason = ""
    reasons = result.get("reasons") or []
    if reasons:
        first_reason = str(reasons[0])
    sign = f"+{score}" if score > 0 else str(score)
    detail = f"({first_reason})" if first_reason else ""
    return float(score), [f"强弱分{sign}{detail}"], metrics


def capital_flow_bonus(
    code: str,
    compare_context: Dict[str, Any],
    *,
    industry: str = "",
    boards: int = 0,
) -> Tuple[float, List[str]]:
    """板块涨跌幅加分（强势板块联动）。

    历史上还包含龙虎榜净买额 + 解读细分加分，但 LHB 数据源不稳定且
    游资席位语义参差，整体删掉了。函数名保留是为了不破坏调用方签名
    （cont / first / fresh / wrap 评分都从这里拿"行业联动分"）。

    boards 参数也保留以向后兼容（之前用于"高位连板惩罚"，跟着 LHB
    一起删了）。
    """
    bonus = 0.0
    reasons: List[str] = []

    # 板块涨跌幅加分（强势板块联动）
    board_strength = compare_context.get("board_strength") or {}
    if industry and isinstance(board_strength, dict):
        chg = board_strength.get(industry)
        if isinstance(chg, (int, float)):
            if chg >= 5.0:
                bonus += 6
                reasons.append(f"板块{industry}涨{chg:.1f}%强势+6")
            elif chg >= 3.0:
                bonus += 4
                reasons.append(f"板块{industry}涨{chg:.1f}%+4")
            elif chg >= 1.5:
                bonus += 2
                reasons.append(f"板块{industry}涨{chg:.1f}%+2")
            elif chg <= -2.5:
                bonus -= 3
                reasons.append(f"板块{industry}跌{chg:.1f}%-3")

    return bonus, reasons


def vol_ratio_with_baseline(
    volume: pd.Series,
    t: int,
) -> Tuple[Optional[float], Optional[float]]:
    """同时计算 5 日量比与 20 日量比。

    20 日量比用于校验"5 日缩量调整后小放量"的假信号——
    若 5 日量比看起来很大，但 20 日量比仍 < 1，说明只是相对前 5 天放量，
    和真正爆量不是一回事。
    """
    if volume is None or volume.empty or t < 5 or pd.isna(volume.iloc[t]):
        return None, None
    cur = float(volume.iloc[t])
    if cur <= 0:
        return None, None

    prev5 = volume.iloc[max(0, t - 5):t].dropna()
    ratio5 = round(cur / float(prev5.mean()), 2) if not prev5.empty and float(prev5.mean()) > 0 else None

    prev20 = volume.iloc[max(0, t - 20):t].dropna()
    ratio20 = round(cur / float(prev20.mean()), 2) if not prev20.empty and float(prev20.mean()) > 0 else None
    return ratio5, ratio20
