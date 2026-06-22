"""跨 scorer 复用的评分调节因子。

5 个无状态函数：
- parse_full_pool: 把涨停池 DataFrame 转 records 列表
- count_pool_industries: 涨停池行业分布
- theme_bonus: AI 题材聚类热度加分
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
