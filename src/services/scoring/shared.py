"""跨 scorer 复用的评分调节因子。

5 个无状态函数：
- parse_full_pool: 把涨停池 DataFrame 转 records 列表
- count_pool_industries: 涨停池行业分布
- theme_bonus: AI 题材聚类热度加分
- capital_flow_bonus: 龙虎榜 + 北向资金 + 板块涨跌幅加分
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


def capital_flow_bonus(
    code: str,
    compare_context: Dict[str, Any],
    *,
    industry: str = "",
    boards: int = 0,
) -> Tuple[float, List[str]]:
    """龙虎榜 + 北向资金 + 板块涨跌幅加分（含 LHB 解读字段细分）。

    基础（按净买额）：
    - ≥5000 万净买 → +8 / >0 → +5 / ≤-3000 万 → -5 / <0 → -2

    解读细分（在基础之上叠加，最多 +8）：
    - 主力做T / 营业部接力T接 → +4（强游资接力）
    - 知名游资地区买入（西藏/宁波/上海/江苏/深圳/广东/浙江）→ +3
    - 机构买入 ≥2 家 → +5；1 家 → +3
    - 机构卖出 → -4
    - 历史成功率 ≥45% → +2；<25% → -2
    - 普通席位单独上榜 → -1（散户接力，弱信号）
      · boards>=3 时升级为 -5（高位连板没有机构/游资接力 = 见顶特征）

    北向 3 日加仓：
    - ≥5000 万 → +5；≥1000 万 → +3；≥200 万 → +1
    - ≤-3000 万 → -3；≤-500 万 → -1
    """
    bonus = 0.0
    reasons: List[str] = []

    lhb = (compare_context.get("lhb_map") or {}).get(code)
    if isinstance(lhb, dict):
        net = float(lhb.get("net_buy") or 0)
        if net >= 5e7:
            bonus += 8
            reasons.append(f"龙虎榜净买{net/1e8:.2f}亿+8")
        elif net > 0:
            bonus += 5
            reasons.append(f"龙虎榜净买{net/1e6:.0f}万+5")
        elif net <= -3e7:
            bonus -= 5
            reasons.append(f"龙虎榜净卖{net/1e8:.2f}亿-5")
        elif net < 0:
            bonus -= 2
            reasons.append(f"龙虎榜净卖{abs(net)/1e6:.0f}万-2")

        # 解读字段细分加分（仅在主力买入主导时给正向加分）
        jiedu_parsed = lhb.get("jiedu_parsed") or {}
        if isinstance(jiedu_parsed, dict):
            is_buy = jiedu_parsed.get("is_buy_dominant", False) or net > 0
            inst_buy = int(jiedu_parsed.get("institution_buy") or 0)
            inst_sell = int(jiedu_parsed.get("institution_sell") or 0)
            main_t = bool(jiedu_parsed.get("main_t_trade"))
            region = jiedu_parsed.get("hot_money_region")
            ordinary = bool(jiedu_parsed.get("ordinary_seats_only"))
            rate = jiedu_parsed.get("success_rate")

            if is_buy and main_t:
                bonus += 4
                reasons.append("主力做T接力+4")
            if is_buy and region:
                bonus += 3
                reasons.append(f"{region}游资买入+3")
            if inst_buy >= 2:
                bonus += 5
                reasons.append(f"{inst_buy}家机构买入+5")
            elif inst_buy == 1:
                bonus += 3
                reasons.append("1家机构买入+3")
            if inst_sell >= 1:
                bonus -= 4
                reasons.append(f"{inst_sell}家机构卖出-4")
            if isinstance(rate, (int, float)):
                if rate >= 45:
                    bonus += 2
                    reasons.append(f"历史成功率{rate:.0f}%+2")
                elif rate < 25:
                    bonus -= 2
                    reasons.append(f"历史成功率仅{rate:.0f}%-2")
            if is_buy and ordinary and inst_buy == 0 and not main_t and not region:
                # 高位连板还只有散户接力 = 机构/游资不愿进场，是典型见顶特征
                if boards >= 3:
                    bonus -= 5
                    reasons.append(f"{boards}连板仅普通席位接力-5")
                else:
                    bonus -= 1
                    reasons.append("普通席位接力-1")

    nb_change = (compare_context.get("northbound_map") or {}).get(code)
    if isinstance(nb_change, (int, float)):
        # 单位：万元（akshare "3日增持估计-市值" 接口）
        if nb_change >= 5000:
            bonus += 5
            reasons.append(f"北向加仓{nb_change/1e4:.1f}亿+5")
        elif nb_change >= 1000:
            bonus += 3
            reasons.append(f"北向加仓{nb_change:.0f}万+3")
        elif nb_change >= 200:
            bonus += 1
            reasons.append(f"北向小幅加仓{nb_change:.0f}万+1")
        elif nb_change <= -3000:
            bonus -= 3
            reasons.append(f"北向减仓{abs(nb_change)/1e4:.1f}亿-3")
        elif nb_change <= -500:
            bonus -= 1
            reasons.append(f"北向小幅减仓{abs(nb_change):.0f}万-1")

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
