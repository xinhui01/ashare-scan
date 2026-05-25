"""首板候选（first_board）评分 + lhb / spot / strong / pullback / northbound helpers。

10 个模块级函数（参数注入模式）：
- scan_first_board_candidates_cached: 从今日强势股 + MA5 回踩股池扫候选并按 profile 评分
- score_first_board_by_profile: 用涨停前兆画像对强势股打分
- parse_lhb_jiedu: 解析龙虎榜「解读」字段（静态纯函数）
- load_lhb_for_date: 加载指定交易日的龙虎榜数据（带缓存）
- load_industry_board_strength: 加载东财行业板块涨跌幅
- load_northbound_accumulation: 加载北向资金 3 日加仓榜
- fetch_spot_snapshot: 获取全市场实时行情快照（东财→新浪 fallback）
- parse_spot_record: 从实时行情行解析基础记录（静态纯函数）
- filter_strong_stocks: 从行情快照筛选 +3%~+9.95% 强势股
- filter_ma5_pullback_stocks: 从行情快照筛选 -5%~+3% 回踩 MA5 候选
- filter_wrap_candidate_stocks: 从行情快照筛选 -10.5%~+3% 断板反包候选（专供反包）

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn / build_local_cache_history_plan_fn。
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.services.scoring.helpers import _count_historical_any_limit_up

logger = logging.getLogger(__name__)


def _default_limit_up_threshold_pct(code: str) -> float:
    """A股各板块涨停阈值（百分比）。fallback 用，与 stock_filter._limit_up_threshold_pct 同。"""
    c = (code or "").strip()
    if c.startswith(("30", "68")):
        return 19.5
    if c.startswith(("43", "83", "87", "88", "92")):
        return 29.5
    return 9.5


def parse_lhb_jiedu(jiedu: str) -> Dict[str, Any]:
    """解析龙虎榜「解读」字段。

    返回:
      institution_buy: 机构买入家数
      institution_sell: 机构卖出家数
      main_t_trade: 是否"主力做T"
      hot_money_region: 命中的知名游资地区（西藏/宁波/上海/江苏/深圳/广东/浙江），无则 None
      ordinary_seats_only: 是否只是"普通席位"（弱信号）
      top1_dominant: 是否"买一主买"（单一席位主导）
      success_rate: 历史接力成功率 %（None=无）
      is_buy_dominant: 整体偏买入还是卖出

    迁自 StockFilter._parse_lhb_jiedu；行为零变化。
    """
    info: Dict[str, Any] = {
        "institution_buy": 0,
        "institution_sell": 0,
        "main_t_trade": False,
        "hot_money_region": None,
        "ordinary_seats_only": False,
        "top1_dominant": False,
        "success_rate": None,
        "is_buy_dominant": False,
    }
    if not jiedu:
        return info
    import re
    m = re.search(r"(\d+)家机构买入", jiedu)
    if m:
        info["institution_buy"] = int(m.group(1))
        info["is_buy_dominant"] = True
    m = re.search(r"(\d+)家机构卖出", jiedu)
    if m:
        info["institution_sell"] = int(m.group(1))
    if "主力做T" in jiedu or "营业部接力T接" in jiedu:
        info["main_t_trade"] = True
        if "买入" in jiedu or "T接" in jiedu:
            info["is_buy_dominant"] = True
    for region in ("西藏", "宁波", "上海", "江苏", "深圳", "广东", "浙江", "北京"):
        if region in jiedu and "买入" in jiedu and "卖出" not in jiedu.split(region, 1)[1][:20]:
            info["hot_money_region"] = region
            info["is_buy_dominant"] = True
            break
    if "普通席位" in jiedu:
        info["ordinary_seats_only"] = True
    if "买一主买" in jiedu:
        info["top1_dominant"] = True
        info["is_buy_dominant"] = True
    m = re.search(r"成功率(\d+(?:\.\d+)?)%", jiedu)
    if m:
        try:
            info["success_rate"] = float(m.group(1))
        except ValueError:
            pass
    return info


def load_lhb_for_date(
    trade_date: str,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, Dict[str, Any]]:
    """加载指定交易日的龙虎榜数据。

    返回 dict: code → {
        "net_buy": 净买入额,
        "buy": 买入额,
        "sell": 卖出额,
        "reason": 上榜原因,
        "jiedu": 解读原文,
        "jiedu_parsed": 解析后的 dict（机构家数/游资地区/成功率等）
    }
    网络失败/无数据返回空 dict（不阻塞预测）。

    缓存键: stock_filter_lhb_<trade_date>

    迁自 StockFilter._load_lhb_for_date；行为零变化。
    """
    from stock_store import load_app_config, save_app_config
    cache_key = f"stock_filter_lhb_{str(trade_date).strip()}"
    cached = load_app_config(cache_key, default=None)
    # 旧缓存里没有 jiedu_parsed，需要重建一次
    if isinstance(cached, dict) and cached:
        sample = next(iter(cached.values())) if cached else None
        if isinstance(sample, dict) and "jiedu_parsed" in sample:
            return cached  # type: ignore[return-value]

    try:
        import akshare as ak
        from stock_data import _retry_ak_call
        df = _retry_ak_call(
            ak.stock_lhb_detail_em,
            start_date=str(trade_date).strip(),
            end_date=str(trade_date).strip(),
        )
    except Exception as exc:
        if log_fn:
            log_fn(f"涨停预测：龙虎榜拉取失败 {exc}")
        return {}

    if df is None or df.empty:
        return {}

    result: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        try:
            code = str(row.get("代码", "")).strip().zfill(6)
            if not code or len(code) != 6:
                continue
            net = float(row.get("龙虎榜净买额") or 0)
            buy = float(row.get("龙虎榜买入额") or 0)
            sell = float(row.get("龙虎榜卖出额") or 0)
            reason = str(row.get("上榜原因") or "").strip()
            jiedu = str(row.get("解读") or "").strip()
            # 同一天可能多次上榜，累加金额并保留首次解读
            if code in result:
                result[code]["net_buy"] += net
                result[code]["buy"] += buy
                result[code]["sell"] += sell
            else:
                result[code] = {
                    "net_buy": net,
                    "buy": buy,
                    "sell": sell,
                    "reason": reason,
                    "jiedu": jiedu,
                    "jiedu_parsed": parse_lhb_jiedu(jiedu),
                }
        except (TypeError, ValueError):
            continue

    if result:
        try:
            save_app_config(cache_key, result)
        except Exception:
            pass
    return result


def load_industry_board_strength(
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, float]:
    """加载东财行业板块涨跌幅，识别强势板块。

    返回 dict: 行业名 → 当日涨跌幅 %

    迁自 StockFilter._load_industry_board_strength；行为零变化。
    """
    from stock_store import load_app_config, save_app_config
    from datetime import datetime as _dt
    today_key = _dt.now().strftime("%Y%m%d_%H")  # 小时级缓存（盘中变化）
    cache_key = f"stock_filter_board_strength_{today_key}"
    cached = load_app_config(cache_key, default=None)
    if isinstance(cached, dict) and cached:
        return cached  # type: ignore[return-value]

    try:
        import akshare as ak
        from stock_data import _retry_ak_call
        df = _retry_ak_call(ak.stock_board_industry_name_em)
    except Exception as exc:
        if log_fn:
            log_fn(f"涨停预测：板块涨跌幅拉取失败 {exc}")
        return {}

    if df is None or df.empty:
        return {}

    result: Dict[str, float] = {}
    for _, row in df.iterrows():
        try:
            name = str(row.get("板块名称", "")).strip()
            chg = float(row.get("涨跌幅") or 0)
            if name:
                result[name] = chg
        except (TypeError, ValueError):
            continue

    if result:
        try:
            save_app_config(cache_key, result)
        except Exception:
            pass
    return result


def load_northbound_accumulation(
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, float]:
    """北向资金 3 日加仓榜——数据源已停更。

    港交所 2024-08-17 起把"北向资金每日成交/持股明细"改为按季度披露，
    东财 `RPT_MUTUAL_STOCK_NORTHSTA` 表自此不再产出新数据，所有公开免费
    源（akshare / 东财 datacenter / 同花顺 / 新浪）都无法获取日级数据。
    保留函数签名仅为向后兼容上游 thin delegate，永远返回空 dict。
    """
    return {}


def fetch_spot_snapshot(
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Optional[pd.DataFrame]:
    """获取全市场实时行情快照（只调一次 API）。
    优先东财，东财熔断时自动回退到新浪。

    迁自 StockFilter._fetch_spot_snapshot；行为零变化。
    """
    import akshare as ak
    from stock_data import _retry_ak_call, _eastmoney_circuit_breaker_open
    # 东财可用时优先东财
    if not _eastmoney_circuit_breaker_open():
        try:
            if log_fn:
                log_fn("涨停预测：正在获取全市场实时行情快照（东财）...")
            return _retry_ak_call(ak.stock_zh_a_spot_em)
        except Exception as e:
            if log_fn:
                log_fn(f"涨停预测：东财实时行情失败: {e}，尝试新浪备选...")
    # 新浪备选
    try:
        if log_fn:
            log_fn("涨停预测：正在获取全市场实时行情快照（新浪，约30s）...")
        df = _retry_ak_call(ak.stock_zh_a_spot)
        if df is not None and not df.empty:
            # 新浪代码带交易所前缀（如 sh600000），去掉前缀统一为纯数字
            if "代码" in df.columns:
                df["代码"] = df["代码"].astype(str).str.replace(r"^(sh|sz|bj)", "", regex=True).str.strip().str.zfill(6)
            return df
    except Exception as e2:
        if log_fn:
            log_fn(f"涨停预测：新浪实时行情也失败: {e2}")
    return None


def parse_spot_record(row, exclude_codes: set) -> Optional[Dict[str, Any]]:
    """从实时行情行中解析基础记录，返回 None 表示需跳过。

    迁自 StockFilter._parse_spot_record；行为零变化。
    """
    code = str(row.get("代码", "")).strip().zfill(6)
    if code in exclude_codes:
        return None
    name = str(row.get("名称", ""))
    if "ST" in name.upper():
        return None
    close = float(row["最新价"]) if pd.notna(row.get("最新价")) else None
    if close is None or close <= 0:
        return None
    change_pct = float(row["涨跌幅"]) if pd.notna(row.get("涨跌幅")) else None
    amount_val = float(row["成交额"]) if pd.notna(row.get("成交额")) else None
    if amount_val is not None and amount_val < 5000_0000:
        return None
    volume_val = float(row["成交量"]) if pd.notna(row.get("成交量")) else None
    turnover = float(row["换手率"]) if pd.notna(row.get("换手率")) else None
    industry = str(
        row.get("所属行业", row.get("行业", row.get("板块", ""))) or ""
    ).strip()
    return {
        "code": code, "name": name, "change_pct": change_pct,
        "close": close, "volume": volume_val, "amount": amount_val,
        "turnover": turnover, "industry": industry,
    }


def filter_strong_stocks(
    spot_df: pd.DataFrame, exclude_codes: set
) -> List[Dict[str, Any]]:
    """从行情快照中筛选涨幅 3%~9.95% 的强势股（含擦边没封板的 9.x% 票）。

    历史 K 线已统一从本地缓存读取，无需再做 top-N 截断。

    迁自 StockFilter._filter_strong_stocks；行为零变化。
    """
    records = []
    for _, row in spot_df.iterrows():
        rec = parse_spot_record(row, exclude_codes)
        if rec is None:
            continue
        chg = rec.get("change_pct")
        if chg is None or chg < 3.0 or chg >= 9.95:
            continue
        records.append(rec)
    records.sort(key=lambda x: -(x.get("change_pct") or 0))
    return records


def filter_ma5_pullback_stocks(
    spot_df: pd.DataFrame, exclude_codes: set
) -> List[Dict[str, Any]]:
    """从行情快照中筛选涨跌幅 -5%~+3% 的回踩MA5候选。

    历史 K 线已统一从本地缓存读取，无需再做 top-N 截断。

    迁自 StockFilter._filter_ma5_pullback_stocks；行为零变化。
    """
    records = []
    for _, row in spot_df.iterrows():
        rec = parse_spot_record(row, exclude_codes)
        if rec is None:
            continue
        chg = rec.get("change_pct")
        if chg is None or chg < -5.0 or chg >= 3.0:
            continue
        records.append(rec)
    records.sort(key=lambda x: -(x.get("amount") or 0))
    return records


def filter_wrap_candidate_stocks(
    spot_df: pd.DataFrame, exclude_codes: set
) -> List[Dict[str, Any]]:
    """筛选"断板反包"候选 T0 形态池（chg ∈ [-10.5%, +3%)），专供反包评分。

    回测口径（91185 个 T0 事件，T+1 反包基线 4.42%）：
      T0 ∈ [-10.5%, -5%)   硬阴线，反包率 6.06-6.53%
      T0 ∈ [-5%, -3%)      小阴线，反包率 4.85%
      T0 ∈ [-3%, +3%)      消化区，反包率 3.10-4.05%（仍可进，靠"连板数"过滤）
      T0 ∈ [-?, -10.5%)    跌停打死，反包率 2.39%   ← 砍
      T0 ∈ [+3%, +9.95%)   强势上涨，不算反包形态（归 trend/fresh）  ← 砍

    精度由下游 score_broken_board_wrap 的"前置连板数 ≥2"硬性条件保证：
    1 板反包率仅 3.97%，2 板 6.53%，3 板 7.92%，≥4 板 8.80%，每板近线性提升。
    """
    records = []
    for _, row in spot_df.iterrows():
        rec = parse_spot_record(row, exclude_codes)
        if rec is None:
            continue
        chg = rec.get("change_pct")
        if chg is None or chg < -10.5 or chg >= 3.0:
            continue
        records.append(rec)
    records.sort(key=lambda x: -(x.get("amount") or 0))
    return records


def score_first_board_by_profile(
    rec: Dict[str, Any],
    hot_industries: Dict[str, int],
    profile: Dict[str, Any],
    *,
    fetcher,
    log_fn: Optional[Callable[[str], None]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
) -> Dict[str, Any]:
    """用涨停前兆画像对强势股打分。

    核心思路：把当前股票的特征和画像中涨停股 T-1 日特征对比，
    越接近画像中位数/均值的，得分越高。

    迁自 StockFilter._score_first_board_by_profile；2026-05-21 加入"股性活跃度"加分。
    """
    threshold_fn = limit_up_threshold_pct_fn or _default_limit_up_threshold_pct

    code = rec["code"]
    name = rec.get("name", "")
    score = 0.0
    reasons: List[str] = []
    change_pct = rec.get("change_pct", 0)
    turnover = rec.get("turnover")

    # 当日涨幅
    if change_pct is not None:
        if change_pct >= 8:
            score += 18
            reasons.append(f"涨{change_pct:.1f}%接近涨停+18")
        elif change_pct >= 6:
            score += 12
            reasons.append(f"涨{change_pct:.1f}%+12")
        elif change_pct >= 3:
            score += 6
            reasons.append(f"涨{change_pct:.1f}%+6")

    # 获取历史数据计算特征（已预取到缓存，直接读取）
    try:
        # 只使用本地缓存，不发起网络请求
        request_plan = (
            build_local_cache_history_plan_fn(reason="predict-first-board-cache-only")
            if build_local_cache_history_plan_fn is not None
            else None
        )
        history = fetcher.get_history_data(
            code, days=120, force_refresh=False,
            request_plan=request_plan,
        )
    except Exception as exc:
        logger.debug("预测首板获取历史 %s 失败: %s", code, exc)
        history = None

    industry = ""
    vol_ratio = None
    position_60d = None
    trend_10d = None
    ma_bullish = False

    if history is not None and not history.empty and len(history) >= 10:
        df = history.sort_values("date").reset_index(drop=True)
        close = pd.to_numeric(df["close"], errors="coerce")
        volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
        amount = pd.to_numeric(df.get("amount"), errors="coerce") if "amount" in df.columns else pd.Series(dtype=float)
        latest_close = float(close.iloc[-1]) if not pd.isna(close.iloc[-1]) else None
        t = len(df) - 1  # 当前最新一行

        ma5 = close.rolling(5, min_periods=5).mean()
        ma10 = close.rolling(10, min_periods=10).mean()
        ma20 = close.rolling(20, min_periods=20).mean()
        ma5_val = float(ma5.iloc[t]) if not pd.isna(ma5.iloc[t]) else None
        ma10_val = float(ma10.iloc[t]) if not pd.isna(ma10.iloc[t]) else None
        ma20_val = float(ma20.iloc[t]) if not pd.isna(ma20.iloc[t]) else None

        # --- 量比匹配 ---
        if len(volume) >= 6 and not pd.isna(volume.iloc[t]):
            vol_window = volume.iloc[max(0, t - 5):t].dropna()
            if not vol_window.empty and float(vol_window.mean()) > 0:
                vol_ratio = round(float(volume.iloc[t]) / float(vol_window.mean()), 2)
                p = profile.get("vol_ratio_t1", {})
                p_med = p.get("median")
                p_p25 = p.get("p25")
                p_p75 = p.get("p75")
                if p_med is not None and p_p25 is not None and p_p75 is not None:
                    if p_p25 <= vol_ratio <= p_p75:
                        score += 15
                        reasons.append(f"量比{vol_ratio:.1f}x吻合画像[{p_p25:.1f}~{p_p75:.1f}]+15")
                    elif vol_ratio >= p_med * 0.6:
                        score += 8
                        reasons.append(f"量比{vol_ratio:.1f}x接近画像+8")
                elif vol_ratio >= 1.5:
                    score += 8
                    reasons.append(f"放量{vol_ratio:.1f}x+8")

        # --- 额比匹配 ---
        if len(amount) >= 6 and not pd.isna(amount.iloc[t]):
            amt_window = amount.iloc[max(0, t - 5):t].dropna()
            if not amt_window.empty and float(amt_window.mean()) > 0:
                amt_ratio = round(float(amount.iloc[t]) / float(amt_window.mean()), 2)
                p = profile.get("amt_ratio_t1", {})
                p_med = p.get("median")
                if p_med is not None and amt_ratio >= p_med * 0.8:
                    score += 5
                    reasons.append(f"额比{amt_ratio:.1f}x匹配+5")

        # --- 均线匹配 ---
        if ma5_val is not None and ma10_val is not None and ma20_val is not None:
            if ma5_val > ma10_val > ma20_val:
                ma_bullish = True
                p_bull = profile.get("ma_bullish", {})
                if p_bull.get("ratio", 0) >= 50:
                    score += 10
                    reasons.append(f"多头排列(画像{p_bull['ratio']:.0f}%)+10")
                else:
                    score += 5
                    reasons.append("多头排列+5")

        # 站上MA5
        if latest_close is not None and ma5_val is not None and latest_close > ma5_val:
            p_above = profile.get("above_ma5", {})
            if p_above.get("ratio", 0) >= 60:
                score += 5
                reasons.append(f"站上MA5(画像{p_above['ratio']:.0f}%)+5")

        # --- MA5 距离匹配 ---
        if latest_close and ma5_val and ma5_val > 0:
            dist_ma5 = round((latest_close / ma5_val - 1) * 100, 2)
            p = profile.get("dist_ma5_pct", {})
            p_p25 = p.get("p25")
            p_p75 = p.get("p75")
            if p_p25 is not None and p_p75 is not None:
                if p_p25 <= dist_ma5 <= p_p75:
                    score += 5
                    reasons.append(f"距MA5 {dist_ma5:+.1f}%吻合+5")

        # --- 回踩MA5检测 ---
        # 收盘接近或略低于MA5（-3%~+1%），且前几日曾站上MA5
        if latest_close and ma5_val and ma5_val > 0:
            dist_ma5_now = (latest_close / ma5_val - 1) * 100
            if -3.0 <= dist_ma5_now <= 1.0:
                was_above_ma5 = False
                for lb in range(2, min(6, t + 1)):
                    idx_b = t - lb
                    if idx_b >= 0 and not pd.isna(close.iloc[idx_b]) and not pd.isna(ma5.iloc[idx_b]):
                        if float(close.iloc[idx_b]) > float(ma5.iloc[idx_b]) * 1.01:
                            was_above_ma5 = True
                            break
                if was_above_ma5:
                    # 回踩MA5，这是涨停前常见形态
                    p_pb = profile.get("ma5_pullback", {})
                    pb_ratio = p_pb.get("ratio", 0)
                    if pb_ratio >= 20:
                        score += 15
                        reasons.append(f"回踩MA5(画像{pb_ratio:.0f}%)+15")
                    else:
                        score += 10
                        reasons.append(f"回踩MA5(距{dist_ma5_now:+.1f}%)+10")

        # --- 60日位置匹配 ---
        if len(close) >= 20 and latest_close is not None:
            window = close.tail(min(60, len(close))).dropna()
            if len(window) >= 10:
                position_60d = round(float((window < latest_close).sum()) / len(window) * 100, 1)
                p = profile.get("position_60d", {})
                p_med = p.get("median")
                p_p25 = p.get("p25")
                p_p75 = p.get("p75")
                if p_med is not None and p_p25 is not None and p_p75 is not None:
                    if p_p25 <= position_60d <= p_p75:
                        score += 8
                        reasons.append(f"位置{position_60d:.0f}%吻合画像[{p_p25:.0f}~{p_p75:.0f}]+8")
                    elif position_60d < 30:
                        score += 5
                        reasons.append(f"低位{position_60d:.0f}%+5")

        # --- 10日趋势 ---
        if t >= 10 and not pd.isna(close.iloc[t - 10]) and close.iloc[t - 10] > 0:
            trend_10d = round((float(close.iloc[t]) / float(close.iloc[t - 10]) - 1) * 100, 1)

        # --- 缩量蓄势匹配 ---
        if len(volume) >= 6:
            vol_3 = volume.iloc[max(0, t - 3):t].dropna()
            vol_5 = volume.iloc[max(0, t - 5):t].dropna()
            if not vol_3.empty and not vol_5.empty and float(vol_5.mean()) > 0:
                shrink = round(float(vol_3.mean()) / float(vol_5.mean()), 2)
                p = profile.get("shrink_ratio_t1", {})
                p_med = p.get("median")
                if p_med is not None and shrink <= p_med and vol_ratio is not None and vol_ratio >= 1.5:
                    score += 10
                    reasons.append(f"缩量蓄势后放量(缩{shrink:.2f}/量比{vol_ratio:.1f}x)+10")

    # 板块热度
    if industry and hot_industries.get(industry, 0) >= 3:
        score += 10
        reasons.append(f"热门板块({hot_industries[industry]}只)+10")
    elif industry and hot_industries.get(industry, 0) >= 2:
        score += 5
        reasons.append(f"板块有{hot_industries[industry]}只+5")

    # 换手率
    if turnover is not None:
        p = profile.get("turnover_t1", {})
        p_p25 = p.get("p25")
        p_p75 = p.get("p75")
        if p_p25 is not None and p_p75 is not None:
            if p_p25 <= turnover <= p_p75:
                score += 5
                reasons.append(f"换手{turnover:.1f}%吻合画像+5")
        elif 3 <= turnover <= 20:
            score += 3
            reasons.append(f"换手{turnover:.1f}%适中+3")
        if turnover > 40:
            score -= 5
            reasons.append(f"换手{turnover:.1f}%过高-5")

    # 股性活跃度（近 60 日任意涨停次数）：有涨停记录的股更易再次涨停，僵尸股惩罚
    if history is not None and not history.empty:
        occ_count, last_hit_days = _count_historical_any_limit_up(
            history, code, lookback_days=60, threshold_fn=threshold_fn,
        )
        if occ_count >= 5:
            stock_bonus, label = 6, "妖股性"
        elif occ_count >= 3:
            stock_bonus, label = 4, "股性活跃"
        elif occ_count >= 1:
            stock_bonus, label = 2, "曾涨停"
        else:
            stock_bonus, label = -3, "僵尸股"
        if stock_bonus > 0 and last_hit_days is not None and last_hit_days <= 20:
            stock_bonus = min(stock_bonus + 1, 6)
            reasons.append(f"近60日{occ_count}次涨停{label}(最近{last_hit_days}日){stock_bonus:+d}")
        elif stock_bonus > 0:
            reasons.append(f"近60日{occ_count}次涨停{label}{stock_bonus:+d}")
        else:
            reasons.append(f"近60日无涨停{label}{stock_bonus:+d}")
        score += stock_bonus

    final_score = max(0, min(100, int(round(score))))
    return {
        "code": code,
        "name": name,
        "industry": industry,
        "close": rec.get("close"),
        "change_pct": change_pct,
        "turnover": turnover,
        "vol_ratio": vol_ratio,
        "position_60d": position_60d,
        "trend_10d": trend_10d,
        "ma_bullish": ma_bullish,
        "score": final_score,
        "reasons": " / ".join(reasons[:8]),
        "predict_type": "首板候选",
    }


def scan_first_board_candidates_cached(
    today_pool_df: pd.DataFrame,
    hot_industries: Dict[str, int],
    profile: Dict[str, Any],
    spot_df: Optional[pd.DataFrame],
    zt_codes: set,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    *,
    fetcher,
    log_fn: Optional[Callable[[str], None]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
) -> List[Dict[str, Any]]:
    """用画像匹配候选股（行情和历史数据均已提前缓存）。

    迁自 StockFilter._scan_first_board_candidates_cached；行为零变化。
    """
    if spot_df is None or spot_df.empty:
        return []

    strong_stocks = filter_strong_stocks(spot_df, zt_codes)
    ma5_pullback_stocks = filter_ma5_pullback_stocks(spot_df, zt_codes)

    seen_codes = set()
    merged: List[Dict[str, Any]] = []
    for rec in strong_stocks:
        if rec["code"] not in seen_codes:
            seen_codes.add(rec["code"])
            merged.append(rec)
    for rec in ma5_pullback_stocks:
        if rec["code"] not in seen_codes:
            seen_codes.add(rec["code"])
            merged.append(rec)

    if not merged:
        return []

    if log_fn:
        log_fn(f"涨停预测：强势股 {len(strong_stocks)} 只 + 回踩MA5 {len(ma5_pullback_stocks)} 只，"
               f"合并去重后 {len(merged)} 只")

    # 历史数据已在阶段3统一预取，这里直接评分
    candidates = []
    total = len(merged)
    for idx, rec in enumerate(merged):
        score_info = score_first_board_by_profile(
            rec, hot_industries, profile,
            fetcher=fetcher,
            log_fn=log_fn,
            build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
            limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
        )
        if score_info["score"] >= 50:
            candidates.append(score_info)
        if progress_callback:
            progress_callback(idx + 1, total, f"首板匹配 {rec['code']} {rec.get('name', '')}")

    candidates.sort(key=lambda x: -x["score"])
    return candidates[:50]
