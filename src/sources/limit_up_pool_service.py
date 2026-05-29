"""涨停池服务（参数注入模式）。

迁自 stock_data.py 的涨停池相关函数（StockDataFetcher 内 9 个方法 + 2 个
模块级 intraday 派生 helper）。

通过 fetcher 参数注入访问 StockDataFetcher 的状态字段：
- fetcher._limit_up_pool_cache / fetcher._prev_limit_up_pool_cache
- fetcher._last_pool_source / fetcher._last_prev_pool_source
- fetcher._normalize_trade_date(...) / fetcher._recent_trade_dates(...)
- fetcher._sanitize_limit_up_pool(...) / fetcher.enrich_limit_up_reason_fields(...)
- fetcher.get_intraday_data(...)

`_eastmoney_circuit_breaker_open` / `_retry_ak_call` 通过函数内 import 实现
late-binding，便于测试 monkey-patch `stock_data.*`。
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

import akshare as ak
import pandas as pd
import requests

from src.utils import parsing as _utils_parsing

_safe_float = _utils_parsing.safe_float


def normalize_sina_spot_df(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """规范新浪 spot 返回；遇到反爬页或异常结构时返回 None。"""
    if df is None or df.empty:
        return df
    if "代码" not in df.columns:
        return None
    out = df.copy()
    out["代码"] = (
        out["代码"].astype(str)
        .str.replace(r"^(sh|sz|bj)", "", regex=True)
        .str.strip().str.zfill(6)
    )
    return out


def _enrich_spot_industry_from_universe(df: pd.DataFrame) -> pd.DataFrame:
    """尽量用本地 universe 表回填行业，减少 fallback 源缺列时的影响。"""
    if df is None or df.empty or "代码" not in df.columns:
        return df
    out = df.copy()
    target_col = "所属行业"
    if target_col not in out.columns:
        out[target_col] = ""
    try:
        from stock_store import load_universe

        universe = load_universe()
    except Exception:
        universe = None
    if universe is None or universe.empty or "code" not in universe.columns:
        return out
    code_to_industry = {
        str(row["code"]).strip().zfill(6): str(row.get("industry") or "").strip()
        for _, row in universe.iterrows()
        if str(row.get("industry") or "").strip()
    }
    if not code_to_industry:
        return out
    missing_mask = out[target_col].fillna("").astype(str).str.strip() == ""
    if missing_mask.any():
        out.loc[missing_mask, target_col] = (
            out.loc[missing_mask, "代码"]
            .astype(str)
            .str.strip()
            .str.zfill(6)
            .map(code_to_industry)
            .fillna("")
        )
    return out


def normalize_tencent_spot_df(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """把腾讯全市场列表规范成现有 spot 消费方可直接使用的列。"""
    if df is None or df.empty or "code" not in df.columns:
        return None
    blank_text = pd.Series([""] * len(df), index=df.index, dtype="object")
    out = pd.DataFrame(
        {
            "代码": df["code"].astype(str).str.replace(r"^(sh|sz|bj)", "", regex=True).str.strip().str.zfill(6),
            "名称": df.get("name", blank_text).astype(str).str.strip(),
            "最新价": pd.to_numeric(df.get("zxj"), errors="coerce"),
            "涨跌额": pd.to_numeric(df.get("zd"), errors="coerce"),
            "涨跌幅": pd.to_numeric(df.get("zdf"), errors="coerce"),
            "成交量": pd.to_numeric(df.get("volume"), errors="coerce") * 100,
            "成交额": pd.to_numeric(df.get("turnover"), errors="coerce") * 10000,
            "换手率": pd.to_numeric(df.get("hsl"), errors="coerce"),
            "所属行业": "",
        }
    )
    return _enrich_spot_industry_from_universe(out)


def fetch_tencent_spot_df() -> Optional[pd.DataFrame]:
    """腾讯全市场排行接口，分页拉全量 A 股，作为新浪失效后的兜底。"""
    url = "https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList"
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Referer": "https://stockapp.finance.qq.com/mstats/",
            "Accept": "application/json, text/plain, */*",
        }
    )
    page_size = 200
    offset = 0
    total = None
    frames: List[pd.DataFrame] = []
    while total is None or offset < int(total):
        params = {
            "_appver": "11.17.0",
            "board_code": "aStock",
            "sort_type": "price",
            "direct": "down",
            "offset": str(offset),
            "count": str(page_size),
        }
        resp = session.get(url, params=params, timeout=12)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data") or {}
        rows = data.get("rank_list") or []
        if not rows:
            break
        frames.append(pd.DataFrame(rows))
        total = int(data.get("total") or 0)
        offset += page_size
    if not frames:
        return None
    return normalize_tencent_spot_df(pd.concat(frames, ignore_index=True))


# ============== 模块级 intraday 派生 helper ==============

def derive_seal_time_from_intraday(
    intraday_df: "pd.DataFrame",
    limit_up_price: float,
    tolerance_pct: float = 0.1,
) -> Optional[str]:
    """从 1min 分时找首次封板时间。

    定义：close >= limit_up_price * (1 - tolerance_pct/100) 即视为封板。
    返回 "HH:MM:SS" 字符串，无封板返回 None。
    """
    if intraday_df is None or intraday_df.empty or "close" not in intraday_df.columns:
        return None
    if "time" not in intraday_df.columns:
        return None
    if not limit_up_price or limit_up_price <= 0:
        return None
    threshold = float(limit_up_price) * (1 - float(tolerance_pct) / 100)
    for _, row in intraday_df.iterrows():
        try:
            close = float(row.get("close"))
        except (TypeError, ValueError):
            continue
        if close >= threshold:
            t = row.get("time")
            if pd.isna(t):
                continue
            # 兼容 datetime / Timestamp / str
            if hasattr(t, "strftime"):
                return t.strftime("%H:%M:%S")
            return str(t)[-8:]  # 取末 8 字符 "HH:MM:SS"
    return None


def count_intraday_breaks(
    intraday_df: "pd.DataFrame",
    limit_up_price: float,
    tolerance_pct: float = 0.1,
) -> int:
    """数封板后又跌破涨停价的次数（炸板次数）。

    定义：进入"封板状态"（close >= threshold）后又出现 close < threshold -> 1 次炸板。
    """
    if intraday_df is None or intraday_df.empty or "close" not in intraday_df.columns:
        return 0
    if not limit_up_price or limit_up_price <= 0:
        return 0
    threshold = float(limit_up_price) * (1 - float(tolerance_pct) / 100)
    breaks = 0
    state_sealed = False
    for _, row in intraday_df.iterrows():
        try:
            close = float(row.get("close"))
        except (TypeError, ValueError):
            continue
        if state_sealed:
            if close < threshold:
                breaks += 1
                state_sealed = False
        else:
            if close >= threshold:
                state_sealed = True
    return breaks


# ============== StockDataFetcher 方法迁移（参数注入）==============

def fetch_spot_with_fallback(
    fetcher,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Optional[pd.DataFrame]:
    """全市场实时行情快照，东财→新浪自动兜底。

    东财熔断或失败时，回落到 ak.stock_zh_a_spot（新浪），并把代码列
    归一化为不带 sh/sz/bj 前缀的 6 位形式。
    """
    # 在函数内 import 以支持测试 monkey-patch `stock_data.*`
    from stock_data import _eastmoney_circuit_breaker_open, _retry_ak_call

    if not _eastmoney_circuit_breaker_open():
        try:
            if log_fn:
                log_fn("全市场 spot 快照：东财...")
            return _retry_ak_call(ak.stock_zh_a_spot_em)
        except Exception as exc:
            if log_fn:
                log_fn(f"全市场 spot 东财失败: {exc}，尝试新浪兜底")
    try:
        if log_fn:
            log_fn("全市场 spot 快照：新浪兜底（约 30s）...")
        df = _enrich_spot_industry_from_universe(
            normalize_sina_spot_df(_retry_ak_call(ak.stock_zh_a_spot))
        )
        if df is not None and not df.empty:
            return df
    except Exception as exc:
        if log_fn:
            log_fn(f"全市场 spot 新浪兜底失败: {exc}，尝试腾讯兜底")
    try:
        if log_fn:
            log_fn("全市场 spot 快照：腾讯兜底...")
        df = fetch_tencent_spot_df()
        if df is not None and not df.empty:
            return df
    except Exception as exc:
        if log_fn:
            log_fn(f"全市场 spot 腾讯兜底也失败: {exc}")
    return None


def derive_limit_up_pool_from_spot(
    fetcher,
    trade_date: str,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
    prev_pool_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """从全市场 spot 派生今日涨停池（东财涨停池失败时的兜底）。

    步骤：
    1. fetch_spot_with_fallback 拿全市场快照
    2. 按代码前缀算涨停阈值（主板 10 / 创业·科创 20 / 北交所 30）
    3. 过滤涨幅 >= threshold - 0.3 的股票
    4. 用 prev_pool_df 递推连板数：昨日涨停连板=N -> 今日连板=N+1；
       昨日未涨停 -> 连板=1
    5. 合成兼容 ak.stock_zt_pool_em 列名的 DataFrame

    注：通过 fetcher._fetch_spot_with_fallback() 调用 spot 抓取，便于测试用
    `patch.object(fetcher, "_fetch_spot_with_fallback", ...)` 注入 mock。
    """
    spot = fetcher._fetch_spot_with_fallback()
    if spot is None or spot.empty:
        return pd.DataFrame()

    def _threshold_for(code: str) -> float:
        c = str(code).strip().zfill(6)
        if c.startswith(("300", "301", "688")):
            return 20.0
        if c.startswith(("8", "9")):
            # 北交所 8xx / 9xx
            return 30.0
        return 10.0

    # 标准化代码列
    spot = spot.copy()
    if "代码" in spot.columns:
        spot["代码"] = spot["代码"].astype(str).str.strip().str.zfill(6)
    if "涨跌幅" not in spot.columns:
        return pd.DataFrame()

    rows: List[Any] = []
    for _, row in spot.iterrows():
        code = str(row.get("代码", "")).strip()
        if not code:
            continue
        try:
            chg = float(row.get("涨跌幅") or 0)
        except (TypeError, ValueError):
            continue
        thresh = _threshold_for(code)
        if chg < thresh - 0.3:
            continue
        rows.append(row)
    if not rows:
        return pd.DataFrame()

    # 递推连板数：必须有昨日 pool 才能推断；否则整批 spot_fallback 拒绝产出，
    # 避免把真实 3~5 板的票当首板（boards=1）喂给 scorer。
    prev_lookup: Dict[str, int] = {}
    prev_pool_usable = (
        prev_pool_df is not None
        and not prev_pool_df.empty
        and "代码" in prev_pool_df.columns
        and "连板数" in prev_pool_df.columns
    )
    if not prev_pool_usable:
        if log_fn:
            log_fn(
                f"涨停池 {trade_date} spot_fallback 拒绝出池：昨日 pool 缺失，"
                f"无法推断连板数（避免把 N 板票当首板）"
            )
        return pd.DataFrame()

    prev_pool_df = prev_pool_df.copy()
    prev_pool_df["代码"] = prev_pool_df["代码"].astype(str).str.strip().str.zfill(6)
    for _, r in prev_pool_df.iterrows():
        c = str(r.get("代码") or "").strip()
        try:
            n = int(r.get("连板数") or 0)
        except (TypeError, ValueError):
            n = 0
        if c and n > 0:
            prev_lookup[c] = n

    out: List[Dict[str, Any]] = []
    derive_failed = 0
    for row in rows:
        code = str(row.get("代码", "")).strip()
        prev_n = prev_lookup.get(code, 0)
        # prev_lookup 命中 → 续板；未命中 → 今日新涨停，首板（boards=1）
        # 此分支只有 prev_pool 有效时才走，所以"未命中=首板"才是合理推断
        boards = prev_n + 1 if prev_n > 0 else 1

        # 派生 首次封板时间 + 炸板次数（从 1min 分时计算）
        seal_time = ""
        breaks_count = 0
        try:
            limit_up_price = _safe_float(row.get("最新价")) or 0.0
            if limit_up_price > 0:
                intraday_df = fetcher.get_intraday_data(
                    code,
                    day_offset=0,
                    target_trade_date=trade_date,
                    include_meta=False,
                )
                if intraday_df is not None and not intraday_df.empty:
                    seal_time = derive_seal_time_from_intraday(
                        intraday_df, limit_up_price, tolerance_pct=0.1,
                    ) or ""
                    breaks_count = count_intraday_breaks(
                        intraday_df, limit_up_price, tolerance_pct=0.1,
                    )
        except Exception as exc:
            derive_failed += 1
            if log_fn:
                log_fn(f"涨停池 {trade_date} {code} intraday 派生失败: {exc}")

        out.append({
            "代码": code,
            "名称": str(row.get("名称", "") or ""),
            "最新价": row.get("最新价"),
            "涨跌幅": row.get("涨跌幅"),
            "换手率": row.get("换手率"),
            "流通市值": row.get("流通市值"),
            "总市值": row.get("总市值"),
            "连板数": boards,
            "首次封板时间": seal_time,
            "最后封板时间": "",
            "炸板次数": breaks_count,
            "所属行业": str(row.get("所属行业", "") or ""),
            "涨停统计": "",
            "涨停原因": "",
        })
    if derive_failed > 0 and log_fn:
        log_fn(f"涨停池 {trade_date} intraday 派生失败 {derive_failed}/{len(rows)} 只")
    return pd.DataFrame(out)


def get_limit_up_pool(
    fetcher,
    trade_date: str,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    """获取指定日期的涨停板池。

    三级缓存：内存 → SQLite → 网络请求。
    历史日期的数据一旦入库，后续永远从本地读取。
    """
    # 在函数内 import 以支持测试 monkey-patch `stock_data.*`
    from stock_data import _eastmoney_circuit_breaker_open, _retry_ak_call

    date_key = fetcher._normalize_trade_date(trade_date)
    if not date_key:
        return pd.DataFrame()

    # 1. 内存缓存
    mem_cached = fetcher._limit_up_pool_cache.get(date_key)
    if mem_cached is not None and not mem_cached.empty:
        fetcher._last_pool_source[date_key] = "cache_memory"
        return mem_cached
    if mem_cached is not None and mem_cached.empty and log_fn:
        log_fn(f"涨停池 {date_key} 内存缓存为空，重新尝试数据源")

    # 2. SQLite 持久缓存（也做一次过滤，防止历史脏数据继续展示）
    from stock_store import load_limit_up_pool, save_limit_up_pool
    db_cached = load_limit_up_pool(date_key)
    if db_cached is not None and not db_cached.empty:
        # 缓存可能是反推/spot 派生池（无封板时间是正常的），故不按封板时间剔除
        db_cached = fetcher._sanitize_limit_up_pool(db_cached, drop_missing_seal_time=False)
        if db_cached is not None and not db_cached.empty:
            fetcher._limit_up_pool_cache[date_key] = db_cached
            fetcher._last_pool_source[date_key] = "cache_db"
            if log_fn:
                log_fn(f"涨停池 {date_key} 从本地缓存加载 {len(db_cached)} 只")
            return db_cached
        # 缓存清洗后变空：不缓存空值，继续往下走东财 / spot 兜底（含腾讯）
        if log_fn:
            log_fn(f"涨停池 {date_key} 本地缓存清洗后为空，继续尝试在线源")

    # 3. 东财涨停池接口（正常路径）
    em_ok = not _eastmoney_circuit_breaker_open()
    if em_ok:
        try:
            df = _retry_ak_call(ak.stock_zt_pool_em, date=date_key)
            if df is not None and not df.empty:
                raw_count = len(df)
                df = fetcher._sanitize_limit_up_pool(df)
                dropped = raw_count - len(df)
                fetcher._limit_up_pool_cache[date_key] = df
                save_limit_up_pool(date_key, df)
                fetcher._last_pool_source[date_key] = "eastmoney"
                if log_fn:
                    drop_note = f"，过滤 {dropped} 条脏数据" if dropped > 0 else ""
                    log_fn(f"涨停池 {date_key} 东财 {len(df)} 只{drop_note}，已保存")
                return df
            # 东财返空（非异常）：合法结果（如节假日），不触发 spot 兜底（避免无谓 30s 等）
            fetcher._last_pool_source[date_key] = "empty"
            if log_fn:
                log_fn(f"涨停池 {date_key} 东财返空，返回空（不缓存，下次重试）")
            return pd.DataFrame()
        except Exception as exc:
            if log_fn:
                log_fn(f"涨停池 {date_key} 东财失败: {exc}，尝试 spot 兜底")
    else:
        if log_fn:
            log_fn(f"涨停池 {date_key} 东财熔断中，尝试 spot 兜底")

    # 4. spot 兜底：仅在东财异常/熔断时触发，从全市场 spot 派生
    recent = fetcher._recent_trade_dates(date_key, 2)
    prev_date = recent[0] if len(recent) >= 2 else ""
    prev_pool = load_limit_up_pool(prev_date) if prev_date else None
    try:
        derived = fetcher._derive_limit_up_pool_from_spot(date_key, prev_pool_df=prev_pool)
        if derived is not None and not derived.empty:
            # spot 派生池可能拿不到封板时间（东财熔断时无分钟数据），不据此剔除
            derived = fetcher._sanitize_limit_up_pool(derived, drop_missing_seal_time=False)
            if derived is not None and not derived.empty:
                fetcher._limit_up_pool_cache[date_key] = derived
                save_limit_up_pool(date_key, derived)
                fetcher._last_pool_source[date_key] = "spot_fallback"
                if log_fn:
                    log_fn(f"涨停池 {date_key} spot 兜底 {len(derived)} 只（连板数推断自昨日 pool），已保存")
                return derived
    except Exception as exc:
        if log_fn:
            log_fn(f"涨停池 {date_key} spot 兜底失败: {exc}")

    fetcher._last_pool_source[date_key] = "empty"
    if log_fn:
        log_fn(f"涨停池 {date_key} 所有源均失败，返回空（不缓存空结果）")
    return pd.DataFrame()


def get_previous_limit_up_pool(
    fetcher,
    trade_date: str,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    """获取指定日期的昨日涨停板池。三级缓存：内存 → SQLite → 网络。"""
    from stock_data import _eastmoney_circuit_breaker_open, _retry_ak_call

    date_key = fetcher._normalize_trade_date(trade_date)
    if not date_key:
        return pd.DataFrame()

    mem_cached = fetcher._prev_limit_up_pool_cache.get(date_key)
    if mem_cached is not None and not mem_cached.empty:
        fetcher._last_prev_pool_source[date_key] = "cache_memory"
        return mem_cached
    if mem_cached is not None and mem_cached.empty and log_fn:
        log_fn(f"昨日涨停池 {date_key} 内存缓存为空，重新尝试数据源")

    from stock_store import load_limit_up_pool, save_limit_up_pool
    db_cached = load_limit_up_pool(date_key, pool_type="previous")
    if db_cached is not None and not db_cached.empty:
        fetcher._prev_limit_up_pool_cache[date_key] = db_cached
        fetcher._last_prev_pool_source[date_key] = "cache_db"
        return db_cached

    # 东财昨日涨停池接口
    em_ok = not _eastmoney_circuit_breaker_open()
    if em_ok:
        try:
            df = _retry_ak_call(ak.stock_zt_pool_previous_em, date=date_key)
            if df is not None and not df.empty:
                fetcher._prev_limit_up_pool_cache[date_key] = df
                save_limit_up_pool(date_key, df, pool_type="previous")
                fetcher._last_prev_pool_source[date_key] = "eastmoney"
                return df
            # 东财返空（非异常）：不触发 spot 兜底
            fetcher._last_prev_pool_source[date_key] = "empty"
            if log_fn:
                log_fn(f"昨日涨停池 {date_key} 东财返空，返回空")
            return pd.DataFrame()
        except Exception as exc:
            if log_fn:
                log_fn(f"昨日涨停池 {date_key} 东财失败: {exc}，尝试 spot 兜底")
    else:
        if log_fn:
            log_fn(f"昨日涨停池 {date_key} 东财熔断中，尝试 spot 兜底")

    # spot 兜底：用 date_key 当日 spot 派生，prev_pool 用 date_key 的前一交易日 pool
    try:
        recent = fetcher._recent_trade_dates(date_key, 2)
        prev_date = recent[0] if len(recent) >= 2 else ""
        prev_pool = load_limit_up_pool(prev_date) if prev_date else None
        derived = fetcher._derive_limit_up_pool_from_spot(date_key, prev_pool_df=prev_pool)
        if derived is not None and not derived.empty:
            fetcher._prev_limit_up_pool_cache[date_key] = derived
            save_limit_up_pool(date_key, derived, pool_type="previous")
            fetcher._last_prev_pool_source[date_key] = "spot_fallback"
            if log_fn:
                log_fn(f"昨日涨停池 {date_key} spot 兜底 {len(derived)} 只，已保存")
            return derived
    except Exception as exc:
        if log_fn:
            log_fn(f"昨日涨停池 {date_key} spot 兜底失败: {exc}")

    fetcher._last_prev_pool_source[date_key] = "empty"
    if log_fn:
        log_fn(f"昨日涨停池 {date_key} 所有源均失败，返回空（不缓存空结果）")
    return pd.DataFrame()


def get_pool_source(
    fetcher,
    date_key: str,
    *,
    previous: bool = False,
) -> str:
    """返回最近一次 get_limit_up_pool / get_previous_limit_up_pool 对该日期的数据来源。

    取值：
    - "cache_memory" — 内存缓存命中
    - "cache_db" — SQLite 缓存命中
    - "eastmoney" — 东财在线
    - "spot_fallback" — spot 兜底派生
    - "empty" — 所有源失败 / 东财返空
    - "unknown" — 未查询过此日期
    """
    key = str(date_key or "").strip()
    if not key:
        return "unknown"
    source_map = fetcher._last_prev_pool_source if previous else fetcher._last_pool_source
    return source_map.get(key, "unknown")


def compare_limit_up_pools(
    fetcher,
    today_date: str,
    yesterday_date: str,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """对比今日与昨日首次涨停股票的差异。

    返回:
        today_first: 今日首次涨停列表（连板数=1）
        yesterday_first: 昨日首次涨停列表（昨日连板数=1）
        new_codes: 今日新增的首板股票代码（昨日未涨停）
        continued_codes: 昨日首板今日继续涨停的代码
        lost_codes: 昨日首板今日未涨停的代码
        industry_today: 今日首板行业分布
        industry_yesterday: 昨日首板行业分布
        industry_new: 今日新增首板行业分布
        summary: 文字总结
    """
    if log_fn:
        log_fn(f"正在获取涨停池对比数据: 今日={today_date}, 昨日={yesterday_date}")

    today_pool = fetcher.get_limit_up_pool(today_date)
    prev_pool = fetcher.get_previous_limit_up_pool(today_date)
    yesterday_pool = fetcher.get_limit_up_pool(yesterday_date)

    result: Dict[str, Any] = {
        "today_date": today_date,
        "yesterday_date": yesterday_date,
        "today_pool_count": len(today_pool),
        "yesterday_pool_count": len(yesterday_pool),
        "today_first": [],
        "yesterday_first": [],
        "new_codes": [],
        "continued_codes": [],
        "lost_codes": [],
        "industry_today": {},
        "industry_yesterday": {},
        "industry_new": {},
        "summary": "",
    }

    # ---- 今日首板：连板数=1 的股票 ----
    today_first_df = pd.DataFrame()
    if not today_pool.empty and "连板数" in today_pool.columns:
        today_first_df = today_pool[today_pool["连板数"] == 1].copy()
        result["today_first"] = pool_to_records(today_first_df, "today")
        fetcher.enrich_limit_up_reason_fields(result["today_first"], today_date)

    # ---- 昨日首板：从昨日涨停池中取连板数=1 的 ----
    yesterday_first_df = pd.DataFrame()
    if not yesterday_pool.empty and "连板数" in yesterday_pool.columns:
        yesterday_first_df = yesterday_pool[yesterday_pool["连板数"] == 1].copy()
        result["yesterday_first"] = pool_to_records(yesterday_first_df, "yesterday")
        fetcher.enrich_limit_up_reason_fields(result["yesterday_first"], yesterday_date)

    # ---- 对比：新增 / 延续 / 流失 ----
    today_codes = set()
    if not today_first_df.empty and "代码" in today_first_df.columns:
        today_codes = set(today_first_df["代码"].astype(str).str.strip().str.zfill(6))

    yesterday_codes = set()
    if not yesterday_first_df.empty and "代码" in yesterday_first_df.columns:
        yesterday_codes = set(yesterday_first_df["代码"].astype(str).str.strip().str.zfill(6))

    # 昨日首板今日继续涨停（不限于首板，包括晋级二板）
    today_all_codes = set()
    if not today_pool.empty and "代码" in today_pool.columns:
        today_all_codes = set(today_pool["代码"].astype(str).str.strip().str.zfill(6))

    result["new_codes"] = sorted(today_codes - yesterday_codes)
    result["continued_codes"] = sorted(yesterday_codes & today_all_codes)
    result["lost_codes"] = sorted(yesterday_codes - today_all_codes)

    # ---- 行业分布 ----
    result["industry_today"] = count_industry(today_first_df)
    result["industry_yesterday"] = count_industry(yesterday_first_df)
    # 新增首板的行业分布
    if result["new_codes"] and not today_first_df.empty and "代码" in today_first_df.columns:
        new_set = set(result["new_codes"])
        new_df = today_first_df[today_first_df["代码"].astype(str).str.strip().str.zfill(6).isin(new_set)]
        result["industry_new"] = count_industry(new_df)

    # ---- 昨日首板今日表现（从 previous pool 取） ----
    yesterday_first_today_perf = []
    if not prev_pool.empty and "代码" in prev_pool.columns and yesterday_codes:
        prev_pool_codes = prev_pool.copy()
        prev_pool_codes["_code"] = prev_pool_codes["代码"].astype(str).str.strip().str.zfill(6)
        match = prev_pool_codes[prev_pool_codes["_code"].isin(yesterday_codes)]
        if not match.empty:
            for row in match.to_dict("records"):
                code = str(row.get("代码", "") or "").strip().zfill(6)
                yesterday_first_today_perf.append({
                    "code": code,
                    "name": str(row.get("名称", "") or ""),
                    "change_pct": float(row["涨跌幅"]) if pd.notna(row.get("涨跌幅")) else None,
                    "close": float(row["最新价"]) if pd.notna(row.get("最新价")) else None,
                    "still_limit_up": code in today_all_codes,
                })
    result["yesterday_first_today_performance"] = yesterday_first_today_perf

    # ---- 文字总结 ----
    lines = []
    lines.append(f"今日({today_date}) 涨停 {result['today_pool_count']} 只，首板 {len(result['today_first'])} 只")
    lines.append(f"昨日({yesterday_date}) 涨停 {result['yesterday_pool_count']} 只，首板 {len(result['yesterday_first'])} 只")
    lines.append(f"今日新增首板: {len(result['new_codes'])} 只")
    lines.append(f"昨日首板今日继续涨停(含晋级): {len(result['continued_codes'])} 只")
    lines.append(f"昨日首板今日未涨停: {len(result['lost_codes'])} 只")
    if result["industry_today"]:
        top3 = sorted(result["industry_today"].items(), key=lambda x: -x[1])[:3]
        lines.append(f"今日首板 TOP3 行业: {'、'.join(f'{k}({v})' for k, v in top3)}")
    if result["industry_yesterday"]:
        top3 = sorted(result["industry_yesterday"].items(), key=lambda x: -x[1])[:3]
        lines.append(f"昨日首板 TOP3 行业: {'、'.join(f'{k}({v})' for k, v in top3)}")
    if yesterday_codes:
        rate = len(result['continued_codes']) / len(yesterday_codes) * 100
        lines.append(f"昨日首板晋级率: {rate:.1f}%")
    result["summary"] = "\n".join(lines)

    if log_fn:
        log_fn(result["summary"])
    return result


def compare_limit_up_pools_window(
    fetcher,
    today_date: str,
    compare_days: int = 2,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    window_days = max(2, int(compare_days or 2))
    trade_dates = fetcher._recent_trade_dates(today_date, window_days)
    if len(trade_dates) < 2:
        fallback_today = fetcher._normalize_trade_date(today_date)
        fallback_prev = fetcher._recent_trade_dates(today_date, 2)
        if len(fallback_prev) >= 2:
            trade_dates = fallback_prev
        elif fallback_today:
            trade_dates = [fallback_today, fallback_today]
        else:
            trade_dates = []
    if len(trade_dates) < 2:
        return {
            "today_date": str(today_date or ""),
            "yesterday_date": "",
            "compare_days": 0,
            "trade_dates": [],
            "daily_stats": [],
            "summary": "未能解析有效交易日范围",
        }

    # get_limit_up_pool 已有三级缓存（内存→SQLite→网络），直接调用即可
    result = fetcher.compare_limit_up_pools(trade_dates[-1], trade_dates[-2])

    daily_stats: List[Dict[str, Any]] = []
    for trade_date in trade_dates:
        pool_df = fetcher.get_limit_up_pool(trade_date)  # 命中缓存，不会重复请求
        first_df = pd.DataFrame()
        if not pool_df.empty and "连板数" in pool_df.columns:
            first_df = pool_df[pool_df["连板数"] == 1].copy()
        industry_top = sorted(count_industry(first_df).items(), key=lambda x: -x[1])[:3]
        daily_stats.append({
            "trade_date": trade_date,
            "pool_count": int(len(pool_df)),
            "first_count": int(len(first_df)),
            "top_industries": industry_top,
        })

    first_counts = [item["first_count"] for item in daily_stats]
    avg_first = sum(first_counts) / len(first_counts) if first_counts else 0.0
    max_day = max(daily_stats, key=lambda item: item["first_count"]) if daily_stats else None
    min_day = min(daily_stats, key=lambda item: item["first_count"]) if daily_stats else None
    latest_delta = 0
    if len(daily_stats) >= 2:
        latest_delta = int(daily_stats[-1]["first_count"] - daily_stats[-2]["first_count"])

    summary_lines = [result.get("summary", "")]
    summary_lines.append("")
    summary_lines.append(f"最近 {len(trade_dates)} 个交易日首板概览:")
    for item in daily_stats:
        industries_text = "、".join(f"{name}({count})" for name, count in item["top_industries"]) or "-"
        summary_lines.append(
            f"{item['trade_date']}: 涨停 {item['pool_count']} 只，首板 {item['first_count']} 只，TOP行业 {industries_text}"
        )
    summary_lines.append(f"近{len(trade_dates)}日首板均值: {avg_first:.1f} 只")
    if max_day is not None and min_day is not None:
        summary_lines.append(
            f"首板高点/低点: {max_day['trade_date']} ({max_day['first_count']}只) / "
            f"{min_day['trade_date']} ({min_day['first_count']}只)"
        )
    if len(daily_stats) >= 2:
        sign = "+" if latest_delta > 0 else ""
        summary_lines.append(f"今日较前一交易日首板变化: {sign}{latest_delta} 只")

    result["compare_days"] = len(trade_dates)
    result["trade_dates"] = trade_dates
    result["daily_stats"] = daily_stats
    result["summary"] = "\n".join(line for line in summary_lines if line is not None)
    return result


def pool_to_records(df: pd.DataFrame, tag: str) -> List[Dict[str, Any]]:
    """将涨停池 DataFrame 转为标准记录列表。

    把 iterrows 换成 `to_dict("records")`：pandas 一次性向量化拷贝成纯 dict，
    循环里只做字段取值/类型转换，CPU 开销比 iterrows 明显低。
    """
    if df.empty:
        return []

    def _opt_float(v: Any) -> Optional[float]:
        return float(v) if pd.notna(v) else None

    def _opt_int(v: Any) -> int:
        return int(v) if pd.notna(v) else 0

    raw_rows = df.to_dict("records")
    records: List[Dict[str, Any]] = []
    for row in raw_rows:
        rec: Dict[str, Any] = {
            "code": str(row.get("代码", "") or "").strip().zfill(6),
            "name": str(row.get("名称", "") or ""),
            "change_pct": _opt_float(row.get("涨跌幅")),
            "close": _opt_float(row.get("最新价")),
            "industry": str(row.get("所属行业", "") or ""),
            "amount": _opt_float(row.get("成交额")),
            "market_cap": _opt_float(row.get("流通市值")),
            "turnover": _opt_float(row.get("换手率")),
        }
        if tag == "today":
            rec["first_board_time"] = str(row.get("首次封板时间", "") or "")
            rec["last_board_time"] = str(row.get("最后封板时间", "") or "")
            rec["break_count"] = _opt_int(row.get("炸板次数"))
            rec["board_amount"] = _opt_float(row.get("封板资金"))
        records.append(rec)
    return records


def count_industry(df: pd.DataFrame) -> Dict[str, int]:
    if df.empty or "所属行业" not in df.columns:
        return {}
    counts = df["所属行业"].astype(str).value_counts().to_dict()
    return {k: int(v) for k, v in counts.items() if k and k.lower() != "nan"}
