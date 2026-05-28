"""首板候选（first_board）评分 + spot / strong / pullback helpers。

模块级函数（参数注入模式）：
- scan_first_board_candidates_cached: 从今日强势股 + MA5 回踩股池扫候选并按 profile 评分
- score_first_board_by_profile: 用涨停前兆画像对强势股打分
- load_industry_board_strength: 加载东财行业板块涨跌幅（实时）
- load_industry_board_strength_for_date: 加载历史日各行业涨跌幅（按日缓存）
- load_industry_board_strength_for_date_ths: 同花顺历史日 K 估算行业涨跌（EM 死时 fallback）
- backfill_universe_industries: 一次性回填 universe.industry 字段（同花顺成分股 scrape）
- backfill_universe_industries_baostock: 一次性回填 universe.industry（Baostock 证监会行业，单接口拉全市场，比 THS scrape 稳）
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
from src.sources.limit_up_pool_service import (
    fetch_tencent_spot_df,
    normalize_sina_spot_df,
    _enrich_spot_industry_from_universe,
)

logger = logging.getLogger(__name__)


def _default_limit_up_threshold_pct(code: str) -> float:
    """A股各板块涨停阈值（百分比）。fallback 用，与 stock_filter._limit_up_threshold_pct 同。"""
    c = (code or "").strip()
    if c.startswith(("30", "68")):
        return 19.5
    if c.startswith(("43", "83", "87", "88", "92")):
        return 29.5
    return 9.5


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


def load_industry_board_strength_for_date(
    trade_date: str,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, float]:
    """历史日各东财行业板块涨跌幅 (% mean across industries).

    `ak.stock_board_industry_hist_em(symbol=行业名, start_date=date, end_date=date)`
    每行业一次调用。结果按 trade_date 缓存到 app_config。

    网络挂 / 数据空时返回空 dict，调用方应当兜底（合成 spot 聚合）。
    """
    from stock_store import load_app_config, save_app_config
    td = str(trade_date or "").strip()
    if not td:
        return {}
    cache_key = f"stock_filter_board_strength_history_{td}"
    cached = load_app_config(cache_key, default=None)
    if isinstance(cached, dict) and cached:
        return {str(k): float(v) for k, v in cached.items()}

    try:
        import akshare as ak
        from stock_data import _retry_ak_call
        names_df = _retry_ak_call(ak.stock_board_industry_name_em)
    except Exception as exc:
        if log_fn:
            log_fn(f"涨停预测[历史]：拉东财行业列表失败 {exc}")
        return {}
    if names_df is None or names_df.empty or "板块名称" not in names_df.columns:
        return {}

    result: Dict[str, float] = {}
    industry_names = [str(n).strip() for n in names_df["板块名称"].tolist() if str(n).strip()]
    for idx, name in enumerate(industry_names):
        try:
            import akshare as ak
            from stock_data import _retry_ak_call
            df = _retry_ak_call(
                ak.stock_board_industry_hist_em,
                symbol=name,
                start_date=td,
                end_date=td,
                period="日k",
                adjust="",
            )
        except Exception:
            continue
        if df is None or df.empty or "涨跌幅" not in df.columns:
            continue
        try:
            chg = float(df.iloc[0]["涨跌幅"])
        except (TypeError, ValueError):
            continue
        result[name] = round(chg, 2)
        if log_fn and (idx + 1) % 20 == 0:
            log_fn(f"涨停预测[历史]：行业板块强度进度 {idx + 1}/{len(industry_names)}")

    if result:
        try:
            save_app_config(cache_key, result)
        except Exception:
            pass
    return result


def load_industry_board_strength_for_date_ths(
    trade_date: str,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, float]:
    """同花顺历史日各行业涨跌幅 — 当东财死掉时的二级 fallback。

    用 ``ak.stock_board_industry_index_ths(symbol=行业名, start_date, end_date)``
    每行业拉一根日 K，按 (close/open - 1) * 100 估算当日涨跌幅。

    注意命名差异：THS 行业名跟东财不一致（"半导体" vs "半导体及元件"），
    下游 ``board_strength.get(industry)`` 会因 EM 命名而 lookup miss 一部分。
    所以这只是"东财死时让日志至少有板块强度可看"的兜底，不保证完美匹配。

    缓存键: stock_filter_board_strength_ths_<trade_date>
    """
    from stock_store import load_app_config, save_app_config
    td = str(trade_date or "").strip()
    if not td:
        return {}
    cache_key = f"stock_filter_board_strength_ths_{td}"
    cached = load_app_config(cache_key, default=None)
    if isinstance(cached, dict) and cached:
        return {str(k): float(v) for k, v in cached.items()}

    try:
        import akshare as ak
        from stock_data import _retry_ak_call
        names_df = _retry_ak_call(ak.stock_board_industry_name_ths)
    except Exception as exc:
        if log_fn:
            log_fn(f"涨停预测[THS]：拉同花顺行业列表失败 {exc}")
        return {}
    if names_df is None or names_df.empty or "name" not in names_df.columns:
        return {}

    industry_names = [
        str(n).strip() for n in names_df["name"].tolist() if str(n).strip()
    ]
    result: Dict[str, float] = {}
    for idx, name in enumerate(industry_names):
        try:
            import akshare as ak
            from stock_data import _retry_ak_call
            df = _retry_ak_call(
                ak.stock_board_industry_index_ths,
                symbol=name,
                start_date=td,
                end_date=td,
            )
        except Exception:
            continue
        if df is None or df.empty:
            continue
        try:
            row = df.iloc[0]
            open_p = float(row["开盘价"])
            close_p = float(row["收盘价"])
            if open_p <= 0:
                continue
            # THS 没直接返回涨跌幅，按当日 open→close 估算
            chg = round((close_p / open_p - 1) * 100, 2)
            result[name] = chg
        except (TypeError, ValueError, KeyError):
            continue
        if log_fn and (idx + 1) % 20 == 0:
            log_fn(
                f"涨停预测[THS]：行业板块强度进度 {idx + 1}/{len(industry_names)}"
            )

    if result:
        try:
            save_app_config(cache_key, result)
        except Exception:
            pass
    return result


def _new_ths_session():
    """创建带 trust_env=False 的 requests Session，跨多次行业 scrape 复用。"""
    import requests
    sess = requests.Session()
    sess.trust_env = False
    sess.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    })
    return sess


def _ths_industry_constituents(
    industry_code: str,
    *,
    session=None,
    log_fn: Optional[Callable[[str], None]] = None,
    max_pages: int = 20,
    rate_limit_sec: float = 0.6,
) -> List[str]:
    """scrape 同花顺行业 detail 页面拿成分股 6 位代码。

    分页规则：
    - page=1 走主 URL ``http://q.10jqka.com.cn/thshy/detail/code/{ind_code}/``
      （建 session、拿 cookie；否则 ajax 接口直接 401）
    - page=2+ 走 ajax URL，必须带 ``X-Requested-With: XMLHttpRequest`` 头
    遇到空表 / 非 200 / 整页都是已见过的代码 三种情况终止。

    ``session=None`` 时每次新建（适合单次测试）；批量 scrape 时上层应该
    传一个复用的 session 避免 THS 把每个新连接当独立来源做风控。
    """
    import time as _time
    from bs4 import BeautifulSoup

    main_url = f"http://q.10jqka.com.cn/thshy/detail/code/{industry_code}/"
    sess = session if session is not None else _new_ths_session()

    def _extract_codes(html_text: str) -> List[str]:
        soup = BeautifulSoup(html_text, "lxml")
        out: List[str] = []
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            code_text = tds[1].get_text(strip=True)
            if len(code_text) == 6 and code_text.isdigit():
                out.append(code_text)
        return out

    codes: List[str] = []
    seen: set = set()

    for page in range(1, max_pages + 1):
        if page == 1:
            url = main_url
            # 主页面请求清掉 X-Requested-With 避免误伤
            sess.headers.pop("X-Requested-With", None)
        else:
            url = (
                f"http://q.10jqka.com.cn/thshy/detail/field/199112/order/desc/"
                f"page/{page}/ajax/1/code/{industry_code}"
            )
            sess.headers["Referer"] = main_url
            sess.headers["X-Requested-With"] = "XMLHttpRequest"

        try:
            r = sess.get(url, timeout=10)
        except Exception as exc:
            if log_fn:
                log_fn(f"补全行业：THS {industry_code} page {page} 请求失败 {exc}")
            break
        # 401 / 403 多半是临时风控，sleep 5s 重试一次再放弃
        if r.status_code in (401, 403):
            if log_fn:
                log_fn(
                    f"补全行业：THS {industry_code} page {page} HTTP {r.status_code} "
                    f"（疑似临时风控），sleep 5s 后重试..."
                )
            _time.sleep(5.0)
            try:
                r = sess.get(url, timeout=10)
            except Exception as exc:
                if log_fn:
                    log_fn(
                        f"补全行业：THS {industry_code} page {page} 重试失败 {exc}"
                    )
                break
        if r.status_code != 200:
            if log_fn:
                log_fn(
                    f"补全行业：THS {industry_code} page {page} HTTP {r.status_code}，"
                    f"放弃该行业的分页"
                )
            break

        page_codes = _extract_codes(r.text)
        if not page_codes:
            break
        new_codes = [c for c in page_codes if c not in seen]
        if not new_codes:
            break
        for c in new_codes:
            seen.add(c)
            codes.append(c)
        _time.sleep(rate_limit_sec)

    return codes


def backfill_universe_industries(
    *,
    log_fn: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, Any]:
    """一次性回填 universe.industry。

    改用同花顺一级行业（90 个）+ scrape 各行业 detail 页面分页拿成分股。
    之前用东财 ``stock_board_industry_cons_em`` 走 push2 已死，无法替代。

    NB: universe.industry 写入的是 THS 命名（"半导体" 而非"半导体及元件"），
    跟 limit_up_stock_meta.industry（EM 命名）不一致。``load_spot_snapshot_at``
    用 ``COALESCE(NULLIF(u.industry,''), m.industry, '')``，u.industry 优先，
    最终 spot 的"所属行业"列以 THS 命名为主 → 板块强度 fallback 走 THS 时
    命中率最高，走合成 spot 兜底时也是 THS 命名（自洽）。

    返回 dict: {industries, mapped_codes, updated, errors}
    """
    from stock_store import update_universe_industries
    import akshare as ak
    from stock_data import _retry_ak_call

    errors: List[str] = []
    try:
        names_df = _retry_ak_call(ak.stock_board_industry_name_ths)
    except Exception as exc:
        msg = f"拉同花顺行业列表失败: {exc}"
        if log_fn:
            log_fn(f"补全行业：{msg}")
        return {"industries": 0, "mapped_codes": 0, "updated": 0, "errors": [msg]}

    if names_df is None or names_df.empty or "code" not in names_df.columns:
        return {
            "industries": 0,
            "mapped_codes": 0,
            "updated": 0,
            "errors": ["同花顺行业列表为空"],
        }

    industries = [
        (str(row["name"]).strip(), str(row["code"]).strip())
        for _, row in names_df.iterrows()
        if str(row.get("name", "")).strip() and str(row.get("code", "")).strip()
    ]
    if log_fn:
        log_fn(
            f"补全行业：同花顺一级行业共 {len(industries)} 个，"
            f"开始 scrape 各行业 detail 页（~每页 0.15s 限速，预计 5-8 分钟）..."
        )

    code_to_industry: Dict[str, str] = {}
    # 一个 session 跨所有行业复用：保留 cookie + keep-alive，
    # 不至于 THS 把每个新连接当独立来源做风控
    shared_session = _new_ths_session()
    for idx, (name, ind_code) in enumerate(industries):
        if progress_callback:
            progress_callback(idx, len(industries), f"拉取 {name}")
        try:
            stock_codes = _ths_industry_constituents(
                ind_code, session=shared_session, log_fn=log_fn,
            )
        except Exception as exc:
            errors.append(f"{name}: {exc}")
            continue
        if not stock_codes:
            errors.append(f"{name}: 无成分股")
            continue
        for c in stock_codes:
            # 首次见到的代码保留，重复（出现在多个行业）保留首个
            code_to_industry.setdefault(c, name)
        if log_fn and (idx + 1) % 10 == 0:
            log_fn(
                f"补全行业：进度 {idx + 1}/{len(industries)}，"
                f"已累计映射 {len(code_to_industry)} 只票"
            )

    if progress_callback:
        progress_callback(len(industries), len(industries), "写入数据库")

    updated = update_universe_industries(code_to_industry)
    if log_fn:
        log_fn(
            f"补全行业：完成，覆盖 {len(industries)} 个 THS 行业，"
            f"映射 {len(code_to_industry)} 只票，DB 写入 {updated} 行"
            + (f"，{len(errors)} 个行业有问题" if errors else "")
        )
    return {
        "industries": len(industries),
        "mapped_codes": len(code_to_industry),
        "updated": updated,
        "errors": errors,
    }


def backfill_universe_industries_baostock(
    *,
    log_fn: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, Any]:
    """用 Baostock 一次性回填 universe.industry（证监会行业分类）。

    与 THS scrape 版的差异：
    - 一次 ``bs.query_stock_industry()`` 拉全市场 5500+ 票，无需逐行业爬，10s 内完成
    - 接口稳定不会 401/限流，盘后可重复运行
    - 行业命名是证监会（GB/T 4754）标准，如 "C39 计算机、通信和其他电子设备制造业"
      → 这里去掉前缀代码（"C39"）只留中文名，方便和 THS / EM 命名风格对齐
    - **CSRC 行业较粗（≈80 个一级行业），而 THS / EM 用的是更细的板块概念**；
      混用时 ``load_spot_snapshot_at`` 走 ``COALESCE(NULLIF(u.industry,''), m.industry, '')``
      universe 优先：如果先用 baostock 跑过再用 THS 跑会被 THS 覆盖（行为符合预期）

    返回 dict: {industries, mapped_codes, updated, errors}（结构与 THS 版一致，便于上层复用）
    """
    import re
    from stock_store import update_universe_industries

    errors: List[str] = []

    try:
        import baostock as bs  # type: ignore
    except ImportError as exc:
        msg = f"未安装 baostock：{exc}（pip install baostock）"
        if log_fn:
            log_fn(f"补全行业(baostock)：{msg}")
        return {"industries": 0, "mapped_codes": 0, "updated": 0, "errors": [msg]}

    if progress_callback:
        progress_callback(0, 1, "登录 Baostock")

    lg = bs.login()
    if lg.error_code != "0":
        msg = f"Baostock 登录失败：{lg.error_code} {lg.error_msg}"
        if log_fn:
            log_fn(f"补全行业(baostock)：{msg}")
        return {"industries": 0, "mapped_codes": 0, "updated": 0, "errors": [msg]}

    try:
        if progress_callback:
            progress_callback(0, 1, "拉取行业映射")
        rs = bs.query_stock_industry()
        if rs.error_code != "0":
            msg = f"query_stock_industry 失败：{rs.error_code} {rs.error_msg}"
            if log_fn:
                log_fn(f"补全行业(baostock)：{msg}")
            return {"industries": 0, "mapped_codes": 0, "updated": 0, "errors": [msg]}

        rows: List[List[str]] = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            msg = "query_stock_industry 返回空"
            if log_fn:
                log_fn(f"补全行业(baostock)：{msg}")
            return {"industries": 0, "mapped_codes": 0, "updated": 0, "errors": [msg]}

        fields = rs.fields
        code_idx = fields.index("code") if "code" in fields else 1
        ind_idx = fields.index("industry") if "industry" in fields else 3

        # 形如 "C39计算机、通信和其他电子设备制造业" → "计算机、通信和其他电子设备制造业"
        # 部分行业前缀是单字母+两位数（C39/J66），少数是单字母+一位数（A01）
        prefix_re = re.compile(r"^[A-Z]\d{1,2}\s*")

        code_to_industry: Dict[str, str] = {}
        empty_count = 0
        for row in rows:
            bs_code = str(row[code_idx]).strip()
            industry_raw = str(row[ind_idx]).strip()
            if not bs_code:
                continue
            # "sh.600000" → "600000"，"sz.000001" → "000001"，"bj.430047" → "430047"
            if "." in bs_code:
                _, digit = bs_code.split(".", 1)
            else:
                digit = bs_code
            digit = digit.strip()
            if not digit.isdigit():
                continue
            if not industry_raw:
                empty_count += 1
                continue
            industry = prefix_re.sub("", industry_raw).strip()
            if not industry:
                empty_count += 1
                continue
            code_to_industry[digit] = industry

        if log_fn:
            log_fn(
                f"补全行业(baostock)：API 返回 {len(rows)} 行，"
                f"映射 {len(code_to_industry)} 只票，{empty_count} 只无行业字段"
            )

        if progress_callback:
            progress_callback(1, 1, "写入数据库")
        updated = update_universe_industries(code_to_industry)

        # 统计有多少不同的行业名（用于报告"覆盖了多少个 CSRC 行业"）
        unique_industries = len(set(code_to_industry.values()))

        if log_fn:
            log_fn(
                f"补全行业(baostock)：完成，覆盖 {unique_industries} 个证监会行业，"
                f"映射 {len(code_to_industry)} 只票，DB 写入 {updated} 行"
            )

        return {
            "industries": unique_industries,
            "mapped_codes": len(code_to_industry),
            "updated": updated,
            "errors": errors,
        }
    finally:
        try:
            bs.logout()
        except Exception:
            pass


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
            df = _retry_ak_call(ak.stock_zh_a_spot_em)
            return _enrich_spot_industry_from_universe(df)
        except Exception as e:
            if log_fn:
                log_fn(f"涨停预测：东财实时行情失败: {e}，尝试新浪备选...")
    # 新浪备选
    try:
        if log_fn:
            log_fn("涨停预测：正在获取全市场实时行情快照（新浪，约30s）...")
        df = _enrich_spot_industry_from_universe(
            normalize_sina_spot_df(_retry_ak_call(ak.stock_zh_a_spot))
        )
        if df is not None and not df.empty:
            return df
    except Exception as e2:
        if log_fn:
            log_fn(f"涨停预测：新浪实时行情失败: {e2}，尝试腾讯兜底...")
    try:
        if log_fn:
            log_fn("涨停预测：正在获取全市场实时行情快照（腾讯）...")
        df = fetch_tencent_spot_df()
        if df is not None and not df.empty:
            return df
    except Exception as e3:
        if log_fn:
            log_fn(f"涨停预测：腾讯实时行情也失败: {e3}")
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
