"""股票→概念标签反查索引。

通过 akshare 的东财 / 同花顺概念板块接口，遍历每个概念拉成份股，
反向构建 code → [concepts] 索引，写入 SQLite stock_concept_tags 表。

构建一次约 10-15 分钟（东财 ~300 概念 + 同花顺 ~370 概念）。
概念归类变化不频繁，建议周更。后续单股查询走 SQLite 索引，O(1)。
"""
from __future__ import annotations

import time
from datetime import datetime
from functools import lru_cache
from io import StringIO
from typing import Any, Callable, Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd
import requests
from bs4 import BeautifulSoup

import stock_store
from stock_logger import get_logger

logger = get_logger(__name__)


# ============== 内部：带重试的 akshare 调用 ==============

def _retry_call(fn, retries: int = 3, base_delay: float = 0.5):
    last_exc: Optional[Exception] = None
    for i in range(retries):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            time.sleep(base_delay * (i + 1))
    if last_exc is not None:
        raise last_exc
    return None


# ============== 东财概念板块 ==============

def _fetch_em_concept_names() -> List[str]:
    """拉取东财所有概念板块名（用作 stock_board_concept_cons_em 的 symbol 入参）。"""
    df = _retry_call(ak.stock_board_concept_name_em)
    if df is None or getattr(df, "empty", True):
        return []
    # 列名常见为 "板块名称" 或 "概念名称"，兜底取首列
    candidates = [c for c in df.columns if c in ("板块名称", "概念名称")]
    name_col = candidates[0] if candidates else df.columns[0]
    out: List[str] = []
    seen: set = set()
    for v in df[name_col].tolist():
        s = str(v or "").strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _fetch_em_concept_members(name: str) -> List[str]:
    """拉取某个东财概念板块的成份股代码列表。"""
    try:
        df = _retry_call(lambda: ak.stock_board_concept_cons_em(symbol=name))
    except Exception:
        return []
    if df is None or getattr(df, "empty", True):
        return []
    code_col = next((c for c in df.columns if c in ("代码", "股票代码")), None)
    if not code_col:
        return []
    codes: List[str] = []
    for v in df[code_col].tolist():
        s = str(v or "").strip().zfill(6)
        if s and len(s) == 6 and s.isdigit():
            codes.append(s)
    return codes


# ============== 同花顺概念板块 ==============

def _normalize_stock_code(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    if len(digits) >= 6:
        digits = digits[-6:]
    else:
        digits = digits.zfill(6)
    return digits if len(digits) == 6 and digits.isdigit() else ""


def _extract_stock_codes_from_df(df: pd.DataFrame) -> List[str]:
    if df is None or getattr(df, "empty", True):
        return []
    code_col = next(
        (
            c for c in df.columns
            if str(c).strip() in ("代码", "股票代码", "code")
        ),
        None,
    )
    if code_col is None:
        return []
    out: List[str] = []
    seen: set = set()
    for v in df[code_col].tolist():
        code = _normalize_stock_code(v)
        if code and code not in seen:
            seen.add(code)
            out.append(code)
    return out


@lru_cache(maxsize=1)
def _fetch_ths_concept_name_code_map() -> Dict[str, str]:
    df = _retry_call(ak.stock_board_concept_name_ths)
    if df is None or getattr(df, "empty", True):
        return {}
    name_col = "name" if "name" in df.columns else df.columns[0]
    code_col = "code" if "code" in df.columns else (
        df.columns[1] if len(df.columns) > 1 else ""
    )
    if not code_col:
        return {}
    out: Dict[str, str] = {}
    for _, row in df.iterrows():
        name = str(row.get(name_col) or "").strip()
        code = str(row.get(code_col) or "").strip()
        if name and code:
            out[name] = code
    return out


def _fetch_ths_concept_names() -> List[str]:
    """同花顺概念名列表。返回 DataFrame 列 ['name', 'code']，这里只取 name。"""
    return list(_fetch_ths_concept_name_code_map().keys())


_THS_MEMBERS_FN = getattr(
    ak, "stock_board_concept_cons_ths", None,
)


@lru_cache(maxsize=1)
def _ths_cookie_value() -> str:
    try:
        import py_mini_racer
        from akshare.datasets import get_ths_js
        with open(get_ths_js("ths.js"), encoding="utf-8") as f:
            js_content = f.read()
        js_code = py_mini_racer.MiniRacer()
        js_code.eval(js_content)
        return str(js_code.call("v") or "")
    except Exception:
        logger.debug("生成同花顺 v cookie 失败", exc_info=True)
        return ""


def _make_ths_headers(referer: str = "") -> Dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
    }
    if referer:
        headers["Referer"] = referer
    cookie = _ths_cookie_value()
    if cookie:
        headers["Cookie"] = f"v={cookie}"
    return headers


def _extract_ths_member_codes_from_html(html: str) -> List[str]:
    if not html:
        return []
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    out: List[str] = []
    seen: set = set()
    for df in tables or []:
        for code in _extract_stock_codes_from_df(df):
            if code not in seen:
                seen.add(code)
                out.append(code)
    return out


def _extract_ths_page_count(html: str) -> int:
    try:
        soup = BeautifulSoup(html or "", features="lxml")
        node = soup.find(name="span", attrs={"class": "page_info"})
        text = node.get_text(strip=True) if node else ""
        if "/" in text:
            return max(1, int(text.split("/")[-1]))
    except Exception:
        pass
    return 1


def _fetch_ths_concept_members_from_page(name: str) -> List[str]:
    code_map = _fetch_ths_concept_name_code_map()
    concept_code = code_map.get(name) or (
        str(name).strip() if str(name).strip().isdigit() else ""
    )
    if not concept_code:
        return []
    base_url = f"http://q.10jqka.com.cn/gn/detail/code/{concept_code}/"
    try:
        r = requests.get(
            base_url,
            headers=_make_ths_headers(base_url),
            timeout=20,
        )
        if hasattr(r, "raise_for_status"):
            r.raise_for_status()
    except Exception:
        logger.debug("同花顺概念页拉取失败: %s", name, exc_info=True)
        return []
    out = _extract_ths_member_codes_from_html(getattr(r, "text", ""))
    total_pages = _extract_ths_page_count(getattr(r, "text", ""))
    for page in range(2, total_pages + 1):
        page_url = (
            f"http://q.10jqka.com.cn/gn/detail/code/{concept_code}/"
            f"page/{page}/ajax/1/"
        )
        try:
            pr = requests.get(
                page_url,
                headers=_make_ths_headers(base_url),
                timeout=20,
            )
            if hasattr(pr, "raise_for_status"):
                pr.raise_for_status()
        except Exception:
            logger.debug("同花顺概念分页拉取失败: %s page=%s", name, page, exc_info=True)
            continue
        for code in _extract_ths_member_codes_from_html(getattr(pr, "text", "")):
            if code not in out:
                out.append(code)
    return out


def _fetch_ths_concept_members(name: str) -> List[str]:
    """同花顺成份股查询；当前 akshare 缺成员函数时解析概念详情页表格。"""
    if _THS_MEMBERS_FN is not None:
        try:
            df = _retry_call(lambda: _THS_MEMBERS_FN(symbol=name))
            codes = _extract_stock_codes_from_df(df)
            if codes:
                return codes
        except Exception:
            logger.debug("akshare 同花顺概念成份接口失败: %s", name, exc_info=True)
    return _fetch_ths_concept_members_from_page(name)


# ============== 主入口：构建反查索引 ==============

def build_concept_reverse_index(
    sources: Tuple[str, ...] = ("em", "ths"),
    *,
    cancel_check: Optional[Callable[[], bool]] = None,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    flush_batch_size: int = 2000,
    prune_stale: bool = True,
) -> Dict[str, Any]:
    """构建/刷新反查索引；流式写入避免长事务。

    参数：
      sources: ("em",) / ("ths",) / ("em","ths")
      cancel_check: 返回 True 即中止
      progress_cb: (done_boards, total_boards, current_label) 进度回调
      flush_batch_size: 每累计这么多 (code,concept) 就 flush 一次到 SQLite
      prune_stale: True 时构建完成后删除该 source 下旧时间戳的记录

    返回 {em_boards, em_pairs, ths_boards, ths_pairs, total_codes,
          duration_seconds, cancelled, started_at}。
    """
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.time()
    summary: Dict[str, Any] = {
        "started_at": started_at,
        "em_boards": 0, "em_pairs": 0,
        "ths_boards": 0, "ths_pairs": 0,
        "total_codes": 0,
        "duration_seconds": 0.0,
        "cancelled": False,
        "warnings": [],
    }

    pending_rows: List[Dict[str, Any]] = []
    all_codes: set = set()

    def _flush(force: bool = False) -> None:
        if pending_rows and (force or len(pending_rows) >= flush_batch_size):
            try:
                stock_store.save_concept_tags_bulk(pending_rows)
            except Exception:
                logger.exception("flush 概念标签到 SQLite 失败")
            pending_rows.clear()

    def _process_source(
        src: str, names: List[str], fetcher: Callable[[str], List[str]],
    ) -> Tuple[int, int]:
        boards_done = 0
        pairs = 0
        total = len(names)
        for i, name in enumerate(names):
            if cancel_check and cancel_check():
                return boards_done, pairs
            try:
                codes = fetcher(name)
            except Exception:
                codes = []
            for c in codes:
                pending_rows.append({
                    "code": c,
                    "concept_name": name,
                    "source": src,
                    "updated_at": started_at,
                })
                all_codes.add(c)
                pairs += 1
            boards_done += 1
            _flush()
            if progress_cb:
                try:
                    progress_cb(i + 1, total, f"{src}:{name}")
                except Exception:
                    pass
        return boards_done, pairs

    # === 东财 ===
    if "em" in sources and not (cancel_check and cancel_check()):
        try:
            em_names = _fetch_em_concept_names()
        except Exception as exc:
            logger.exception("拉取东财概念板块列表失败")
            summary["warnings"].append(f"东财概念列表失败: {exc}")
            em_names = []
        if em_names:
            boards, pairs = _process_source("em", em_names, _fetch_em_concept_members)
            summary["em_boards"] = boards
            summary["em_pairs"] = pairs
            if boards > 0 and pairs == 0:
                summary["warnings"].append("东财成份股解析为0")
        else:
            summary["warnings"].append("东财概念列表为空")

    # === 同花顺 ===
    if "ths" in sources and not (cancel_check and cancel_check()):
        try:
            ths_names = _fetch_ths_concept_names()
        except Exception as exc:
            logger.exception("拉取同花顺概念板块列表失败")
            summary["warnings"].append(f"同花顺概念列表失败: {exc}")
            ths_names = []
        if ths_names:
            boards, pairs = _process_source("ths", ths_names, _fetch_ths_concept_members)
            summary["ths_boards"] = boards
            summary["ths_pairs"] = pairs
            if boards > 0 and pairs == 0:
                summary["warnings"].append("同花顺成份股解析为0")
        else:
            summary["warnings"].append("同花顺概念列表为空")

    _flush(force=True)

    if cancel_check and cancel_check():
        summary["cancelled"] = True

    # === 清理过期记录（只在未取消、且某 source 至少跑过一些板块时清）===
    if prune_stale and not summary["cancelled"]:
        try:
            if summary["em_boards"] > 0:
                stock_store.prune_stale_concept_tags("em", started_at)
            if summary["ths_boards"] > 0:
                stock_store.prune_stale_concept_tags("ths", started_at)
        except Exception:
            logger.exception("清理过期概念标签失败")

    summary["total_codes"] = len(all_codes)
    summary["duration_seconds"] = round(time.time() - t0, 1)
    return summary
