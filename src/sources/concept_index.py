"""股票→概念标签反查索引。

通过 akshare 的东财 / 同花顺概念板块接口，遍历每个概念拉成份股，
反向构建 code → [concepts] 索引，写入 SQLite stock_concept_tags 表。

构建一次约 10-15 分钟（东财 ~300 概念 + 同花顺 ~370 概念）。
概念归类变化不频繁，建议周更。后续单股查询走 SQLite 索引，O(1)。
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd

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

def _fetch_ths_concept_names() -> List[str]:
    """同花顺概念名列表。返回 DataFrame 列 ['name', 'code']，这里只取 name。"""
    df = _retry_call(ak.stock_board_concept_name_ths)
    if df is None or getattr(df, "empty", True):
        return []
    name_col = "name" if "name" in df.columns else df.columns[0]
    out: List[str] = []
    seen: set = set()
    for v in df[name_col].tolist():
        s = str(v or "").strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


_THS_MEMBERS_FN = getattr(
    ak, "stock_board_concept_cons_ths", None,
) or getattr(ak, "stock_board_concept_info_ths", None)


def _fetch_ths_concept_members(name: str) -> List[str]:
    """同花顺成份股查询：优先用 cons_ths，老版本 akshare 回退 info_ths。"""
    if _THS_MEMBERS_FN is None:
        return []
    try:
        df = _retry_call(lambda: _THS_MEMBERS_FN(symbol=name))
    except Exception:
        return []
    if df is None or getattr(df, "empty", True):
        return []
    code_col = next((c for c in df.columns if c in ("代码", "股票代码", "code")), None)
    if not code_col:
        return []
    codes: List[str] = []
    for v in df[code_col].tolist():
        s = str(v or "").strip().zfill(6)
        if s and len(s) == 6 and s.isdigit():
            codes.append(s)
    return codes


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
        except Exception:
            logger.exception("拉取东财概念板块列表失败")
            em_names = []
        if em_names:
            boards, pairs = _process_source("em", em_names, _fetch_em_concept_members)
            summary["em_boards"] = boards
            summary["em_pairs"] = pairs

    # === 同花顺 ===
    if "ths" in sources and not (cancel_check and cancel_check()):
        try:
            ths_names = _fetch_ths_concept_names()
        except Exception:
            logger.exception("拉取同花顺概念板块列表失败")
            ths_names = []
        if ths_names:
            boards, pairs = _process_source("ths", ths_names, _fetch_ths_concept_members)
            summary["ths_boards"] = boards
            summary["ths_pairs"] = pairs

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
