"""lookback 硬回测：同一批历史交易日分别用不同 lookback 重放预测，对比命中率。

用法：
    .venv/Scripts/python.exe scripts/backtest_lookback.py --limit 5 --lookbacks 5,25

安全性（重要）：
    predict_limit_up_candidates 内部会调 save_limit_up_prediction_record /
    save_last_limit_up_prediction 写库。回测会对同一天跑多次预测，若不拦截会
    直接覆盖 limit_up_prediction 表里的真实历史记录。本脚本在导入预测模块前
    先把这些写库函数替换为 no-op，保证回测**只读不写**。

命中口径与线上 prediction_accuracy_service 完全一致：
    cont 类 -> hit_strict（必须真涨停）；其余类别 -> hit_loose（涨停或开盘买收盘 >= 5%）；
    且都要求 hit_buyable（排除一字板买不到）。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_builtin_print = print


def print(*a, **k):  # noqa: A001 - 强制 flush，便于实时观察长回测进度
    k.setdefault("flush", True)
    _builtin_print(*a, **k)


def _ts() -> str:
    return time.strftime("%H:%M:%S")


# ---------------------------------------------------------------- write guard
def install_write_guards() -> Dict[str, int]:
    """把所有会落库的预测/评估写函数替换成 no-op，防止回测污染真实数据。"""
    import stock_store

    stats = {"blocked": 0}

    def _make_noop(label: str):
        def _noop(*_a, **_k):
            stats["blocked"] += 1
            return None
        _noop.__name__ = f"noop_{label}"
        return _noop

    for fn_name in (
        "save_limit_up_prediction_record",
        "save_last_limit_up_prediction",
        "save_prediction_accuracy_records",
    ):
        if hasattr(stock_store, fn_name):
            setattr(stock_store, fn_name, _make_noop(fn_name))
    return stats


def install_offline_guard() -> bool:
    """离线模式：所有 HTTP 请求立即失败，强制走本地缓存 + 产品既有降级路径。

    动机：本机到东财 push2his 的直连受网络层限制，资金流兜底走同花顺时需要
    「按 code 二分翻页」定位每只股票，几十只候选会产生数百个 HTTP 请求并大量
    超时，实测单次预测被拖到 9 分钟以上，无法完成多日回测。

    离线后：
      - 资金流缓存未命中 -> get_fund_flow_data 返回空 -> fresh.py 自动降级为
        量比口径（该降级是产品既有逻辑，见 fresh.py:233 注释）；
      - 历史 K 线预取失败 -> 使用本地 sqlite 缓存（历史 K 线本就以缓存为准）；
      - 板块强度联网失败 -> 回落到合成 spot 聚合（predict.py:1869 既有兜底）。
    两组 lookback 处于完全相同的降级条件下，横向对比依然公平。
    """
    try:
        from requests.exceptions import ConnectionError as ReqConnErr
        from requests.sessions import Session

        def _blocked(self, *_a, **_k):  # noqa: ANN001
            raise ReqConnErr("backtest offline mode: network disabled")

        Session.request = _blocked  # type: ignore[method-assign]
        return True
    except Exception:
        return False


def install_socket_timeout(seconds: float = 8.0) -> bool:
    """给所有未显式设置超时的原生 socket 兜一个默认超时。

    install_offline_guard 只能拦 requests。历史 K 线兜底源 baostock 走的是
    原生 socket，且未设超时 —— 本机网络受限时会永久阻塞（实测单次预测卡死
    19 分钟仍未返回，进程内存纹丝不动）。这里设全局默认超时作为硬兜底。
    """
    try:
        import socket

        socket.setdefaulttimeout(seconds)
        return True
    except Exception:
        return False


def install_cache_only_history() -> bool:
    """历史 K 线只读本地 sqlite，缓存 miss 直接返回空，绝不联网补拉。

    历史 K 线本就以本地缓存为准（见项目约定），而联网补拉是回测最大的时间
    黑洞。短路到缓存层后，两组 lookback 处于完全相同的数据条件下，横向对比
    依然公平。
    """
    try:
        from datetime import datetime as _dt

        import stock_data

        def _cache_only(
            self,  # noqa: ANN001
            stock_code,
            days: int = 10,
            force_refresh: bool = False,
            preferred_mirror=None,
            mirror_pool=None,
            request_plan=None,
            as_of_trade_date: str = "",
        ):
            code = str(stock_code).strip().zfill(6)
            end_date = str(as_of_trade_date or "").strip().replace("-", "")
            if len(end_date) != 8 or not end_date.isdigit():
                end_date = _dt.now().strftime("%Y%m%d")
            n = max(1, int(days or 1))
            try:
                df = stock_data._load_history_store(code, n, end_date, None)
            except Exception:
                return None
            if df is None or getattr(df, "empty", True):
                return None
            return df.tail(n).reset_index(drop=True)

        stock_data.StockDataFetcher.get_history_data = _cache_only  # type: ignore[method-assign]
        return True
    except Exception:
        return False


def install_cache_only_fundflow() -> bool:
    """资金流只读本地缓存，miss 直接返回 None（走产品既有的量比降级）。

    根因（堆栈实测）：requests 被离线补丁拦截后，同花顺 _ths_page_frame 的
    重试循环每次 except 都 time.sleep(0.3)，而 _ths_locate_code_row 要二分
    30 次定位单只股票 —— 几百只候选叠加就是几十分钟的纯 sleep。
    fresh.py:233 明确写了「缺失/失败降级为量比口径，不引入回归」，
    所以短路到缓存层是产品既有语义，且两组 lookback 条件一致。
    """
    try:
        import stock_data

        def _cache_only(
            self,  # noqa: ANN001
            stock_code,
            days: int = 5,
            force_refresh: bool = False,
            **_kw,
        ):
            code = str(stock_code).strip().zfill(6)
            try:
                cached = stock_data._load_fund_flow_store(code, min_rows=1, log=None)
            except Exception:
                return None
            if cached is None or getattr(cached, "empty", True):
                return None
            return cached.tail(max(1, int(days or 1))).reset_index(drop=True)

        stock_data.StockDataFetcher.get_fund_flow_data = _cache_only  # type: ignore[method-assign]
        return True
    except Exception:
        return False


def install_fast_relative_strength() -> bool:
    """把 relative_strength_service._normalize_history 换成向量化实现。

    原实现对每行 date 调 Python 函数 map(_date_key)/map(_date_dash)，而
    score_stock_relative_strength 会对每只候选的 120 行历史各跑一遍 —— 堆栈
    实测这是纯 CPU 热点。向量化后语义完全等价（同样是「去掉 - 和 /，要求
    恰好 8 位数字，否则置空」），只是快一到两个数量级。
    """
    try:
        import pandas as pd

        from src.services import relative_strength_service as rss

        def _normalize_history_fast(df):  # noqa: ANN001
            if df is None or getattr(df, "empty", True):
                return None
            if "date" not in df.columns or "close" not in df.columns:
                return None
            work = df[["date", "close"]].copy()
            s = (
                work["date"]
                .astype(str)
                .str.strip()
                .str.replace("-", "", regex=False)
                .str.replace("/", "", regex=False)
            )
            valid = s.str.fullmatch(r"\d{8}").fillna(False)
            key = s.where(valid, "")
            work["date_key"] = key
            work["date"] = (
                key.str.slice(0, 4) + "-" + key.str.slice(4, 6) + "-" + key.str.slice(6, 8)
            ).where(valid, "")
            work["close"] = pd.to_numeric(work["close"], errors="coerce")
            work = work.dropna(subset=["close"])
            work = work[work["date_key"].str.len() == 8]
            if work.empty:
                return None
            work = work.drop_duplicates(subset=["date_key"], keep="last")
            return work.sort_values("date_key").reset_index(drop=True)

        rss._normalize_history = _normalize_history_fast  # type: ignore[attr-defined]

        # 记忆化整个强弱评分：同一 code + 同一末日 + 同长度 + 同类别，结果确定。
        # scan_followthrough_candidates_cached 会对上千只候选逐只调用，而每次都
        # 重新归一化同一份指数历史 + 重跑 merge —— 堆栈实测这是最大的 CPU 热点。
        _orig_score = rss.score_stock_relative_strength
        _cache: Dict[Any, Dict[str, Any]] = {}

        def _score_cached(code, stock_history, index_history, *, category="", boards=0):  # noqa: ANN001
            try:
                last = str(stock_history.iloc[-1]["date"]) if len(stock_history) else ""
                key = (str(code), last, len(stock_history), str(category), int(boards or 0))
            except Exception:
                return _orig_score(
                    code, stock_history, index_history, category=category, boards=boards
                )
            if key not in _cache:
                if len(_cache) > 20000:
                    _cache.clear()
                _cache[key] = _orig_score(
                    code, stock_history, index_history, category=category, boards=boards
                )
            got = _cache[key]
            return dict(got) if isinstance(got, dict) else got

        rss.score_stock_relative_strength = _score_cached  # type: ignore[attr-defined]
        return True
    except Exception:
        return False


def preopen_em_breaker(hours: int = 24) -> bool:
    """预先打开东财熔断器，让所有东财请求立即短路。

    本机到 push2his.eastmoney.com 的直连受网络层限制（见项目记忆），回测中
    每次预测都会重试到熔断、再空等 600s 冷却，单次预测被拖到 8 分钟以上。
    线上实际运行时同样处于熔断状态，因此预置熔断反而更贴近真实环境，
    且不同 lookback 组条件一致，对比依然公平。
    """
    try:
        from src.utils.em_circuit_breaker import EMCircuitBreaker

        breaker = EMCircuitBreaker.instance()
        with breaker._lock:  # type: ignore[attr-defined]
            breaker._open_until = breaker._clock() + hours * 3600  # type: ignore[attr-defined]
        return True
    except Exception:
        return False


# ---------------------------------------------------------------- evaluation
class Evaluator:
    """按线上口径评估候选，带 history 缓存。"""

    def __init__(self) -> None:
        import stock_store
        from src.services import prediction_accuracy_service as pas

        self._store = stock_store
        self._pas = pas
        self._history: Dict[str, Any] = {}

    def _load_history(self, code: str):
        if code not in self._history:
            try:
                self._history[code] = self._store.load_history(code)
            except Exception:
                self._history[code] = None
        return self._history[code]

    def evaluate_result(
        self, result: Dict[str, Any], trade_date: str
    ) -> Optional[List[Dict[str, Any]]]:
        """把一次预测结果展开成逐候选的评估记录列表。"""
        pas = self._pas
        verify_date = pas._next_trading_day_yyyymmdd(trade_date)
        if not verify_date:
            return None
        td_dash = pas._to_dash_date(trade_date)
        vd_dash = pas._to_dash_date(verify_date)

        records: List[Dict[str, Any]] = []
        seen = set()
        for cat_key, payload_key in pas.CATEGORY_KEYS.items():
            for cand in result.get(payload_key) or []:
                if not isinstance(cand, dict):
                    continue
                code = str(cand.get("code") or "").strip().zfill(6)
                if not code or (code, cat_key) in seen:
                    continue
                seen.add((code, cat_key))

                name = str(cand.get("name") or "")
                ev = self._evaluate_one(code, name, td_dash, vd_dash)
                hit = self._is_hit(cat_key, ev)
                records.append(
                    {
                        "code": code,
                        "name": name,
                        "category": cat_key,
                        "score": cand.get("score"),
                        "buyable": bool(ev.get("hit_buyable")),
                        "hit": hit,
                        "open_close_pct": ev.get("t1_open_close_pct"),
                        "t1_limit_up": bool(ev.get("t1_limit_up")),
                    }
                )
        return records

    def _evaluate_one(self, code: str, name: str, td_dash: str, vd_dash: str) -> Dict[str, Any]:
        ev = self._pas._evaluate_candidate(
            code=code,
            name=name,
            history_df=self._load_history(code),
            trade_date_dash=td_dash,
            verify_date_dash=vd_dash,
        )
        if ev is None:
            return {
                "hit_strict": False, "hit_loose": False, "hit_buyable": False,
                "t1_open_close_pct": None, "t1_limit_up": False,
            }
        return ev

    def _is_hit(self, cat_key: str, ev: Dict[str, Any]) -> bool:
        if not ev.get("hit_buyable"):
            return False
        if self._pas._is_cont_category(cat_key):
            return bool(ev.get("hit_strict"))
        return bool(ev.get("hit_loose"))


# ---------------------------------------------------------------- aggregation
def summarize(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(records)
    buyable = [r for r in records if r["buyable"]]
    hits = [r for r in buyable if r["hit"]]
    rets = [r["open_close_pct"] for r in buyable if r["open_close_pct"] is not None]
    return {
        "candidates": total,
        "buyable": len(buyable),
        "hits": len(hits),
        "hit_rate": (len(hits) / len(buyable) * 100.0) if buyable else 0.0,
        "avg_ret": (sum(rets) / len(rets)) if rets else 0.0,
    }


def summarize_by_category(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in records:
        groups[r["category"]].append(r)
    return {cat: summarize(rows) for cat, rows in groups.items()}


# ---------------------------------------------------------------- main
def pick_dates(limit: int) -> List[str]:
    """选已评估过的历史交易日（保证 T+1 K 线齐全），取最近 limit 个。"""
    import stock_store

    dates = stock_store.list_prediction_accuracy_dates() or []
    dates = sorted({str(d).strip() for d in dates if str(d).strip()})
    return dates[-limit:] if limit > 0 else dates


def main() -> int:
    parser = argparse.ArgumentParser(description="lookback 5 vs 25 硬回测")
    parser.add_argument("--limit", type=int, default=5, help="回测最近 N 个交易日")
    parser.add_argument("--lookbacks", default="5,25", help="逗号分隔的 lookback 列表")
    parser.add_argument("--out", default="", help="结果 JSON 输出路径")
    parser.add_argument(
        "--allow-em",
        action="store_true",
        help="允许东财联网重试（默认预置熔断，避免每次预测空等 600s 冷却）",
    )
    parser.add_argument(
        "--online",
        action="store_true",
        help="允许联网（默认离线，强制本地缓存 + 既有降级路径，避免被网络超时拖垮）",
    )
    parser.add_argument(
        "--stack-dump",
        type=int,
        default=0,
        help="每 N 秒 dump 一次线程堆栈（诊断卡顿用，0=关闭）",
    )
    parser.add_argument(
        "--socket-timeout",
        type=float,
        default=8.0,
        help="原生 socket 全局默认超时秒数（防 baostock 无超时永久阻塞）",
    )
    parser.add_argument(
        "--allow-history-net",
        action="store_true",
        help="允许历史 K 线/资金流联网补拉（默认纯本地缓存，这是回测最大的时间黑洞）",
    )
    parser.add_argument(
        "--no-fast-rs",
        action="store_true",
        help="关闭相对强度向量化提速（默认开启，语义等价）",
    )
    args = parser.parse_args()

    if args.stack_dump > 0:
        import faulthandler

        faulthandler.dump_traceback_later(args.stack_dump, repeat=True)
        print(f"[diag] 已开启堆栈 dump，每 {args.stack_dump}s 一次")

    lookbacks = [int(x) for x in str(args.lookbacks).split(",") if x.strip()]

    guard = install_write_guards()
    print(f"[guard] 写库拦截已安装（回测只读不写）")

    if not args.allow_em:
        ok = preopen_em_breaker()
        print(f"[guard] 东财熔断预置：{'成功' if ok else '失败'}（避免空等冷却，两组条件一致）")

    if not args.online:
        ok = install_offline_guard()
        print(f"[guard] 离线模式：{'已启用' if ok else '启用失败'}（本地缓存 + 既有降级路径）")
        ok = install_socket_timeout(args.socket_timeout)
        print(f"[guard] 原生 socket 默认超时 {args.socket_timeout}s：{'已设置' if ok else '设置失败'}（防 baostock 永久阻塞）")

    if not args.allow_history_net:
        ok = install_cache_only_history()
        print(f"[guard] 历史 K 线纯缓存：{'已启用' if ok else '启用失败'}（缓存 miss 不联网补拉）")
        ok = install_cache_only_fundflow()
        print(f"[guard] 资金流纯缓存：{'已启用' if ok else '启用失败'}（避开同花顺二分翻页的重试 sleep）")

    if not args.no_fast_rs:
        ok = install_fast_relative_strength()
        print(f"[perf]  相对强度向量化：{'已启用' if ok else '启用失败'}（语义等价，纯提速）")

    # 走 StockFilter 包装层：底层 predict_limit_up_candidates 的 fetcher /
    # limit_up_threshold_pct_fn 等均为必填 keyword-only 参数，由它注入。
    from stock_filter import StockFilter

    stock_filter = StockFilter()

    dates = pick_dates(args.limit)
    if not dates:
        print("没有可回测的日期（limit_up_prediction_accuracy 为空）")
        return 1
    print(f"[plan] 回测日期 {len(dates)} 个: {dates[0]} ~ {dates[-1]}")
    print(f"[plan] lookback 组: {lookbacks}\n")

    evaluator = Evaluator()

    def _silent(*_a, **_k):
        return None

    # 静默预测内部日志，否则回测输出会被淹没
    try:
        stock_filter._log = _silent  # type: ignore[attr-defined]
    except Exception:
        pass

    all_records: Dict[int, List[Dict[str, Any]]] = {lb: [] for lb in lookbacks}
    per_day: List[Dict[str, Any]] = []

    for td in dates:
        day_row: Dict[str, Any] = {"date": td}
        for lb in lookbacks:
            t0 = time.time()
            print(f"  [{_ts()}] {td} lookback={lb} 预测开始...")
            try:
                result = stock_filter.predict_limit_up_candidates(
                    td,
                    lookback_days=lb,
                    progress_callback=_silent,
                    historical_mode=True,
                )
            except Exception as exc:  # noqa: BLE001
                import traceback

                print(f"  [{td}] lookback={lb} 预测失败: {type(exc).__name__}: {exc}")
                traceback.print_exc()
                continue
            elapsed = time.time() - t0
            print(f"  [{_ts()}] {td} lookback={lb} 预测完成，耗时 {elapsed:.0f}s，开始评估...")

            recs = evaluator.evaluate_result(result, td)
            if recs is None:
                print(f"  [{td}] lookback={lb} 无法确定 verify_date，跳过")
                continue
            all_records[lb].extend(recs)
            s = summarize(recs)
            day_row[f"lb{lb}"] = s
            print(
                f"  [{td}] lookback={lb:>2}  候选 {s['candidates']:>3}  "
                f"可买 {s['buyable']:>3}  命中 {s['hits']:>3}  "
                f"命中率 {s['hit_rate']:>5.1f}%  均收益 {s['avg_ret']:>+6.2f}%  "
                f"({elapsed:.0f}s)"
            )
        per_day.append(day_row)

        # 增量落盘：长回测中途被打断也能保住已完成日期的结果
        if args.out:
            try:
                snapshot = {
                    "dates_done": [r["date"] for r in per_day],
                    "lookbacks": lookbacks,
                    "overall_so_far": {
                        str(lb): summarize(all_records[lb]) for lb in lookbacks
                    },
                    "by_category_so_far": {
                        str(lb): summarize_by_category(all_records[lb]) for lb in lookbacks
                    },
                    "per_day": per_day,
                    "partial": True,
                }
                Path(args.out).write_text(
                    json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            except Exception:
                pass
        print()

    # ---- 总表 ----
    print("=" * 78)
    print("总体对比（口径与线上一致：cont=hit_strict，其余=hit_loose，均要求可买）")
    print("=" * 78)
    header = f"{'lookback':>9} | {'候选':>5} | {'可买':>5} | {'命中':>5} | {'命中率':>7} | {'均收益':>8}"
    print(header)
    print("-" * 78)
    overall: Dict[str, Any] = {}
    for lb in lookbacks:
        s = summarize(all_records[lb])
        overall[str(lb)] = s
        print(
            f"{lb:>9} | {s['candidates']:>5} | {s['buyable']:>5} | {s['hits']:>5} | "
            f"{s['hit_rate']:>6.2f}% | {s['avg_ret']:>+7.2f}%"
        )

    # ---- 分类别 ----
    print("\n" + "=" * 78)
    print("分类别命中率")
    print("=" * 78)
    cat_labels = {
        "cont": "保留涨停", "first": "二波接力", "fresh": "首板涨停",
        "wrap": "反包", "trend": "趋势涨停",
    }
    by_cat: Dict[str, Dict[str, Any]] = {}
    for lb in lookbacks:
        cats = summarize_by_category(all_records[lb])
        by_cat[str(lb)] = cats
        print(f"\n-- lookback={lb} --")
        print(f"{'类别':<10} | {'可买':>5} | {'命中':>5} | {'命中率':>7} | {'均收益':>8}")
        print("-" * 52)
        for cat in ("wrap", "cont", "first", "fresh", "trend"):
            if cat not in cats:
                continue
            s = cats[cat]
            print(
                f"{cat_labels.get(cat, cat):<10} | {s['buyable']:>5} | {s['hits']:>5} | "
                f"{s['hit_rate']:>6.2f}% | {s['avg_ret']:>+7.2f}%"
            )

    print(f"\n[guard] 本次拦截写库调用 {guard['blocked']} 次（数据库未被修改）")

    if args.out:
        payload = {
            "dates": dates,
            "lookbacks": lookbacks,
            "overall": overall,
            "by_category": by_cat,
            "per_day": per_day,
        }
        Path(args.out).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"[out] 结果已写入 {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
