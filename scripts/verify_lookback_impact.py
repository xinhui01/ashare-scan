"""实证：lookback 参数到底影响了什么。

静态分析已经证明 lookback 在预测链路里只剩两条路径：
  1) build_compare_market_context -> avg_continuation_rate
     但 cont.py:309 是 `ref_rate = latest_rate if latest_rate is not None else avg_rate`，
     只有 latest 为 None 时 avg 才生效。
  2) scan_followthrough_candidates_cached(lookback_days=...) -> score_followthrough_candidate
     但该函数体 555 行里根本没用这个参数（内部写死 lookback_days=90）。

本脚本对最近 N 个交易日做实测，回答两件事：
  A. latest_continuation_rate 有多大比例不为 None（若接近 100%，avg 几乎永不生效）
  B. lookback=5 与 lookback=25 算出的 compare_context 究竟差在哪
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def install_guards() -> None:
    """离线 + 熔断预置，避免被本机网络限制拖住（涨停池对比走本地缓存）。"""
    try:
        from requests.exceptions import ConnectionError as ReqConnErr
        from requests.sessions import Session

        def _blocked(self, *_a, **_k):  # noqa: ANN001
            raise ReqConnErr("verify offline mode")

        Session.request = _blocked  # type: ignore[method-assign]
    except Exception:
        pass
    try:
        import socket

        socket.setdefaulttimeout(8.0)
    except Exception:
        pass
    try:
        from src.utils.em_circuit_breaker import EMCircuitBreaker

        b = EMCircuitBreaker.instance()
        with b._lock:  # type: ignore[attr-defined]
            b._open_until = b._clock() + 86400  # type: ignore[attr-defined]
    except Exception:
        pass


def main() -> int:
    install_guards()

    import stock_store
    from src.services.scoring.predict import build_compare_market_context
    from stock_filter import StockFilter

    sf = StockFilter()
    try:
        sf._log = lambda *_a, **_k: None  # type: ignore[attr-defined]
    except Exception:
        pass
    fetcher = sf.fetcher

    dates = sorted({str(d).strip() for d in (stock_store.list_prediction_accuracy_dates() or [])})
    dates = [d for d in dates if d][-20:]
    if not dates:
        print("没有可用交易日")
        return 1

    print(f"实测交易日 {len(dates)} 个：{dates[0]} ~ {dates[-1]}\n")
    header = (
        f"{'交易日':<10} | {'lb5 pairs':>9} | {'lb25 pairs':>10} | "
        f"{'lb5 avg':>8} | {'lb25 avg':>8} | {'avg 差':>7} | {'latest':>7} | {'latest 相同':>10}"
    )
    print(header)
    print("-" * len(header))

    latest_not_none = 0
    latest_same = 0
    avg_diff_sum = 0.0
    avg_diff_n = 0
    rows = 0

    for td in dates:
        try:
            c5 = build_compare_market_context(td, 5, fetcher=fetcher)
            c25 = build_compare_market_context(td, 25, fetcher=fetcher)
        except Exception as exc:  # noqa: BLE001
            print(f"{td:<10} | 计算失败: {type(exc).__name__}: {exc}")
            continue

        rows += 1
        a5 = c5.get("avg_continuation_rate")
        a25 = c25.get("avg_continuation_rate")
        l5 = c5.get("latest_continuation_rate")
        l25 = c25.get("latest_continuation_rate")

        if l5 is not None:
            latest_not_none += 1
        if l5 == l25:
            latest_same += 1

        if a5 is not None and a25 is not None:
            diff = a5 - a25
            avg_diff_sum += abs(diff)
            avg_diff_n += 1
            diff_txt = f"{diff:+.1f}"
        else:
            diff_txt = "n/a"

        print(
            f"{td:<10} | {c5.get('pair_count', 0):>9} | {c25.get('pair_count', 0):>10} | "
            f"{(f'{a5:.1f}' if a5 is not None else 'None'):>8} | "
            f"{(f'{a25:.1f}' if a25 is not None else 'None'):>8} | "
            f"{diff_txt:>7} | {(f'{l5:.1f}' if l5 is not None else 'None'):>7} | "
            f"{('是' if l5 == l25 else '否'):>10}"
        )

    print("\n" + "=" * 78)
    print("结论")
    print("=" * 78)
    if rows:
        print(f"1. latest_continuation_rate 非空占比：{latest_not_none}/{rows} = {latest_not_none / rows * 100:.1f}%")
        print(f"   -> cont.py:309 用的是 `latest if latest is not None else avg`，")
        print(f"      非空占比越高，说明唯一受 lookback 影响的 avg_continuation_rate 越用不上。")
        print(f"2. latest_continuation_rate 在两组 lookback 下完全相同：{latest_same}/{rows} = {latest_same / rows * 100:.1f}%")
        print(f"   -> latest 取的是 pair_stats[-1]，永远是最近一对，与窗口长度无关。")
        if avg_diff_n:
            print(f"3. avg_continuation_rate 平均绝对差：{avg_diff_sum / avg_diff_n:.2f} 个百分点（共 {avg_diff_n} 天可比）")
            print(f"   -> 即便这个差值存在，也只在 latest 为 None 时才会被读取。")
    print("\n4. 二波接力路径：scan_followthrough_candidates_cached 把 lookback_days 透传给")
    print("   score_followthrough_candidate，但该函数体 555 行内从未使用该参数")
    print("   （内部调用写死 lookback_days=90）。该路径对 lookback 完全不敏感。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
