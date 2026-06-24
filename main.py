"""
网络相关：SSL 校验见 USE_INSECURE_SSL / ASHARE_SCAN_INSECURE_SSL；
代理报错见 USE_BYPASS_PROXY / ASHARE_SCAN_BYPASS_PROXY（见 README）。
"""
import argparse
import os
import platform
import sys
import tkinter as tk
from datetime import datetime
from typing import Optional, Sequence

from src.services.scoring.predict import (
    DEFAULT_PREDICT_LOOKBACK_DAYS,
    MAX_PREDICT_LOOKBACK_DAYS,
    MIN_PREDICT_LOOKBACK_DAYS,
    normalize_predict_lookback,
)
from stock_filter import StockFilter
from stock_store import ensure_store_ready

# 默认沿用系统/环境代理（trust_env=True）。网络出口交给启动 bat 的 _set_proxy.bat：
# 本地 Clash(7897) 开着就走 Clash，没开就走直连。改回 True 可强制全程不走代理（旧行为）。
BYPASS_PROXY = False


def _check_runtime() -> None:
    is_macos = platform.system() == "Darwin"
    is_pyenv = ".pyenv" in (sys.executable or "")
    is_py313 = sys.version_info[:2] >= (3, 13)
    if is_macos and is_pyenv and is_py313:
        raise SystemExit(
            "当前运行环境是 macOS + pyenv Python 3.13，这个组合在 Tk GUI 下容易直接 bus error/abort。\n"
            "建议改用 Python 3.11/3.12 重新创建虚拟环境后再启动。"
        )


def _drop_dead_http_proxies() -> None:
    """清除指向死代理的 HTTP(S)_PROXY 环境变量。

    历史遗留：曾在 env 里手动设过 http://118.89.136.118:31283，
    但该代理早已下线，导致 requests/akshare 默认调用全部卡死。
    BYPASS_PROXY=True 已经能让项目内 Session 走 trust_env=False，
    但 pip / 其它子进程仍会读到这条 env，所以从根上清掉。
    """
    DEAD_HOSTS = ("118.89.136.118:31283",)
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY"):
        val = os.environ.get(key, "")
        if any(h in val for h in DEAD_HOSTS):
            os.environ.pop(key, None)


def _prepare_runtime() -> None:
    _check_runtime()
    _drop_dead_http_proxies()
    if BYPASS_PROXY:
        os.environ["ASHARE_SCAN_BYPASS_PROXY"] = "1"
    else:
        os.environ.pop("ASHARE_SCAN_BYPASS_PROXY", None)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="A股筛选工具：无参数启动 GUI，也可通过命令行更新缓存和预测。",
    )
    subparsers = parser.add_subparsers(dest="command")

    def add_cache_options(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--max-stocks", type=int, default=0, help="更新股票数量，0 表示全量。")
        subparser.add_argument("--days", type=int, default=60, help="历史缓存天数，默认 60。")
        subparser.add_argument("--workers", type=int, default=3, help="并发线程数，默认 3。")
        subparser.add_argument("--source", default="auto", help="历史数据源，默认 auto。")
        subparser.add_argument("--refresh-universe", action="store_true", help="重新拉取股票池。")
        subparser.add_argument(
            "--board",
            action="append",
            default=[],
            help="限制板块，可重复传入；不传则不限制。",
        )

    update_cache = subparsers.add_parser("update-cache", help="更新历史缓存。")
    add_cache_options(update_cache)

    predict_today = subparsers.add_parser("predict-today", help="开始预测当天或最近交易日数据。")
    predict_today.add_argument("--date", default="", help="预测日期，支持 YYYYMMDD / YYYY-MM-DD / M/D。")
    predict_today.add_argument(
        "--lookback",
        type=int,
        default=DEFAULT_PREDICT_LOOKBACK_DAYS,
        help=(
            f"回溯天数，范围 {MIN_PREDICT_LOOKBACK_DAYS}-{MAX_PREDICT_LOOKBACK_DAYS}，"
            f"默认 {DEFAULT_PREDICT_LOOKBACK_DAYS}。"
        ),
    )
    predict_today.add_argument("--historical", action="store_true", help="按历史模式预测指定日期。")

    update_and_predict = subparsers.add_parser(
        "update-and-predict",
        help="先更新历史缓存，再开始预测当天或最近交易日数据。",
    )
    add_cache_options(update_and_predict)
    update_and_predict.add_argument("--date", default="", help="预测日期，支持 YYYYMMDD / YYYY-MM-DD / M/D。")
    update_and_predict.add_argument(
        "--lookback",
        type=int,
        default=DEFAULT_PREDICT_LOOKBACK_DAYS,
        help=(
            f"回溯天数，范围 {MIN_PREDICT_LOOKBACK_DAYS}-{MAX_PREDICT_LOOKBACK_DAYS}，"
            f"默认 {DEFAULT_PREDICT_LOOKBACK_DAYS}。"
        ),
    )
    update_and_predict.add_argument("--historical", action="store_true", help="按历史模式预测指定日期。")

    sentiment = subparsers.add_parser(
        "sentiment",
        help="获取今日（或指定日期）市场情绪综合评分；数据缺失时打印前置步骤。",
    )
    sentiment.add_argument("--date", default="", help="目标日期，支持 YYYYMMDD / YYYY-MM-DD / M/D；默认今天。")
    sentiment.add_argument("--no-external", action="store_true", help="不联网拉跌停池/大盘指数，仅用本地涨停池。")

    return parser


def _normalize_predict_date(value: str) -> str:
    if not value:
        return ""
    from src.gui.tabs.predict import _normalize_predict_trade_date

    return _normalize_predict_trade_date(value)


def _default_predict_trade_date() -> str:
    from src.utils.trade_calendar import _get_trade_calendar, _is_trading_day, _previous_trading_day

    today = datetime.now().date()
    calendar = _get_trade_calendar()
    if _is_trading_day(today, calendar):
        return today.strftime("%Y%m%d")
    return _previous_trading_day(today, calendar).strftime("%Y%m%d")


def _resolve_predict_trade_date(date_arg: str) -> str:
    trade_date = _normalize_predict_date(date_arg) if date_arg else _default_predict_trade_date()
    if not trade_date:
        raise ValueError(f"无法识别预测日期: {date_arg}")
    from src.utils.trade_calendar import _get_trade_calendar, _is_trading_day, _previous_trading_day

    parsed = datetime.strptime(trade_date, "%Y%m%d").date()
    calendar = _get_trade_calendar()
    if not _is_trading_day(parsed, calendar):
        trade_date = _previous_trading_day(parsed, calendar).strftime("%Y%m%d")
    return trade_date


def _print_cache_progress(current: int, total: int, code: str, name: str, updated: int, failed: int, skipped: int) -> None:
    if total <= 0 or current == total or current % 50 == 0:
        print(f"缓存进度 {current}/{total}: {code} {name} 成功{updated} 跳过{skipped} 失败{failed}")


def _run_update_cache(args: argparse.Namespace) -> int:
    ensure_store_ready()
    stock_filter = StockFilter()
    stock_filter.set_history_source_preference(args.source)
    result = stock_filter.fetcher.update_history_cache(
        max_stocks=max(0, int(args.max_stocks)),
        days=max(1, int(args.days)),
        source=args.source,
        workers=max(1, int(args.workers)),
        progress_callback=_print_cache_progress,
        refresh_universe=bool(args.refresh_universe),
        allowed_boards=list(args.board or []),
    )
    print(
        "历史缓存更新完成："
        f"总计 {result.get('total', 0)}，"
        f"成功 {result.get('updated', 0)}，"
        f"跳过 {result.get('skipped', 0)}，"
        f"失败 {result.get('failed', 0)}。"
    )
    return 0 if int(result.get("failed", 0) or 0) == 0 else 1


def _run_predict_today(args: argparse.Namespace) -> int:
    ensure_store_ready()
    trade_date = _resolve_predict_trade_date(str(args.date or ""))
    lookback = normalize_predict_lookback(args.lookback)
    stock_filter = StockFilter()

    def progress_callback(current: int, total: int, info: str) -> None:
        if total <= 0 or current == total or current % 10 == 0:
            print(f"预测进度 {current}/{total}: {info}")

    result = stock_filter.predict_limit_up_candidates(
        trade_date,
        lookback_days=lookback,
        progress_callback=progress_callback,
        historical_mode=bool(args.historical),
    )
    counts = {
        "保留涨停": len(result.get("continuation_candidates") or []),
        "二波接力": len(result.get("first_board_candidates") or []),
        "首板涨停": len(result.get("fresh_first_board_candidates") or []),
        "断板反包": len(result.get("broken_board_wrap_candidates") or []),
        "趋势涨停": len(result.get("trend_limit_up_candidates") or []),
    }
    detail = "，".join(f"{name} {count}" for name, count in counts.items())
    print(f"涨停预测完成：交易日 {result.get('trade_date') or trade_date}，{detail}。")
    return 0


def _resolve_sentiment_trade_date(date_arg: str) -> str:
    """情绪默认看『今天』本身（不静默回退到上一交易日）。

    用户要的是当日情绪，今天没数据时应提示前置步骤，而不是悄悄给昨天的结论。
    只有显式传 --date 时才归一化该日期。
    """
    if date_arg:
        target = _normalize_predict_date(date_arg)
        if not target:
            raise ValueError(f"无法识别日期: {date_arg}")
        return target
    return datetime.now().strftime("%Y%m%d")


def _print_sentiment_success(result: dict) -> None:
    date = result.get("trade_date", "")
    score = result.get("score", 0)
    pos = result.get("position_suggest") or {}
    state = result.get("market_state") or {}
    strat = state.get("strategy") or {}
    print("")
    print(f"========== 市场情绪 {date} ==========")
    print(f"综合评分: {score}/100  →  建议仓位: {pos.get('label', '-')}（{pos.get('ratio', '-')}）")
    print(
        f"市场状态: {state.get('label', '-')}  置信 {state.get('confidence', '-')}"
        f"  → {strat.get('label', '-')}"
    )
    if strat.get("notes"):
        print(f"  打法: {strat['notes']}")
    print(f"一句话: {result.get('summary', '')}")

    signals = result.get("signals") or []
    if signals:
        print("")
        print("信号明细:")
        for s in signals:
            delta = s.get("delta", 0)
            print(f"  {s.get('name', '')}: {s.get('value', '')}  ({delta:+d})  {s.get('note', '')}")

    prev = result.get("previous") or {}
    if prev:
        prev_state = (prev.get("market_state") or {}).get("label", "-")
        print("")
        print(f"昨日({prev.get('trade_date', '')}): {prev.get('score', '-')} 分 / 状态 {prev_state}")


def _print_sentiment_prerequisites(target: str, result: dict) -> None:
    summary = str(result.get("summary") or "无法计算市场情绪。")
    raw = result.get("raw") or {}
    missing = [str(d) for d in (raw.get("missing_pool_dates") or [])]

    is_trading = True
    try:
        from src.utils.trade_calendar import _get_trade_calendar, _is_trading_day
        parsed = datetime.strptime(target, "%Y%m%d").date()
        is_trading = _is_trading_day(parsed, _get_trade_calendar())
    except (ValueError, ImportError):
        pass

    print("")
    print(f"[!] 无法获取 {target} 的市场情绪")
    print(f"  原因: {summary}")
    print("")
    print("请按以下前置步骤排查：")
    if not is_trading:
        print(f"  1. {target} 不是交易日（周末/节假日），没有涨停数据。请在交易日收盘后再运行。")
        print("  2. 查看最近一个交易日：sentiment.bat --date <YYYYMMDD>")
    else:
        print("  1. 是否已收盘？涨停池要等当天收盘后（约 15:00 之后）才完整，建议 15:30 之后再跑。")
        print("  2. 网络是否可达？东财涨停池接口需直连（项目默认绕过系统代理）；")
        print("     先确认能访问 push2.eastmoney.com，公司网/受限网络可能拦截。")
        print("  3. 情绪需要『今日 + 最近 5 个交易日』的涨停池，缺失会自动联网补齐，补不齐就会失败。")
        if missing:
            print(f"     当前仍缺: {'、'.join(missing)}")
        print("  4. 兜底：先运行 update_and_predict.bat（预测流程会顺带把今日涨停池写入本地），再重跑本脚本；")
        print("     东财间歇性熔断时，稍等几分钟重试即可。")
    print("")


def _run_sentiment(args: argparse.Namespace) -> int:
    ensure_store_ready()
    try:
        target = _resolve_sentiment_trade_date(str(args.date or ""))
    except ValueError as exc:
        print(str(exc))
        return 2

    from src.services.market_sentiment_service import analyze_market_sentiment

    result = analyze_market_sentiment(
        target,
        fetch_external=not bool(args.no_external),
        log=lambda msg: print(msg),
    )

    if result.get("market_state"):
        _print_sentiment_success(result)
        return 0

    _print_sentiment_prerequisites(target, result)
    return 1


def _run_gui() -> int:
    from stock_logger import get_logger
    logger = get_logger(__name__)
    logger.info("应用启动")

    from stock_gui import StockMonitorApp

    ensure_store_ready()
    root = tk.Tk()
    app = StockMonitorApp(root)
    logger.info("主窗口已初始化，进入主循环")
    root.mainloop()
    logger.info("应用退出")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    _prepare_runtime()
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        return _run_gui()
    if args.command == "update-cache":
        return _run_update_cache(args)
    if args.command == "predict-today":
        return _run_predict_today(args)
    if args.command == "update-and-predict":
        cache_rc = _run_update_cache(args)
        predict_rc = _run_predict_today(args)
        return predict_rc if predict_rc != 0 else cache_rc
    if args.command == "sentiment":
        return _run_sentiment(args)
    parser.error(f"未知命令: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
