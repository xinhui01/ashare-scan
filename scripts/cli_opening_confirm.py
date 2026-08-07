"""命令行竞价确认：消费 git 同步的预测快照，盘前拉实时竞价，输出买点清单。

适用场景：另一台不能开 GUI 的电脑。前置条件：
  - 已 `git pull` 拉到最新 `snapshots/latest_prediction.json`（bat 会自动 pull）
  - 能联网拉实时竞价（东财 → 腾讯 → 新浪逐级兜底，全程直连，不依赖本地 K 线缓存）
  - 已 `pip install -r requirements.txt`

用法：
  python scripts/cli_opening_confirm.py            # 文本输出（人读）
  python scripts/cli_opening_confirm.py --json     # JSON 输出（方便其他工具消费）

说明：
  - 竞价确认逻辑与 GUI 完全一致（opening_confirmation_service.confirm_candidate_lists）。
  - 候选名单来自 git 同步的快照（启动前自动 import 进本地 SQLite）。
  - 竞价窗口为 09:25~10:00；窗口外运行会跳过实时拉取，仅展示候选（标注原因）。
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# ---- 路径：保证 stock_data / stock_store（根）与 opening_confirmation_service（src/services）均可导入 ----
PROJECT_ROOT = Path(__file__).resolve().parents[1]
for _p in (
    str(PROJECT_ROOT),
    str(PROJECT_ROOT / "src"),
    str(PROJECT_ROOT / "src" / "services"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# 网络补丁（直连）须在 import akshare / requests 前执行（stock_data 模块导入即生效）
import stock_data  # noqa: F401  (side-effect: 应用 _apply_network_patches)

from stock_store import (
    list_limit_up_prediction_dates,
    load_last_limit_up_prediction,
    load_limit_up_prediction_by_date,
)
from prediction_snapshot_service import import_snapshot_if_newer
import opening_confirmation_service  # noqa: E402


_STATUS_ORDER = ["可买", "观察", "放弃", "风险过高"]

_CANDIDATE_KEYS = (
    "continuation_candidates",
    "first_board_candidates",
    "fresh_first_board_candidates",
    "broken_board_wrap_candidates",
    "trend_limit_up_candidates",
)


def _count_candidates(payload: dict) -> int:
    return sum(len(payload.get(k) or []) for k in _CANDIDATE_KEYS)


def _load_best_payload() -> tuple[dict | None, str]:
    """优先读 last；last 缺失或候选为空时，回退历史表里最近一条非空预测。

    盘前网络故障产生的空预测会顶掉 last（predict 中止分支已改为不落库，
    这里再兜一层，兼容旧库里已写入的空记录）。
    """
    payload = load_last_limit_up_prediction()
    if isinstance(payload, dict) and _count_candidates(payload) > 0:
        return payload, ""
    for trade_date in list_limit_up_prediction_dates():
        rec = load_limit_up_prediction_by_date(trade_date)
        if isinstance(rec, dict) and _count_candidates(rec) > 0:
            last_date = str((payload or {}).get("trade_date") or "-")
            return rec, f"最新预测({last_date})没有候选，已回退到 {trade_date} 的历史预测"
    return (payload if isinstance(payload, dict) else None), ""


def _last_trading_day_before(today_key: str) -> str:
    """返回 today_key 之前最近的交易日（YYYYMMDD），失败返回空串。"""
    try:
        from datetime import timedelta
        from src.utils.trade_calendar import _get_trade_calendar, _is_trading_day

        cal = _get_trade_calendar()
        day = datetime.strptime(today_key, "%Y%m%d").date() - timedelta(days=1)
        for _ in range(15):
            if _is_trading_day(day, cal):
                return day.strftime("%Y%m%d")
            day -= timedelta(days=1)
    except Exception:
        pass
    return ""


def _staleness_warning(trade_date) -> str:
    """候选交易日早于最近盘后应有日期时给出过期告警。"""
    td = str(trade_date or "").strip().replace("-", "")
    if not td:
        return ""
    expected = _last_trading_day_before(datetime.now().strftime("%Y%m%d"))
    if expected and td < expected:
        return (
            f"候选来自 {td} 盘后预测，早于最近盘后({expected})，已过期仅供参考；"
            f"请先在有数据的电脑跑盘后预测并推送快照"
        )
    return ""


def _build_candidate_lists(payload: dict) -> dict:
    """从预测 payload 取出与 GUI 完全一致的候选分桶结构。"""
    return {
        "cont": payload.get("continuation_candidates") or [],
        "first": payload.get("first_board_candidates") or [],
        "fresh": payload.get("fresh_first_board_candidates") or [],
        "wrap": payload.get("broken_board_wrap_candidates") or [],
        "trend": payload.get("trend_limit_up_candidates") or [],
    }


def _fmt_num(value) -> str:
    if value is None:
        return "-"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return "-"
    if abs(f) >= 1e8:
        return f"{f / 1e8:.2f}亿"
    if abs(f) >= 1e4:
        return f"{f / 1e4:.1f}万"
    return f"{f:.0f}"


def _category_label(category: str) -> str:
    return {
        "cont": "连板",
        "first": "二波",
        "fresh": "首板",
        "wrap": "反包",
        "trend": "趋势",
    }.get(category, category)


def _format_human(payload: dict, lists: dict, result: dict) -> str:
    lines: list[str] = []
    trade_date = str(payload.get("trade_date") or "").strip()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines.append("=" * 64)
    lines.append(f"  竞价确认 · 交易日 {trade_date or '-'} · 生成本地 {now}")
    lines.append("=" * 64)

    all_confs = [
        (rec.get("opening_confirmation") or {})
        for recs in lists.values()
        for rec in recs or []
    ]
    total = len(all_confs)
    auction_hits = sum(1 for c in all_confs if c.get("auction_gap_pct") is not None)
    open_hits = sum(1 for c in all_confs if c.get("open_gap_pct") is not None)

    mode_note = str(result.get("skipped_reason") or result.get("mode_note") or "").strip()
    if result.get("fetched_auction"):
        src_line = f"  数据源: 实时竞价(09:25) 命中 {auction_hits}/{total}"
        if result.get("fetched_intraday"):
            src_line += f" | 开盘分时 命中 {open_hits}/{total}"
        lines.append(src_line)
        if auction_hits == 0 and open_hits == 0:
            lines.append("  [警告] 竞价价格全部缺失（东财/腾讯/新浪均未命中），以下状态按无实时信息降级")
    elif result.get("fetched_intraday"):
        lines.append(f"  数据源: 实时开盘分时(已过竞价窗口) 命中 {open_hits}/{total}")
    elif mode_note:
        lines.append(f"  注意: {mode_note}，未请求实时竞价接口")

    counts = result.get("status_counts") or {}
    if counts:
        parts = " / ".join(f"{s} {counts.get(s, 0)}" for s in _STATUS_ORDER if counts.get(s, 0))
        lines.append(f"  结果: {parts}")

    lines.append(f"  候选总数: {total}")
    lines.append("")

    # 把候选按状态分组
    grouped: dict[str, list] = {s: [] for s in _STATUS_ORDER}
    for category, recs in lists.items():
        for rec in recs or []:
            conf = rec.get("opening_confirmation") or {}
            status = str(conf.get("status") or "观察")
            grouped.setdefault(status, []).append((category, rec, conf))

    # T+1 卖点建议图例：类别后的 →留到收盘 / →竞价可走 由本地 accuracy 表实证得出
    seen_hints: dict[str, str] = {}
    for _c, _r, conf in [x for v in grouped.values() for x in v]:
        hint = str(conf.get("exit_hint") or "").strip()
        cat = str(conf.get("category") or "").strip()
        if hint and cat not in seen_hints:
            seen_hints[cat] = hint
    if seen_hints:
        lines.append("── T+1 卖点建议（括号内为 收盘卖-竞价卖 的实证收益差/样本数）──")
        for cat, hint in seen_hints.items():
            lines.append(f"  {cat}: {hint}")
        lines.append("")

    for status in _STATUS_ORDER:
        items = grouped.get(status) or []
        if not items:
            continue
        lines.append(f"── {status} ({len(items)}) ──")
        for category, rec, conf in items:
            code = str(rec.get("code") or "").strip().zfill(6)
            name = str(rec.get("name") or "").strip()
            gap = conf.get("auction_gap_pct")
            gap_txt = f"竞价{gap:+.1f}%" if isinstance(gap, (int, float)) else (
                f"开盘{conf.get('open_gap_pct'):+.1f}%" if isinstance(conf.get("open_gap_pct"), (int, float)) else "竞价-"
            )
            score = conf.get("score")
            score_txt = str(score) if isinstance(score, (int, float)) else "-"
            reason = str(conf.get("reason") or "").strip()
            hint = str(conf.get("exit_hint") or "").strip()
            cat_txt = _category_label(category)
            if hint:
                cat_txt = f"{cat_txt}→{hint.split('(')[0]}"
            lines.append(
                f"  {code} {name:<8} [{cat_txt}] "
                f"{gap_txt:<9} 额{_fmt_num(conf.get('auction_amount')):<7} 分{score_txt:<4} {reason}"
            )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _format_json(payload: dict, lists: dict, result: dict, warnings: list[str] | None = None) -> str:
    candidates: list[dict] = []
    for category, recs in lists.items():
        for rec in recs or []:
            conf = rec.get("opening_confirmation") or {}
            candidates.append({
                "code": str(rec.get("code") or "").strip().zfill(6),
                "name": str(rec.get("name") or "").strip(),
                "category": _category_label(category),
                "status": str(conf.get("status") or "观察"),
                "auction_gap_pct": conf.get("auction_gap_pct"),
                "open_gap_pct": conf.get("open_gap_pct"),
                "auction_amount": conf.get("auction_amount"),
                "score": conf.get("score"),
                "reason": str(conf.get("reason") or "").strip(),
                "exit_hint": str(conf.get("exit_hint") or "").strip(),
            })
    return json.dumps(
        {
            "trade_date": str(payload.get("trade_date") or "").strip(),
            "warnings": warnings or [],
            "generated_at": result.get("generated_at"),
            "fetched_auction": result.get("fetched_auction"),
            "fetched_intraday": result.get("fetched_intraday"),
            "skipped_reason": result.get("skipped_reason"),
            "status_counts": result.get("status_counts"),
            "candidates": candidates,
        },
        ensure_ascii=False,
        indent=2,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="命令行竞价确认（消费 git 同步的预测快照）")
    parser.add_argument("--json", action="store_true", help="输出 JSON 而非文本")
    args = parser.parse_args()

    # 1) 把 git pull 下来的快照导入本地 SQLite（与 GUI 启动流程一致）
    try:
        snap_status = import_snapshot_if_newer(log_fn=print)
    except Exception as exc:  # noqa: BLE001
        print(f"[快照] 导入失败: {exc}")
        snap_status = "missing"
    print(f"[快照] import 状态: {snap_status}")

    # 2) 读取最新预测候选（last 为空/无候选时回退历史表最近一条非空预测）
    payload, fallback_note = _load_best_payload()
    if not isinstance(payload, dict):
        print("[错误] 本地没有预测候选。请先在有数据的电脑跑盘后预测并 git push 快照，"
              "然后在本机 git pull 后再运行。")
        return 2
    warnings = [n for n in (fallback_note, _staleness_warning(payload.get("trade_date"))) if n]
    for note in warnings:
        print(f"[警告] {note}")

    lists = _build_candidate_lists(payload)
    total = sum(len(v or []) for v in lists.values())
    if total <= 0:
        print("[提示] 最新预测没有候选股，无需竞价确认。")
        return 0

    # 3) 构造实时 fetcher 并跑竞价确认（不依赖本地 K 线缓存）
    try:
        fetcher = stock_data.StockDataFetcher()
    except Exception as exc:  # noqa: BLE001
        print(f"[错误] 无法构造行情 fetcher: {exc}")
        return 3

    try:
        result = opening_confirmation_service.confirm_candidate_lists(
            lists,
            fetcher=fetcher,
            max_workers=2,
            log_fn=print,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[错误] 竞价确认失败: {exc}")
        return 4

    # 4) 输出
    if args.json:
        print(_format_json(payload, lists, result, warnings=warnings))
    else:
        print(_format_human(payload, lists, result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
