"""预测快照同步体检：一条命令核对 本地库 <-> 快照文件 <-> GitHub 远程 三层状态。

背景：盘后预测 -> 保存本地库 -> 导出快照文件 -> git 提交推送，这条链路任何
一环失败都只写日志，用户很难注意到"今晚的快照到底同步上去没有"。
本脚本把三层状态并排打印并给出结论与行动建议；挂在 predict_today.bat /
update_and_predict.bat 末尾自动执行，也可随时单独跑。

用法:
  python scripts/check_snapshot_sync.py          # 只检查并打印结论
  python scripts/check_snapshot_sync.py --push   # 检查后自动补救(补导出/补提交推送)

输出用 ASCII 标记([OK]/[!]/[?])，GBK 控制台安全。退出码: 0=一致, 1=有问题。
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "src"), str(PROJECT_ROOT / "src" / "services")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from stock_store import (  # noqa: E402
    get_limit_up_prediction_saved_at,
    list_limit_up_prediction_dates,
    load_limit_up_prediction_by_date,
)
from src.services.prediction_snapshot_service import (  # noqa: E402
    SNAPSHOT_PATH,
    _GIT_LOCAL_TIMEOUT_SEC,
    _GIT_NETWORK_TIMEOUT_SEC,
    _run_git,
)

_CANDIDATE_KEYS = (
    "continuation_candidates",
    "first_board_candidates",
    "fresh_first_board_candidates",
    "broken_board_wrap_candidates",
    "trend_limit_up_candidates",
)


def _count_candidates(payload: dict) -> int:
    return sum(len(payload.get(k) or []) for k in _CANDIDATE_KEYS)


def _local_db_state() -> tuple[str, str, int, str]:
    """返回 (最新有效预测日, saved_at, 候选数, 附注)。空预测残留会被跳过。"""
    note = ""
    for trade_date in list_limit_up_prediction_dates():
        payload = load_limit_up_prediction_by_date(trade_date)
        if not isinstance(payload, dict):
            continue
        count = _count_candidates(payload)
        if count <= 0:
            note = f"(库里 {trade_date} 是空预测残留，已忽略)"
            continue
        return trade_date, get_limit_up_prediction_saved_at(trade_date) or "-", count, note
    return "", "-", 0, note


def _snapshot_file_state() -> tuple[str, str, int]:
    """返回快照文件的 (trade_date, exported_at, 候选数)；无文件返回空。"""
    if not SNAPSHOT_PATH.is_file():
        return "", "-", 0
    try:
        data = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
        prediction = data.get("prediction") or {}
        return (
            str(data.get("trade_date") or "").strip(),
            str(data.get("exported_at") or "-").strip(),
            _count_candidates(prediction),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[!] 快照文件解析失败: {exc}")
        return "", "-", 0


def _remote_snapshot_state() -> tuple[str, str, bool]:
    """返回 (远程快照 trade_date, 说明, 远程是否可达)。

    用 git ls-remote 拿远程 main 的 SHA（在受限环境比跟踪引用可靠），
    再从本地对象库读该 commit 里的快照内容；本地没有该对象则 fetch 一次。
    网络卡死/超时(_run_git 抛 TimeoutExpired)一律降级为"远程不可达"，
    不能让盘后 bat 末尾的体检崩溃或长时间挂住。
    """
    try:
        return _remote_snapshot_state_inner()
    except Exception as exc:  # noqa: BLE001
        return "", f"远程校验异常({type(exc).__name__})，跳过", False


def _remote_snapshot_state_inner() -> tuple[str, str, bool]:
    # 网络探测限 20s：只为盘后确认状态，不值得让 bat 卡一分钟
    ls = _run_git(["ls-remote", "origin", "main"], 20)
    if ls.returncode != 0 or not (ls.stdout or "").strip():
        return "", "无法连接远程（离线/代理问题），跳过远程校验", False
    remote_sha = ls.stdout.split()[0]

    rel = SNAPSHOT_PATH.relative_to(PROJECT_ROOT).as_posix()
    have = _run_git(["cat-file", "-e", remote_sha], _GIT_LOCAL_TIMEOUT_SEC)
    if have.returncode != 0:
        _run_git(["fetch", "origin", "main"], _GIT_NETWORK_TIMEOUT_SEC)
        have = _run_git(["cat-file", "-e", remote_sha], _GIT_LOCAL_TIMEOUT_SEC)
    if have.returncode != 0:
        return "", f"远程领先本地(commit {remote_sha[:8]})，先 git pull 再检查", True

    show = _run_git(["show", f"{remote_sha}:{rel}"], _GIT_LOCAL_TIMEOUT_SEC)
    if show.returncode != 0:
        return "", "远程 commit 里没有快照文件", True
    try:
        data = json.loads(show.stdout)
        return str(data.get("trade_date") or "").strip(), f"commit {remote_sha[:8]}", True
    except Exception:
        return "", "远程快照内容解析失败", True


def _snapshot_file_dirty() -> bool:
    """快照文件是否有未提交改动。"""
    rel = SNAPSHOT_PATH.relative_to(PROJECT_ROOT).as_posix()
    st = _run_git(["status", "--porcelain", "--", rel], _GIT_LOCAL_TIMEOUT_SEC)
    return bool((st.stdout or "").strip())


def _latest_closed_trading_day() -> str:
    """最近一个已收盘的交易日（今天 15:30 前不算今天）；失败返回空串。"""
    try:
        from src.utils.trade_calendar import _get_trade_calendar, _is_trading_day

        cal = _get_trade_calendar()
        now = datetime.now()
        day = now.date()
        if now.strftime("%H%M") < "1530" or not _is_trading_day(day, cal):
            day -= timedelta(days=1)
        for _ in range(15):
            if _is_trading_day(day, cal):
                return day.strftime("%Y%m%d")
            day -= timedelta(days=1)
    except Exception:
        pass
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="预测快照三层同步体检")
    parser.add_argument("--push", action="store_true", help="发现落后时自动补导出/补提交推送")
    args = parser.parse_args()

    db_date, db_saved, db_count, db_note = _local_db_state()
    file_date, file_exported, file_count = _snapshot_file_state()
    remote_date, remote_note, remote_ok = _remote_snapshot_state()
    dirty = _snapshot_file_dirty()

    print("=" * 56)
    print("  预测快照同步体检")
    print("=" * 56)
    print(f"  本地库最新有效预测: {db_date or '-'}  (saved {db_saved}, 候选 {db_count} 只) {db_note}")
    print(f"  快照文件:           {file_date or '-'}  (exported {file_exported}, 候选 {file_count} 只)"
          + ("  [未提交改动]" if dirty else ""))
    if remote_ok and remote_date:
        print(f"  GitHub 远程:        {remote_date}  ({remote_note})")
    else:
        print(f"  GitHub 远程:        [?] {remote_note}")

    problems: list[str] = []
    if not db_date:
        problems.append("本地库里没有任何有效预测（先跑一次盘后预测）")
    elif file_date < db_date:
        problems.append(
            f"预测 {db_date} 已跑但快照文件停在 {file_date or '无'}：导出环节没执行或失败"
        )
    if dirty:
        problems.append("快照文件有未提交改动：提交推送环节没完成")
    if remote_ok and remote_date and file_date and remote_date < file_date and not dirty:
        problems.append(f"快照 {file_date} 已提交本地但远程还是 {remote_date}：推送失败")

    expected = _latest_closed_trading_day()
    if expected and db_date and db_date < expected:
        problems.append(f"最近已收盘交易日 {expected} 的盘后预测还没跑（本地最新 {db_date}）")

    if not problems:
        print(f"  结论: [OK] 三层一致，另一台电脑 git pull 即可使用")
        print("=" * 56)
        return 0

    print("  结论:")
    for p in problems:
        print(f"    [!] {p}")

    repaired = False
    if args.push and db_date:
        if file_date < db_date:
            print(f"  [--push] 重新导出并推送 {db_date} 快照...")
            from src.services.prediction_snapshot_service import export_snapshot

            payload = load_limit_up_prediction_by_date(db_date)
            repaired = bool(payload) and export_snapshot(payload, log_fn=print)
        elif dirty or (remote_ok and remote_date and remote_date < file_date):
            print(f"  [--push] 补提交/推送快照 {file_date}...")
            from src.services.prediction_snapshot_service import _git_publish

            repaired = _git_publish(file_date, log_fn=print)
        print(f"  [--push] 补救{'成功' if repaired else '失败，请按上面提示手动处理'}")
    elif problems and not args.push:
        print("  提示: 加 --push 可自动补导出/补推送（或运行 check_snapshot.bat --push）")
    print("=" * 56)
    return 0 if repaired else 1


if __name__ == "__main__":
    raise SystemExit(main())
