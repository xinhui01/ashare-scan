"""预测快照 JSON 导出/导入：跨电脑同步最新一次预测。

导出端（跑预测的电脑）：预测保存进 SQLite 的同时把最新预测写到
`snapshots/latest_prediction.json`（git 跟踪），并自动 git commit + push；
任何 git 失败只记日志告警，绝不影响预测流程。

导入端（另一台电脑）：`git pull` 之后 GUI 启动时调用
`import_snapshot_if_newer()`，把比本地更新的快照写回本地 SQLite
（`limit_up_predictions` 记录 + `last_limit_up_prediction`），
竞价确认、模拟买入等既有功能零改动可用。

快照裁剪：`concept_hype_result`（概念分析明细，约 3MB）不进快照——
下游消费方都以 `.get(...) or {}` 容错，GUI 会在本机自动补齐题材数据。
"""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from stock_logger import get_logger

logger = get_logger(__name__)

SCHEMA_VERSION = 1
REPO_ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT_PATH = REPO_ROOT / "snapshots" / "latest_prediction.json"

_GIT_LOCAL_TIMEOUT_SEC = 15
_GIT_NETWORK_TIMEOUT_SEC = 60
# 概念分析明细占 payload 九成体积，且各机器可自行重建，不进快照
_EXCLUDED_PAYLOAD_KEYS = ("concept_hype_result",)


def _log(log_fn: Optional[Callable[[str], None]], message: str) -> None:
    logger.info(message)
    if callable(log_fn):
        try:
            log_fn(message)
        except Exception:
            pass


def build_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    """把预测 payload 拼装成快照结构（含派生的模拟买入，仅供查看）。"""
    from src.services.simulated_buy_service import build_simulated_buy_picks

    prediction = {k: v for k, v in payload.items() if k not in _EXCLUDED_PAYLOAD_KEYS}
    try:
        picks = build_simulated_buy_picks(payload, limit=2)
    except Exception:
        logger.exception("构建模拟买入快照失败")
        picks = []
    return {
        "schema_version": SCHEMA_VERSION,
        "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date": str(payload.get("trade_date") or "").strip(),
        "prediction": prediction,
        "simulated_buys": picks,
    }


def export_snapshot(
    payload: Dict[str, Any],
    *,
    auto_push: bool = True,
    log_fn: Optional[Callable[[str], None]] = None,
) -> bool:
    """把预测 payload 写成最新快照文件，并（可选）git 提交推送。

    payload.trade_date 早于本地已保存的最新预测日期时跳过，防止历史
    回放把旧结果覆盖成"最新预测"。任何失败只记日志，不抛出。
    """
    if not isinstance(payload, dict):
        return False
    trade_date = str(payload.get("trade_date") or "").strip()
    if not trade_date:
        return False
    try:
        from stock_store import list_limit_up_prediction_dates

        dates = list_limit_up_prediction_dates()
        latest = dates[0] if dates else ""
    except Exception:
        latest = ""
    if latest and trade_date < latest:
        _log(log_fn, f"预测快照跳过导出: {trade_date} 早于本地最新预测 {latest}")
        return False
    try:
        from stock_store import _normalize_json_value

        snapshot = _normalize_json_value(build_snapshot(payload))
        text = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
        SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        SNAPSHOT_PATH.write_text(text, encoding="utf-8")
    except Exception:
        logger.exception("写预测快照文件失败")
        return False
    _log(log_fn, f"预测快照已写入 {SNAPSHOT_PATH.name} ({trade_date})")
    if auto_push:
        _git_publish(trade_date, log_fn=log_fn)
    return True


def import_snapshot_if_newer(
    log_fn: Optional[Callable[[str], None]] = None,
) -> str:
    """把 git pull 下来的快照导入本地 SQLite（仅当比本地新）。

    返回状态标记（便于测试/日志）：
    "missing" 无快照文件 / "corrupt" 解析失败 / "invalid" 结构不完整 /
    "stale" 本地不旧于快照 / "imported:<trade_date>" 导入成功。
    """
    if not SNAPSHOT_PATH.is_file():
        return "missing"
    try:
        snapshot = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("解析预测快照失败")
        _log(log_fn, "预测快照文件损坏，跳过导入")
        return "corrupt"
    if not isinstance(snapshot, dict):
        _log(log_fn, "预测快照格式不正确，跳过导入")
        return "invalid"
    prediction = snapshot.get("prediction")
    if not isinstance(prediction, dict):
        _log(log_fn, "预测快照缺少预测数据，跳过导入")
        return "invalid"
    trade_date = str(
        prediction.get("trade_date") or snapshot.get("trade_date") or ""
    ).strip()
    if not trade_date:
        _log(log_fn, "预测快照缺少交易日，跳过导入")
        return "invalid"
    exported_at = str(snapshot.get("exported_at") or "").strip()

    from stock_store import (
        get_limit_up_prediction_saved_at,
        load_last_limit_up_prediction,
        save_last_limit_up_prediction,
        save_limit_up_prediction_record,
    )

    local_saved_at = get_limit_up_prediction_saved_at(trade_date)
    # 时间串是 "YYYY-mm-dd HH:MM:SS"，字典序即时间序；跨机器时钟差按分钟级容忍
    if local_saved_at and (not exported_at or exported_at <= local_saved_at):
        return "stale"

    save_limit_up_prediction_record(prediction)
    # 只有快照不早于本机"上次预测"时才覆盖 last（竞价确认 fallback / 启动加载用）
    last = load_last_limit_up_prediction()
    last_date = str((last or {}).get("trade_date") or "").strip()
    if not last_date or trade_date >= last_date:
        save_last_limit_up_prediction(prediction)
    _log(log_fn, f"已导入预测快照 {trade_date} (导出于 {exported_at or '未知时间'})")
    return f"imported:{trade_date}"


def _run_git(args: List[str], timeout: int) -> "subprocess.CompletedProcess[str]":
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"  # 无凭证时快速失败，避免卡住预测线程
    return subprocess.run(
        ["git", *args],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=env,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def _git_publish(
    trade_date: str,
    log_fn: Optional[Callable[[str], None]] = None,
) -> bool:
    """只提交快照这一个文件并推送；冲突时 pull --rebase 重试一次。

    绝不 stash、绝不碰用户其他改动；所有失败降级为日志告警。
    """
    try:
        rel = SNAPSHOT_PATH.relative_to(REPO_ROOT).as_posix()
        add = _run_git(["add", "--", rel], _GIT_LOCAL_TIMEOUT_SEC)
        if add.returncode != 0:
            _log(log_fn, f"预测快照 git add 失败: {add.stderr.strip()}")
            return False
        staged = _run_git(
            ["diff", "--cached", "--quiet", "--", rel], _GIT_LOCAL_TIMEOUT_SEC
        )
        if staged.returncode == 0:
            _log(log_fn, "预测快照内容无变化，跳过提交")
            return True
        # pathspec commit 只提交这一个文件，用户已暂存的其他改动保持不动
        commit = _run_git(
            ["commit", "-m", f"chore: 更新预测快照 {trade_date}", "--", rel],
            _GIT_LOCAL_TIMEOUT_SEC,
        )
        if commit.returncode != 0:
            _log(
                log_fn,
                f"预测快照 git commit 失败: {(commit.stderr or commit.stdout).strip()}",
            )
            return False
        push = _run_git(["push"], _GIT_NETWORK_TIMEOUT_SEC)
        if push.returncode == 0:
            _log(log_fn, "预测快照已推送到远程仓库")
            return True
        pull = _run_git(["pull", "--rebase"], _GIT_NETWORK_TIMEOUT_SEC)
        if pull.returncode != 0:
            _run_git(["rebase", "--abort"], _GIT_LOCAL_TIMEOUT_SEC)
            _log(log_fn, "预测快照已提交本地，但推送失败(远程有新提交且无法自动 rebase)，请手动 git pull && git push")
            return False
        push_again = _run_git(["push"], _GIT_NETWORK_TIMEOUT_SEC)
        if push_again.returncode == 0:
            _log(log_fn, "预测快照已推送到远程仓库")
            return True
        _log(log_fn, "预测快照已提交本地，但推送失败，请稍后手动 git push")
        return False
    except subprocess.TimeoutExpired:
        _log(log_fn, "预测快照 git 操作超时，文件已写好，请手动提交推送")
        return False
    except Exception:
        logger.exception("预测快照 git 发布失败")
        return False
