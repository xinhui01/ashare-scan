"""数据库管理员操作：备份、恢复、清理。

与 `stock_store` 的职责切分：
- `stock_store` 负责 schema / 连接管理 / 业务表的 CRUD
- 本模块负责跨表的"管理员"动作：整库备份/恢复、定期清理

为了避免循环导入，本模块只在被调用时读取 `stock_store` 的 module-level 状态
（`_DATA_DIR`, `_DB_PATH`, `_DB_WRITE_LOCK`, `reset_all_connections`）。
`stock_store` 公开的 `backup_database/restore_database/...` 符号会转发到这里。
"""

from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from stock_logger import get_logger

logger = get_logger(__name__)


def _db_path() -> Path:
    import stock_store
    return stock_store._DB_PATH


def _data_dir() -> Path:
    import stock_store
    return stock_store._DATA_DIR


def _write_lock():
    import stock_store
    return stock_store._DB_WRITE_LOCK


def _reset_connections() -> None:
    import stock_store
    stock_store.reset_all_connections()


def backup_database(backup_dir: Optional[str] = None) -> Path:
    """备份数据库到指定目录，返回备份文件路径。

    默认备份到 data/backups/ 下，文件名含时间戳。
    """
    if backup_dir:
        dest_dir = Path(backup_dir)
    else:
        dest_dir = _data_dir() / "backups"
    dest_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_file = dest_dir / f"stock_store_{stamp}.sqlite3"

    # 使用 SQLite 的 backup API 保证一致性
    src_conn = sqlite3.connect(str(_db_path()), timeout=30.0)
    dst_conn = sqlite3.connect(str(dest_file))
    try:
        src_conn.backup(dst_conn)
        logger.info("数据库备份完成：%s", dest_file)
    finally:
        dst_conn.close()
        src_conn.close()
    return dest_file


def restore_database(backup_path: str) -> bool:
    """从备份文件恢复数据库。

    流程与 stock_store.restore_database 完全相同；迁移到本模块只是归类，
    语义不变。关键点：
    1. 取写锁，阻塞并发写入。
    2. 关闭所有已发放的 SQLite 连接，防止覆盖到"活"文件后还在被读写。
    3. 先备份当前数据库（尽力而为，失败只记录不中断）。
    4. copy2 覆盖 DB 文件。
    5. 重置连接状态/schema 初始化标记。

    失败分两类：备份文件缺失直接返回 False；覆盖阶段失败会尝试把"恢复前备份"
    回滚为当前 DB，保证用户手里的数据库至少不是半损坏状态。
    """
    src = Path(backup_path)
    if not src.is_file():
        logger.error("备份文件不存在：%s", backup_path)
        return False

    with _write_lock():
        _reset_connections()

        pre_restore_backup: Optional[Path] = None
        try:
            pre_restore_backup = backup_database()
        except Exception as exc:
            logger.warning("恢复前备份失败：%s", exc)

        try:
            shutil.copy2(str(src), str(_db_path()))
        except Exception as exc:
            logger.error("数据库恢复失败：%s", exc)
            if pre_restore_backup is not None and pre_restore_backup.is_file():
                try:
                    shutil.copy2(str(pre_restore_backup), str(_db_path()))
                    logger.info("已使用恢复前备份回滚：%s", pre_restore_backup)
                except Exception as rollback_exc:
                    logger.error("恢复前备份回滚失败：%s", rollback_exc)
            _reset_connections()
            return False

        _reset_connections()
        logger.info("数据库恢复完成，来源：%s", backup_path)
        return True


def list_backups(backup_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """列出所有备份文件，返回 [{path, size_mb, created_at}]。"""
    if backup_dir:
        d = Path(backup_dir)
    else:
        d = _data_dir() / "backups"
    if not d.is_dir():
        return []
    files = sorted(d.glob("stock_store_*.sqlite3"), reverse=True)
    result = []
    for f in files:
        stat = f.stat()
        result.append({
            "path": str(f),
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "created_at": datetime.fromtimestamp(stat.st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        })
    return result


def cleanup_all(
    history_keep_days: int = 365,
    intraday_keep_days: int = 30,
    snapshot_keep_count: int = 20,
) -> Dict[str, int]:
    """执行所有清理操作，返回各表删除行数汇总。"""
    import stock_store
    return {
        "history": stock_store.cleanup_old_history(history_keep_days),
        "intraday": stock_store.cleanup_old_intraday(intraday_keep_days),
        "scan_snapshots": stock_store.cleanup_old_scan_snapshots(snapshot_keep_count),
    }


class SafeRestoreOrchestrator:
    """把 "关闭后台 → 等待线程 → 执行 restore → 清理连接" 的流程聚成一个入口。

    不依赖 Tk。GUI 层只负责弹窗确认，把线程句柄与取消广播注入进来。这样
    可以单独对"安全恢复"这条路径做单测（不需要真实 DB/线程）。
    """

    def __init__(
        self,
        *,
        broadcast_cancel,
        thread_sources,
        wait_timeout_sec: float = 5.0,
        reset_connections=None,
        restore_impl=None,
    ) -> None:
        """
        :param broadcast_cancel: () -> None，在 restore 前广播取消
        :param thread_sources: 可迭代对象，每次 iter 返回线程对象（或 None），
            orchestrator 会 join(timeout=wait_timeout_sec)
        :param wait_timeout_sec: 每个线程的最长等待时间
        :param reset_connections: 默认走 stock_store.reset_all_connections
        :param restore_impl: 默认走模块级 restore_database
        """
        self._broadcast = broadcast_cancel
        self._thread_sources = thread_sources
        self._wait_timeout = float(wait_timeout_sec)
        self._reset = reset_connections or _reset_connections
        self._restore = restore_impl or restore_database

    def execute(self, backup_path: str) -> bool:
        self._broadcast()
        for t in self._thread_sources():
            if t is not None and t.is_alive():
                t.join(timeout=self._wait_timeout)
        ok = self._restore(backup_path)
        if ok:
            # 恢复成功后再显式重置一次，保证发起恢复的线程本地连接也失效。
            self._reset()
        return ok


