"""prediction_snapshot_service 的单元测试——临时目录 + mock git。"""
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from src.services import prediction_snapshot_service as svc


def _completed(returncode: int = 0, stdout: str = "", stderr: str = ""):
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


def _payload(trade_date: str = "20260731"):
    return {
        "trade_date": trade_date,
        "summary": "测试摘要",
        "continuation_candidates": [
            {"code": "600001", "name": "甲", "score": 80, "reasons": "封板质量高"},
        ],
        "first_board_candidates": [],
        "fresh_first_board_candidates": [
            {"code": "600002", "name": "乙", "score": 90, "reasons": "资金接入"},
        ],
        "broken_board_wrap_candidates": [],
        "trend_limit_up_candidates": [],
        "compare_context": {"sentiment_score": 60, "sentiment_label": "偏暖"},
        "concept_hype_result": {"big": "blob"},
    }


class SnapshotPathTestCase(unittest.TestCase):
    """每个用例把 SNAPSHOT_PATH 指到独立临时目录。"""

    def setUp(self):
        self._tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self._tmp, ignore_errors=True)
        self.snapshot_path = self._tmp / "latest_prediction.json"
        patcher = mock.patch.object(svc, "SNAPSHOT_PATH", self.snapshot_path)
        patcher.start()
        self.addCleanup(patcher.stop)


class TestBuildSnapshot(unittest.TestCase):
    def test_structure_and_trimming(self):
        snapshot = svc.build_snapshot(_payload())
        self.assertEqual(snapshot["schema_version"], svc.SCHEMA_VERSION)
        self.assertEqual(snapshot["trade_date"], "20260731")
        self.assertNotIn("concept_hype_result", snapshot["prediction"])
        self.assertEqual(
            snapshot["prediction"]["compare_context"]["sentiment_label"], "偏暖"
        )
        codes = {p["code"] for p in snapshot["simulated_buys"]}
        self.assertTrue(codes)
        self.assertLessEqual(len(snapshot["simulated_buys"]), 2)


class TestExportSnapshot(SnapshotPathTestCase):
    def test_writes_file_without_git(self):
        with mock.patch("stock_store.list_limit_up_prediction_dates", return_value=[]), \
                mock.patch.object(svc, "_git_publish") as publish:
            ok = svc.export_snapshot(_payload(), auto_push=False)
        self.assertTrue(ok)
        publish.assert_not_called()
        data = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        self.assertEqual(data["prediction"]["trade_date"], "20260731")
        self.assertNotIn("concept_hype_result", data["prediction"])

    def test_auto_push_calls_git_publish(self):
        with mock.patch("stock_store.list_limit_up_prediction_dates", return_value=[]), \
                mock.patch.object(svc, "_git_publish") as publish:
            ok = svc.export_snapshot(_payload())
        self.assertTrue(ok)
        publish.assert_called_once()

    def test_skips_when_older_than_local_latest(self):
        with mock.patch(
            "stock_store.list_limit_up_prediction_dates", return_value=["20260801"]
        ), mock.patch.object(svc, "_git_publish") as publish:
            ok = svc.export_snapshot(_payload("20260731"))
        self.assertFalse(ok)
        self.assertFalse(self.snapshot_path.exists())
        publish.assert_not_called()

    def test_allows_equal_trade_date(self):
        with mock.patch(
            "stock_store.list_limit_up_prediction_dates", return_value=["20260731"]
        ), mock.patch.object(svc, "_git_publish"):
            ok = svc.export_snapshot(_payload("20260731"))
        self.assertTrue(ok)

    def test_invalid_payload_returns_false(self):
        self.assertFalse(svc.export_snapshot(None))
        self.assertFalse(svc.export_snapshot({"summary": "无日期"}))


class TestGitPublish(unittest.TestCase):
    """_run_git 全部 mock，不碰真实 git。SNAPSHOT_PATH 指向仓库内路径。"""

    def setUp(self):
        patcher = mock.patch.object(
            svc, "SNAPSHOT_PATH", svc.REPO_ROOT / "snapshots" / "_test_only.json"
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _git_args(self, run_git_mock):
        return [item.args[0] for item in run_git_mock.call_args_list]

    def test_full_success_sequence(self):
        results = [
            _completed(0),  # add
            _completed(1),  # diff --cached：有变化
            _completed(0),  # commit
            _completed(0),  # push
        ]
        with mock.patch.object(svc, "_run_git", side_effect=results) as run_git:
            self.assertTrue(svc._git_publish("20260731"))
        args = self._git_args(run_git)
        self.assertEqual(args[0][0], "add")
        self.assertEqual(args[1][0], "diff")
        self.assertEqual(args[2][0], "commit")
        self.assertEqual(args[3][0], "push")

    def test_no_change_skips_commit(self):
        results = [_completed(0), _completed(0)]  # add / diff 无变化
        with mock.patch.object(svc, "_run_git", side_effect=results) as run_git:
            self.assertTrue(svc._git_publish("20260731"))
        self.assertEqual(len(run_git.call_args_list), 2)

    def test_push_failure_then_rebase_retry(self):
        results = [
            _completed(0),  # add
            _completed(1),  # diff
            _completed(0),  # commit
            _completed(1, stderr="rejected"),  # push 失败
            _completed(0),  # pull --rebase
            _completed(0),  # push 重试成功
        ]
        with mock.patch.object(svc, "_run_git", side_effect=results) as run_git:
            self.assertTrue(svc._git_publish("20260731"))
        args = self._git_args(run_git)
        self.assertEqual(args[4][0], "pull")
        self.assertEqual(args[5][0], "push")

    def test_rebase_failure_aborts_and_degrades(self):
        results = [
            _completed(0),  # add
            _completed(1),  # diff
            _completed(0),  # commit
            _completed(1),  # push 失败
            _completed(1, stderr="conflict"),  # pull --rebase 失败
            _completed(0),  # rebase --abort
        ]
        with mock.patch.object(svc, "_run_git", side_effect=results) as run_git:
            self.assertFalse(svc._git_publish("20260731"))
        args = self._git_args(run_git)
        self.assertEqual(args[5], ["rebase", "--abort"])

    def test_unexpected_exception_never_raises(self):
        with mock.patch.object(svc, "_run_git", side_effect=OSError("git 不存在")):
            self.assertFalse(svc._git_publish("20260731"))


class TestImportSnapshot(SnapshotPathTestCase):
    def _write_snapshot(self, trade_date="20260731", exported_at="2026-07-31 19:00:00"):
        snapshot = {
            "schema_version": 1,
            "exported_at": exported_at,
            "trade_date": trade_date,
            "prediction": {k: v for k, v in _payload(trade_date).items()
                           if k != "concept_hype_result"},
            "simulated_buys": [],
        }
        self.snapshot_path.write_text(
            json.dumps(snapshot, ensure_ascii=False), encoding="utf-8"
        )

    def test_missing_file(self):
        self.assertEqual(svc.import_snapshot_if_newer(), "missing")

    def test_corrupt_file(self):
        self.snapshot_path.write_text("{broken", encoding="utf-8")
        self.assertEqual(svc.import_snapshot_if_newer(), "corrupt")

    def test_invalid_structure(self):
        self.snapshot_path.write_text(
            json.dumps({"exported_at": "x"}), encoding="utf-8"
        )
        self.assertEqual(svc.import_snapshot_if_newer(), "invalid")

    def test_imports_when_local_missing(self):
        self._write_snapshot()
        with mock.patch(
            "stock_store.get_limit_up_prediction_saved_at", return_value=None
        ), mock.patch(
            "stock_store.save_limit_up_prediction_record"
        ) as save_record, mock.patch(
            "stock_store.save_last_limit_up_prediction"
        ) as save_last, mock.patch(
            "stock_store.load_last_limit_up_prediction", return_value=None
        ):
            self.assertEqual(svc.import_snapshot_if_newer(), "imported:20260731")
        save_record.assert_called_once()
        self.assertEqual(save_record.call_args.args[0]["trade_date"], "20260731")
        save_last.assert_called_once()

    def test_stale_snapshot_skipped(self):
        self._write_snapshot(exported_at="2026-07-31 19:00:00")
        with mock.patch(
            "stock_store.get_limit_up_prediction_saved_at",
            return_value="2026-07-31 20:00:00",
        ), mock.patch(
            "stock_store.save_limit_up_prediction_record"
        ) as save_record, mock.patch(
            "stock_store.save_last_limit_up_prediction"
        ) as save_last:
            self.assertEqual(svc.import_snapshot_if_newer(), "stale")
        save_record.assert_not_called()
        save_last.assert_not_called()

    def test_does_not_downgrade_newer_last_prediction(self):
        self._write_snapshot(trade_date="20260731")
        with mock.patch(
            "stock_store.get_limit_up_prediction_saved_at", return_value=None
        ), mock.patch(
            "stock_store.save_limit_up_prediction_record"
        ) as save_record, mock.patch(
            "stock_store.save_last_limit_up_prediction"
        ) as save_last, mock.patch(
            "stock_store.load_last_limit_up_prediction",
            return_value={"trade_date": "20260801"},
        ):
            self.assertEqual(svc.import_snapshot_if_newer(), "imported:20260731")
        save_record.assert_called_once()
        save_last.assert_not_called()


class TestImportSnapshotWithRealStore(SnapshotPathTestCase):
    """走真实（临时）SQLite，覆盖 get_limit_up_prediction_saved_at 与全链路导入。"""

    def setUp(self):
        super().setUp()
        import stock_store

        self._db_tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self._db_tmp, ignore_errors=True)
        for name, value in (
            ("_DATA_DIR", self._db_tmp),
            ("_DB_PATH", self._db_tmp / "test.sqlite3"),
        ):
            patcher = mock.patch.object(stock_store, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_end_to_end_import_then_stale(self):
        import stock_store

        # exported_at 用确定性的过去时间：首次导入不受影响（本地无记录），
        # 二次导入时本地 saved_at（导入时刻）必然更新 → stale
        snapshot = {
            "schema_version": 1,
            "exported_at": "2020-01-01 00:00:00",
            "trade_date": "20260731",
            "prediction": {k: v for k, v in _payload().items()
                           if k != "concept_hype_result"},
            "simulated_buys": [],
        }
        self.snapshot_path.write_text(
            json.dumps(snapshot, ensure_ascii=False), encoding="utf-8"
        )

        self.assertEqual(svc.import_snapshot_if_newer(), "imported:20260731")
        loaded = stock_store.load_limit_up_prediction_by_date("20260731")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["summary"], "测试摘要")
        self.assertTrue(stock_store.get_limit_up_prediction_saved_at("20260731"))
        self.assertEqual(
            stock_store.load_last_limit_up_prediction()["trade_date"], "20260731"
        )
        # 本地 saved_at 已是导入时刻（新于快照 exported_at）→ 二次导入跳过
        self.assertEqual(svc.import_snapshot_if_newer(), "stale")


if __name__ == "__main__":
    unittest.main()
