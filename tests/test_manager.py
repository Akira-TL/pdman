import asyncio
import json
import types

from pdman.downloader import Downloader
from pdman.manager import Manager
from pdman.status import TaskReason, TaskResult, TaskStatus


def test_max_downloads_limits_url_task_concurrency(tmp_path):
    async def run_case():
        manager = Manager(max_downloads=1, log_path=None)
        state = {"active": 0, "max_seen": 0}

        async def fake_start_download(self):
            state["active"] += 1
            state["max_seen"] = max(state["max_seen"], state["active"])
            await asyncio.sleep(0.02)
            state["active"] -= 1
            return self.record_result(
                TaskStatus.COMPLETED,
                reason="download completed",
            )

        for index in range(3):
            url = f"https://example.com/file-{index}.bin"
            downloader = Downloader(manager, url, str(tmp_path))
            downloader.filename = f"file-{index}.bin"
            downloader.start_download = types.MethodType(fake_start_download, downloader)
            manager._urls[url] = downloader

        await manager._download_once()

        assert state["max_seen"] == 1
        assert manager._downloaders == []

    asyncio.run(run_case())


def test_manager_summary_counts_task_results():
    manager = Manager(log_path=None)
    manager.record_task_result(
        TaskResult(
            url="https://example.com/ok.bin",
            filename="ok.bin",
            status=TaskStatus.COMPLETED,
            reason="download completed",
            downloaded_bytes=1024,
            total_bytes=1024,
        )
    )
    manager.record_task_result(
        TaskResult(
            url="https://example.com/existing.bin",
            filename="existing.bin",
            status=TaskStatus.SKIPPED,
            reason="target already exists",
            reason_code=TaskReason.TARGET_EXISTS,
        )
    )
    manager.record_task_result(
        TaskResult(
            url="https://example.com/status.bin",
            filename="status.bin",
            status=TaskStatus.FAILED,
            reason="HTTP 503 during header check",
            reason_code=TaskReason.HTTP_STATUS,
        )
    )

    summary = manager.summarize_results()

    assert "completed: 1" in summary
    assert "skipped: 1" in summary
    assert "failed: 1" in summary
    assert "downloaded: 1.0 KiB" in summary
    assert "existing.bin - target already exists" in summary
    assert "status.bin - HTTP 503 during header check" in summary
    assert manager.exit_code == 1


def test_manager_writes_run_metadata_and_history(tmp_path):
    manager = Manager(
        log_path=None,
        cache_dir=str(tmp_path / "cache"),
    )

    manager._start_runtime_run()
    assert manager.runtime_paths.active_run_path.exists()

    manager.record_task_result(
        TaskResult(
            url="https://example.com/ok.bin",
            filename="ok.bin",
            status=TaskStatus.COMPLETED,
            reason="download completed",
            downloaded_bytes=1024,
            total_bytes=1024,
        )
    )
    manager._finish_runtime_run()

    assert not manager.runtime_paths.active_run_path.exists()
    assert manager.runtime_paths.final_run_path.exists()
    assert not manager.runtime_paths.run_dir.exists()

    final_run = json.loads(manager.runtime_paths.final_run_path.read_text())
    assert final_run["task_counts"] == {
        "completed": 1,
        "skipped": 0,
        "failed": 0,
    }
    history = manager.runtime_paths.history_path.read_text().splitlines()
    assert len(history) == 1
    history_record = json.loads(history[0])
    assert history_record["status"] == "completed"
    assert history_record["filename"] == "ok.bin"
    assert final_run["tmp_cleanup"] == {
        "policy": "cleanup_on_finish",
        "kept": False,
        "run_dir": str(manager.runtime_paths.run_dir),
        "error": None,
    }


def test_manager_keep_tmp_preserves_failed_run_dir(tmp_path):
    manager = Manager(
        log_path=None,
        cache_dir=str(tmp_path / "cache"),
        keep_tmp=True,
    )
    manager._start_runtime_run()
    marker = manager.runtime_paths.run_dir / "debug.marker"
    marker.write_text("keep me")

    manager.record_task_result(
        TaskResult(
            url="https://example.com/bad.bin",
            filename="bad.bin",
            status=TaskStatus.FAILED,
            reason="temporary directory has insufficient free space",
            reason_code=TaskReason.TMP_SPACE_INSUFFICIENT,
        )
    )
    manager._finish_runtime_run()

    final_run = json.loads(manager.runtime_paths.final_run_path.read_text())
    assert manager.runtime_paths.run_dir.exists()
    assert marker.exists()
    assert final_run["tmp_cleanup"] == {
        "policy": "keep_failed",
        "kept": True,
        "run_dir": str(manager.runtime_paths.run_dir),
        "error": None,
    }


def test_manager_without_keep_tmp_cleans_failed_run_dir(tmp_path):
    manager = Manager(
        log_path=None,
        cache_dir=str(tmp_path / "cache"),
        keep_tmp=False,
    )
    manager._start_runtime_run()
    marker = manager.runtime_paths.run_dir / "debug.marker"
    marker.write_text("delete me")

    manager.record_task_result(
        TaskResult(
            url="https://example.com/bad.bin",
            filename="bad.bin",
            status=TaskStatus.FAILED,
            reason="temporary directory has insufficient free space",
            reason_code=TaskReason.TMP_SPACE_INSUFFICIENT,
        )
    )
    manager._finish_runtime_run()

    final_run = json.loads(manager.runtime_paths.final_run_path.read_text())
    assert not manager.runtime_paths.run_dir.exists()
    assert final_run["tmp_cleanup"]["policy"] == "cleanup_on_finish"
    assert final_run["tmp_cleanup"]["kept"] is False
