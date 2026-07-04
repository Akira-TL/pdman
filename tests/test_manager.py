import asyncio
import json
import types
from io import StringIO

from rich.console import Console

from pdman.downloader import Downloader
from pdman.manager import Manager
from pdman.output_modes import OutputMode
from pdman.output_renderers import (
    NoOpProgress,
    PlainOutputRenderer,
    RichOutputRenderer,
    StructuredOutputRenderer,
)
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


def test_manager_uses_rich_renderer_for_rich_output():
    manager = Manager(log_path=None, output_mode="rich")

    assert manager.output_mode is OutputMode.RICH
    assert isinstance(manager._output_renderer, RichOutputRenderer)
    assert not isinstance(manager._progress, NoOpProgress)


def test_manager_uses_plain_renderer_for_non_rich_output():
    manager = Manager(log_path=None, output_mode="plain")

    assert manager.output_mode is OutputMode.PLAIN
    assert isinstance(manager._output_renderer, PlainOutputRenderer)
    assert isinstance(manager._progress, NoOpProgress)


def test_manager_uses_structured_renderer_for_machine_output():
    manager = Manager(log_path=None, output_mode="jsonl")

    assert manager.output_mode is OutputMode.JSONL
    assert isinstance(manager._output_renderer, StructuredOutputRenderer)
    assert isinstance(manager._progress, NoOpProgress)


def test_manager_plain_output_emits_low_frequency_lifecycle_lines():
    capture = StringIO()
    manager = Manager(log_path=None, output_mode="plain")
    manager._console = Console(file=capture, force_terminal=False, width=120)

    manager._start_runtime_run()
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
    manager.print_summary()

    output = capture.getvalue()
    assert f"Run started: {manager.run_id}" in output
    assert "Task completed: ok.bin" in output
    assert "task_id=" in output
    assert "reason=download completed" in output
    assert "Summary:" in output
    assert "completed: 1" in output


def test_manager_json_output_emits_final_summary_only(capsys):
    manager = Manager(log_path=None, output_mode="json")

    manager._start_runtime_run()
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
    manager.run_finished_at = "2026-07-04T00:00:01Z"
    manager.print_summary()

    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["schema_version"] == 1
    assert payload["kind"] == "download_summary"
    assert payload["run_id"] == manager.run_id
    assert payload["status"] == "completed"
    assert payload["exit_code"] == 0
    assert payload["counts"] == {"completed": 1, "skipped": 0, "failed": 0}
    assert payload["tasks"][0]["task_id"] == manager._task_id_for_url(
        "https://example.com/ok.bin"
    )
    assert payload["tasks"][0]["status"] == "completed"
    assert payload["tasks"][0]["resume_rejection"]["present"] is False


def test_manager_structured_output_suppresses_human_lifecycle_lines():
    capture = StringIO()
    manager = Manager(log_path=None, output_mode="jsonl")
    manager._console = Console(file=capture, force_terminal=False, width=120)

    manager._start_runtime_run()
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
    manager.print_summary()

    assert capture.getvalue() == ""


def test_manager_rich_output_keeps_summary_without_plain_lifecycle_lines():
    capture = StringIO()
    manager = Manager(log_path=None, output_mode="rich")
    manager._console = Console(file=capture, force_terminal=False, width=120)

    manager._start_runtime_run()
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
    manager.print_summary()

    output = capture.getvalue()
    assert "Run started:" not in output
    assert "Task completed:" not in output
    assert "Summary:" in output


def test_manager_console_log_sink_does_not_add_blank_lines():
    capture = StringIO()
    manager = Manager(log_path=None)
    manager._console = Console(file=capture, force_terminal=False, width=120)
    manager._reparse_logging()

    manager._logger.info("hello")

    lines = capture.getvalue().splitlines()
    assert len(lines) == 1
    assert "hello" in lines[0]


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


def test_manager_summary_shows_resume_rejection_visibility():
    manager = Manager(log_path=None)
    manager.record_task_result(
        TaskResult(
            url="https://example.com/ok.bin",
            filename="ok.bin",
            status=TaskStatus.COMPLETED,
            reason="download completed",
            downloaded_bytes=1024,
            total_bytes=1024,
            resume_rejection_code="file_size_mismatch",
            resume_rejection_reason="Resume rejected [file_size_mismatch]: file_size mismatch",
        )
    )

    summary = manager.summarize_results()

    assert "Resume:" in summary
    assert "ok.bin - Resume rejected [file_size_mismatch]: file_size mismatch" in summary
    assert manager.exit_code == 0


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
    assert history_record["resume_rejection_code"] is None
    assert history_record["resume_rejection_reason"] is None
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
