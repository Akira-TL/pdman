import json

import pytest

from pdman.queue import (
    QUEUE_SCHEMA_VERSION,
    QueueRecord,
    append_queue,
    clear_queue,
    create_queue_records,
    format_queue,
    format_queue_validation,
    load_queue,
    query_queue,
    queue_lock_path,
    queue_path,
    recover_running,
    remove_queue_records,
    repair_queue,
    rewrite_queue,
    update_queue_from_results,
    validate_queue,
)
from pdman.status import TaskResult, TaskStatus
from pdman.task_input import TaskInput


def test_load_queue_missing_returns_empty(tmp_path):
    assert load_queue(str(tmp_path)) == []
    assert format_queue([]) == "No queue records found."


def test_queue_lock_path_is_cache_local(tmp_path):
    assert queue_lock_path(str(tmp_path)) == tmp_path / "queue.lock"


def test_append_and_query_queue_records(tmp_path):
    records = create_queue_records(
        [
            TaskInput("https://example.com/a.bin", file_name="a.bin"),
            TaskInput("https://example.com/b.bin", file_name="b.bin"),
        ]
    )
    records[1].status = "failed"
    records[1].last_error = "HTTP 503 during header check"

    append_queue(records, str(tmp_path))

    assert queue_lock_path(str(tmp_path)).exists()
    loaded = load_queue(str(tmp_path))
    assert loaded[0].schema_version == QUEUE_SCHEMA_VERSION
    assert [record.file_name for record in loaded] == ["a.bin", "b.bin"]
    assert [record.file_name for record in query_queue(str(tmp_path), status="failed")] == ["b.bin"]
    assert [record.file_name for record in query_queue(str(tmp_path), last=1)] == ["b.bin"]


def test_rewrite_queue_replaces_records(tmp_path):
    append_queue([QueueRecord(queue_id="q1", url="https://example.com/a.bin")], str(tmp_path))
    rewrite_queue([QueueRecord(queue_id="q2", url="https://example.com/b.bin")], str(tmp_path))

    loaded = load_queue(str(tmp_path))
    assert [record.queue_id for record in loaded] == ["q2"]


def test_load_queue_skips_malformed_and_future_schema_lines(tmp_path):
    path = queue_path(str(tmp_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "{bad-json}\n"
        + json.dumps({"schema_version": 99, "queue_id": "future", "url": "https://example.com/f.bin"})
        + "\n"
        + json.dumps(
            QueueRecord(queue_id="q1", url="https://example.com/a.bin").to_dict()
        )
        + "\n"
    )

    assert [record.queue_id for record in load_queue(str(tmp_path))] == ["q1"]


def test_legacy_queue_record_without_schema_version_loads_as_v1(tmp_path):
    path = queue_path(str(tmp_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"queue_id": "q1", "url": "https://example.com/a.bin"}) + "\n"
    )

    record = load_queue(str(tmp_path))[0]

    assert record.schema_version == QUEUE_SCHEMA_VERSION
    assert record.queue_id == "q1"
    assert record.attempts == 0


def test_validate_queue_reports_invalid_attempts(tmp_path):
    path = queue_path(str(tmp_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "queue_id": "q1",
                "url": "https://example.com/a.bin",
                "attempts": "bad",
            }
        )
        + "\n"
    )

    report = validate_queue(str(tmp_path))

    assert report.invalid == 1
    assert "invalid attempts" in format_queue_validation(report)


def test_validate_queue_reports_bad_lines_and_duplicates(tmp_path):
    path = queue_path(str(tmp_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "{bad-json}\n"
        + json.dumps({"queue_id": "q1", "url": "https://example.com/a.bin"})
        + "\n"
        + json.dumps({"queue_id": "q1", "url": "https://example.com/b.bin"})
        + "\n"
        + json.dumps({"queue_id": "q2"})
        + "\n"
        + json.dumps({"schema_version": 99, "queue_id": "q3", "url": "https://example.com/c.bin"})
        + "\n"
        + json.dumps({"queue_id": "q4", "url": "https://example.com/d.bin", "status": "bad"})
        + "\n"
    )

    report = validate_queue(str(tmp_path))
    text = format_queue_validation(report)

    assert report.valid == 1
    assert report.malformed == 1
    assert report.duplicate_ids == 1
    assert report.invalid == 2
    assert report.unsupported_schema == 1
    assert not report.ok
    assert "Queue validation:" in text
    assert "duplicate queue_id" in text


def test_repair_queue_fixes_repairable_records_and_drops_unrepairable(tmp_path):
    path = queue_path(str(tmp_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "{bad-json}\n"
        + json.dumps({"queue_id": "q1", "url": "https://example.com/a.bin"})
        + "\n"
        + json.dumps({"queue_id": "q1", "url": "https://example.com/b.bin"})
        + "\n"
        + json.dumps({"queue_id": "q2"})
        + "\n"
        + json.dumps({"schema_version": 99, "queue_id": "q3", "url": "https://example.com/c.bin"})
        + "\n"
        + json.dumps({"queue_id": "q4", "url": "https://example.com/d.bin", "status": "bad"})
        + "\n"
        + json.dumps({"queue_id": "q5", "url": "https://example.com/e.bin", "attempts": "bad"})
        + "\n"
    )

    stats = repair_queue(str(tmp_path))
    records = load_queue(str(tmp_path))

    assert stats["kept"] == 4
    assert stats["dropped_malformed"] == 1
    assert stats["dropped_invalid"] == 1
    assert stats["dropped_unsupported_schema"] == 1
    assert stats["fixed"] == 4
    assert len({record.queue_id for record in records}) == 4
    assert records[-2].status == "failed"
    assert records[-1].attempts == 0
    assert validate_queue(str(tmp_path)).ok


def test_recover_remove_and_clear_queue_records(tmp_path):
    records = [
        QueueRecord(queue_id="q1", url="https://example.com/a.bin", status="running"),
        QueueRecord(queue_id="q2", url="https://example.com/b.bin", status="completed"),
        QueueRecord(queue_id="q3", url="https://example.com/c.bin", status="failed"),
    ]
    rewrite_queue(records, str(tmp_path))

    assert recover_running(str(tmp_path)) == 1
    loaded = load_queue(str(tmp_path))
    assert loaded[0].status == "pending"
    assert loaded[0].last_error == "recovered from stale running state"

    assert remove_queue_records(["q3"], str(tmp_path)) == 1
    assert [record.queue_id for record in load_queue(str(tmp_path))] == ["q1", "q2"]

    assert clear_queue(status="completed", cache_dir=str(tmp_path)) == 1
    assert [record.queue_id for record in load_queue(str(tmp_path))] == ["q1"]

    assert clear_queue(all_records=True, cache_dir=str(tmp_path)) == 1
    assert load_queue(str(tmp_path)) == []


def test_clear_queue_requires_status_or_all(tmp_path):
    with pytest.raises(ValueError):
        clear_queue(cache_dir=str(tmp_path))


def test_mark_records_running_increments_attempts(tmp_path):
    from pdman.queue import start_queue_records

    records = [QueueRecord(queue_id="q1", url="https://example.com/a.bin")]
    rewrite_queue(records, str(tmp_path))

    selected = start_queue_records(
        cache_dir=str(tmp_path),
        status="pending",
        limit=1,
        run_id="run-1",
    )

    assert selected[0].status == "running"
    assert selected[0].attempts == 1
    assert load_queue(str(tmp_path))[0].attempts == 1


def test_update_queue_from_results_completed_and_failed():
    records = [
        QueueRecord(queue_id="q1", url="https://example.com/a.bin"),
        QueueRecord(queue_id="q2", url="https://example.com/b.bin"),
    ]
    selected = records.copy()
    results = [
        TaskResult(
            url="https://example.com/a.bin",
            filename="a.bin",
            status=TaskStatus.COMPLETED,
            reason="download completed",
        ),
        TaskResult(
            url="https://example.com/b.bin",
            filename="b.bin",
            status=TaskStatus.FAILED,
            reason="HTTP 503 during header check",
        ),
    ]

    updated = update_queue_from_results(records, selected, results, "run-1")

    assert [record.status for record in updated] == ["completed", "failed"]
    assert [record.last_run_id for record in updated] == ["run-1", "run-1"]
    assert updated[0].last_error is None
    assert updated[1].last_error == "HTTP 503 during header check"


def test_format_queue_includes_status_and_error():
    text = format_queue(
        [
            QueueRecord(
                queue_id="q1",
                url="https://example.com/a.bin",
                file_name="a.bin",
                status="failed",
                last_error="HTTP 503 during header check",
            )
        ]
    )

    assert "failed" in text
    assert "q1" in text
    assert "a.bin" in text
    assert "HTTP 503" in text
    assert "attempts=0" in text
