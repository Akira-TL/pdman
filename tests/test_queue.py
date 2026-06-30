import json

from pdman.queue import (
    QueueRecord,
    append_queue,
    create_queue_records,
    format_queue,
    load_queue,
    query_queue,
    queue_path,
    rewrite_queue,
    update_queue_from_results,
)
from pdman.status import TaskResult, TaskStatus
from pdman.task_input import TaskInput


def test_load_queue_missing_returns_empty(tmp_path):
    assert load_queue(str(tmp_path)) == []
    assert format_queue([]) == "No queue records found."


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

    loaded = load_queue(str(tmp_path))
    assert [record.file_name for record in loaded] == ["a.bin", "b.bin"]
    assert [record.file_name for record in query_queue(str(tmp_path), status="failed")] == ["b.bin"]
    assert [record.file_name for record in query_queue(str(tmp_path), last=1)] == ["b.bin"]


def test_rewrite_queue_replaces_records(tmp_path):
    append_queue([QueueRecord(queue_id="q1", url="https://example.com/a.bin")], str(tmp_path))
    rewrite_queue([QueueRecord(queue_id="q2", url="https://example.com/b.bin")], str(tmp_path))

    loaded = load_queue(str(tmp_path))
    assert [record.queue_id for record in loaded] == ["q2"]


def test_load_queue_skips_malformed_lines(tmp_path):
    path = queue_path(str(tmp_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "{bad-json}\n"
        + json.dumps(
            QueueRecord(queue_id="q1", url="https://example.com/a.bin").to_dict()
        )
        + "\n"
    )

    assert [record.queue_id for record in load_queue(str(tmp_path))] == ["q1"]


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
