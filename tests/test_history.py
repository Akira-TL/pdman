import json

from pdman.history import (
    format_history,
    format_run_detail,
    format_runs,
    list_runs,
    load_run,
    query_history,
)


def write_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")


def test_query_history_returns_empty_when_missing(tmp_path):
    assert query_history(str(tmp_path)) == []
    assert format_history([]) == "No history found."


def test_query_history_filters_last_status_and_run_id(tmp_path):
    records = [
        {
            "run_id": "run-1",
            "filename": "a.bin",
            "status": "completed",
            "downloaded_bytes": 1024,
            "finished_at": "2026-01-01T00:00:00Z",
        },
        {
            "run_id": "run-1",
            "filename": "b.bin",
            "status": "failed",
            "reason": "HTTP 503 during header check",
            "downloaded_bytes": 0,
            "finished_at": "2026-01-01T00:01:00Z",
        },
        {
            "run_id": "run-2",
            "filename": "c.bin",
            "status": "skipped",
            "reason": "target already exists",
            "downloaded_bytes": 0,
            "finished_at": "2026-01-01T00:02:00Z",
        },
    ]
    write_jsonl(tmp_path / "history.jsonl", records)

    assert [r["filename"] for r in query_history(str(tmp_path), last=2)] == [
        "b.bin",
        "c.bin",
    ]
    assert [r["filename"] for r in query_history(str(tmp_path), status="failed")] == [
        "b.bin"
    ]
    assert [r["filename"] for r in query_history(str(tmp_path), run_id="run-2")] == [
        "c.bin"
    ]


def test_query_history_skips_malformed_lines(tmp_path):
    path = tmp_path / "history.jsonl"
    path.write_text(
        "{bad-json}\n"
        + json.dumps({"run_id": "run-1", "filename": "ok.bin", "status": "completed"})
        + "\n"
    )

    assert [r["filename"] for r in query_history(str(tmp_path))] == ["ok.bin"]


def test_list_runs_and_load_run(tmp_path):
    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    (run_dir / "run-1.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "status": "finished",
                "finished_at": "2026-01-01T00:00:00Z",
                "task_counts": {"completed": 1, "skipped": 0, "failed": 0},
                "exit_code": 0,
            }
        )
    )
    (run_dir / "run-2.json").write_text(
        json.dumps(
            {
                "run_id": "run-2",
                "status": "finished",
                "finished_at": "2026-01-01T00:01:00Z",
                "task_counts": {"completed": 0, "skipped": 0, "failed": 1},
                "exit_code": 1,
            }
        )
    )

    assert [r["run_id"] for r in list_runs(str(tmp_path), last=1)] == ["run-2"]
    assert load_run("run-1", str(tmp_path))["run_id"] == "run-1"
    assert load_run("missing", str(tmp_path)) is None


def test_formatters_include_status_counts_and_tasks():
    history_text = format_history(
        [
            {
                "filename": "failed.bin",
                "status": "failed",
                "reason": "MD5 mismatch",
                "downloaded_bytes": 0,
                "finished_at": "2026-01-01T00:00:00Z",
            }
        ]
    )
    assert "failed.bin" in history_text
    assert "MD5 mismatch" in history_text

    runs_text = format_runs(
        [
            {
                "run_id": "run-1",
                "status": "finished",
                "task_counts": {"completed": 1, "skipped": 0, "failed": 1},
                "exit_code": 1,
            }
        ]
    )
    assert "run-1" in runs_text
    assert "completed=1" in runs_text
    assert "failed=1" in runs_text

    detail_text = format_run_detail(
        {
            "run_id": "run-1",
            "status": "finished",
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": "2026-01-01T00:01:00Z",
            "tmp_policy": "auto",
            "task_counts": {"completed": 1, "skipped": 0, "failed": 0},
            "exit_code": 0,
        },
        [{"filename": "ok.bin", "status": "completed", "downloaded_bytes": 2048}],
    )
    assert "Run run-1" in detail_text
    assert "ok.bin" in detail_text
    assert "2.0 KiB" in detail_text
