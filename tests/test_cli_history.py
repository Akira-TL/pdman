import json

import pdman.cli as cli


class ExplodingManager:
    def __init__(self, *args, **kwargs):
        raise AssertionError("query commands must not construct Manager")


def write_history(cache_dir, records):
    path = cache_dir / "history.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")


def write_run(cache_dir, run):
    path = cache_dir / "runs" / f"{run['run_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(run))


def test_cli_history_does_not_start_download_manager(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "Manager", ExplodingManager)
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "filename": "ok.bin",
                "status": "completed",
                "downloaded_bytes": 1024,
                "finished_at": "2026-01-01T00:00:00Z",
            }
        ],
    )

    exit_code = cli.main(["history", "--cache-dir", str(tmp_path)])

    assert exit_code == 0
    assert "ok.bin" in capsys.readouterr().out


def test_cli_history_shows_resume_rejection(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "filename": "resumed.bin",
                "status": "completed",
                "downloaded_bytes": 1024,
                "resume_rejection_code": "file_size_mismatch",
                "resume_rejection_reason": "Resume rejected [file_size_mismatch]: file_size mismatch",
            }
        ],
    )

    exit_code = cli.main(["history", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "resumed.bin" in output
    assert "Resume rejected [file_size_mismatch]: file_size mismatch" in output


def test_cli_history_json_includes_resume_rejection_payload(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "filename": "resumed.bin",
                "status": "completed",
                "downloaded_bytes": 1024,
                "resume_rejection_code": "file_size_mismatch",
                "resume_rejection_reason": "Resume rejected [file_size_mismatch]: file_size mismatch",
            }
        ],
    )

    exit_code = cli.main(["history", "--json", "--cache-dir", str(tmp_path)])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["count"] == 1
    assert payload["records"][0]["filename"] == "resumed.bin"
    assert payload["records"][0]["resume_rejection"] == {
        "present": True,
        "code": "file_size_mismatch",
        "reason": "Resume rejected [file_size_mismatch]: file_size mismatch",
    }


def test_cli_history_jsonl_outputs_one_record_per_line(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "filename": "resumed.bin",
                "status": "completed",
                "downloaded_bytes": 1024,
                "resume_rejection_code": "url_mismatch",
                "resume_rejection_reason": "Resume rejected [url_mismatch]: url mismatch",
            },
            {
                "run_id": "run-1",
                "filename": "fresh.bin",
                "status": "completed",
                "downloaded_bytes": 2048,
            },
        ],
    )

    exit_code = cli.main(["history", "--jsonl", "--cache-dir", str(tmp_path)])

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert len(lines) == 2
    first = json.loads(lines[0])
    second = json.loads(lines[1])
    assert first["filename"] == "resumed.bin"
    assert first["resume_rejection"] == {
        "present": True,
        "code": "url_mismatch",
        "reason": "Resume rejected [url_mismatch]: url mismatch",
    }
    assert second["filename"] == "fresh.bin"
    assert second["resume_rejection"] == {
        "present": False,
        "code": None,
        "reason": None,
    }


def test_cli_history_failed_filter(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "filename": "ok.bin",
                "status": "completed",
                "downloaded_bytes": 1024,
            },
            {
                "run_id": "run-1",
                "filename": "bad.bin",
                "status": "failed",
                "reason": "HTTP 503 during header check",
                "downloaded_bytes": 0,
            },
        ],
    )

    exit_code = cli.main(["history", "--failed", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "bad.bin" in output
    assert "ok.bin" not in output


def test_cli_runs_lists_run_summaries(tmp_path, capsys):
    write_run(
        tmp_path,
        {
            "run_id": "run-1",
            "status": "finished",
            "finished_at": "2026-01-01T00:00:00Z",
            "task_counts": {"completed": 1, "skipped": 0, "failed": 0},
            "exit_code": 0,
        },
    )

    exit_code = cli.main(["runs", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "run-1" in output
    assert "completed=1" in output


def test_cli_run_detail_shows_tasks(tmp_path, capsys):
    write_run(
        tmp_path,
        {
            "run_id": "run-1",
            "status": "finished",
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": "2026-01-01T00:01:00Z",
            "tmp_policy": "auto",
            "task_counts": {"completed": 1, "skipped": 0, "failed": 0},
            "exit_code": 0,
        },
    )
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "filename": "ok.bin",
                "status": "completed",
                "downloaded_bytes": 2048,
            }
        ],
    )

    exit_code = cli.main(["run", "run-1", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Run run-1" in output
    assert "ok.bin" in output


def test_cli_run_missing_returns_one(tmp_path, capsys):
    exit_code = cli.main(["run", "missing", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Run not found: missing" in output
