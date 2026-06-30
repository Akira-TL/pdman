import json

import pdman.cli as cli
from pdman.queue import load_queue, queue_path, rewrite_queue


def test_cli_queue_add_url_and_list(tmp_path, capsys):
    exit_code = cli.main(
        [
            "queue",
            "add",
            "--cache-dir",
            str(tmp_path),
            "-d",
            str(tmp_path / "downloads"),
            "--file-name",
            "file.bin",
            "https://example.com/file.bin",
        ]
    )

    assert exit_code == 0
    records = load_queue(str(tmp_path))
    assert len(records) == 1
    assert records[0].url == "https://example.com/file.bin"
    assert records[0].file_name == "file.bin"
    assert records[0].dir_path == str(tmp_path / "downloads")

    exit_code = cli.main(["queue", "list", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Queue:" in output
    assert "file.bin" in output
    assert "attempts=0" in output


def test_cli_queue_add_input_file(tmp_path):
    task_file = tmp_path / "tasks.yaml"
    task_file.write_text(
        "\n".join(
            [
                "https://example.com/a.bin:",
                "  file_name: a.bin",
                f"  dir_path: {tmp_path / 'downloads'}",
            ]
        )
    )

    exit_code = cli.main(
        ["queue", "add", "--cache-dir", str(tmp_path), "-i", str(task_file)]
    )

    assert exit_code == 0
    records = load_queue(str(tmp_path))
    assert len(records) == 1
    assert records[0].file_name == "a.bin"
    assert records[0].dir_path == str(tmp_path / "downloads")


def test_cli_queue_list_status_and_attempt_filters(tmp_path, capsys):
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/a.bin"])
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/b.bin"])
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/c.bin"])
    records = load_queue(str(tmp_path))
    records[0].status = "failed"
    records[0].attempts = 1
    records[0].last_error = "HTTP 503 during header check"
    records[1].status = "failed"
    records[1].attempts = 3
    records[1].last_error = "Connection timed out"
    records[2].status = "completed"
    records[2].attempts = 4
    rewrite_queue(records, str(tmp_path))

    exit_code = cli.main(
        [
            "queue",
            "list",
            "--cache-dir",
            str(tmp_path),
            "--status",
            "failed",
            "--attempts-ge",
            "3",
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "b.bin" in output
    assert "a.bin" not in output
    assert "attempts=3" in output

    exit_code = cli.main(
        ["queue", "list", "--cache-dir", str(tmp_path), "--attempts-lt", "3"]
    )
    output = capsys.readouterr().out
    assert exit_code == 0
    assert "a.bin" in output
    assert "b.bin" not in output
    assert "c.bin" not in output


def test_cli_queue_start_no_pending_returns_zero(tmp_path, capsys):
    exit_code = cli.main(["queue", "start", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "No queue records to start" in output


def test_cli_queue_retry_failed_no_failed_returns_zero(tmp_path, capsys):
    exit_code = cli.main(["queue", "retry-failed", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "No failed queue records to retry" in output


def test_cli_queue_retry_failed_selects_failed_only_and_honors_limit(tmp_path):
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/pending.bin"])
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "http://127.0.0.1:1/fail-1.bin"])
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "http://127.0.0.1:1/fail-2.bin"])
    records = load_queue(str(tmp_path))
    records[1].status = "failed"
    records[1].last_error = "first failure"
    records[2].status = "failed"
    records[2].last_error = "second failure"
    rewrite_queue(records, str(tmp_path))

    exit_code = cli.main(
        [
            "queue",
            "retry-failed",
            "--cache-dir",
            str(tmp_path),
            "--limit",
            "1",
            "--retry",
            "0",
        ]
    )

    records = load_queue(str(tmp_path))
    assert exit_code == 1
    assert records[0].status == "pending"
    assert records[0].attempts == 0
    assert records[1].status == "failed"
    assert records[1].attempts == 1
    assert records[1].last_error
    assert records[2].status == "failed"
    assert records[2].attempts == 0
    assert records[2].last_error == "second failure"


def test_cli_queue_retry_failed_dry_run_does_not_mutate_queue(tmp_path, capsys):
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/a.bin"])
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/b.bin"])
    records = load_queue(str(tmp_path))
    records[0].status = "failed"
    records[0].attempts = 1
    records[0].last_error = "HTTP 503 during header check"
    records[1].status = "failed"
    records[1].attempts = 3
    records[1].last_error = "Connection timed out"
    rewrite_queue(records, str(tmp_path))

    exit_code = cli.main(
        [
            "queue",
            "retry-failed",
            "--cache-dir",
            str(tmp_path),
            "--dry-run",
            "--max-attempts",
            "3",
            "--error-contains",
            "503",
        ]
    )

    output = capsys.readouterr().out
    after = load_queue(str(tmp_path))
    assert exit_code == 0
    assert "Retry candidates:" in output
    assert "a.bin" in output
    assert "b.bin" not in output
    assert after[0].attempts == 1
    assert after[0].status == "failed"
    assert after[1].attempts == 3


def test_cli_queue_retry_failed_max_attempts_and_error_contains_filters(tmp_path):
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "http://127.0.0.1:1/a.bin"])
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "http://127.0.0.1:1/b.bin"])
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "http://127.0.0.1:1/c.bin"])
    records = load_queue(str(tmp_path))
    records[0].status = "failed"
    records[0].attempts = 1
    records[0].last_error = "HTTP 503 during header check"
    records[1].status = "failed"
    records[1].attempts = 3
    records[1].last_error = "HTTP 503 during header check"
    records[2].status = "failed"
    records[2].attempts = 1
    records[2].last_error = "Connection timed out"
    rewrite_queue(records, str(tmp_path))

    exit_code = cli.main(
        [
            "queue",
            "retry-failed",
            "--cache-dir",
            str(tmp_path),
            "--max-attempts",
            "3",
            "--error-contains",
            "http 503",
            "--retry",
            "0",
        ]
    )

    records = load_queue(str(tmp_path))
    assert exit_code == 1
    assert records[0].attempts == 2
    assert records[0].status == "failed"
    assert records[1].attempts == 3
    assert records[1].last_error == "HTTP 503 during header check"
    assert records[2].attempts == 1
    assert records[2].last_error == "Connection timed out"


def test_cli_queue_validate_valid_and_invalid(tmp_path, capsys):
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/a.bin"])

    assert cli.main(["queue", "validate", "--cache-dir", str(tmp_path)]) == 0
    valid_output = capsys.readouterr().out
    assert "valid: 1" in valid_output

    with queue_path(str(tmp_path)).open("a") as f:
        f.write("{bad-json}\n")

    assert cli.main(["queue", "validate", "--cache-dir", str(tmp_path)]) == 1
    invalid_output = capsys.readouterr().out
    assert "malformed: 1" in invalid_output


def test_cli_queue_repair_fixes_invalid_queue(tmp_path, capsys):
    path = queue_path(str(tmp_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "{bad-json}\n"
        + json.dumps({"queue_id": "q1", "url": "https://example.com/a.bin"})
        + "\n"
        + json.dumps({"queue_id": "q1", "url": "https://example.com/b.bin"})
        + "\n"
    )

    assert cli.main(["queue", "repair", "--cache-dir", str(tmp_path)]) == 0
    output = capsys.readouterr().out
    records = load_queue(str(tmp_path))
    assert "Repaired queue:" in output
    assert len(records) == 2
    assert len({record.queue_id for record in records}) == 2


def test_cli_queue_recover_remove_and_clear(tmp_path, capsys):
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/a.bin"])
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/b.bin"])
    records = load_queue(str(tmp_path))
    records[0].status = "running"
    records[1].status = "completed"
    rewrite_queue(records, str(tmp_path))

    assert cli.main(["queue", "recover", "--cache-dir", str(tmp_path)]) == 0
    recover_output = capsys.readouterr().out
    records = load_queue(str(tmp_path))
    assert "Recovered 1" in recover_output
    assert records[0].status == "pending"

    assert cli.main(["queue", "remove", records[0].queue_id, "--cache-dir", str(tmp_path)]) == 0
    remove_output = capsys.readouterr().out
    assert "Removed 1" in remove_output
    assert [record.queue_id for record in load_queue(str(tmp_path))] == [records[1].queue_id]

    assert cli.main(["queue", "clear", "--cache-dir", str(tmp_path)]) == 1
    clear_error_output = capsys.readouterr().out
    assert "requires --status" in clear_error_output

    assert cli.main(["queue", "clear", "--status", "completed", "--cache-dir", str(tmp_path)]) == 0
    clear_output = capsys.readouterr().out
    assert "Cleared 1" in clear_output
    assert load_queue(str(tmp_path)) == []


def test_cli_queue_clear_all(tmp_path, capsys):
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/a.bin"])
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/b.bin"])

    assert cli.main(["queue", "clear", "--all", "--cache-dir", str(tmp_path)]) == 0
    output = capsys.readouterr().out
    assert "Cleared 2" in output
    assert load_queue(str(tmp_path)) == []
