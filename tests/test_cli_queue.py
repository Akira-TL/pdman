import json

import pdman.cli as cli
from pdman.queue import load_queue, rewrite_queue


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


def test_cli_queue_list_status_filter(tmp_path, capsys):
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/a.bin"])
    cli.main(["queue", "add", "--cache-dir", str(tmp_path), "https://example.com/b.bin"])
    records = load_queue(str(tmp_path))
    records[1].status = "failed"
    records[1].last_error = "HTTP 503 during header check"
    rewrite_queue(records, str(tmp_path))

    exit_code = cli.main(
        ["queue", "list", "--cache-dir", str(tmp_path), "--status", "failed"]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "b.bin" in output
    assert "HTTP 503" in output


def test_cli_queue_start_no_pending_returns_zero(tmp_path, capsys):
    exit_code = cli.main(["queue", "start", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "No queue records to start" in output
