import json

import pdman.cli as cli


def metadata_payload():
    return {
        "schema_version": 1,
        "mode": "dynamic",
        "file_size": 3072,
        "range_size": 1024,
        "stats": {
            "total_ranges": 3,
            "pending_count": 1,
            "active_count": 0,
            "completed_count": 1,
            "failed_count": 1,
            "retried_count": 1,
            "requeue_count": 1,
            "split_count": 0,
            "completed_bytes": 1024,
        },
        "ranges": [
            {
                "index": 0,
                "start": 0,
                "end": 1023,
                "path": "file.bin.range.0-1023",
                "attempts": 1,
                "last_error": None,
                "downloaded_bytes": 1024,
                "existing_size": 1024,
                "expected_size": 1024,
                "last_speed_bps": 4096.0,
                "state": "completed",
            },
            {
                "index": 1,
                "start": 1024,
                "end": 2047,
                "path": "file.bin.range.1024-2047",
                "attempts": 2,
                "last_error": "short body failure: expected 1024 bytes, got 128",
                "downloaded_bytes": 128,
                "existing_size": 128,
                "expected_size": 1024,
                "last_speed_bps": 128.0,
                "state": "failed",
            },
            {
                "index": 2,
                "start": 2048,
                "end": 3071,
                "path": "file.bin.range.2048-3071",
                "attempts": 0,
                "last_error": None,
                "downloaded_bytes": 0,
                "existing_size": 0,
                "expected_size": 1024,
                "last_speed_bps": None,
                "state": "pending",
            },
        ],
    }


def write_metadata(tmp_path, payload=None):
    metadata_path = tmp_path / "dynamic-ranges.json"
    metadata_path.write_text(json.dumps(payload or metadata_payload()), encoding="utf-8")
    return metadata_path


def test_cli_debug_ranges_readable(tmp_path, capsys):
    metadata_path = write_metadata(tmp_path)

    exit_code = cli.main(["debug", "ranges", str(metadata_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert f"Dynamic range metadata: {metadata_path}" in output
    assert "ranges: total=3 completed=1 failed=1 pending=1" in output
    assert "Failed ranges:" in output
    assert "short body failure" in output


def test_cli_debug_ranges_state_filter(tmp_path, capsys):
    metadata_path = write_metadata(tmp_path)

    exit_code = cli.main(["debug", "ranges", str(metadata_path), "--state", "failed"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Ranges state=failed:" in output
    assert "#1 1024-2047 state=failed" in output
    assert "#0 0-1023" not in output


def test_cli_debug_ranges_json(tmp_path, capsys):
    metadata_path = write_metadata(tmp_path)

    exit_code = cli.main(["debug", "ranges", str(metadata_path), "--state", "failed", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filter"] == {"state": "failed"}
    assert payload["count"] == 1
    assert payload["ranges"][0]["index"] == 1
    assert payload["stats"]["failed_count"] == 1


def test_cli_debug_ranges_jsonl(tmp_path, capsys):
    metadata_path = write_metadata(tmp_path)

    exit_code = cli.main(["debug", "ranges", str(metadata_path), "--jsonl"])

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert len(lines) == 3
    assert [json.loads(line)["index"] for line in lines] == [0, 1, 2]


def test_cli_debug_ranges_jsonl_with_state_filter(tmp_path, capsys):
    metadata_path = write_metadata(tmp_path)

    exit_code = cli.main(["debug", "ranges", str(metadata_path), "--state", "completed", "--jsonl"])

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert len(lines) == 1
    assert json.loads(lines[0])["state"] == "completed"


def test_cli_debug_ranges_missing_file_exits_non_zero(tmp_path, capsys):
    metadata_path = tmp_path / "missing.json"

    exit_code = cli.main(["debug", "ranges", str(metadata_path)])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Error: Unable to read metadata file" in output


def test_cli_debug_ranges_wrong_schema_exits_non_zero(tmp_path, capsys):
    payload = metadata_payload()
    payload["schema_version"] = 2
    metadata_path = write_metadata(tmp_path, payload)

    exit_code = cli.main(["debug", "ranges", str(metadata_path)])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Unsupported dynamic range metadata schema_version" in output
