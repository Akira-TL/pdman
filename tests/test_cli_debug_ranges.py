import json
import os

import pytest

import pdman.cli as cli
from pdman.range_metadata_inspect import (
    find_latest_range_metadata,
    find_latest_range_metadata_diagnostics,
)


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
    metadata = metadata_payload()
    selector = {
        "requested_mode": "auto",
        "selected_mode": "dynamic",
        "fallback_reason": None,
        "reason": "dynamic_eligible",
    }
    metadata["selector"] = selector
    metadata_path = write_metadata(tmp_path, metadata)

    exit_code = cli.main(["debug", "ranges", str(metadata_path), "--state", "failed", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filter"] == {"state": "failed"}
    assert payload["selector"] == selector
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


def test_range_latest_diagnostics_selects_newest_valid_and_counts_invalid(tmp_path):
    older_dir = tmp_path / "old"
    newer_dir = tmp_path / "new"
    invalid_dir = tmp_path / "invalid"
    older_dir.mkdir()
    newer_dir.mkdir()
    invalid_dir.mkdir()
    older = write_metadata(older_dir)
    newer = write_metadata(newer_dir)
    invalid = invalid_dir / "dynamic-ranges.json"
    invalid.write_text(json.dumps({"schema_version": 999}), encoding="utf-8")
    os.utime(older, (100, 100))
    os.utime(newer, (200, 200))
    os.utime(invalid, (300, 300))

    search = find_latest_range_metadata_diagnostics([tmp_path])

    assert search.roots == [str(tmp_path)]
    assert search.selected_path == newer
    assert search.valid_count == 2
    assert search.skipped_invalid_count == 1
    assert find_latest_range_metadata([tmp_path]) == newer



def test_range_latest_diagnostics_handles_file_root_and_missing_root(tmp_path):
    valid = write_metadata(tmp_path)
    missing_root = tmp_path / "missing"

    search = find_latest_range_metadata_diagnostics([missing_root, valid])

    assert search.roots == [str(missing_root), str(valid)]
    assert search.selected_path == valid
    assert search.valid_count == 1
    assert search.skipped_invalid_count == 0



def test_cli_debug_ranges_latest_uses_newest_metadata(tmp_path, capsys):
    older_dir = tmp_path / "old" / "task"
    newer_dir = tmp_path / "new" / "task"
    older_dir.mkdir(parents=True)
    newer_dir.mkdir(parents=True)
    older = write_metadata(older_dir)
    newer = write_metadata(newer_dir)
    os.utime(older, (100, 100))
    os.utime(newer, (200, 200))

    exit_code = cli.main([
        "debug",
        "ranges",
        "--latest",
        "--search-root",
        str(tmp_path),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(newer)
    assert payload["count"] == 3


def test_cli_debug_ranges_latest_jsonl_honors_state_filter(tmp_path, capsys):
    metadata_path = write_metadata(tmp_path)
    os.utime(metadata_path, (200, 200))

    exit_code = cli.main([
        "debug",
        "ranges",
        "--latest",
        "--search-root",
        str(tmp_path),
        "--state",
        "failed",
        "--jsonl",
    ])

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert len(lines) == 1
    assert json.loads(lines[0])["state"] == "failed"


def test_cli_debug_ranges_latest_defaults_to_cache_only(tmp_path, monkeypatch, capsys):
    default_tmp = tmp_path / "default-tmp"
    default_cache = tmp_path / "default-cache"
    default_tmp.mkdir()
    default_cache.mkdir()
    tmp_candidate = write_metadata(default_tmp)
    cache_candidate = write_metadata(default_cache)
    os.utime(tmp_candidate, (300, 300))
    os.utime(cache_candidate, (100, 100))
    monkeypatch.setattr(cli, "default_system_tmp_root", lambda: default_tmp)
    monkeypatch.setattr(cli, "default_cache_root", lambda: default_cache)

    exit_code = cli.main([
        "debug",
        "ranges",
        "--latest",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(cache_candidate)



def test_cli_debug_ranges_latest_cache_dir_is_strict(tmp_path, monkeypatch, capsys):
    selected_cache = tmp_path / "selected-cache"
    default_tmp = tmp_path / "default-tmp"
    default_cache = tmp_path / "default-cache"
    selected_cache.mkdir()
    default_tmp.mkdir()
    default_cache.mkdir()
    selected = write_metadata(selected_cache)
    tmp_candidate = write_metadata(default_tmp)
    cache_candidate = write_metadata(default_cache)
    os.utime(selected, (100, 100))
    os.utime(tmp_candidate, (300, 300))
    os.utime(cache_candidate, (400, 400))
    monkeypatch.setattr(cli, "default_system_tmp_root", lambda: default_tmp)
    monkeypatch.setattr(cli, "default_cache_root", lambda: default_cache)

    exit_code = cli.main([
        "debug",
        "ranges",
        "--latest",
        "--cache-dir",
        str(selected_cache),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(selected)



def test_cli_debug_ranges_latest_search_root_is_strict(
    tmp_path, monkeypatch, capsys
):
    search_root = tmp_path / "search-root"
    default_tmp = tmp_path / "default-tmp"
    default_cache = tmp_path / "default-cache"
    search_root.mkdir()
    default_tmp.mkdir()
    default_cache.mkdir()
    selected = write_metadata(search_root)
    tmp_candidate = write_metadata(default_tmp)
    cache_candidate = write_metadata(default_cache)
    os.utime(selected, (100, 100))
    os.utime(tmp_candidate, (300, 300))
    os.utime(cache_candidate, (400, 400))
    monkeypatch.setattr(cli, "default_system_tmp_root", lambda: default_tmp)
    monkeypatch.setattr(cli, "default_cache_root", lambda: default_cache)

    exit_code = cli.main([
        "debug",
        "ranges",
        "--latest",
        "--search-root",
        str(search_root),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(selected)



def test_cli_debug_ranges_latest_skips_newer_invalid_cache_metadata(tmp_path, capsys):
    valid_dir = tmp_path / "valid"
    invalid_dir = tmp_path / "invalid"
    valid_dir.mkdir()
    invalid_dir.mkdir()
    valid = write_metadata(valid_dir)
    invalid = invalid_dir / "dynamic-ranges.json"
    invalid.write_text(json.dumps({"schema_version": 999}), encoding="utf-8")
    os.utime(valid, (100, 100))
    os.utime(invalid, (500, 500))

    exit_code = cli.main([
        "debug",
        "ranges",
        "--latest",
        "--cache-dir",
        str(tmp_path),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(valid)



def test_cli_debug_ranges_latest_json_omits_readable_latest_diagnostics(tmp_path, capsys):
    valid_dir = tmp_path / "valid"
    invalid_dir = tmp_path / "invalid"
    valid_dir.mkdir()
    invalid_dir.mkdir()
    valid = write_metadata(valid_dir)
    invalid = invalid_dir / "dynamic-ranges.json"
    invalid.write_text(json.dumps({"schema_version": 999}), encoding="utf-8")
    os.utime(valid, (100, 100))
    os.utime(invalid, (500, 500))

    exit_code = cli.main([
        "debug",
        "ranges",
        "--latest",
        "--cache-dir",
        str(tmp_path),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(valid)
    assert "latest_search" not in payload
    assert "search_roots" not in payload
    assert "valid_count" not in payload
    assert "skipped_invalid_count" not in payload



def test_cli_debug_ranges_latest_readable_shows_search_roots_and_skips(tmp_path, capsys):
    valid_dir = tmp_path / "valid"
    invalid_dir = tmp_path / "invalid"
    valid_dir.mkdir()
    invalid_dir.mkdir()
    valid = write_metadata(valid_dir)
    invalid = invalid_dir / "dynamic-ranges.json"
    invalid.write_text(json.dumps({"schema_version": 999}), encoding="utf-8")
    os.utime(valid, (100, 100))
    os.utime(invalid, (500, 500))

    exit_code = cli.main([
        "debug",
        "ranges",
        "--latest",
        "--cache-dir",
        str(tmp_path),
    ])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Latest search:" in output
    assert f"  root: {tmp_path}" in output
    assert "  skipped_invalid: 1" in output
    assert f"Dynamic range metadata: {valid}" in output


def test_cli_debug_ranges_latest_missing_metadata_exits_non_zero(tmp_path, capsys):
    exit_code = cli.main([
        "debug",
        "ranges",
        "--latest",
        "--search-root",
        str(tmp_path),
    ])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "No dynamic range metadata found" in output
    assert "Searched:" in output
    assert f"  {tmp_path}" in output
