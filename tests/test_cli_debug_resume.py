import json
import os

import pytest

import pdman.cli as cli
from pdman.resume_metadata_inspect import (
    find_latest_resume_metadata,
    find_latest_resume_metadata_diagnostics,
)


def resume_payload(tmp_path):
    first = tmp_path / "file.bin.0"
    second = tmp_path / "file.bin.1024"
    first.write_bytes(b"x" * 1024)
    second.write_bytes(b"y" * 512)
    return {
        "schema_version": 2,
        "kind": "resume",
        "mode": "static",
        "url": "https://example.com/file.bin",
        "filename": "file.bin",
        "target_path": str(tmp_path / "file.bin"),
        "file_size": 2048,
        "etag": "abc123",
        "last_modified": "Wed, 01 Jan 2025 00:00:00 GMT",
        "created_at": None,
        "updated_at": None,
        "segments": [
            {
                "index": 0,
                "start": 0,
                "end": 1023,
                "path": str(first),
                "expected_size": 1024,
                "existing_size": 0,
                "state": "pending",
            },
            {
                "index": 1,
                "start": 1024,
                "end": 2047,
                "path": str(second),
                "expected_size": 1024,
                "existing_size": 0,
                "state": "pending",
            },
        ],
    }


def write_resume_metadata(tmp_path, payload=None):
    metadata_path = tmp_path / "resume-metadata.json"
    metadata_path.write_text(json.dumps(payload or resume_payload(tmp_path)), encoding="utf-8")
    return metadata_path


def test_cli_debug_resume_readable(tmp_path, capsys):
    metadata_path = write_resume_metadata(tmp_path)

    exit_code = cli.main(["debug", "resume", "--metadata", str(metadata_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert f"Resume metadata: {metadata_path}" in output
    assert "mode: static" in output
    assert "file: file.bin size=2048" in output
    assert "segments: total=2 completed=1 partial=1 pending=0 failed=0" in output
    assert "#1 1024-2047 state=partial existing=512/1024" in output


def test_cli_debug_resume_json(tmp_path, capsys):
    metadata_path = write_resume_metadata(tmp_path)

    exit_code = cli.main(["debug", "resume", "--metadata", str(metadata_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(metadata_path)
    assert payload["mode"] == "static"
    assert payload["filename"] == "file.bin"
    assert payload["stats"] == {
        "total_segments": 2,
        "completed_count": 1,
        "partial_count": 1,
        "pending_count": 0,
        "failed_count": 0,
        "existing_bytes": 1536,
        "expected_bytes": 2048,
    }
    assert payload["segments"][0]["state"] == "completed"
    assert payload["segments"][1]["state"] == "partial"


def test_cli_debug_resume_jsonl(tmp_path, capsys):
    metadata_path = write_resume_metadata(tmp_path)

    exit_code = cli.main(["debug", "resume", "--metadata", str(metadata_path), "--jsonl"])

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert len(lines) == 2
    assert [json.loads(line)["index"] for line in lines] == [0, 1]
    assert [json.loads(line)["state"] for line in lines] == ["completed", "partial"]


def test_cli_debug_resume_state_filter_readable(tmp_path, capsys):
    metadata_path = write_resume_metadata(tmp_path)

    exit_code = cli.main([
        "debug",
        "resume",
        "--metadata",
        str(metadata_path),
        "--state",
        "partial",
    ])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "filter: state=partial" in output
    assert "filtered: total=1 completed=0 partial=1 pending=0 failed=0" in output
    assert "#1 1024-2047 state=partial" in output
    assert "#0 0-1023 state=completed" not in output


def test_cli_debug_resume_state_filter_json(tmp_path, capsys):
    metadata_path = write_resume_metadata(tmp_path)

    exit_code = cli.main([
        "debug",
        "resume",
        "--metadata",
        str(metadata_path),
        "--state",
        "completed",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filter"] == {"state": "completed"}
    assert payload["count"] == 1
    assert payload["filtered_stats"] == {
        "total_segments": 1,
        "completed_count": 1,
        "partial_count": 0,
        "pending_count": 0,
        "failed_count": 0,
        "existing_bytes": 1024,
        "expected_bytes": 1024,
    }
    assert [item["index"] for item in payload["segments"]] == [0]


def test_cli_debug_resume_state_filter_jsonl(tmp_path, capsys):
    metadata_path = write_resume_metadata(tmp_path)

    exit_code = cli.main([
        "debug",
        "resume",
        "--metadata",
        str(metadata_path),
        "--state",
        "partial",
        "--jsonl",
    ])

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert len(lines) == 1
    assert json.loads(lines[0])["state"] == "partial"


def test_cli_debug_resume_invalid_metadata_exits_non_zero(tmp_path, capsys):
    payload = resume_payload(tmp_path)
    payload["schema_version"] = 1
    metadata_path = write_resume_metadata(tmp_path, payload)

    exit_code = cli.main(["debug", "resume", "--metadata", str(metadata_path)])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Resume rejected [schema_version_unsupported]" in output


def test_resume_latest_diagnostics_selects_newest_valid_and_counts_invalid(tmp_path):
    older_dir = tmp_path / "old"
    newer_dir = tmp_path / "new"
    invalid_dir = tmp_path / "invalid"
    older_dir.mkdir()
    newer_dir.mkdir()
    invalid_dir.mkdir()
    older = write_resume_metadata(older_dir)
    newer = write_resume_metadata(newer_dir)
    invalid = invalid_dir / "resume-metadata.json"
    invalid.write_text(json.dumps({"schema_version": 999}), encoding="utf-8")
    os.utime(older, (100, 100))
    os.utime(newer, (200, 200))
    os.utime(invalid, (300, 300))

    search = find_latest_resume_metadata_diagnostics([tmp_path])

    assert search.roots == [str(tmp_path)]
    assert search.selected_path == newer
    assert search.valid_count == 2
    assert search.skipped_invalid_count == 1
    assert find_latest_resume_metadata([tmp_path]) == newer



def test_resume_latest_diagnostics_handles_file_root_and_missing_root(tmp_path):
    valid = write_resume_metadata(tmp_path)
    missing_root = tmp_path / "missing"

    search = find_latest_resume_metadata_diagnostics([missing_root, valid])

    assert search.roots == [str(missing_root), str(valid)]
    assert search.selected_path == valid
    assert search.valid_count == 1
    assert search.skipped_invalid_count == 0



def test_cli_debug_resume_latest_uses_newest_metadata(tmp_path, capsys):
    older_dir = tmp_path / "old" / "task"
    newer_dir = tmp_path / "new" / "task"
    older_dir.mkdir(parents=True)
    newer_dir.mkdir(parents=True)
    older = write_resume_metadata(older_dir)
    newer = write_resume_metadata(newer_dir)
    os.utime(older, (100, 100))
    os.utime(newer, (200, 200))

    exit_code = cli.main([
        "debug",
        "resume",
        "--latest",
        "--search-root",
        str(tmp_path),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(newer)
    assert payload["filename"] == "file.bin"


def test_cli_debug_resume_latest_jsonl(tmp_path, capsys):
    metadata_path = write_resume_metadata(tmp_path)
    os.utime(metadata_path, (200, 200))

    exit_code = cli.main([
        "debug",
        "resume",
        "--latest",
        "--search-root",
        str(tmp_path),
        "--jsonl",
    ])

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert len(lines) == 2
    assert [json.loads(line)["state"] for line in lines] == ["completed", "partial"]


def test_cli_debug_resume_latest_defaults_to_cache_only(tmp_path, monkeypatch, capsys):
    default_tmp = tmp_path / "default-tmp"
    default_cache = tmp_path / "default-cache"
    default_tmp.mkdir()
    default_cache.mkdir()
    tmp_candidate = write_resume_metadata(default_tmp)
    cache_candidate = write_resume_metadata(default_cache)
    os.utime(tmp_candidate, (300, 300))
    os.utime(cache_candidate, (100, 100))
    monkeypatch.setattr(cli, "default_system_tmp_root", lambda: default_tmp)
    monkeypatch.setattr(cli, "default_cache_root", lambda: default_cache)

    exit_code = cli.main([
        "debug",
        "resume",
        "--latest",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(cache_candidate)



def test_cli_debug_resume_latest_cache_dir_is_strict(tmp_path, monkeypatch, capsys):
    selected_cache = tmp_path / "selected-cache"
    default_tmp = tmp_path / "default-tmp"
    default_cache = tmp_path / "default-cache"
    selected_cache.mkdir()
    default_tmp.mkdir()
    default_cache.mkdir()
    selected = write_resume_metadata(selected_cache)
    ignored_tmp = write_resume_metadata(default_tmp)
    ignored_cache = write_resume_metadata(default_cache)
    os.utime(selected, (100, 100))
    os.utime(ignored_tmp, (300, 300))
    os.utime(ignored_cache, (400, 400))
    monkeypatch.setattr(cli, "default_system_tmp_root", lambda: default_tmp)
    monkeypatch.setattr(cli, "default_cache_root", lambda: default_cache)

    exit_code = cli.main([
        "debug",
        "resume",
        "--latest",
        "--cache-dir",
        str(selected_cache),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(selected)



def test_cli_debug_resume_latest_search_root_ignores_default_tmp_and_cache(
    tmp_path, monkeypatch, capsys
):
    search_root = tmp_path / "search-root"
    default_tmp = tmp_path / "default-tmp"
    default_cache = tmp_path / "default-cache"
    search_root.mkdir()
    default_tmp.mkdir()
    default_cache.mkdir()
    selected = write_resume_metadata(search_root)
    ignored_tmp = write_resume_metadata(default_tmp)
    ignored_cache = write_resume_metadata(default_cache)
    os.utime(selected, (100, 100))
    os.utime(ignored_tmp, (300, 300))
    os.utime(ignored_cache, (400, 400))
    monkeypatch.setattr(cli, "default_system_tmp_root", lambda: default_tmp)
    monkeypatch.setattr(cli, "default_cache_root", lambda: default_cache)

    exit_code = cli.main([
        "debug",
        "resume",
        "--latest",
        "--search-root",
        str(search_root),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(selected)



def test_cli_debug_resume_latest_skips_newer_invalid_cache_metadata(tmp_path, capsys):
    valid_dir = tmp_path / "valid"
    invalid_dir = tmp_path / "invalid"
    valid_dir.mkdir()
    invalid_dir.mkdir()
    valid = write_resume_metadata(valid_dir)
    invalid = invalid_dir / "resume-metadata.json"
    invalid.write_text(json.dumps({"schema_version": 999}), encoding="utf-8")
    os.utime(valid, (100, 100))
    os.utime(invalid, (500, 500))

    exit_code = cli.main([
        "debug",
        "resume",
        "--latest",
        "--cache-dir",
        str(tmp_path),
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source_path"] == str(valid)



def test_cli_debug_resume_latest_json_omits_readable_latest_diagnostics(tmp_path, capsys):
    valid_dir = tmp_path / "valid"
    invalid_dir = tmp_path / "invalid"
    valid_dir.mkdir()
    invalid_dir.mkdir()
    valid = write_resume_metadata(valid_dir)
    invalid = invalid_dir / "resume-metadata.json"
    invalid.write_text(json.dumps({"schema_version": 999}), encoding="utf-8")
    os.utime(valid, (100, 100))
    os.utime(invalid, (500, 500))

    exit_code = cli.main([
        "debug",
        "resume",
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



def test_cli_debug_resume_latest_readable_shows_search_roots_and_skips(tmp_path, capsys):
    valid_dir = tmp_path / "valid"
    invalid_dir = tmp_path / "invalid"
    valid_dir.mkdir()
    invalid_dir.mkdir()
    valid = write_resume_metadata(valid_dir)
    invalid = invalid_dir / "resume-metadata.json"
    invalid.write_text(json.dumps({"schema_version": 999}), encoding="utf-8")
    os.utime(valid, (100, 100))
    os.utime(invalid, (500, 500))

    exit_code = cli.main([
        "debug",
        "resume",
        "--latest",
        "--cache-dir",
        str(tmp_path),
    ])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Latest search:" in output
    assert f"  root: {tmp_path}" in output
    assert "  skipped_invalid: 1" in output
    assert f"Resume metadata: {valid}" in output


def test_cli_debug_resume_latest_missing_metadata_exits_non_zero(tmp_path, capsys):
    exit_code = cli.main([
        "debug",
        "resume",
        "--latest",
        "--search-root",
        str(tmp_path),
    ])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "No resume metadata found" in output
    assert "Searched:" in output
    assert f"  {tmp_path}" in output


def test_cli_debug_resume_help_documents_contract(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["debug", "resume", "--help"])

    output = capsys.readouterr().out
    assert exc_info.value.code == 0
    assert "--metadata" in output
    assert "--latest" in output
    assert "--search-root" in output
    assert "--state {completed,partial,pending,failed}" in output
    assert "--json" in output
    assert "--jsonl" in output


def test_cli_debug_resume_rejects_metadata_and_latest_together(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["debug", "resume", "--metadata", "resume-metadata.json", "--latest"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert "not allowed with argument" in captured.err


def test_cli_debug_resume_requires_metadata_path(capsys):
    exit_code = cli.main(["debug", "resume"])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "--metadata is required, or use --latest" in output
