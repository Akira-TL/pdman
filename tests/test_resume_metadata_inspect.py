import json
import os

from pdman.resume_metadata_inspect import (
    find_latest_resume_metadata,
    format_resume_metadata_summary,
    resume_metadata_summary,
)


def resume_payload(tmp_path):
    completed = tmp_path / "file.bin.0"
    partial = tmp_path / "file.bin.1024"
    completed.write_bytes(b"x" * 1024)
    partial.write_bytes(b"y" * 512)
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
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:01:00Z",
        "segments": [
            {
                "index": 0,
                "start": 0,
                "end": 1023,
                "path": str(completed),
                "expected_size": 1024,
                "existing_size": 0,
                "state": "pending",
            },
            {
                "index": 1,
                "start": 1024,
                "end": 2047,
                "path": str(partial),
                "expected_size": 1024,
                "existing_size": 0,
                "state": "pending",
            },
        ],
    }


def write_metadata(tmp_path, payload=None):
    path = tmp_path / "resume-metadata.json"
    path.write_text(json.dumps(payload or resume_payload(tmp_path)), encoding="utf-8")
    return path


def test_resume_metadata_summary_contract(tmp_path):
    path = write_metadata(tmp_path)

    summary = resume_metadata_summary(path)

    assert summary["source_path"] == str(path)
    assert summary["schema_version"] == 2
    assert summary["kind"] == "resume"
    assert summary["mode"] == "static"
    assert summary["url"] == "https://example.com/file.bin"
    assert summary["filename"] == "file.bin"
    assert summary["target_path"] == str(tmp_path / "file.bin")
    assert summary["file_size"] == 2048
    assert summary["etag"] == "abc123"
    assert summary["last_modified"] == "Wed, 01 Jan 2025 00:00:00 GMT"
    assert summary["created_at"] == "2026-01-01T00:00:00Z"
    assert summary["updated_at"] == "2026-01-01T00:01:00Z"
    assert summary["filter"] == {"state": None}
    assert summary["count"] == 2
    assert summary["stats"] == {
        "total_segments": 2,
        "completed_count": 1,
        "partial_count": 1,
        "pending_count": 0,
        "failed_count": 0,
        "existing_bytes": 1536,
        "expected_bytes": 2048,
    }
    assert summary["filtered_stats"] == summary["stats"]
    assert [segment["state"] for segment in summary["segments"]] == [
        "completed",
        "partial",
    ]


def test_resume_metadata_summary_state_filter_contract(tmp_path):
    path = write_metadata(tmp_path)

    summary = resume_metadata_summary(path, state="partial")

    assert summary["filter"] == {"state": "partial"}
    assert summary["count"] == 1
    assert summary["stats"]["total_segments"] == 2
    assert summary["filtered_stats"] == {
        "total_segments": 1,
        "completed_count": 0,
        "partial_count": 1,
        "pending_count": 0,
        "failed_count": 0,
        "existing_bytes": 512,
        "expected_bytes": 1024,
    }
    assert [segment["index"] for segment in summary["segments"]] == [1]


def test_format_resume_metadata_summary_shows_filter_boundary(tmp_path):
    path = write_metadata(tmp_path)
    summary = resume_metadata_summary(path, state="completed")

    rendered = format_resume_metadata_summary(summary)

    assert "segments: total=2 completed=1 partial=1 pending=0 failed=0" in rendered
    assert "filter: state=completed" in rendered
    assert "filtered: total=1 completed=1 partial=0 pending=0 failed=0" in rendered
    assert "#0 0-1023 state=completed" in rendered
    assert "#1 1024-2047 state=partial" not in rendered


def test_find_latest_resume_metadata_ignores_invalid_metadata(tmp_path):
    old_dir = tmp_path / "old"
    new_dir = tmp_path / "new"
    bad_dir = tmp_path / "bad"
    old_dir.mkdir()
    new_dir.mkdir()
    bad_dir.mkdir()
    old = write_metadata(old_dir)
    new = write_metadata(new_dir)
    bad_payload = resume_payload(bad_dir)
    bad_payload["schema_version"] = 1
    bad = write_metadata(bad_dir, bad_payload)
    os.utime(old, (100, 100))
    os.utime(new, (200, 200))
    os.utime(bad, (300, 300))

    assert find_latest_resume_metadata([tmp_path]) == new
