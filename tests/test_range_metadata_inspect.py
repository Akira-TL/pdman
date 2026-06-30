import json
import os

import pytest

from pdman.range_metadata_inspect import (
    RangeMetadataError,
    find_latest_range_metadata,
    filter_ranges,
    format_range_metadata,
    load_range_metadata,
    range_metadata_summary,
)


def metadata_payload():
    return {
        "schema_version": 1,
        "mode": "dynamic",
        "file_size": 4096,
        "range_size": 1024,
        "stats": {
            "total_ranges": 4,
            "pending_count": 1,
            "active_count": 0,
            "completed_count": 2,
            "failed_count": 1,
            "retried_count": 1,
            "requeue_count": 1,
            "split_count": 0,
            "completed_bytes": 2048,
        },
        "ranges": [
            {
                "index": 2,
                "start": 2048,
                "end": 3071,
                "path": "file.bin.range.2048-3071",
                "attempts": 2,
                "last_error": "Content-Range start mismatch: expected 2048, got 0",
                "downloaded_bytes": 0,
                "existing_size": 0,
                "expected_size": 1024,
                "last_speed_bps": None,
                "state": "failed",
            },
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
                "last_speed_bps": 2048.0,
                "state": "completed",
            },
            {
                "index": 1,
                "start": 1024,
                "end": 2047,
                "path": "file.bin.range.1024-2047",
                "attempts": 1,
                "last_error": None,
                "downloaded_bytes": 1024,
                "existing_size": 1024,
                "expected_size": 1024,
                "last_speed_bps": 1024.5,
                "state": "completed",
            },
            {
                "index": 3,
                "start": 3072,
                "end": 4095,
                "path": "file.bin.range.3072-4095",
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


def test_load_range_metadata_rejects_missing_file(tmp_path):
    with pytest.raises(RangeMetadataError, match="Unable to read metadata file"):
        load_range_metadata(tmp_path / "missing.json")


def test_load_range_metadata_rejects_bad_json(tmp_path):
    metadata_path = tmp_path / "dynamic-ranges.json"
    metadata_path.write_text("{not json", encoding="utf-8")

    with pytest.raises(RangeMetadataError, match="Invalid JSON metadata file"):
        load_range_metadata(metadata_path)


def test_load_range_metadata_rejects_wrong_schema(tmp_path):
    payload = metadata_payload()
    payload["schema_version"] = 99
    metadata_path = write_metadata(tmp_path, payload)

    with pytest.raises(RangeMetadataError, match="Unsupported dynamic range metadata schema_version"):
        load_range_metadata(metadata_path)


def test_load_range_metadata_rejects_wrong_mode(tmp_path):
    payload = metadata_payload()
    payload["mode"] = "static"
    metadata_path = write_metadata(tmp_path, payload)

    with pytest.raises(RangeMetadataError, match="Unsupported range metadata mode"):
        load_range_metadata(metadata_path)


def test_load_range_metadata_rejects_missing_ranges_or_stats(tmp_path):
    payload = metadata_payload()
    payload["ranges"] = {}
    metadata_path = write_metadata(tmp_path, payload)

    with pytest.raises(RangeMetadataError, match="ranges as a list"):
        load_range_metadata(metadata_path)

    payload = metadata_payload()
    payload["stats"] = []
    metadata_path = write_metadata(tmp_path, payload)

    with pytest.raises(RangeMetadataError, match="stats as an object"):
        load_range_metadata(metadata_path)


def test_filter_ranges_filters_failed_and_sorts_by_start():
    payload = metadata_payload()

    ranges = filter_ranges(payload, state="failed")

    assert len(ranges) == 1
    assert ranges[0]["index"] == 2
    assert ranges[0]["last_error"].startswith("Content-Range")


def test_summary_includes_filtered_ranges_and_counts():
    payload = metadata_payload()
    selector = {
        "requested_mode": "auto",
        "selected_mode": "dynamic",
        "fallback_reason": None,
        "reason": "dynamic_eligible",
    }
    payload["selector"] = selector

    summary = range_metadata_summary(payload, state="completed")

    assert summary["schema_version"] == 1
    assert summary["mode"] == "dynamic"
    assert summary["selector"] == selector
    assert summary["filter"] == {"state": "completed"}
    assert summary["count"] == 2
    assert summary["state_counts"] == {
        "completed": 2,
        "failed": 1,
        "pending": 1,
    }
    assert [item["index"] for item in summary["ranges"]] == [0, 1]


def test_format_readable_shows_stats_and_failed_error(tmp_path):
    metadata_path = write_metadata(tmp_path)
    payload = load_range_metadata(metadata_path)

    output = format_range_metadata(payload, source_path=metadata_path)

    assert f"Dynamic range metadata: {metadata_path}" in output
    assert "schema_version: 1" in output
    assert "ranges: total=4 completed=2 failed=1 pending=1 active=0 unknown=0" in output
    assert "retry: retried=1 requeued=1 split=0" in output
    assert "completed_bytes: 2048" in output
    assert "Failed ranges:" in output
    assert "#2 2048-3071 state=failed attempts=2 bytes=0/1024" in output
    assert "Content-Range start mismatch" in output


def test_format_readable_state_filter_shows_matching_ranges():
    payload = metadata_payload()
    payload["selector"] = {
        "requested_mode": "auto",
        "selected_mode": "dynamic",
        "fallback_reason": None,
        "reason": "dynamic_eligible",
    }

    output = format_range_metadata(payload, state="completed")

    assert "selector: requested=auto selected=dynamic reason=dynamic_eligible fallback_reason=None" in output
    assert "Ranges state=completed:" in output
    assert "#0 0-1023 state=completed attempts=1 bytes=1024/1024 speed=2048.00B/s" in output
    assert "#1 1024-2047 state=completed attempts=1 bytes=1024/1024 speed=1024.50B/s" in output
    assert "#2 2048-3071" not in output


def test_json_payload_includes_filter():
    payload = metadata_payload()

    output = range_metadata_summary(payload, state="failed")

    encoded = json.loads(json.dumps(output))
    assert encoded["filter"] == {"state": "failed"}
    assert encoded["count"] == 1
    assert encoded["ranges"][0]["index"] == 2


def test_jsonl_shape_can_emit_one_object_per_line():
    payload = metadata_payload()

    lines = [json.dumps(item, sort_keys=True) for item in filter_ranges(payload)]

    assert len(lines) == 4
    assert [json.loads(line)["index"] for line in lines] == [0, 1, 2, 3]


def test_find_latest_range_metadata_returns_newest_valid_file(tmp_path):
    older_dir = tmp_path / "old" / "task"
    newer_dir = tmp_path / "new" / "task"
    older_dir.mkdir(parents=True)
    newer_dir.mkdir(parents=True)
    older = write_metadata(older_dir)
    newer = write_metadata(newer_dir)
    os.utime(older, (100, 100))
    os.utime(newer, (200, 200))

    assert find_latest_range_metadata([tmp_path]) == newer


def test_find_latest_range_metadata_skips_invalid_files(tmp_path):
    invalid_dir = tmp_path / "invalid"
    valid_dir = tmp_path / "valid"
    invalid_dir.mkdir()
    valid_dir.mkdir()
    invalid = invalid_dir / "dynamic-ranges.json"
    invalid.write_text(json.dumps({"schema_version": 99}), encoding="utf-8")
    valid = write_metadata(valid_dir)

    assert find_latest_range_metadata([tmp_path]) == valid


def test_find_latest_range_metadata_returns_none_when_missing(tmp_path):
    assert find_latest_range_metadata([tmp_path]) is None
