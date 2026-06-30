import json

from pdman.range_allocator import RangeAllocator
from pdman.range_metadata import (
    DYNAMIC_RANGE_METADATA_SCHEMA_VERSION,
    range_allocator_payload,
    range_task_payload,
    write_range_metadata,
)


def test_range_task_payload_serializes_core_fields(tmp_path):
    allocator = RangeAllocator(
        file_size=8,
        range_size=4,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    task = allocator.ranges[0]
    task.path.write_bytes(b"abc")
    task.downloaded_bytes = 3
    task.last_speed_bps = 123.4
    task.last_error = "slow"

    payload = range_task_payload(task, state="active")

    assert payload["index"] == 0
    assert payload["start"] == 0
    assert payload["end"] == 3
    assert payload["path"] == str(task.path)
    assert payload["downloaded_bytes"] == 3
    assert payload["existing_size"] == 3
    assert payload["expected_size"] == 4
    assert payload["last_speed_bps"] == 123.4
    assert payload["last_error"] == "slow"
    assert payload["state"] == "active"


def test_allocator_payload_includes_stats_and_states(tmp_path):
    allocator = RangeAllocator(
        file_size=8,
        range_size=4,
        tmp_dir=tmp_path,
        filename="file.bin",
        max_retries=1,
    )
    first = allocator.claim_next()
    assert first is not None
    first.path.write_bytes(b"1234")
    allocator.mark_completed(first)
    second = allocator.claim_next()
    assert second is not None
    allocator.mark_failed(second, "temporary failure")

    payload = range_allocator_payload(allocator)

    assert payload["schema_version"] == DYNAMIC_RANGE_METADATA_SCHEMA_VERSION
    assert payload["mode"] == "dynamic"
    assert payload["file_size"] == 8
    assert payload["range_size"] == 4
    assert payload["stats"]["total_ranges"] == 2
    assert payload["stats"]["completed_count"] == 1
    assert payload["stats"]["pending_count"] == 1
    assert payload["stats"]["requeue_count"] == 1
    states = {item["index"]: item["state"] for item in payload["ranges"]}
    assert states[first.index] == "completed"
    assert states[second.index] == "pending"


def test_allocator_payload_includes_split_child_range(tmp_path):
    allocator = RangeAllocator(
        file_size=10,
        range_size=10,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    task = allocator.claim_next()
    assert task is not None
    task.path.write_bytes(b"abc")

    child = allocator.split_remaining(task, min_size=1)
    assert child is not None

    payload = range_allocator_payload(allocator)

    assert payload["stats"]["split_count"] == 1
    ranges = {(item["start"], item["end"]): item for item in payload["ranges"]}
    assert ranges[(0, 2)]["state"] == "completed"
    assert ranges[(3, 9)]["state"] == "pending"


def test_allocator_payload_can_include_selector_diagnostics(tmp_path):
    allocator = RangeAllocator(
        file_size=8,
        range_size=4,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    selector = {
        "requested_mode": "auto",
        "selected_mode": "dynamic",
        "fallback_reason": None,
        "reason": "dynamic_eligible",
    }

    payload = range_allocator_payload(allocator, selector=selector)

    assert payload["selector"] == selector


def test_write_range_metadata_writes_valid_json(tmp_path):
    allocator = RangeAllocator(
        file_size=4,
        range_size=4,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    metadata_path = tmp_path / "dynamic-ranges.json"
    selector = {
        "requested_mode": "dynamic",
        "selected_mode": "dynamic",
        "fallback_reason": None,
        "reason": "dynamic_eligible",
    }

    write_range_metadata(metadata_path, allocator, selector=selector)

    payload = json.loads(metadata_path.read_text())
    assert payload["schema_version"] == DYNAMIC_RANGE_METADATA_SCHEMA_VERSION
    assert payload["mode"] == "dynamic"
    assert payload["selector"] == selector
    assert payload["ranges"][0]["state"] == "pending"
