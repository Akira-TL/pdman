import json
from types import SimpleNamespace

import pytest

from pdman.range_allocator import RangeAllocator
from pdman.resume_metadata import (
    RESUME_METADATA_KIND,
    RESUME_METADATA_SCHEMA_VERSION,
    ResumeMetadataError,
    ResumeRejectionCode,
    format_resume_rejection,
    dynamic_resume_metadata_payload,
    inspect_resume_segments,
    load_resume_metadata,
    static_resume_metadata_payload,
    validate_resume_metadata,
    write_resume_metadata,
)


def resume_payload(tmp_path):
    return {
        "schema_version": RESUME_METADATA_SCHEMA_VERSION,
        "kind": RESUME_METADATA_KIND,
        "mode": "static",
        "url": "https://example.invalid/file.bin",
        "filename": "file.bin",
        "target_path": str(tmp_path / "file.bin"),
        "file_size": 2048,
        "etag": "abc123",
        "last_modified": "Wed, 01 Jan 2025 00:00:00 GMT",
        "created_at": "2026-06-30T00:00:00Z",
        "updated_at": "2026-06-30T00:00:00Z",
        "segments": [
            {
                "index": 0,
                "start": 0,
                "end": 1023,
                "path": str(tmp_path / "file.bin.0"),
                "expected_size": 1024,
                "existing_size": 1024,
                "state": "completed",
            },
            {
                "index": 1,
                "start": 1024,
                "end": 2047,
                "path": str(tmp_path / "file.bin.1024"),
                "expected_size": 1024,
                "existing_size": 512,
                "state": "partial",
            },
        ],
    }


def test_resume_metadata_error_exposes_reason_code():
    error = ResumeMetadataError("bad metadata", ResumeRejectionCode.SCHEMA_VERSION_UNSUPPORTED)

    assert error.reason_code == ResumeRejectionCode.SCHEMA_VERSION_UNSUPPORTED
    assert format_resume_rejection(error) == "Resume rejected [schema_version_unsupported]: bad metadata"


def test_validate_resume_metadata_sets_rejection_codes(tmp_path):
    payload = resume_payload(tmp_path)
    payload["schema_version"] = 1
    with pytest.raises(ResumeMetadataError) as exc_info:
        validate_resume_metadata(payload)
    assert exc_info.value.reason_code == ResumeRejectionCode.SCHEMA_VERSION_UNSUPPORTED

    payload = resume_payload(tmp_path)
    payload["kind"] = "debug"
    with pytest.raises(ResumeMetadataError) as exc_info:
        validate_resume_metadata(payload)
    assert exc_info.value.reason_code == ResumeRejectionCode.KIND_MISMATCH

    payload = resume_payload(tmp_path)
    with pytest.raises(ResumeMetadataError) as exc_info:
        validate_resume_metadata(payload, expected_url="https://example.invalid/other.bin")
    assert exc_info.value.reason_code == ResumeRejectionCode.URL_MISMATCH

    payload = resume_payload(tmp_path)
    payload["segments"][1]["existing_size"] = 2048
    with pytest.raises(ResumeMetadataError) as exc_info:
        validate_resume_metadata(payload)
    assert exc_info.value.reason_code == ResumeRejectionCode.PARTIAL_TOO_LARGE


def test_dynamic_resume_metadata_payload_serializes_range_tasks(tmp_path):
    allocator = RangeAllocator(
        file_size=2048,
        range_size=1024,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    first = allocator.claim_next()
    assert first is not None
    first.path.write_bytes(b"x" * first.expected_size)
    allocator.mark_completed(first)
    second = allocator.claim_next()
    assert second is not None
    second.path.write_bytes(b"y" * 12)

    payload = dynamic_resume_metadata_payload(
        url="https://example.invalid/file.bin",
        filename="file.bin",
        target_path=tmp_path / "file.bin",
        file_size=2048,
        allocator=allocator,
        etag="abc123",
        last_modified="Wed, 01 Jan 2025 00:00:00 GMT",
        created_at="2026-06-30T00:00:00Z",
        updated_at="2026-06-30T00:00:00Z",
    )

    assert payload["schema_version"] == RESUME_METADATA_SCHEMA_VERSION
    assert payload["kind"] == RESUME_METADATA_KIND
    assert payload["mode"] == "dynamic"
    assert payload["url"] == "https://example.invalid/file.bin"
    assert payload["target_path"] == str(tmp_path / "file.bin")
    assert payload["file_size"] == 2048
    assert payload["etag"] == "abc123"
    assert payload["last_modified"] == "Wed, 01 Jan 2025 00:00:00 GMT"
    assert payload["segments"] == [
        {
            "index": 0,
            "start": 0,
            "end": 1023,
            "path": str(first.path),
            "expected_size": 1024,
            "existing_size": 1024,
            "state": "completed",
        },
        {
            "index": 1,
            "start": 1024,
            "end": 2047,
            "path": str(second.path),
            "expected_size": 1024,
            "existing_size": 12,
            "state": "partial",
        },
    ]
    validate_resume_metadata(payload)


def test_dynamic_resume_metadata_payload_preserves_split_range_layout(tmp_path):
    allocator = RangeAllocator(
        file_size=2048,
        range_size=2048,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    task = allocator.claim_next()
    assert task is not None
    task.path.write_bytes(b"x" * 512)

    child = allocator.split_remaining(task, min_size=128)
    assert child is not None

    payload = dynamic_resume_metadata_payload(
        url="https://example.invalid/file.bin",
        filename="file.bin",
        target_path=tmp_path / "file.bin",
        file_size=2048,
        allocator=allocator,
    )

    assert payload["segments"] == [
        {
            "index": 0,
            "start": 0,
            "end": 511,
            "path": str(task.path),
            "expected_size": 512,
            "existing_size": 512,
            "state": "completed",
        },
        {
            "index": 1,
            "start": 512,
            "end": 2047,
            "path": str(child.path),
            "expected_size": 1536,
            "existing_size": 0,
            "state": "pending",
        },
    ]
    validate_resume_metadata(payload)


def test_static_resume_metadata_payload_serializes_chunk_chain(tmp_path):
    first_path = tmp_path / "file.bin.0"
    second_path = tmp_path / "file.bin.1024"
    first_path.write_bytes(b"x" * 1024)
    second_path.write_bytes(b"y" * 12)
    first = SimpleNamespace(start=0, end=1023, chunk_path=str(first_path))
    second = SimpleNamespace(start=1024, end=2047, chunk_path=str(second_path))

    payload = static_resume_metadata_payload(
        url="https://example.invalid/file.bin",
        filename="file.bin",
        target_path=tmp_path / "file.bin",
        file_size=2048,
        chunks=[first, second],
        etag="abc123",
        last_modified="Wed, 01 Jan 2025 00:00:00 GMT",
        created_at="2026-06-30T00:00:00Z",
        updated_at="2026-06-30T00:00:00Z",
    )

    assert payload["schema_version"] == RESUME_METADATA_SCHEMA_VERSION
    assert payload["kind"] == RESUME_METADATA_KIND
    assert payload["mode"] == "static"
    assert payload["url"] == "https://example.invalid/file.bin"
    assert payload["target_path"] == str(tmp_path / "file.bin")
    assert payload["file_size"] == 2048
    assert payload["etag"] == "abc123"
    assert payload["last_modified"] == "Wed, 01 Jan 2025 00:00:00 GMT"
    assert payload["segments"] == [
        {
            "index": 0,
            "start": 0,
            "end": 1023,
            "path": str(first_path),
            "expected_size": 1024,
            "existing_size": 1024,
            "state": "completed",
        },
        {
            "index": 1,
            "start": 1024,
            "end": 2047,
            "path": str(second_path),
            "expected_size": 1024,
            "existing_size": 12,
            "state": "partial",
        },
    ]
    validate_resume_metadata(payload)


def test_validate_resume_metadata_accepts_static_and_dynamic_modes(tmp_path):
    payload = resume_payload(tmp_path)
    validate_resume_metadata(payload)

    payload["mode"] = "dynamic"
    validate_resume_metadata(payload)


def test_validate_resume_metadata_rejects_wrong_schema_kind_or_mode(tmp_path):
    payload = resume_payload(tmp_path)
    payload["schema_version"] = 1
    with pytest.raises(ResumeMetadataError, match="Unsupported resume metadata schema_version"):
        validate_resume_metadata(payload)

    payload = resume_payload(tmp_path)
    payload["kind"] = "debug"
    with pytest.raises(ResumeMetadataError, match="Unsupported resume metadata kind"):
        validate_resume_metadata(payload)

    payload = resume_payload(tmp_path)
    payload["mode"] = "auto"
    with pytest.raises(ResumeMetadataError, match="Unsupported resume metadata mode"):
        validate_resume_metadata(payload)


def test_validate_resume_metadata_rejects_missing_or_invalid_segments(tmp_path):
    payload = resume_payload(tmp_path)
    payload.pop("segments")
    with pytest.raises(ResumeMetadataError, match="segments as a list"):
        validate_resume_metadata(payload)

    payload = resume_payload(tmp_path)
    payload["segments"] = ["bad"]
    with pytest.raises(ResumeMetadataError, match="segment 0 must be an object"):
        validate_resume_metadata(payload)

    payload = resume_payload(tmp_path)
    payload["segments"][0]["expected_size"] = 999
    with pytest.raises(ResumeMetadataError, match="expected_size mismatch"):
        validate_resume_metadata(payload)


def test_validate_resume_metadata_rejects_file_size_mismatch(tmp_path):
    payload = resume_payload(tmp_path)

    with pytest.raises(ResumeMetadataError, match="file_size mismatch"):
        validate_resume_metadata(payload, expected_file_size=4096)


def test_validate_resume_metadata_rejects_identity_mismatch(tmp_path):
    payload = resume_payload(tmp_path)

    with pytest.raises(ResumeMetadataError, match="url mismatch"):
        validate_resume_metadata(payload, expected_url="https://example.invalid/other.bin")
    with pytest.raises(ResumeMetadataError, match="target_path mismatch"):
        validate_resume_metadata(payload, expected_target_path=str(tmp_path / "other.bin"))
    with pytest.raises(ResumeMetadataError, match="etag mismatch"):
        validate_resume_metadata(payload, expected_etag="different")
    with pytest.raises(ResumeMetadataError, match="last_modified mismatch"):
        validate_resume_metadata(payload, expected_last_modified="Thu, 02 Jan 2025 00:00:00 GMT")


def test_validate_resume_metadata_rejects_missing_identity_fields(tmp_path):
    for field in ("url", "filename", "target_path"):
        payload = resume_payload(tmp_path)
        payload[field] = ""
        with pytest.raises(ResumeMetadataError, match=f"{field} must be a non-empty string"):
            validate_resume_metadata(payload)


def test_validate_resume_metadata_rejects_segment_layout_mismatch(tmp_path):
    payload = resume_payload(tmp_path)
    expected_segments = [
        {"index": 0, "start": 0, "end": 2047, "expected_size": 2048},
    ]

    with pytest.raises(ResumeMetadataError, match="segment layout mismatch"):
        validate_resume_metadata(payload, expected_segments=expected_segments)


def test_validate_resume_metadata_rejects_partial_larger_than_expected(tmp_path):
    payload = resume_payload(tmp_path)
    payload["segments"][1]["existing_size"] = 2048

    with pytest.raises(ResumeMetadataError, match="partial larger than expected"):
        validate_resume_metadata(payload)


def test_load_resume_metadata_rejects_missing_file_and_bad_json(tmp_path):
    with pytest.raises(ResumeMetadataError, match="Unable to read resume metadata file"):
        load_resume_metadata(tmp_path / "missing.json")

    metadata_path = tmp_path / "resume.json"
    metadata_path.write_text("{bad", encoding="utf-8")
    with pytest.raises(ResumeMetadataError, match="Invalid JSON resume metadata file"):
        load_resume_metadata(metadata_path)


def test_write_and_load_resume_metadata_round_trips_json(tmp_path):
    payload = resume_payload(tmp_path)
    metadata_path = tmp_path / "resume.json"

    write_resume_metadata(metadata_path, payload)

    encoded = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert encoded["schema_version"] == RESUME_METADATA_SCHEMA_VERSION
    assert encoded["kind"] == RESUME_METADATA_KIND
    assert encoded["segments"][0]["state"] == "completed"
    assert load_resume_metadata(metadata_path) == encoded


def test_inspect_resume_segments_reads_existing_partial_sizes(tmp_path):
    payload = resume_payload(tmp_path)
    first_path = tmp_path / "file.bin.0"
    second_path = tmp_path / "file.bin.1024"
    first_path.write_bytes(b"x" * 1024)
    second_path.write_bytes(b"y" * 12)

    inspected = inspect_resume_segments(payload)

    assert inspected[0]["existing_size"] == 1024
    assert inspected[0]["state"] == "completed"
    assert inspected[1]["existing_size"] == 12
    assert inspected[1]["state"] == "partial"
    assert payload["segments"][1]["existing_size"] == 512


def test_inspect_resume_segments_marks_missing_partials_as_pending(tmp_path):
    payload = resume_payload(tmp_path)

    inspected = inspect_resume_segments(payload)

    assert inspected[0]["existing_size"] == 0
    assert inspected[0]["state"] == "pending"
    assert inspected[1]["existing_size"] == 0
    assert inspected[1]["state"] == "pending"


def test_validate_resume_metadata_can_reject_actual_partial_larger_than_expected(tmp_path):
    payload = resume_payload(tmp_path)
    too_large_path = tmp_path / "file.bin.1024"
    too_large_path.write_bytes(b"z" * 2048)

    with pytest.raises(ResumeMetadataError, match="partial larger than expected"):
        validate_resume_metadata(payload, inspect_partials=True)
