import json

import pytest

from pdman.resume_metadata import (
    RESUME_METADATA_KIND,
    RESUME_METADATA_SCHEMA_VERSION,
    ResumeMetadataError,
    load_resume_metadata,
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
