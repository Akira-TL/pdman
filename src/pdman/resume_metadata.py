from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any


RESUME_METADATA_SCHEMA_VERSION = 2
RESUME_METADATA_KIND = "resume"
RESUME_METADATA_MODES = {"static", "dynamic"}
RESUME_SEGMENT_STATES = {"completed", "partial", "pending", "failed"}


class ResumeMetadataError(ValueError):
    """Raised when resume metadata v2 is invalid or unsafe to reuse."""


def load_resume_metadata(path: str | Path) -> dict[str, Any]:
    metadata_path = Path(path)
    try:
        raw = metadata_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ResumeMetadataError(
            f"Unable to read resume metadata file: {metadata_path}"
        ) from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ResumeMetadataError(
            f"Invalid JSON resume metadata file: {metadata_path}"
        ) from exc

    if not isinstance(payload, dict):
        raise ResumeMetadataError("Resume metadata must be a JSON object")
    validate_resume_metadata(payload)
    return payload


def write_resume_metadata(path: str | Path, payload: dict[str, Any]) -> None:
    validate_resume_metadata(payload)
    metadata_path = Path(path)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=metadata_path.parent,
            prefix=f".{metadata_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_file.write(rendered)
            temp_path = Path(temp_file.name)
        temp_path.replace(metadata_path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


def validate_resume_metadata(
    payload: dict[str, Any],
    *,
    expected_file_size: int | None = None,
    expected_segments: list[dict[str, Any]] | None = None,
) -> None:
    schema_version = payload.get("schema_version")
    if schema_version != RESUME_METADATA_SCHEMA_VERSION:
        raise ResumeMetadataError(
            "Unsupported resume metadata schema_version: " f"{schema_version!r}"
        )

    kind = payload.get("kind")
    if kind != RESUME_METADATA_KIND:
        raise ResumeMetadataError("Unsupported resume metadata kind: " f"{kind!r}")

    mode = payload.get("mode")
    if mode not in RESUME_METADATA_MODES:
        raise ResumeMetadataError("Unsupported resume metadata mode: " f"{mode!r}")

    file_size = _require_int(payload, "file_size", minimum=0)
    if expected_file_size is not None and file_size != expected_file_size:
        raise ResumeMetadataError(
            "file_size mismatch: " f"metadata={file_size} expected={expected_file_size}"
        )

    segments = payload.get("segments")
    if not isinstance(segments, list):
        raise ResumeMetadataError("Resume metadata must include segments as a list")
    if not segments:
        raise ResumeMetadataError("Resume metadata segments must not be empty")

    normalized_segments = [_validate_segment(item, index) for index, item in enumerate(segments)]
    _validate_segment_order(normalized_segments)
    _validate_file_size_coverage(normalized_segments, file_size)

    if expected_segments is not None:
        _validate_expected_layout(normalized_segments, expected_segments)


def _require_int(
    payload: dict[str, Any],
    key: str,
    *,
    minimum: int | None = None,
) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise ResumeMetadataError(f"{key} must be an integer")
    if minimum is not None and value < minimum:
        raise ResumeMetadataError(f"{key} must be >= {minimum}")
    return value


def _require_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ResumeMetadataError(f"{key} must be a non-empty string")
    return value


def _validate_segment(segment: Any, ordinal: int) -> dict[str, Any]:
    if not isinstance(segment, dict):
        raise ResumeMetadataError(f"segment {ordinal} must be an object")

    index = _require_int(segment, "index", minimum=0)
    start = _require_int(segment, "start", minimum=0)
    end = _require_int(segment, "end", minimum=0)
    if end < start:
        raise ResumeMetadataError(
            f"segment {index} end must be greater than or equal to start"
        )

    path = _require_str(segment, "path")
    expected_size = _require_int(segment, "expected_size", minimum=1)
    actual_expected_size = end - start + 1
    if expected_size != actual_expected_size:
        raise ResumeMetadataError(
            "segment expected_size mismatch: "
            f"index={index} metadata={expected_size} expected={actual_expected_size}"
        )

    existing_size = _require_int(segment, "existing_size", minimum=0)
    if existing_size > expected_size:
        raise ResumeMetadataError(
            "segment partial larger than expected: "
            f"index={index} existing_size={existing_size} expected_size={expected_size}"
        )

    state = segment.get("state")
    if state not in RESUME_SEGMENT_STATES:
        raise ResumeMetadataError(
            "Unsupported resume segment state: " f"index={index} state={state!r}"
        )

    return {
        "index": index,
        "start": start,
        "end": end,
        "path": path,
        "expected_size": expected_size,
        "existing_size": existing_size,
        "state": state,
    }


def _validate_segment_order(segments: list[dict[str, Any]]) -> None:
    previous_end = -1
    seen_indexes: set[int] = set()
    for ordinal, segment in enumerate(segments):
        index = segment["index"]
        if index in seen_indexes:
            raise ResumeMetadataError(f"duplicate segment index: {index}")
        seen_indexes.add(index)
        if index != ordinal:
            raise ResumeMetadataError(
                f"segment index mismatch: ordinal={ordinal} index={index}"
            )
        if segment["start"] != previous_end + 1:
            raise ResumeMetadataError(
                "segment layout mismatch: "
                f"index={index} start={segment['start']} expected={previous_end + 1}"
            )
        previous_end = segment["end"]


def _validate_file_size_coverage(
    segments: list[dict[str, Any]],
    file_size: int,
) -> None:
    expected_end = file_size - 1
    actual_end = segments[-1]["end"]
    if actual_end != expected_end:
        raise ResumeMetadataError(
            "segment layout mismatch: " f"last_end={actual_end} expected={expected_end}"
        )


def _validate_expected_layout(
    segments: list[dict[str, Any]],
    expected_segments: list[dict[str, Any]],
) -> None:
    if len(segments) != len(expected_segments):
        raise ResumeMetadataError(
            "segment layout mismatch: "
            f"metadata_count={len(segments)} expected_count={len(expected_segments)}"
        )

    for ordinal, (segment, expected) in enumerate(zip(segments, expected_segments)):
        expected_index = expected.get("index", ordinal)
        expected_start = expected.get("start")
        expected_end = expected.get("end")
        expected_size = expected.get("expected_size")
        if expected_size is None and isinstance(expected_start, int) and isinstance(expected_end, int):
            expected_size = expected_end - expected_start + 1
        if (
            segment["index"] != expected_index
            or segment["start"] != expected_start
            or segment["end"] != expected_end
            or segment["expected_size"] != expected_size
        ):
            raise ResumeMetadataError(
                "segment layout mismatch: "
                f"index={segment['index']} expected_index={expected_index}"
            )
