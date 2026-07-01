from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .range_metadata import (
    DYNAMIC_RANGE_METADATA_FILENAME,
    DYNAMIC_RANGE_METADATA_SCHEMA_VERSION,
)


class RangeMetadataError(ValueError):
    """Raised when a dynamic range metadata file cannot be inspected."""


def load_range_metadata(path: str | Path) -> dict[str, Any]:
    metadata_path = Path(path)
    try:
        raw = metadata_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RangeMetadataError(f"Unable to read metadata file: {metadata_path}") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RangeMetadataError(f"Invalid JSON metadata file: {metadata_path}") from exc

    if not isinstance(payload, dict):
        raise RangeMetadataError("Dynamic range metadata must be a JSON object")
    validate_range_metadata(payload)
    return payload


def validate_range_metadata(payload: dict[str, Any]) -> None:
    schema_version = payload.get("schema_version")
    if schema_version != DYNAMIC_RANGE_METADATA_SCHEMA_VERSION:
        raise RangeMetadataError(
            "Unsupported dynamic range metadata schema_version: "
            f"{schema_version!r}"
        )
    if payload.get("mode") != "dynamic":
        raise RangeMetadataError(
            "Unsupported range metadata mode: " f"{payload.get('mode')!r}"
        )
    if not isinstance(payload.get("ranges"), list):
        raise RangeMetadataError("Dynamic range metadata must include ranges as a list")
    if not isinstance(payload.get("stats"), dict):
        raise RangeMetadataError("Dynamic range metadata must include stats as an object")


def _range_start(item: dict[str, Any]) -> tuple[int, int]:
    start = item.get("start")
    index = item.get("index")
    return (
        start if isinstance(start, int) else 0,
        index if isinstance(index, int) else 0,
    )


def _ranges(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in payload["ranges"] if isinstance(item, dict)]


@dataclass(frozen=True)
class LatestRangeMetadataSearch:
    roots: list[str]
    selected_path: Path | None
    valid_count: int
    skipped_invalid_count: int


def find_latest_range_metadata_diagnostics(
    roots: list[str | Path],
) -> LatestRangeMetadataSearch:
    candidates: list[tuple[float, Path]] = []
    skipped_invalid_count = 0
    resolved_roots = [str(Path(root).expanduser()) for root in roots]
    for root in roots:
        root_path = Path(root).expanduser()
        if not root_path.exists():
            continue
        if root_path.is_file():
            paths = [root_path] if root_path.name == DYNAMIC_RANGE_METADATA_FILENAME else []
        else:
            paths = root_path.rglob(DYNAMIC_RANGE_METADATA_FILENAME)
        for path in paths:
            try:
                load_range_metadata(path)
                mtime = path.stat().st_mtime
            except (OSError, RangeMetadataError):
                skipped_invalid_count += 1
                continue
            candidates.append((mtime, path))
    selected_path = None
    if candidates:
        selected_path = max(candidates, key=lambda item: (item[0], str(item[1])))[1]
    return LatestRangeMetadataSearch(
        roots=resolved_roots,
        selected_path=selected_path,
        valid_count=len(candidates),
        skipped_invalid_count=skipped_invalid_count,
    )


def find_latest_range_metadata(
    roots: list[str | Path],
) -> Path | None:
    return find_latest_range_metadata_diagnostics(roots).selected_path


def filter_ranges(
    payload: dict[str, Any],
    *,
    state: str | None = None,
) -> list[dict[str, Any]]:
    ranges = _ranges(payload)
    if state is not None:
        ranges = [item for item in ranges if item.get("state") == state]
    return sorted(ranges, key=_range_start)


def range_metadata_summary(
    payload: dict[str, Any],
    *,
    state: str | None = None,
) -> dict[str, Any]:
    ranges = filter_ranges(payload, state=state)
    all_ranges = _ranges(payload)
    state_counts = Counter(str(item.get("state", "unknown")) for item in all_ranges)
    filtered_state_counts = Counter(str(item.get("state", "unknown")) for item in ranges)
    summary = {
        "schema_version": payload["schema_version"],
        "mode": payload["mode"],
        "file_size": payload.get("file_size"),
        "range_size": payload.get("range_size"),
        "stats": payload["stats"],
        "filter": {"state": state},
        "count": len(ranges),
        "state_counts": dict(sorted(state_counts.items())),
        "filtered_state_counts": dict(sorted(filtered_state_counts.items())),
        "ranges": ranges,
    }
    selector = payload.get("selector")
    if isinstance(selector, dict):
        summary["selector"] = dict(selector)
    return summary


def _format_size_pair(item: dict[str, Any]) -> str:
    downloaded = item.get("downloaded_bytes")
    expected = item.get("expected_size")
    if downloaded is None and expected is None:
        return ""
    return f" bytes={downloaded or 0}/{expected or 0}"


def _format_speed(item: dict[str, Any]) -> str:
    speed = item.get("last_speed_bps")
    if speed in (None, ""):
        return ""
    if isinstance(speed, (int, float)):
        return f" speed={speed:.2f}B/s"
    return f" speed={speed}"


def _format_error(item: dict[str, Any]) -> str:
    error = item.get("last_error")
    if not error:
        return ""
    return f" error={error}"


def _format_range_line(item: dict[str, Any]) -> str:
    index = item.get("index", "?")
    start = item.get("start", "?")
    end = item.get("end", "?")
    attempts = item.get("attempts", 0)
    state = item.get("state", "unknown")
    return (
        f"  #{index} {start}-{end} state={state} attempts={attempts}"
        f"{_format_size_pair(item)}{_format_speed(item)}{_format_error(item)}"
    )


def format_range_metadata(
    payload: dict[str, Any],
    *,
    state: str | None = None,
    source_path: str | Path | None = None,
) -> str:
    summary = range_metadata_summary(payload, state=state)
    stats = summary["stats"]
    state_counts = summary["state_counts"]
    title = "Dynamic range metadata"
    if source_path is not None:
        title += f": {source_path}"

    lines = [
        title,
        f"schema_version: {summary['schema_version']}",
        f"mode: {summary['mode']}",
    ]
    selector = summary.get("selector")
    if isinstance(selector, dict):
        lines.append(
            "selector: "
            f"requested={selector.get('requested_mode')} "
            f"selected={selector.get('selected_mode')} "
            f"reason={selector.get('reason')} "
            f"fallback_reason={selector.get('fallback_reason')}"
        )
    lines.extend([
        f"file_size: {summary['file_size']}",
        f"range_size: {summary['range_size']}",
        "ranges: "
        f"total={len(_ranges(payload))} "
        f"completed={state_counts.get('completed', 0)} "
        f"failed={state_counts.get('failed', 0)} "
        f"pending={state_counts.get('pending', 0)} "
        f"active={state_counts.get('active', 0)} "
        f"unknown={state_counts.get('unknown', 0)}",
        "retry: "
        f"retried={stats.get('retried_count', 0)} "
        f"requeued={stats.get('requeue_count', 0)} "
        f"split={stats.get('split_count', 0)}",
        f"completed_bytes: {stats.get('completed_bytes', 0)}",
    ])

    if state is None:
        failed_ranges = filter_ranges(payload, state="failed")
        if failed_ranges:
            lines.append("")
            lines.append("Failed ranges:")
            lines.extend(_format_range_line(item) for item in failed_ranges)
        else:
            lines.append("")
            lines.append("No failed ranges.")
    else:
        lines.append("")
        lines.append(f"Ranges state={state}:")
        filtered = summary["ranges"]
        if filtered:
            lines.extend(_format_range_line(item) for item in filtered)
        else:
            lines.append("  No matching ranges.")
    return "\n".join(lines)


__all__ = [
    "LatestRangeMetadataSearch",
    "RangeMetadataError",
    "filter_ranges",
    "find_latest_range_metadata",
    "find_latest_range_metadata_diagnostics",
    "format_range_metadata",
    "load_range_metadata",
    "range_metadata_summary",
]
