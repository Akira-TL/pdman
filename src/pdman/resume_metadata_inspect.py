from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .resume_metadata import (
    RESUME_METADATA_FILENAME,
    ResumeMetadataError,
    inspect_resume_segments,
    load_resume_metadata,
)


def _segment_stats(segments: list[dict[str, Any]]) -> dict[str, int]:
    stats = {
        "total_segments": len(segments),
        "completed_count": 0,
        "partial_count": 0,
        "pending_count": 0,
        "failed_count": 0,
        "existing_bytes": 0,
        "expected_bytes": 0,
    }
    for segment in segments:
        state = str(segment.get("state") or "pending")
        key = f"{state}_count"
        if key in stats:
            stats[key] += 1
        stats["existing_bytes"] += int(segment.get("existing_size") or 0)
        stats["expected_bytes"] += int(segment.get("expected_size") or 0)
    return stats


@dataclass(frozen=True)
class LatestResumeMetadataSearch:
    roots: list[str]
    selected_path: Path | None
    valid_count: int
    skipped_invalid_count: int


def find_latest_resume_metadata_diagnostics(
    roots: list[str | Path],
) -> LatestResumeMetadataSearch:
    candidates: list[tuple[float, Path]] = []
    skipped_invalid_count = 0
    resolved_roots = [str(Path(root).expanduser()) for root in roots]
    for root in roots:
        root_path = Path(root).expanduser()
        if not root_path.exists():
            continue
        if root_path.is_file():
            paths = [root_path] if root_path.name == RESUME_METADATA_FILENAME else []
        else:
            paths = root_path.rglob(RESUME_METADATA_FILENAME)
        for path in paths:
            try:
                load_resume_metadata(path)
                mtime = path.stat().st_mtime
            except (OSError, ResumeMetadataError):
                skipped_invalid_count += 1
                continue
            candidates.append((mtime, path))
    selected_path = None
    if candidates:
        selected_path = max(candidates, key=lambda item: (item[0], str(item[1])))[1]
    return LatestResumeMetadataSearch(
        roots=resolved_roots,
        selected_path=selected_path,
        valid_count=len(candidates),
        skipped_invalid_count=skipped_invalid_count,
    )


def find_latest_resume_metadata(roots: list[str | Path]) -> Path | None:
    return find_latest_resume_metadata_diagnostics(roots).selected_path


def _filter_segments(
    segments: list[dict[str, Any]],
    *,
    state: str | None = None,
) -> list[dict[str, Any]]:
    if state is None:
        return segments
    return [segment for segment in segments if segment.get("state") == state]


def resume_metadata_summary(
    metadata_path: str | Path,
    *,
    state: str | None = None,
) -> dict[str, Any]:
    payload = load_resume_metadata(metadata_path)
    segments = inspect_resume_segments(payload)
    filtered_segments = _filter_segments(segments, state=state)
    return {
        "source_path": str(metadata_path),
        "schema_version": payload["schema_version"],
        "kind": payload["kind"],
        "mode": payload["mode"],
        "url": payload["url"],
        "filename": payload["filename"],
        "target_path": payload["target_path"],
        "file_size": payload["file_size"],
        "etag": payload.get("etag"),
        "last_modified": payload.get("last_modified"),
        "created_at": payload.get("created_at"),
        "updated_at": payload.get("updated_at"),
        "filter": {"state": state},
        "count": len(filtered_segments),
        "stats": _segment_stats(segments),
        "filtered_stats": _segment_stats(filtered_segments),
        "segments": filtered_segments,
    }


def format_resume_metadata_summary(summary: dict[str, Any]) -> str:
    stats = summary["stats"]
    filtered_stats = summary["filtered_stats"]
    state_filter = summary.get("filter", {}).get("state")
    lines = [
        f"Resume metadata: {summary['source_path']}",
        f"mode: {summary['mode']}",
        f"file: {summary['filename']} size={summary['file_size']}",
        f"url: {summary['url']}",
        f"target: {summary['target_path']}",
        "segments: "
        f"total={stats['total_segments']} "
        f"completed={stats['completed_count']} "
        f"partial={stats['partial_count']} "
        f"pending={stats['pending_count']} "
        f"failed={stats['failed_count']}",
    ]
    if state_filter is not None:
        lines.extend([
            f"filter: state={state_filter}",
            "filtered: "
            f"total={filtered_stats['total_segments']} "
            f"completed={filtered_stats['completed_count']} "
            f"partial={filtered_stats['partial_count']} "
            f"pending={filtered_stats['pending_count']} "
            f"failed={filtered_stats['failed_count']}",
        ])
    lines.append("Segments:")
    for segment in summary["segments"]:
        lines.append(
            "  "
            f"#{segment['index']} {segment['start']}-{segment['end']} "
            f"state={segment['state']} "
            f"existing={segment['existing_size']}/{segment['expected_size']} "
            f"path={segment['path']}"
        )
    return "\n".join(lines)


__all__ = [
    "LatestResumeMetadataSearch",
    "find_latest_resume_metadata",
    "find_latest_resume_metadata_diagnostics",
    "format_resume_metadata_summary",
    "resume_metadata_summary",
]
