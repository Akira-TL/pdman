from __future__ import annotations

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


def find_latest_resume_metadata(roots: list[str | Path]) -> Path | None:
    candidates: list[tuple[float, Path]] = []
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
                continue
            candidates.append((mtime, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], str(item[1])))[1]


def resume_metadata_summary(metadata_path: str | Path) -> dict[str, Any]:
    payload = load_resume_metadata(metadata_path)
    segments = inspect_resume_segments(payload)
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
        "stats": _segment_stats(segments),
        "segments": segments,
    }


def format_resume_metadata_summary(summary: dict[str, Any]) -> str:
    stats = summary["stats"]
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
        "Segments:",
    ]
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
    "find_latest_resume_metadata",
    "format_resume_metadata_summary",
    "resume_metadata_summary",
]
