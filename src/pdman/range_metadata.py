from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from .range_allocator import RangeAllocator
from .range_task import RangeTask


DYNAMIC_RANGE_METADATA_FILENAME = "dynamic-ranges.json"
DYNAMIC_RANGE_METADATA_SCHEMA_VERSION = 1


def range_task_payload(task: RangeTask, *, state: str = "unknown") -> dict[str, Any]:
    return {
        "index": task.index,
        "start": task.start,
        "end": task.end,
        "path": str(task.path),
        "attempts": task.attempts,
        "last_error": task.last_error,
        "downloaded_bytes": task.downloaded_bytes,
        "existing_size": task.existing_size(),
        "expected_size": task.expected_size,
        "last_speed_bps": task.last_speed_bps,
        "state": state,
    }


def range_allocator_payload(
    allocator: RangeAllocator,
    *,
    file_size: int | None = None,
    selector: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": DYNAMIC_RANGE_METADATA_SCHEMA_VERSION,
        "mode": "dynamic",
        "file_size": allocator.file_size if file_size is None else file_size,
        "range_size": allocator.range_size,
        "stats": {
            "total_ranges": allocator.total_ranges,
            "pending_count": allocator.pending_count,
            "active_count": allocator.active_count,
            "completed_count": allocator.completed_count,
            "failed_count": allocator.failed_count,
            "retried_count": allocator.retried_count,
            "requeue_count": allocator.requeue_count,
            "split_count": allocator.split_count,
            "completed_bytes": allocator.completed_bytes,
        },
        "ranges": [
            range_task_payload(task, state=allocator.task_state(task))
            for task in allocator.ranges
        ],
    }
    if selector is not None:
        payload["selector"] = dict(selector)
    return payload


def write_range_metadata(
    path: str | Path,
    allocator: RangeAllocator,
    *,
    file_size: int | None = None,
    selector: dict[str, Any] | None = None,
) -> None:
    metadata_path = Path(path)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    payload = range_allocator_payload(
        allocator,
        file_size=file_size,
        selector=selector,
    )
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
