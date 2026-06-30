from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .runtime import default_cache_root, utc_now_iso
from .status import TaskResult, TaskStatus
from .task_input import TaskInput


QUEUE_STATUSES = {"pending", "running", "completed", "skipped", "failed"}


@dataclass
class QueueRecord:
    queue_id: str
    url: str
    file_name: str | None = None
    dir_path: str | None = None
    md5: str | None = None
    status: str = "pending"
    created_at: str | None = None
    updated_at: str | None = None
    last_run_id: str | None = None
    last_error: str | None = None

    def __post_init__(self) -> None:
        if self.status not in QUEUE_STATUSES:
            raise ValueError(f"Invalid queue status: {self.status}")
        now = utc_now_iso()
        if self.created_at is None:
            self.created_at = now
        if self.updated_at is None:
            self.updated_at = self.created_at

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueRecord":
        return cls(
            queue_id=str(data["queue_id"]),
            url=str(data["url"]),
            file_name=data.get("file_name"),
            dir_path=data.get("dir_path"),
            md5=data.get("md5"),
            status=data.get("status", "pending"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            last_run_id=data.get("last_run_id"),
            last_error=data.get("last_error"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def new_queue_id() -> str:
    stamp = utc_now_iso().replace("-", "").replace(":", "").split(".")[0]
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def queue_path(cache_dir: str | None = None) -> Path:
    root = Path(cache_dir).expanduser() if cache_dir else default_cache_root()
    return root / "queue.jsonl"


def load_queue(cache_dir: str | None = None) -> list[QueueRecord]:
    path = queue_path(cache_dir)
    if not path.exists():
        return []
    records: list[QueueRecord] = []
    with path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                if isinstance(data, dict):
                    records.append(QueueRecord.from_dict(data))
            except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                continue
    return records


def append_queue(records: list[QueueRecord], cache_dir: str | None = None) -> None:
    if not records:
        return
    path = queue_path(cache_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        for record in records:
            f.write(json.dumps(record.to_dict(), sort_keys=True) + "\n")


def rewrite_queue(records: list[QueueRecord], cache_dir: str | None = None) -> None:
    path = queue_path(cache_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        "".join(json.dumps(record.to_dict(), sort_keys=True) + "\n" for record in records)
    )
    tmp_path.replace(path)


def query_queue(
    cache_dir: str | None = None,
    *,
    status: str | None = None,
    last: int = 20,
) -> list[QueueRecord]:
    if status is not None and status not in QUEUE_STATUSES:
        raise ValueError(f"Invalid queue status: {status}")
    records = load_queue(cache_dir)
    if status:
        records = [record for record in records if record.status == status]
    if last is not None and last > 0:
        records = records[-last:]
    return records


def task_to_queue_record(task: TaskInput) -> QueueRecord:
    return QueueRecord(
        queue_id=new_queue_id(),
        url=task.url,
        file_name=task.file_name,
        dir_path=task.dir_path,
        md5=task.md5,
    )


def create_queue_records(tasks: list[TaskInput]) -> list[QueueRecord]:
    return [task_to_queue_record(task) for task in tasks]


def mark_records_running(records: list[QueueRecord], run_id: str) -> None:
    now = utc_now_iso()
    for record in records:
        record.status = "running"
        record.last_run_id = run_id
        record.last_error = None
        record.updated_at = now


def _result_status(result: TaskResult) -> str:
    if result.status == TaskStatus.COMPLETED:
        return "completed"
    if result.status == TaskStatus.SKIPPED:
        return "skipped"
    return "failed"


def update_queue_from_results(
    records: list[QueueRecord],
    selected_records: list[QueueRecord],
    results: list[TaskResult],
    run_id: str,
) -> list[QueueRecord]:
    now = utc_now_iso()
    selected_by_id = {record.queue_id: record for record in selected_records}
    remaining_results = list(results)
    for record in records:
        if record.queue_id not in selected_by_id:
            continue
        result_index = None
        for index, result in enumerate(remaining_results):
            if result.url == record.url:
                result_index = index
                break
        if result_index is None:
            record.status = "failed"
            record.last_error = "task did not produce a result"
        else:
            result = remaining_results.pop(result_index)
            record.status = _result_status(result)
            record.last_error = result.reason or result.error
        record.last_run_id = run_id
        record.updated_at = now
    return records


def format_queue(records: list[QueueRecord]) -> str:
    if not records:
        return "No queue records found."
    lines = ["Queue:"]
    for record in records:
        name = record.file_name or record.url
        suffix = f" - {record.last_error}" if record.last_error and record.status == "failed" else ""
        lines.append(
            f"  {record.status:<9} {record.queue_id} {name} {record.url}{suffix}"
        )
    return "\n".join(lines)
