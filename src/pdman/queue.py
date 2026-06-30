from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .filelock import FileLock
from .runtime import default_cache_root, utc_now_iso
from .status import TaskResult, TaskStatus
from .task_input import TaskInput


QUEUE_SCHEMA_VERSION = 1
QUEUE_STATUSES = {"pending", "running", "completed", "skipped", "failed"}


class UnsupportedQueueSchema(ValueError):
    pass


@dataclass
class QueueValidationIssue:
    line_no: int | None
    issue_type: str
    message: str
    queue_id: str | None = None


@dataclass
class QueueValidationReport:
    valid: int = 0
    malformed: int = 0
    invalid: int = 0
    duplicate_ids: int = 0
    unsupported_schema: int = 0
    issues: list[QueueValidationIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.issues


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
    schema_version: int = QUEUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != QUEUE_SCHEMA_VERSION:
            raise UnsupportedQueueSchema(
                f"Unsupported queue schema version: {self.schema_version}"
            )
        if not self.queue_id:
            raise ValueError("queue_id is required")
        if not self.url:
            raise ValueError("url is required")
        if self.status not in QUEUE_STATUSES:
            raise ValueError(f"Invalid queue status: {self.status}")
        now = utc_now_iso()
        if self.created_at is None:
            self.created_at = now
        if self.updated_at is None:
            self.updated_at = self.created_at

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueRecord":
        schema_version = int(data.get("schema_version", QUEUE_SCHEMA_VERSION))
        return cls(
            schema_version=schema_version,
            queue_id=str(data.get("queue_id") or ""),
            url=str(data.get("url") or ""),
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


def queue_lock_path(cache_dir: str | None = None) -> Path:
    root = Path(cache_dir).expanduser() if cache_dir else default_cache_root()
    return root / "queue.lock"


def queue_lock(cache_dir: str | None = None, timeout: float | None = 10.0) -> FileLock:
    return FileLock(queue_lock_path(cache_dir), timeout=timeout)


def _read_jsonl(path: Path) -> list[tuple[int, str]]:
    if not path.exists():
        return []
    with path.open("r") as f:
        return [(line_no, line.strip()) for line_no, line in enumerate(f, start=1)]


def load_queue(cache_dir: str | None = None) -> list[QueueRecord]:
    records: list[QueueRecord] = []
    for _, line in _read_jsonl(queue_path(cache_dir)):
        if not line:
            continue
        try:
            data = json.loads(line)
            if isinstance(data, dict):
                records.append(QueueRecord.from_dict(data))
        except (json.JSONDecodeError, KeyError, ValueError, TypeError):
            continue
    return records


def _append_queue_unlocked(records: list[QueueRecord], cache_dir: str | None = None) -> None:
    if not records:
        return
    path = queue_path(cache_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        for record in records:
            f.write(json.dumps(record.to_dict(), sort_keys=True) + "\n")


def append_queue(records: list[QueueRecord], cache_dir: str | None = None) -> None:
    with queue_lock(cache_dir):
        _append_queue_unlocked(records, cache_dir)


def _rewrite_queue_unlocked(records: list[QueueRecord], cache_dir: str | None = None) -> None:
    path = queue_path(cache_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        "".join(json.dumps(record.to_dict(), sort_keys=True) + "\n" for record in records)
    )
    tmp_path.replace(path)


def rewrite_queue(records: list[QueueRecord], cache_dir: str | None = None) -> None:
    with queue_lock(cache_dir):
        _rewrite_queue_unlocked(records, cache_dir)


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


def _issue(
    report: QueueValidationReport,
    line_no: int | None,
    issue_type: str,
    message: str,
    queue_id: str | None = None,
) -> None:
    report.issues.append(
        QueueValidationIssue(
            line_no=line_no,
            issue_type=issue_type,
            message=message,
            queue_id=queue_id,
        )
    )
    if issue_type == "malformed":
        report.malformed += 1
    elif issue_type == "duplicate_id":
        report.duplicate_ids += 1
    elif issue_type == "unsupported_schema":
        report.unsupported_schema += 1
    else:
        report.invalid += 1


def validate_queue(cache_dir: str | None = None) -> QueueValidationReport:
    report = QueueValidationReport()
    seen_ids: set[str] = set()
    for line_no, line in _read_jsonl(queue_path(cache_dir)):
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError as e:
            _issue(report, line_no, "malformed", f"malformed JSON: {e}")
            continue
        if not isinstance(data, dict):
            _issue(report, line_no, "invalid", "queue record must be an object")
            continue
        queue_id = str(data.get("queue_id") or "")
        schema_version = data.get("schema_version", QUEUE_SCHEMA_VERSION)
        try:
            schema_version = int(schema_version)
        except (TypeError, ValueError):
            _issue(report, line_no, "invalid", "schema_version must be an integer", queue_id)
            continue
        if schema_version > QUEUE_SCHEMA_VERSION:
            _issue(
                report,
                line_no,
                "unsupported_schema",
                f"unsupported schema_version: {schema_version}",
                queue_id or None,
            )
            continue
        if not queue_id:
            _issue(report, line_no, "invalid", "missing queue_id")
            continue
        if queue_id in seen_ids:
            _issue(report, line_no, "duplicate_id", "duplicate queue_id", queue_id)
            continue
        if not data.get("url"):
            _issue(report, line_no, "invalid", "missing url", queue_id)
            continue
        if data.get("status", "pending") not in QUEUE_STATUSES:
            _issue(report, line_no, "invalid", "invalid status", queue_id)
            continue
        seen_ids.add(queue_id)
        report.valid += 1
    return report


def format_queue_validation(report: QueueValidationReport) -> str:
    lines = [
        "Queue validation:",
        f"  valid: {report.valid}",
        f"  malformed: {report.malformed}",
        f"  invalid: {report.invalid}",
        f"  duplicate_ids: {report.duplicate_ids}",
        f"  unsupported_schema: {report.unsupported_schema}",
    ]
    for issue in report.issues:
        location = f"line {issue.line_no}" if issue.line_no is not None else "queue"
        queue_id = f" ({issue.queue_id})" if issue.queue_id else ""
        lines.append(f"  - {location}{queue_id}: {issue.message}")
    return "\n".join(lines)


def repair_queue(cache_dir: str | None = None) -> dict[str, int]:
    with queue_lock(cache_dir):
        path = queue_path(cache_dir)
        seen_ids: set[str] = set()
        repaired: list[QueueRecord] = []
        stats = {
            "kept": 0,
            "dropped_malformed": 0,
            "dropped_invalid": 0,
            "dropped_unsupported_schema": 0,
            "fixed": 0,
        }
        now = utc_now_iso()
        for _, line in _read_jsonl(path):
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                stats["dropped_malformed"] += 1
                continue
            if not isinstance(data, dict):
                stats["dropped_invalid"] += 1
                continue
            fixed = False
            schema_version = data.get("schema_version", QUEUE_SCHEMA_VERSION)
            try:
                schema_version = int(schema_version)
            except (TypeError, ValueError):
                schema_version = QUEUE_SCHEMA_VERSION
                fixed = True
            if schema_version > QUEUE_SCHEMA_VERSION:
                stats["dropped_unsupported_schema"] += 1
                continue
            data["schema_version"] = QUEUE_SCHEMA_VERSION
            queue_id_value = data.get("queue_id")
            queue_id = str(queue_id_value) if queue_id_value else ""
            if not queue_id or queue_id in seen_ids:
                data["queue_id"] = new_queue_id()
                fixed = True
            if not data.get("url"):
                stats["dropped_invalid"] += 1
                continue
            if data.get("status", "pending") not in QUEUE_STATUSES:
                data["status"] = "failed"
                data["last_error"] = "repaired invalid queue status"
                fixed = True
            if not data.get("created_at"):
                data["created_at"] = now
                fixed = True
            if not data.get("updated_at"):
                data["updated_at"] = data["created_at"]
                fixed = True
            try:
                record = QueueRecord.from_dict(data)
            except (ValueError, TypeError):
                stats["dropped_invalid"] += 1
                continue
            seen_ids.add(record.queue_id)
            repaired.append(record)
            stats["kept"] += 1
            if fixed:
                stats["fixed"] += 1
        _rewrite_queue_unlocked(repaired, cache_dir)
        return stats


def recover_running(cache_dir: str | None = None) -> int:
    with queue_lock(cache_dir):
        records = load_queue(cache_dir)
        recovered = 0
        now = utc_now_iso()
        for record in records:
            if record.status == "running":
                record.status = "pending"
                record.last_error = "recovered from stale running state"
                record.updated_at = now
                recovered += 1
        if recovered:
            _rewrite_queue_unlocked(records, cache_dir)
        return recovered


def remove_queue_records(queue_ids: list[str], cache_dir: str | None = None) -> int:
    with queue_lock(cache_dir):
        wanted = set(queue_ids)
        records = load_queue(cache_dir)
        kept = [record for record in records if record.queue_id not in wanted]
        removed = len(records) - len(kept)
        if removed:
            _rewrite_queue_unlocked(kept, cache_dir)
        return removed


def clear_queue(
    *,
    status: str | None = None,
    all_records: bool = False,
    cache_dir: str | None = None,
) -> int:
    if not all_records and status is None:
        raise ValueError("clear_queue requires status or all_records=True")
    if status is not None and status not in QUEUE_STATUSES:
        raise ValueError(f"Invalid queue status: {status}")
    with queue_lock(cache_dir):
        records = load_queue(cache_dir)
        if all_records:
            cleared = len(records)
            kept: list[QueueRecord] = []
        else:
            kept = [record for record in records if record.status != status]
            cleared = len(records) - len(kept)
        if cleared:
            _rewrite_queue_unlocked(kept, cache_dir)
        return cleared


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
            record.last_error = (
                result.reason or result.error if record.status == "failed" else None
            )
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
