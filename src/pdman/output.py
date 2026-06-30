from __future__ import annotations

import json
from typing import Any

from .queue import QueueRecord, QueueValidationReport
from .status import TaskResult


def print_json(data: dict[str, Any]) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True))


def print_jsonl(records: list[dict[str, Any]]) -> None:
    for record in records:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))


def resume_rejection_payload(source: TaskResult | dict[str, Any]) -> dict[str, Any]:
    if isinstance(source, TaskResult):
        code = source.resume_rejection_code
        reason = source.resume_rejection_reason
    else:
        code = source.get("resume_rejection_code")
        reason = source.get("resume_rejection_reason")
    return {
        "present": bool(code or reason),
        "code": code,
        "reason": reason,
    }


def queue_record_to_dict(record: QueueRecord) -> dict[str, Any]:
    return record.to_dict()


def queue_records_payload(records: list[QueueRecord], key: str = "records") -> dict[str, Any]:
    serialized = [queue_record_to_dict(record) for record in records]
    return {key: serialized, "count": len(serialized)}


def queue_add_payload(records: list[QueueRecord]) -> dict[str, Any]:
    payload = queue_records_payload(records)
    payload["added"] = payload["count"]
    return payload


def queue_repair_payload(stats: dict[str, int]) -> dict[str, Any]:
    return dict(stats)


def queue_recover_payload(recovered: int) -> dict[str, Any]:
    return {"recovered": recovered}


def queue_remove_payload(queue_ids: list[str], removed: int) -> dict[str, Any]:
    return {"requested": queue_ids, "removed": removed}


def queue_clear_payload(cleared: int, status: str | None, all_records: bool) -> dict[str, Any]:
    return {"cleared": cleared, "status": status, "all": all_records}


def validation_report_payload(report: QueueValidationReport) -> dict[str, Any]:
    return {
        "ok": report.ok,
        "valid": report.valid,
        "malformed": report.malformed,
        "invalid": report.invalid,
        "duplicate_ids": report.duplicate_ids,
        "unsupported_schema": report.unsupported_schema,
        "issues": [
            {
                "line_no": issue.line_no,
                "issue_type": issue.issue_type,
                "message": issue.message,
                "queue_id": issue.queue_id,
            }
            for issue in report.issues
        ],
    }
