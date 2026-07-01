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


def header_probe_payload(source: TaskResult | dict[str, Any]) -> dict[str, Any]:
    if isinstance(source, TaskResult):
        method = source.header_probe_method
        fallback_reason = source.header_probe_fallback_reason
    else:
        method = source.get("header_probe_method")
        fallback_reason = source.get("header_probe_fallback_reason")
    return {
        "method": method,
        "fallback_used": bool(fallback_reason),
        "fallback_reason": fallback_reason,
    }


def network_error_payload(source: TaskResult | dict[str, Any]) -> dict[str, Any]:
    if isinstance(source, TaskResult):
        phase = source.network_error_phase
        kind = source.network_error_kind
        http_status = source.network_http_status
    else:
        phase = source.get("network_error_phase")
        kind = source.get("network_error_kind")
        http_status = source.get("network_http_status")
    return {
        "present": bool(phase or kind or http_status),
        "phase": phase,
        "kind": kind,
        "http_status": http_status,
    }


def history_records_payload(
    records: list[dict[str, Any]],
    key: str = "records",
) -> dict[str, Any]:
    serialized = []
    for record in records:
        item = dict(record)
        item["resume_rejection"] = resume_rejection_payload(record)
        item["header_probe"] = header_probe_payload(record)
        item["network_error"] = network_error_payload(record)
        serialized.append(item)
    return {key: serialized, "count": len(serialized)}


def run_detail_payload(
    run: dict[str, Any],
    tasks: list[dict[str, Any]],
) -> dict[str, Any]:
    task_payload = history_records_payload(tasks, key="tasks")
    return {
        "run": dict(run),
        "tasks": task_payload["tasks"],
        "task_count": task_payload["count"],
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
