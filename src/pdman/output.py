from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from .queue import QueueRecord, QueueValidationReport
from .status import TaskResult, TaskStatus


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


def _enum_value(value: Any) -> Any:
    return getattr(value, "value", value)


def task_result_payload(
    result: TaskResult,
    *,
    task_id: str | None = None,
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "url": result.url,
        "filename": result.filename,
        "status": _enum_value(result.status),
        "reason_code": _enum_value(result.reason_code),
        "reason": result.reason,
        "error": result.error,
        "downloaded_bytes": result.downloaded_bytes,
        "total_bytes": result.total_bytes,
        "resume_rejection": resume_rejection_payload(result),
        "header_probe": header_probe_payload(result),
        "network_error": network_error_payload(result),
    }


def download_summary_payload(
    *,
    run_id: str,
    results: list[TaskResult],
    exit_code: int,
    task_id_for_url: Callable[[str], str] | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> dict[str, Any]:
    counts = {
        "completed": sum(1 for result in results if result.status == TaskStatus.COMPLETED),
        "skipped": sum(1 for result in results if result.status == TaskStatus.SKIPPED),
        "failed": sum(1 for result in results if result.status == TaskStatus.FAILED),
    }
    status = "failed" if exit_code != 0 or counts["failed"] else "completed"
    return {
        "schema_version": 1,
        "kind": "download_summary",
        "run_id": run_id,
        "status": status,
        "exit_code": exit_code,
        "started_at": started_at,
        "finished_at": finished_at,
        "counts": counts,
        "tasks": [
            task_result_payload(
                result,
                task_id=task_id_for_url(result.url) if task_id_for_url else None,
            )
            for result in results
        ],
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
