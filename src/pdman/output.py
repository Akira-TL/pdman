from __future__ import annotations

import json
from typing import Any

from .queue import QueueRecord, QueueValidationReport


def print_json(data: dict[str, Any]) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True))


def print_jsonl(records: list[dict[str, Any]]) -> None:
    for record in records:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))


def queue_record_to_dict(record: QueueRecord) -> dict[str, Any]:
    return record.to_dict()


def queue_records_payload(records: list[QueueRecord], key: str = "records") -> dict[str, Any]:
    serialized = [queue_record_to_dict(record) for record in records]
    return {key: serialized, "count": len(serialized)}


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
