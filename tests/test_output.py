import json

from pdman.output import print_jsonl, queue_records_payload, validation_report_payload
from pdman.queue import QueueRecord, QueueValidationIssue, QueueValidationReport


def test_queue_records_payload_includes_count_and_records():
    record = QueueRecord(
        queue_id="q1",
        url="https://example.com/file.bin",
        file_name="file.bin",
        status="failed",
        attempts=3,
        last_error="HTTP 503 during header check",
    )

    payload = queue_records_payload([record])

    assert payload["count"] == 1
    assert payload["records"][0]["schema_version"] == 1
    assert payload["records"][0]["queue_id"] == "q1"
    assert payload["records"][0]["url"] == "https://example.com/file.bin"
    assert payload["records"][0]["status"] == "failed"
    assert payload["records"][0]["attempts"] == 3


def test_print_jsonl_emits_one_json_object_per_line(capsys):
    print_jsonl([
        {"queue_id": "q1", "status": "failed"},
        {"queue_id": "q2", "status": "failed"},
    ])

    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"queue_id": "q1", "status": "failed"}
    assert json.loads(lines[1]) == {"queue_id": "q2", "status": "failed"}


def test_validation_report_payload_includes_ok_counts_and_issues():
    report = QueueValidationReport(valid=1, malformed=1)
    report.issues.append(
        QueueValidationIssue(
            line_no=3,
            issue_type="malformed",
            message="malformed JSON: bad",
            queue_id=None,
        )
    )

    payload = validation_report_payload(report)

    assert payload["ok"] is False
    assert payload["valid"] == 1
    assert payload["malformed"] == 1
    assert payload["invalid"] == 0
    assert payload["duplicate_ids"] == 0
    assert payload["unsupported_schema"] == 0
    assert payload["issues"] == [
        {
            "line_no": 3,
            "issue_type": "malformed",
            "message": "malformed JSON: bad",
            "queue_id": None,
        }
    ]
