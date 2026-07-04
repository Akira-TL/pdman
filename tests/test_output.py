import json

from pdman.output import (
    download_summary_payload,
    history_records_payload,
    header_probe_payload,
    network_error_payload,
    print_jsonl,
    run_detail_payload,
    resume_rejection_payload,
    task_result_payload,
    queue_add_payload,
    queue_clear_payload,
    queue_records_payload,
    queue_recover_payload,
    queue_remove_payload,
    queue_repair_payload,
    validation_report_payload,
)
from pdman.queue import QueueRecord, QueueValidationIssue, QueueValidationReport
from pdman.status import TaskReason, TaskResult, TaskStatus


def test_resume_rejection_payload_from_task_result():
    result = TaskResult(
        url="https://example.com/file.bin",
        filename="file.bin",
        status=TaskStatus.COMPLETED,
        resume_rejection_code="file_size_mismatch",
        resume_rejection_reason="Resume rejected [file_size_mismatch]: file_size mismatch",
    )

    assert resume_rejection_payload(result) == {
        "present": True,
        "code": "file_size_mismatch",
        "reason": "Resume rejected [file_size_mismatch]: file_size mismatch",
    }


def test_resume_rejection_payload_from_history_record_and_empty_record():
    record = {
        "resume_rejection_code": "url_mismatch",
        "resume_rejection_reason": "Resume rejected [url_mismatch]: url mismatch",
    }

    assert resume_rejection_payload(record) == {
        "present": True,
        "code": "url_mismatch",
        "reason": "Resume rejected [url_mismatch]: url mismatch",
    }
    assert resume_rejection_payload({}) == {
        "present": False,
        "code": None,
        "reason": None,
    }


def test_header_probe_payload_from_task_result_and_history_record():
    result = TaskResult(
        url="https://example.com/file.bin",
        filename="file.bin",
        status=TaskStatus.COMPLETED,
        header_probe_method="GET",
        header_probe_fallback_reason="head_http_405",
    )
    record = {
        "header_probe_method": "GET",
        "header_probe_fallback_reason": "head_connection_error",
    }

    assert header_probe_payload(result) == {
        "method": "GET",
        "fallback_used": True,
        "fallback_reason": "head_http_405",
    }
    assert header_probe_payload(record) == {
        "method": "GET",
        "fallback_used": True,
        "fallback_reason": "head_connection_error",
    }
    assert header_probe_payload({}) == {
        "method": None,
        "fallback_used": False,
        "fallback_reason": None,
    }



def test_network_error_payload_from_task_result_and_history_record():
    result = TaskResult(
        url="https://example.com/file.bin",
        filename="file.bin",
        status=TaskStatus.FAILED,
        network_error_phase="header_get_probe",
        network_error_kind="http_status",
        network_http_status=500,
    )
    record = {
        "network_error_phase": "header_head",
        "network_error_kind": "http_status",
        "network_http_status": 503,
    }

    assert network_error_payload(result) == {
        "present": True,
        "phase": "header_get_probe",
        "kind": "http_status",
        "http_status": 500,
    }
    assert network_error_payload(record) == {
        "present": True,
        "phase": "header_head",
        "kind": "http_status",
        "http_status": 503,
    }
    assert network_error_payload({}) == {
        "present": False,
        "phase": None,
        "kind": None,
        "http_status": None,
    }



def test_task_result_payload_includes_download_diagnostics():
    result = TaskResult(
        url="https://example.com/file.bin",
        filename="file.bin",
        status=TaskStatus.FAILED,
        reason="HTTP 503 during header check",
        reason_code=TaskReason.HTTP_STATUS,
        downloaded_bytes=128,
        total_bytes=1024,
        header_probe_method="GET",
        header_probe_fallback_reason="head_http_405",
        network_error_phase="header_get_probe",
        network_error_kind="http_status",
        network_http_status=503,
        resume_rejection_code="file_size_mismatch",
        resume_rejection_reason="Resume rejected [file_size_mismatch]: file_size mismatch",
    )

    payload = task_result_payload(result, task_id="abc123")

    assert payload["task_id"] == "abc123"
    assert payload["url"] == "https://example.com/file.bin"
    assert payload["filename"] == "file.bin"
    assert payload["status"] == "failed"
    assert payload["reason_code"] == "http_status"
    assert payload["reason"] == "HTTP 503 during header check"
    assert payload["downloaded_bytes"] == 128
    assert payload["total_bytes"] == 1024
    assert payload["resume_rejection"]["present"] is True
    assert payload["header_probe"]["fallback_used"] is True
    assert payload["network_error"]["http_status"] == 503


def test_download_summary_payload_includes_counts_and_tasks():
    results = [
        TaskResult(
            url="https://example.com/ok.bin",
            filename="ok.bin",
            status=TaskStatus.COMPLETED,
            reason="download completed",
            downloaded_bytes=1024,
            total_bytes=1024,
        ),
        TaskResult(
            url="https://example.com/bad.bin",
            filename="bad.bin",
            status=TaskStatus.FAILED,
            reason="HTTP 503 during header check",
            reason_code=TaskReason.HTTP_STATUS,
        ),
    ]

    payload = download_summary_payload(
        run_id="run-1",
        results=results,
        exit_code=1,
        task_id_for_url=lambda url: "id-" + url.rsplit("/", 1)[-1].split(".", 1)[0],
        started_at="2026-07-04T00:00:00Z",
        finished_at="2026-07-04T00:00:01Z",
    )

    assert payload["schema_version"] == 1
    assert payload["kind"] == "download_summary"
    assert payload["run_id"] == "run-1"
    assert payload["status"] == "failed"
    assert payload["exit_code"] == 1
    assert payload["started_at"] == "2026-07-04T00:00:00Z"
    assert payload["finished_at"] == "2026-07-04T00:00:01Z"
    assert payload["counts"] == {"completed": 1, "skipped": 0, "failed": 1}
    assert payload["tasks"][0]["task_id"] == "id-ok"
    assert payload["tasks"][0]["status"] == "completed"
    assert payload["tasks"][1]["task_id"] == "id-bad"
    assert payload["tasks"][1]["reason_code"] == "http_status"


def test_history_records_payload_includes_resume_diagnostics():
    records = [
        {
            "run_id": "run-1",
            "filename": "resumed.bin",
            "status": "completed",
            "downloaded_bytes": 1024,
            "resume_rejection_code": "file_size_mismatch",
            "resume_rejection_reason": "Resume rejected [file_size_mismatch]: file_size mismatch",
            "header_probe_method": "GET",
            "header_probe_fallback_reason": "head_http_405",
            "network_error_phase": "header_get_probe",
            "network_error_kind": "http_status",
            "network_http_status": 500,
        },
        {
            "run_id": "run-1",
            "filename": "fresh.bin",
            "status": "completed",
            "downloaded_bytes": 2048,
        },
    ]

    payload = history_records_payload(records)

    assert payload["count"] == 2
    assert payload["records"][0]["filename"] == "resumed.bin"
    assert payload["records"][0]["resume_rejection"] == {
        "present": True,
        "code": "file_size_mismatch",
        "reason": "Resume rejected [file_size_mismatch]: file_size mismatch",
    }
    assert payload["records"][0]["header_probe"] == {
        "method": "GET",
        "fallback_used": True,
        "fallback_reason": "head_http_405",
    }
    assert payload["records"][0]["network_error"] == {
        "present": True,
        "phase": "header_get_probe",
        "kind": "http_status",
        "http_status": 500,
    }
    assert payload["records"][1]["resume_rejection"] == {
        "present": False,
        "code": None,
        "reason": None,
    }
    assert payload["records"][1]["header_probe"] == {
        "method": None,
        "fallback_used": False,
        "fallback_reason": None,
    }
    assert payload["records"][1]["network_error"] == {
        "present": False,
        "phase": None,
        "kind": None,
        "http_status": None,
    }


def test_run_detail_payload_includes_resume_diagnostics():
    run = {
        "run_id": "run-1",
        "status": "finished",
        "task_counts": {"completed": 2, "skipped": 0, "failed": 0},
        "exit_code": 0,
    }
    tasks = [
        {
            "run_id": "run-1",
            "filename": "resumed.bin",
            "status": "completed",
            "downloaded_bytes": 1024,
            "resume_rejection_code": "url_mismatch",
            "resume_rejection_reason": "Resume rejected [url_mismatch]: url mismatch",
        },
        {
            "run_id": "run-1",
            "filename": "fresh.bin",
            "status": "completed",
            "downloaded_bytes": 2048,
        },
    ]

    payload = run_detail_payload(run, tasks)

    assert payload["run"] == run
    assert payload["task_count"] == 2
    assert payload["tasks"][0]["filename"] == "resumed.bin"
    assert payload["tasks"][0]["resume_rejection"] == {
        "present": True,
        "code": "url_mismatch",
        "reason": "Resume rejected [url_mismatch]: url mismatch",
    }
    assert payload["tasks"][1]["resume_rejection"] == {
        "present": False,
        "code": None,
        "reason": None,
    }


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


def test_queue_maintenance_payload_helpers():
    record = QueueRecord(queue_id="q1", url="https://example.com/file.bin")

    add_payload = queue_add_payload([record])

    assert add_payload["added"] == 1
    assert add_payload["count"] == 1
    assert add_payload["records"][0]["queue_id"] == "q1"
    assert queue_repair_payload({"kept": 1, "fixed": 0}) == {"kept": 1, "fixed": 0}
    assert queue_recover_payload(2) == {"recovered": 2}
    assert queue_remove_payload(["q1", "q2"], 1) == {
        "requested": ["q1", "q2"],
        "removed": 1,
    }
    assert queue_clear_payload(3, "completed", False) == {
        "cleared": 3,
        "status": "completed",
        "all": False,
    }


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
