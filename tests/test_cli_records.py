import hashlib
import json

import pytest

import pdman.cli as cli


class ExplodingManager:
    def __init__(self, *args, **kwargs):
        raise AssertionError("records query commands must not construct Manager")


def write_history(cache_dir, records):
    path = cache_dir / "history.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")


def test_cli_records_list_does_not_start_download_manager(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "Manager", ExplodingManager)
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": "https://example.com/ok.bin",
                "filename": "ok.bin",
                "status": "completed",
                "total_bytes": 1024,
                "finished_at": "2026-07-02T00:00:00Z",
            }
        ],
    )

    exit_code = cli.main(["records", "list", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Records:" in output
    assert "ok.bin" in output
    assert "run-1/task-1" in output


def test_cli_records_list_json_outputs_agent_summary(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": "https://example.com/bad.bin",
                "filename": "bad.bin",
                "target_path": "/downloads/bad.bin",
                "status": "failed",
                "reason": "HTTP 500 during header check",
                "total_bytes": 4096,
                "started_at": "2026-07-02T00:00:00Z",
                "finished_at": "2026-07-02T00:00:01Z",
                "header_probe_method": "GET",
                "header_probe_fallback_reason": "head_http_405",
                "network_error_phase": "header_get_probe",
                "network_error_kind": "http_status",
                "network_http_status": 500,
            }
        ],
    )

    exit_code = cli.main(["records", "list", "--json", "--cache-dir", str(tmp_path)])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["count"] == 1
    assert payload["records"][0] == {
        "run_id": "run-1",
        "task_id": "task-1",
        "url": "https://example.com/bad.bin",
        "filename": "bad.bin",
        "target_path": "/downloads/bad.bin",
        "status": "failed",
        "file_size": 4096,
        "created_at": "2026-07-02T00:00:00Z",
        "completed_at": "2026-07-02T00:00:01Z",
        "resume_rejection": {
            "present": False,
            "code": None,
            "reason": None,
        },
        "header_probe": {
            "method": "GET",
            "fallback_used": True,
            "fallback_reason": "head_http_405",
        },
        "network_error": {
            "present": True,
            "phase": "header_get_probe",
            "kind": "http_status",
            "http_status": 500,
        },
    }
    assert "reason" not in payload["records"][0]


def test_cli_records_list_jsonl_outputs_one_summary_per_line(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": "https://example.com/one.bin",
                "filename": "one.bin",
                "status": "completed",
            },
            {
                "run_id": "run-2",
                "task_id": "task-2",
                "url": "https://example.com/two.bin",
                "filename": "two.bin",
                "status": "failed",
            },
        ],
    )

    exit_code = cli.main(["records", "list", "--jsonl", "--cache-dir", str(tmp_path)])

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert len(lines) == 2
    assert json.loads(lines[0])["filename"] == "one.bin"
    assert json.loads(lines[1])["filename"] == "two.bin"


def test_cli_records_list_last_limits_recent_records(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {"run_id": "run-1", "task_id": "task-1", "filename": "old.bin", "status": "completed"},
            {"run_id": "run-2", "task_id": "task-2", "filename": "new.bin", "status": "completed"},
        ],
    )

    exit_code = cli.main(["records", "list", "--last", "1", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "new.bin" in output
    assert "old.bin" not in output


def test_cli_records_list_limit_overrides_last(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {"run_id": "run-1", "task_id": "task-1", "filename": "old.bin", "status": "completed"},
            {"run_id": "run-2", "task_id": "task-2", "filename": "new.bin", "status": "completed"},
        ],
    )

    exit_code = cli.main(
        ["records", "list", "--last", "1", "--limit", "0", "--cache-dir", str(tmp_path)]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "new.bin" in output
    assert "old.bin" in output


def test_cli_records_list_filters_status_url_target_and_run_id(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": "https://example.com/a.bin",
                "filename": "a.bin",
                "target_path": "/downloads/a.bin",
                "status": "completed",
            },
            {
                "run_id": "run-1",
                "task_id": "task-2",
                "url": "https://example.com/b.bin",
                "filename": "b.bin",
                "target_path": "/downloads/b.bin",
                "status": "failed",
            },
            {
                "run_id": "run-2",
                "task_id": "task-3",
                "url": "https://example.com/b.bin",
                "filename": "relative-b.bin",
                "target_path": "downloads/b.bin",
                "status": "failed",
            },
        ],
    )

    exit_code = cli.main(
        [
            "records",
            "list",
            "--status",
            "failed",
            "--url",
            "https://example.com/b.bin",
            "--target",
            "/downloads/b.bin",
            "--run-id",
            "run-1",
            "--json",
            "--cache-dir",
            str(tmp_path),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["count"] == 1
    assert payload["records"][0]["task_id"] == "task-2"
    assert payload["records"][0]["target_path"] == "/downloads/b.bin"


def test_cli_records_doctor_json_outputs_health_report(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "task_id": "task-1",
                "status": "failed",
            }
        ],
    )

    exit_code = cli.main(
        ["records", "doctor", "--json", "--cache-dir", str(tmp_path)]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["status"] == "warning"
    assert payload["records_checked"] == 1
    assert [issue["code"] for issue in payload["issues"]] == [
        "run_id_missing",
        "url_missing",
    ]
    assert [group["code"] for group in payload["issue_groups"]] == [
        "run_id_missing",
        "url_missing",
    ]
    assert payload["issue_groups"][0]["count"] == 1
    assert payload["issue_groups"][0]["sample_records"] == [
        {
            "run_id": None,
            "task_id": "task-1",
            "url": None,
            "target_path": None,
        }
    ]


def test_cli_records_doctor_jsonl_outputs_issues(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "status": "unknown",
            }
        ],
    )

    exit_code = cli.main(
        ["records", "doctor", "--jsonl", "--cache-dir", str(tmp_path)]
    )

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    issues = [json.loads(line) for line in lines]
    assert [issue["code"] for issue in issues] == [
        "invalid_status",
        "url_missing",
    ]
    forbidden_summary_keys = {
        "schema_version",
        "status",
        "records_checked",
        "issue_count",
        "total_issue_count",
        "filters",
        "status_counts",
        "metadata_state_counts",
        "issue_groups",
        "issues",
    }
    for issue in issues:
        assert forbidden_summary_keys.isdisjoint(issue)
        assert {
            "code",
            "severity",
            "message",
            "impact",
            "suggested_action",
            "run_id",
            "task_id",
            "url",
            "target_path",
        } <= set(issue)


def test_cli_records_doctor_jsonl_filters_issues(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "status": "unknown",
            }
        ],
    )

    exit_code = cli.main(
        [
            "records",
            "doctor",
            "--severity",
            "warning",
            "--code",
            "invalid_status",
            "--jsonl",
            "--cache-dir",
            str(tmp_path),
        ]
    )

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    issues = [json.loads(line) for line in lines]
    assert [issue["code"] for issue in issues] == ["invalid_status"]
    assert issues[0]["impact"] == (
        "Records filters and health summaries cannot classify this task reliably."
    )
    assert issues[0]["suggested_action"] == (
        "Inspect the source history record and confirm whether the status should be completed, skipped, or failed."
    )
    assert "issue_groups" not in issues[0]
    assert "status_counts" not in issues[0]
    assert "metadata_state_counts" not in issues[0]


def test_cli_records_doctor_filtered_status_controls_fail_on(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "task_id": "task-1",
                "status": "completed",
            }
        ],
    )

    exit_code = cli.main(
        [
            "records",
            "doctor",
            "--severity",
            "error",
            "--fail-on",
            "warning",
            "--json",
            "--cache-dir",
            str(tmp_path),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["issue_count"] == 0
    assert payload["total_issue_count"] == 2


def test_cli_records_doctor_fail_on_warning_returns_one(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "task_id": "task-1",
                "status": "failed",
            }
        ],
    )

    exit_code = cli.main(
        [
            "records",
            "doctor",
            "--fail-on",
            "warning",
            "--cache-dir",
            str(tmp_path),
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "status: warning" in output


def test_cli_records_doctor_fail_on_error_ignores_warnings(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "task_id": "task-1",
                "status": "failed",
            }
        ],
    )

    exit_code = cli.main(
        [
            "records",
            "doctor",
            "--fail-on",
            "error",
            "--json",
            "--cache-dir",
            str(tmp_path),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["status"] == "warning"


def test_cli_records_doctor_readable_outputs_summary(tmp_path, capsys):
    exit_code = cli.main(["records", "doctor", "--cache-dir", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Records doctor:" in output
    assert "status: ok" in output
    assert "issues: none" in output


def test_cli_records_doctor_and_schema_contract_smoke(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "status": "unknown",
            }
        ],
    )

    json_exit = cli.main(["records", "doctor", "--json", "--cache-dir", str(tmp_path)])
    doctor_payload = json.loads(capsys.readouterr().out)
    jsonl_exit = cli.main(["records", "doctor", "--jsonl", "--cache-dir", str(tmp_path)])
    doctor_issues = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    schema_exit = cli.main(["records", "schema", "--surface", "doctor", "--json"])
    schema_payload = json.loads(capsys.readouterr().out)

    assert json_exit == 0
    assert jsonl_exit == 0
    assert schema_exit == 0
    assert "issue_groups" in doctor_payload
    assert [issue["code"] for issue in doctor_issues] == [
        "invalid_status",
        "url_missing",
    ]
    assert all("issue_groups" not in issue for issue in doctor_issues)
    output_contract = schema_payload["commands"]["doctor"]["output_contract"]
    assert output_contract["json"] == "full doctor report with summary fields, issue_groups, and issues"
    assert output_contract["jsonl"] == "issue stream only; one doctor_issue per line; no summary or issue_groups fields"


def test_cli_records_schema_json_outputs_contract(capsys):
    exit_code = cli.main(["records", "schema", "--surface", "show", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["schema_version"] == 1
    assert payload["surface"] == "show"
    assert list(payload["commands"]) == ["show"]
    assert payload["commands"]["show"]["json_shape"]["suggested_debug"] == "list[debug_action]"


def test_cli_records_schema_doctor_json_outputs_output_contract(capsys):
    exit_code = cli.main(["records", "schema", "--surface", "doctor", "--json"])

    payload = json.loads(capsys.readouterr().out)
    contract = payload["commands"]["doctor"]["output_contract"]
    assert exit_code == 0
    assert payload["surface"] == "doctor"
    assert contract["json"] == "full doctor report with summary fields, issue_groups, and issues"
    assert contract["jsonl"] == "issue stream only; one doctor_issue per line; no summary or issue_groups fields"


def test_cli_records_schema_readable_outputs_summary(capsys):
    exit_code = cli.main(["records", "schema"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Records schema:" in output
    assert "doctor: introduced=0.8.7" in output
    assert "list: introduced=0.8.0" in output
    assert "metadata: introduced=0.8.2" in output
    assert "show: introduced=0.8.3" in output


def test_cli_records_schema_doctor_readable_outputs_output_contract(capsys):
    exit_code = cli.main(["records", "schema", "--surface", "doctor"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "output_contract:" in output
    assert "jsonl: issue stream only" in output
    assert "shared_payloads: metadata_locator, debug_action, doctor_issue, doctor_issue_group" in output


def test_cli_records_show_json_outputs_one_task(tmp_path, capsys):
    url = "https://example.com/file.bin"
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:6]
    metadata_dir = tmp_path / "metadata" / url_hash
    metadata_dir.mkdir(parents=True)
    resume = metadata_dir / "resume-metadata.json"
    resume.write_text("not json", encoding="utf-8")
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": url,
                "filename": "file.bin",
                "target_path": "/downloads/file.bin",
                "status": "failed",
                "reason": "HTTP 500 during header check",
                "reason_code": "http_status",
                "error": "HTTP 500",
                "total_bytes": 4096,
                "header_probe_method": "GET",
                "header_probe_fallback_reason": "head_http_405",
                "network_error_phase": "header_get_probe",
                "network_error_kind": "http_status",
                "network_http_status": 500,
            }
        ],
    )

    exit_code = cli.main(
        [
            "records",
            "show",
            "--run-id",
            "run-1",
            "--task-id",
            "task-1",
            "--json",
            "--cache-dir",
            str(tmp_path),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["run_id"] == "run-1"
    assert payload["task_id"] == "task-1"
    assert payload["status"] == "failed"
    assert payload["error"] == {
        "reason": "HTTP 500 during header check",
        "reason_code": "http_status",
        "error": "HTTP 500",
    }
    assert payload["metadata"]["resume"]["exists"] is True
    assert payload["suggested_debug"] == [
        {
            "kind": "resume_metadata",
            "metadata_key": "resume",
            "metadata_path": str(resume),
            "source": "cache",
            "reason": "metadata_exists",
            "argv": ["pdman", "debug", "resume", "--metadata", str(resume)],
            "command": f"pdman debug resume --metadata {resume}",
        }
    ]
    assert payload["suggested_commands"] == [
        f"pdman debug resume --metadata {resume}"
    ]


def test_cli_records_show_readable_output(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": "https://example.com/file.bin",
                "filename": "file.bin",
                "status": "completed",
            }
        ],
    )

    exit_code = cli.main(
        [
            "records",
            "show",
            "--run-id",
            "run-1",
            "--task-id",
            "task-1",
            "--cache-dir",
            str(tmp_path),
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Record: run-1/task-1" in output
    assert "status: completed" in output
    assert "Metadata:" in output
    assert "No debug metadata found." in output


def test_cli_records_show_missing_returns_one(tmp_path, capsys):
    exit_code = cli.main(
        [
            "records",
            "show",
            "--run-id",
            "run-1",
            "--task-id",
            "missing",
            "--cache-dir",
            str(tmp_path),
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Record not found: run-1/missing" in output


def test_cli_records_show_missing_json_returns_structured_error(tmp_path, capsys):
    exit_code = cli.main(
        [
            "records",
            "show",
            "--run-id",
            "run-1",
            "--task-id",
            "missing",
            "--json",
            "--cache-dir",
            str(tmp_path),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload == {
        "error": {
            "code": "record_not_found",
            "message": "Record not found: run-1/missing",
            "run_id": "run-1",
            "task_id": "missing",
        }
    }


def test_cli_records_metadata_json_locates_by_url(tmp_path, capsys):
    url = "https://example.com/file.bin"
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:6]
    metadata_dir = tmp_path / "metadata" / url_hash
    metadata_dir.mkdir(parents=True)
    (metadata_dir / "dynamic-ranges.json").write_text("not json", encoding="utf-8")
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": url,
                "target_path": "/downloads/file.bin",
                "status": "completed",
            }
        ],
    )

    exit_code = cli.main(
        ["records", "metadata", "--url", url, "--json", "--cache-dir", str(tmp_path)]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["query"] == {
        "url": url,
        "target_path": None,
        "run_id": None,
    }
    assert payload["count"] == 1
    assert payload["matches"][0]["run_id"] == "run-1"
    assert payload["matches"][0]["metadata"]["resume"] == {
        "path": str(metadata_dir / "resume-metadata.json"),
        "exists": False,
        "source": "cache",
        "status": "missing",
        "reason": "file_missing",
    }
    assert payload["matches"][0]["metadata"]["dynamic_ranges"] == {
        "path": str(metadata_dir / "dynamic-ranges.json"),
        "exists": True,
        "source": "cache",
        "status": "available",
        "reason": None,
    }
    assert payload["skipped"] == []
    assert payload["skipped_count"] == 0


def test_cli_records_metadata_jsonl_outputs_one_match_per_line(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": "https://example.com/a.bin",
                "target_path": "/downloads/a.bin",
                "status": "completed",
            },
            {
                "run_id": "run-1",
                "task_id": "task-2",
                "url": "https://example.com/b.bin",
                "target_path": "/downloads/b.bin",
                "status": "failed",
            },
        ],
    )

    exit_code = cli.main(
        ["records", "metadata", "--run-id", "run-1", "--jsonl", "--cache-dir", str(tmp_path)]
    )

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert len(lines) == 2
    assert json.loads(lines[0])["task_id"] == "task-1"
    assert json.loads(lines[1])["task_id"] == "task-2"


def test_cli_records_metadata_readable_locates_by_target(tmp_path, capsys):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": "https://example.com/file.bin",
                "target_path": "/downloads/file.bin",
                "status": "completed",
            }
        ],
    )

    exit_code = cli.main(
        [
            "records",
            "metadata",
            "--target",
            "/downloads/file.bin",
            "--cache-dir",
            str(tmp_path),
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Records metadata:" in output
    assert "query: target_path=/downloads/file.bin" in output
    assert "run-1/task-1" in output
    assert "resume: missing" in output


def test_cli_records_metadata_requires_one_query_selector():
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["records", "metadata"])

    assert exc_info.value.code == 2


def test_cli_records_requires_subcommand(capsys):
    exit_code = cli.main(["records"])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Records command required: doctor, list, metadata, schema, or show" in output
