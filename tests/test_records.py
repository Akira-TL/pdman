import hashlib

from pdman.records import (
    format_record_show,
    format_records,
    format_records_doctor,
    format_records_metadata,
    format_records_schema,
    metadata_locator,
    query_records,
    record_summary,
    records_doctor_example_payload,
    records_doctor_exit_code,
    records_doctor_payload,
    records_metadata_payload,
    records_payload,
    records_schema_payload,
    records_show_payload,
    suggested_debug_actions,
    suggested_debug_commands,
)


def write_history(cache_dir, records):
    import json

    path = cache_dir / "history.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")


def test_record_summary_is_compact_agent_view():
    record = {
        "run_id": "run-1",
        "task_id": "task-1",
        "url": "https://example.com/file.bin",
        "filename": "file.bin",
        "target_path": "/downloads/file.bin",
        "status": "failed",
        "reason": "HTTP 500 during header check",
        "downloaded_bytes": 0,
        "total_bytes": 123,
        "started_at": "2026-07-02T00:00:00Z",
        "finished_at": "2026-07-02T00:00:01Z",
        "resume_rejection_code": "url_mismatch",
        "resume_rejection_reason": "Resume rejected [url_mismatch]: url mismatch",
        "header_probe_method": "GET",
        "header_probe_fallback_reason": "head_http_405",
        "network_error_phase": "header_get_probe",
        "network_error_kind": "http_status",
        "network_http_status": 500,
        "resume_metadata": {"must_not": "leak"},
    }

    summary = record_summary(record)

    assert summary == {
        "run_id": "run-1",
        "task_id": "task-1",
        "url": "https://example.com/file.bin",
        "filename": "file.bin",
        "target_path": "/downloads/file.bin",
        "status": "failed",
        "file_size": 123,
        "created_at": "2026-07-02T00:00:00Z",
        "completed_at": "2026-07-02T00:00:01Z",
        "resume_rejection": {
            "present": True,
            "code": "url_mismatch",
            "reason": "Resume rejected [url_mismatch]: url mismatch",
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
    assert "reason" not in summary
    assert "resume_metadata" not in summary


def test_records_payload_includes_count_and_summaries():
    payload = records_payload(
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": "https://example.com/file.bin",
                "filename": "file.bin",
                "status": "completed",
                "total_bytes": 2048,
            }
        ]
    )

    assert payload["count"] == 1
    assert payload["records"][0]["run_id"] == "run-1"
    assert payload["records"][0]["file_size"] == 2048
    assert payload["records"][0]["resume_rejection"] == {
        "present": False,
        "code": None,
        "reason": None,
    }


def test_query_records_reads_recent_history(tmp_path):
    write_history(
        tmp_path,
        [
            {"run_id": "run-1", "task_id": "task-1", "status": "completed"},
            {"run_id": "run-2", "task_id": "task-2", "status": "failed"},
        ],
    )

    records = query_records(str(tmp_path), limit=1)

    assert len(records) == 1
    assert records[0]["run_id"] == "run-2"


def test_query_records_filters_by_exact_status_url_target_and_run_id(tmp_path):
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
            {
                "run_id": "run-2",
                "task_id": "task-3",
                "url": "https://example.com/b.bin",
                "target_path": "downloads/b.bin",
                "status": "failed",
            },
        ],
    )

    records = query_records(
        str(tmp_path),
        limit=0,
        status="failed",
        url="https://example.com/b.bin",
        target="/downloads/b.bin",
        run_id="run-1",
    )

    assert [record["task_id"] for record in records] == ["task-2"]


def test_query_records_target_filter_can_match_filepath(tmp_path):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "filepath": "/downloads/file.bin",
                "status": "completed",
            }
        ],
    )

    records = query_records(str(tmp_path), target="/downloads/file.bin")

    assert len(records) == 1
    assert records[0]["task_id"] == "task-1"


def test_format_records_readable_summary():
    output = format_records(
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": "https://example.com/file.bin",
                "filename": "file.bin",
                "target_path": "/downloads/file.bin",
                "status": "failed",
                "total_bytes": 2048,
                "finished_at": "2026-07-02T00:00:01Z",
                "header_probe_method": "GET",
                "header_probe_fallback_reason": "head_http_405",
                "network_error_phase": "header_get_probe",
                "network_error_kind": "http_status",
                "network_http_status": 500,
            }
        ]
    )

    assert "Records:" in output
    assert "run-1/task-1" in output
    assert "file.bin" in output
    assert "size=2.0 KiB" in output
    assert "target=/downloads/file.bin" in output
    assert "probe=GET fallback=head_http_405" in output
    assert "network=header_get_probe/http_status/500" in output


def test_format_records_empty():
    assert format_records([]) == "No records found."


def test_metadata_locator_derives_cache_paths_without_reading_content(tmp_path):
    url = "https://example.com/file.bin"
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:6]
    metadata_dir = tmp_path / "metadata" / url_hash
    metadata_dir.mkdir(parents=True)
    (metadata_dir / "resume-metadata.json").write_text("not json", encoding="utf-8")

    locator = metadata_locator(str(tmp_path), url)

    assert locator == {
        "resume": {
            "path": str(metadata_dir / "resume-metadata.json"),
            "exists": True,
            "source": "cache",
            "status": "available",
            "reason": None,
        },
        "dynamic_ranges": {
            "path": str(metadata_dir / "dynamic-ranges.json"),
            "exists": False,
            "source": "cache",
            "status": "missing",
            "reason": "file_missing",
        },
    }


def test_records_metadata_payload_matches_url_history_records(tmp_path):
    url = "https://example.com/file.bin"
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:6]
    metadata_dir = tmp_path / "metadata" / url_hash
    metadata_dir.mkdir(parents=True)
    (metadata_dir / "resume-metadata.json").write_text("{}", encoding="utf-8")
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": url,
                "target_path": "/downloads/a.bin",
                "status": "completed",
            },
            {
                "run_id": "run-2",
                "task_id": "task-2",
                "url": url,
                "target_path": "/downloads/b.bin",
                "status": "failed",
            },
        ],
    )

    payload = records_metadata_payload(str(tmp_path), url=url)

    assert payload["query"] == {
        "url": url,
        "target_path": None,
        "run_id": None,
    }
    assert payload["count"] == 2
    assert [match["task_id"] for match in payload["matches"]] == ["task-1", "task-2"]
    assert payload["matches"][0]["metadata"]["resume"] == {
        "path": str(metadata_dir / "resume-metadata.json"),
        "exists": True,
        "source": "cache",
        "status": "available",
        "reason": None,
    }
    assert payload["skipped"] == []
    assert payload["skipped_count"] == 0


def test_records_metadata_payload_can_synthesize_url_only_locator(tmp_path):
    url = "https://example.com/missing-history.bin"

    payload = records_metadata_payload(str(tmp_path), url=url)

    assert payload["count"] == 1
    assert payload["matches"][0]["run_id"] is None
    assert payload["matches"][0]["task_id"] is None
    assert payload["matches"][0]["url"] == url
    assert payload["matches"][0]["metadata"]["resume"]["exists"] is False


def test_records_metadata_payload_filters_by_target_and_run_id(tmp_path):
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
                "run_id": "run-2",
                "task_id": "task-2",
                "url": "https://example.com/b.bin",
                "target_path": "/downloads/b.bin",
                "status": "failed",
            },
        ],
    )

    target_payload = records_metadata_payload(str(tmp_path), target="/downloads/b.bin")
    run_payload = records_metadata_payload(str(tmp_path), run_id="run-1")

    assert [match["task_id"] for match in target_payload["matches"]] == ["task-2"]
    assert [match["task_id"] for match in run_payload["matches"]] == ["task-1"]


def test_records_metadata_payload_reports_skipped_records_without_url(tmp_path):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "target_path": "/downloads/a.bin",
                "status": "completed",
            }
        ],
    )

    payload = records_metadata_payload(str(tmp_path), target="/downloads/a.bin")

    assert payload["count"] == 0
    assert payload["matches"] == []
    assert payload["skipped"] == [
        {
            "run_id": "run-1",
            "task_id": "task-1",
            "target_path": "/downloads/a.bin",
            "reason": "url_missing",
        }
    ]
    assert payload["skipped_count"] == 1


def test_format_records_metadata_readable_summary(tmp_path):
    payload = records_metadata_payload(
        str(tmp_path),
        url="https://example.com/file.bin",
    )

    output = format_records_metadata(payload)

    assert "Records metadata:" in output
    assert "query: url=https://example.com/file.bin" in output
    assert "count: 1" in output
    assert "-/- url=https://example.com/file.bin target=-" in output
    assert "resume: missing" in output
    assert "dynamic_ranges: missing" in output


def test_records_doctor_example_payload_ok_contract():
    payload = records_doctor_example_payload("ok")

    assert payload["schema_version"] == 1
    assert payload["status"] == "ok"
    assert payload["records_checked"] == 0
    assert payload["issue_count"] == 0
    assert payload["total_issue_count"] == 0
    assert payload["filters"] == {"severities": [], "codes": []}
    assert payload["issue_groups"] == []
    assert payload["issues"] == []


def test_records_doctor_example_payload_warning_grouped_contract():
    payload = records_doctor_example_payload("warning_grouped")

    assert payload["schema_version"] == 1
    assert payload["status"] == "warning"
    assert payload["records_checked"] == 2
    assert payload["issue_count"] == 3
    assert payload["total_issue_count"] == 3
    assert [issue["code"] for issue in payload["issues"]] == [
        "run_id_missing",
        "invalid_status",
        "url_missing",
    ]
    assert [group["code"] for group in payload["issue_groups"]] == [
        "run_id_missing",
        "invalid_status",
        "url_missing",
    ]
    assert set(payload["issue_groups"][0]["sample_records"][0]) == {
        "run_id",
        "task_id",
        "url",
        "target_path",
    }


def test_records_doctor_example_payload_filtered_warning_contract():
    payload = records_doctor_example_payload("filtered_warning")

    assert payload["status"] == "warning"
    assert payload["issue_count"] == 1
    assert payload["total_issue_count"] == 3
    assert payload["filters"] == {
        "severities": ["warning"],
        "codes": ["invalid_status"],
    }
    assert [issue["code"] for issue in payload["issues"]] == ["invalid_status"]
    assert [group["code"] for group in payload["issue_groups"]] == ["invalid_status"]


def test_records_doctor_example_payload_rejects_unknown_kind():
    try:
        records_doctor_example_payload("missing")
    except ValueError as exc:
        assert "Unknown records doctor example kind" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_records_doctor_payload_reports_history_and_metadata_health(tmp_path):
    url = "https://example.com/file.bin"
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:6]
    metadata_dir = tmp_path / "metadata" / url_hash
    metadata_dir.mkdir(parents=True)
    (metadata_dir / "resume-metadata.json").write_text("not json", encoding="utf-8")
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": url,
                "status": "completed",
            },
            {
                "task_id": "task-2",
                "target_path": "/downloads/missing-url.bin",
                "status": "unknown",
            },
        ],
    )

    payload = records_doctor_payload(str(tmp_path))

    assert payload["schema_version"] == 1
    assert payload["status"] == "warning"
    assert payload["records_checked"] == 2
    assert payload["status_counts"] == {
        "completed": 1,
        "skipped": 0,
        "failed": 0,
    }
    assert payload["metadata_state_counts"] == {
        "available": 1,
        "missing": 0,
        "unavailable": 1,
    }
    assert [issue["code"] for issue in payload["issues"]] == [
        "invalid_status",
        "run_id_missing",
        "url_missing",
    ]
    assert payload["issues"][0]["impact"] == (
        "Records filters and health summaries cannot classify this task reliably."
    )
    assert payload["issues"][0]["suggested_action"] == (
        "Inspect the source history record and confirm whether the status should be completed, skipped, or failed."
    )
    assert [group["code"] for group in payload["issue_groups"]] == [
        "invalid_status",
        "run_id_missing",
        "url_missing",
    ]
    assert payload["issue_groups"][0]["count"] == 1
    assert payload["issue_groups"][0]["sample_records"] == [
        {
            "run_id": None,
            "task_id": "task-2",
            "url": None,
            "target_path": "/downloads/missing-url.bin",
        }
    ]


def test_records_doctor_payload_filters_issues_by_severity_and_code(tmp_path):
    write_history(
        tmp_path,
        [
            {
                "status": "unknown",
            }
        ],
    )

    payload = records_doctor_payload(
        str(tmp_path),
        severities={"warning"},
        codes={"invalid_status"},
    )

    assert payload["status"] == "warning"
    assert payload["issue_count"] == 1
    assert payload["total_issue_count"] == 4
    assert payload["filters"] == {
        "severities": ["warning"],
        "codes": ["invalid_status"],
    }
    assert [issue["code"] for issue in payload["issues"]] == ["invalid_status"]
    assert [group["code"] for group in payload["issue_groups"]] == ["invalid_status"]
    assert payload["issue_groups"][0]["count"] == 1


def test_records_doctor_payload_filter_can_make_status_ok(tmp_path):
    write_history(
        tmp_path,
        [
            {
                "task_id": "task-1",
                "status": "completed",
            }
        ],
    )

    payload = records_doctor_payload(str(tmp_path), severities={"error"})

    assert payload["status"] == "ok"
    assert payload["issue_count"] == 0
    assert payload["total_issue_count"] == 2


def test_records_doctor_exit_code_honors_fail_on_thresholds(tmp_path):
    write_history(
        tmp_path,
        [
            {
                "task_id": "task-1",
                "status": "completed",
            }
        ],
    )
    payload = records_doctor_payload(str(tmp_path))

    assert payload["status"] == "warning"
    assert records_doctor_exit_code(payload, "never") == 0
    assert records_doctor_exit_code(payload, "warning") == 1
    assert records_doctor_exit_code(payload, "error") == 0


def test_records_doctor_payload_ok_for_empty_history(tmp_path):
    payload = records_doctor_payload(str(tmp_path))

    assert payload["status"] == "ok"
    assert payload["records_checked"] == 0
    assert payload["issues"] == []
    assert payload["issue_groups"] == []


def test_format_records_doctor_readable_summary(tmp_path):
    output = format_records_doctor(records_doctor_payload(str(tmp_path)))

    assert "Records doctor:" in output
    assert "status: ok" in output
    assert "records_checked: 0" in output
    assert "issues: none" in output


def test_format_records_doctor_readable_summary_includes_issue_groups(tmp_path):
    write_history(
        tmp_path,
        [
            {
                "status": "unknown",
            }
        ],
    )

    output = format_records_doctor(records_doctor_payload(str(tmp_path)))

    assert "issue_groups:" in output
    assert "warning invalid_status count=1" in output
    assert "warning run_id_missing count=1" in output
    assert "warning task_id_missing count=1" in output
    assert "info url_missing count=1" in output


def test_format_records_doctor_readable_output_order_is_stable(tmp_path):
    write_history(
        tmp_path,
        [
            {
                "status": "unknown",
            }
        ],
    )

    output = format_records_doctor(records_doctor_payload(str(tmp_path)))
    ordered_markers = [
        "  status:",
        "  records_checked:",
        "  issue_count:",
        "  status_counts:",
        "  metadata_state_counts:",
        "  issue_groups:",
        "  issues:",
    ]
    positions = [output.index(marker) for marker in ordered_markers]

    assert positions == sorted(positions)


def test_records_schema_payload_describes_all_records_surfaces():
    payload = records_schema_payload()

    assert payload["schema_version"] == 1
    assert payload["surface"] == "all"
    assert set(payload["commands"]) == {"doctor", "list", "metadata", "show"}
    assert payload["commands"]["list"]["json_shape"] == {
        "records": "list[record_summary]",
        "count": "int",
    }
    assert payload["commands"]["metadata"]["selector_mode"] == "exactly_one_required"
    assert payload["commands"]["doctor"]["examples"] == [
        "ok",
        "warning_grouped",
        "filtered_warning",
    ]
    assert payload["commands"]["doctor"]["json_shape"]["issue_groups"] == "list[doctor_issue_group]"
    assert payload["commands"]["doctor"]["json_shape"]["issues"] == "list[doctor_issue]"
    assert payload["commands"]["doctor"]["output_contract"] == {
        "readable": "human summary with stable order: summary, status_counts, metadata_state_counts, issue_groups, issues",
        "json": "full doctor report with summary fields, issue_groups, and issues",
        "jsonl": "issue stream only; one doctor_issue per line; no summary or issue_groups fields",
    }
    assert payload["shared_payloads"]["doctor_issue"] == [
        "code",
        "severity",
        "message",
        "impact",
        "suggested_action",
        "run_id",
        "task_id",
        "url",
        "target_path",
    ]
    assert payload["shared_payloads"]["doctor_issue_group"] == [
        "code",
        "severity",
        "count",
        "impact",
        "suggested_action",
        "sample_records",
    ]
    assert payload["commands"]["show"]["json_shape"]["suggested_debug"] == "list[debug_action]"
    assert payload["shared_payloads"]["metadata_locator"] == {
        "resume": ["path", "exists", "source", "status", "reason"],
        "dynamic_ranges": ["path", "exists", "source", "status", "reason"],
    }
    assert "database_index_engine" in payload["non_goals"]


def test_records_schema_payload_can_focus_one_surface():
    payload = records_schema_payload(surface="show")

    assert payload["surface"] == "show"
    assert list(payload["commands"]) == ["show"]
    assert payload["commands"]["show"]["selector_mode"] == "both_required"


def test_format_records_schema_readable_summary():
    output = format_records_schema(records_schema_payload(surface="metadata"))

    assert "Records schema:" in output
    assert "surface: metadata" in output
    assert "metadata: introduced=0.8.2" in output
    assert "shared_payloads: metadata_locator, debug_action, doctor_issue, doctor_issue_group" in output


def test_format_records_schema_readable_doctor_output_contract():
    output = format_records_schema(records_schema_payload(surface="doctor"))

    assert "doctor: introduced=0.8.7 outputs=readable, json, jsonl" in output
    assert "output_contract:" in output
    assert "readable: human summary with stable order" in output
    assert "json: full doctor report" in output
    assert "jsonl: issue stream only" in output


def test_suggested_debug_actions_include_argv_and_shell_command(tmp_path):
    resume = tmp_path / "space dir" / "resume-metadata.json"
    resume.parent.mkdir()
    resume.write_text("not json", encoding="utf-8")
    metadata = {
        "resume": {"path": str(resume), "exists": True, "source": "cache"},
        "dynamic_ranges": {
            "path": str(tmp_path / "dynamic-ranges.json"),
            "exists": False,
            "source": "cache",
        },
    }

    actions = suggested_debug_actions(metadata)

    assert actions == [
        {
            "kind": "resume_metadata",
            "metadata_key": "resume",
            "metadata_path": str(resume),
            "source": "cache",
            "reason": "metadata_exists",
            "argv": ["pdman", "debug", "resume", "--metadata", str(resume)],
            "command": f"pdman debug resume --metadata '{resume}'",
        }
    ]
    assert suggested_debug_commands(metadata) == [
        f"pdman debug resume --metadata '{resume}'"
    ]


def test_records_show_payload_returns_one_task_summary_with_metadata_and_next(tmp_path):
    url = "https://example.com/file.bin"
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:6]
    metadata_dir = tmp_path / "metadata" / url_hash
    metadata_dir.mkdir(parents=True)
    resume = metadata_dir / "resume-metadata.json"
    ranges = metadata_dir / "dynamic-ranges.json"
    resume.write_text("not json", encoding="utf-8")
    ranges.write_text("not json", encoding="utf-8")
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

    payload = records_show_payload(str(tmp_path), run_id="run-1", task_id="task-1")

    assert payload is not None
    assert payload["run_id"] == "run-1"
    assert payload["task_id"] == "task-1"
    assert payload["error"] == {
        "reason": "HTTP 500 during header check",
        "reason_code": "http_status",
        "error": "HTTP 500",
    }
    assert payload["metadata"]["resume"] == {
        "path": str(resume),
        "exists": True,
        "source": "cache",
        "status": "available",
        "reason": None,
    }
    assert payload["metadata"]["dynamic_ranges"] == {
        "path": str(ranges),
        "exists": True,
        "source": "cache",
        "status": "available",
        "reason": None,
    }
    assert payload["diagnostics"] == {
        "record_found": True,
        "url_present": True,
        "metadata_locator": "derived_from_url",
        "suggested_debug_count": 2,
    }
    assert payload["suggested_debug"] == [
        {
            "kind": "resume_metadata",
            "metadata_key": "resume",
            "metadata_path": str(resume),
            "source": "cache",
            "reason": "metadata_exists",
            "argv": ["pdman", "debug", "resume", "--metadata", str(resume)],
            "command": f"pdman debug resume --metadata {resume}",
        },
        {
            "kind": "dynamic_ranges",
            "metadata_key": "dynamic_ranges",
            "metadata_path": str(ranges),
            "source": "cache",
            "reason": "metadata_exists",
            "argv": ["pdman", "debug", "ranges", str(ranges)],
            "command": f"pdman debug ranges {ranges}",
        },
    ]
    assert payload["suggested_commands"] == [
        f"pdman debug resume --metadata {resume}",
        f"pdman debug ranges {ranges}",
    ]


def test_records_show_payload_stabilizes_metadata_when_url_is_missing(tmp_path):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "filename": "file.bin",
                "status": "failed",
            }
        ],
    )

    payload = records_show_payload(str(tmp_path), run_id="run-1", task_id="task-1")

    assert payload is not None
    assert payload["metadata"] == {
        "resume": {
            "path": None,
            "exists": False,
            "source": "cache",
            "status": "unavailable",
            "reason": "url_missing",
        },
        "dynamic_ranges": {
            "path": None,
            "exists": False,
            "source": "cache",
            "status": "unavailable",
            "reason": "url_missing",
        },
    }
    assert payload["diagnostics"] == {
        "record_found": True,
        "url_present": False,
        "metadata_locator": "unavailable_url_missing",
        "suggested_debug_count": 0,
    }
    assert payload["suggested_debug"] == []
    assert payload["suggested_commands"] == []


def test_records_show_payload_returns_none_for_missing_task(tmp_path):
    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": "https://example.com/file.bin",
                "status": "completed",
            }
        ],
    )

    assert records_show_payload(str(tmp_path), run_id="run-1", task_id="missing") is None


def test_format_record_show_readable_summary(tmp_path):
    payload = records_show_payload(
        str(tmp_path),
        run_id="run-1",
        task_id="task-1",
    )
    assert payload is None

    write_history(
        tmp_path,
        [
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "url": "https://example.com/file.bin",
                "filename": "file.bin",
                "target_path": "/downloads/file.bin",
                "status": "completed",
                "total_bytes": 2048,
            }
        ],
    )

    payload = records_show_payload(str(tmp_path), run_id="run-1", task_id="task-1")
    assert payload is not None
    output = format_record_show(payload)

    assert "Record: run-1/task-1" in output
    assert "status: completed" in output
    assert "url: https://example.com/file.bin" in output
    assert "file_size: 2.0 KiB" in output
    assert "Metadata:" in output
    assert "resume: missing" in output
    assert "Next:" in output
    assert "No debug metadata found." in output
    assert payload["suggested_debug"] == []
