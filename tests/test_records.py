import hashlib

from pdman.records import (
    format_record_show,
    format_records,
    format_records_metadata,
    metadata_locator,
    query_records,
    record_summary,
    records_metadata_payload,
    records_payload,
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
        },
        "dynamic_ranges": {
            "path": str(metadata_dir / "dynamic-ranges.json"),
            "exists": False,
            "source": "cache",
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
    }


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


def test_records_metadata_payload_skips_records_without_url(tmp_path):
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
    }
    assert payload["metadata"]["dynamic_ranges"] == {
        "path": str(ranges),
        "exists": True,
        "source": "cache",
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
