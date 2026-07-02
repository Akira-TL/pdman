from __future__ import annotations

import hashlib
import shlex
from pathlib import Path
from typing import Any

from .history import VALID_STATUSES, cache_root, format_bytes, iter_history
from .range_metadata import DYNAMIC_RANGE_METADATA_FILENAME
from .resume_metadata import RESUME_METADATA_FILENAME
from .output import header_probe_payload, network_error_payload, resume_rejection_payload


def _first_present(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = record.get(key)
        if value is not None:
            return value
    return None


def record_summary(record: dict[str, Any]) -> dict[str, Any]:
    """Return the v0.8 records-layer summary for one history record.

    The records layer is an agent-oriented view over existing runtime history.
    It intentionally emits compact diagnostics and locators only; it does not
    expose full resume or dynamic range metadata content.
    """

    file_size = _first_present(record, "file_size", "total_bytes")
    return {
        "run_id": record.get("run_id"),
        "task_id": record.get("task_id"),
        "url": record.get("url"),
        "filename": record.get("filename"),
        "target_path": _first_present(record, "target_path", "filepath"),
        "status": record.get("status"),
        "file_size": file_size,
        "created_at": _first_present(record, "created_at", "started_at"),
        "completed_at": _first_present(record, "completed_at", "finished_at"),
        "resume_rejection": resume_rejection_payload(record),
        "header_probe": header_probe_payload(record),
        "network_error": network_error_payload(record),
    }


def records_payload(records: list[dict[str, Any]]) -> dict[str, Any]:
    serialized = [record_summary(record) for record in records]
    return {"records": serialized, "count": len(serialized)}


def _record_target(record: dict[str, Any]) -> Any:
    return _first_present(record, "target_path", "filepath")


def _metadata_dir_for_url(cache_dir: str | None, url: str) -> Path:
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:6]
    return cache_root(cache_dir) / "metadata" / url_hash


def _metadata_file_payload(path: Path) -> dict[str, Any]:
    exists = path.exists()
    return {
        "path": str(path),
        "exists": exists,
        "source": "cache",
        "status": "available" if exists else "missing",
        "reason": None if exists else "file_missing",
    }


def _unavailable_metadata_file_payload(reason: str) -> dict[str, Any]:
    return {
        "path": None,
        "exists": False,
        "source": "cache",
        "status": "unavailable",
        "reason": reason,
    }


def unavailable_metadata_locator(reason: str) -> dict[str, Any]:
    return {
        "resume": _unavailable_metadata_file_payload(reason),
        "dynamic_ranges": _unavailable_metadata_file_payload(reason),
    }


def metadata_locator(cache_dir: str | None, url: str) -> dict[str, Any]:
    metadata_dir = _metadata_dir_for_url(cache_dir, url)
    return {
        "resume": _metadata_file_payload(metadata_dir / RESUME_METADATA_FILENAME),
        "dynamic_ranges": _metadata_file_payload(
            metadata_dir / DYNAMIC_RANGE_METADATA_FILENAME
        ),
    }


def _apply_limit(records: list[dict[str, Any]], limit: int | None) -> list[dict[str, Any]]:
    if limit is not None and limit > 0:
        return records[-limit:]
    return records


def query_records(
    cache_dir: str | None = None,
    *,
    limit: int | None = 20,
    status: str | None = None,
    url: str | None = None,
    target: str | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    if status is not None and status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {status}")
    records = list(iter_history(cache_dir))
    if status is not None:
        records = [record for record in records if record.get("status") == status]
    if url is not None:
        records = [record for record in records if record.get("url") == url]
    if target is not None:
        records = [record for record in records if _record_target(record) == target]
    if run_id is not None:
        records = [record for record in records if record.get("run_id") == run_id]
    return _apply_limit(records, limit)


_DOCTOR_ISSUE_GUIDANCE = {
    "invalid_status": {
        "impact": "Records filters and health summaries cannot classify this task reliably.",
        "suggested_action": "Inspect the source history record and confirm whether the status should be completed, skipped, or failed.",
    },
    "run_id_missing": {
        "impact": "The record cannot be traced back to a stable run identity.",
        "suggested_action": "Use surrounding history context to identify the original run before relying on run-scoped records queries.",
    },
    "task_id_missing": {
        "impact": "The record cannot be addressed by records show or task-scoped agent workflows.",
        "suggested_action": "Inspect the source history record before using task-scoped records commands.",
    },
    "url_missing": {
        "impact": "Metadata locator and debug suggestions cannot be derived for this record.",
        "suggested_action": "Use target path or surrounding history context to identify the original URL before metadata inspection.",
    },
}


def _doctor_issue_guidance(code: str) -> dict[str, str]:
    return _DOCTOR_ISSUE_GUIDANCE.get(
        code,
        {
            "impact": "The records layer cannot fully interpret this history record.",
            "suggested_action": "Inspect the source history record before relying on automated records workflows.",
        },
    )


def _doctor_issue(
    record: dict[str, Any],
    *,
    code: str,
    severity: str,
    message: str,
) -> dict[str, Any]:
    guidance = _doctor_issue_guidance(code)
    return {
        "code": code,
        "severity": severity,
        "message": message,
        "impact": guidance["impact"],
        "suggested_action": guidance["suggested_action"],
        "run_id": record.get("run_id"),
        "task_id": record.get("task_id"),
        "url": record.get("url"),
        "target_path": _record_target(record),
    }


def _metadata_state_for_record(cache_dir: str | None, record: dict[str, Any]) -> str:
    url = record.get("url")
    if not isinstance(url, str) or not url:
        return "unavailable"
    locator = metadata_locator(cache_dir, url)
    if any(item.get("exists") for item in locator.values()):
        return "available"
    return "missing"


def _filter_doctor_issues(
    issues: list[dict[str, Any]],
    *,
    severities: set[str] | None = None,
    codes: set[str] | None = None,
) -> list[dict[str, Any]]:
    filtered = issues
    if severities:
        filtered = [issue for issue in filtered if issue.get("severity") in severities]
    if codes:
        filtered = [issue for issue in filtered if issue.get("code") in codes]
    return filtered


def _doctor_issue_sample(issue: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": issue.get("run_id"),
        "task_id": issue.get("task_id"),
        "url": issue.get("url"),
        "target_path": issue.get("target_path"),
    }


def _group_doctor_issues(
    issues: list[dict[str, Any]],
    *,
    sample_size: int = 3,
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for issue in issues:
        code = str(issue.get("code") or "unknown")
        if code not in groups:
            groups[code] = {
                "code": code,
                "severity": issue.get("severity") or "unknown",
                "count": 0,
                "impact": issue.get("impact"),
                "suggested_action": issue.get("suggested_action"),
                "sample_records": [],
            }
        group = groups[code]
        group["count"] += 1
        if len(group["sample_records"]) < sample_size:
            group["sample_records"].append(_doctor_issue_sample(issue))
    return list(groups.values())


def records_doctor_payload(
    cache_dir: str | None = None,
    *,
    limit: int | None = 0,
    severities: set[str] | None = None,
    codes: set[str] | None = None,
) -> dict[str, Any]:
    records = query_records(cache_dir, limit=limit)
    status_counts = {status: 0 for status in VALID_STATUSES}
    metadata_state_counts = {"available": 0, "missing": 0, "unavailable": 0}
    issues: list[dict[str, Any]] = []
    for record in records:
        status = record.get("status")
        if status in status_counts:
            status_counts[status] += 1
        else:
            issues.append(
                _doctor_issue(
                    record,
                    code="invalid_status",
                    severity="warning",
                    message="Record status is missing or not supported by the records schema.",
                )
            )
        if not record.get("run_id"):
            issues.append(
                _doctor_issue(
                    record,
                    code="run_id_missing",
                    severity="warning",
                    message="Record is missing run_id.",
                )
            )
        if not record.get("task_id"):
            issues.append(
                _doctor_issue(
                    record,
                    code="task_id_missing",
                    severity="warning",
                    message="Record is missing task_id.",
                )
            )
        if not isinstance(record.get("url"), str) or not record.get("url"):
            issues.append(
                _doctor_issue(
                    record,
                    code="url_missing",
                    severity="info",
                    message="Record is missing url, so metadata locator cannot be derived.",
                )
            )
        metadata_state_counts[_metadata_state_for_record(cache_dir, record)] += 1
    total_issue_count = len(issues)
    issues = _filter_doctor_issues(issues, severities=severities, codes=codes)
    issue_groups = _group_doctor_issues(issues)
    error_count = sum(1 for issue in issues if issue.get("severity") == "error")
    warning_count = sum(1 for issue in issues if issue.get("severity") == "warning")
    return {
        "schema_version": 1,
        "status": "error" if error_count else "warning" if warning_count else "ok",
        "records_checked": len(records),
        "issue_count": len(issues),
        "total_issue_count": total_issue_count,
        "filters": {
            "severities": sorted(severities) if severities else [],
            "codes": sorted(codes) if codes else [],
        },
        "warning_count": warning_count,
        "error_count": error_count,
        "status_counts": status_counts,
        "metadata_state_counts": metadata_state_counts,
        "issue_groups": issue_groups,
        "issues": issues,
    }


def _doctor_payload_from_parts(
    *,
    records_checked: int,
    status_counts: dict[str, int],
    metadata_state_counts: dict[str, int],
    issues: list[dict[str, Any]],
    total_issue_count: int | None = None,
    severities: set[str] | None = None,
    codes: set[str] | None = None,
) -> dict[str, Any]:
    if total_issue_count is None:
        total_issue_count = len(issues)
    issue_groups = _group_doctor_issues(issues)
    error_count = sum(1 for issue in issues if issue.get("severity") == "error")
    warning_count = sum(1 for issue in issues if issue.get("severity") == "warning")
    return {
        "schema_version": 1,
        "status": "error" if error_count else "warning" if warning_count else "ok",
        "records_checked": records_checked,
        "issue_count": len(issues),
        "total_issue_count": total_issue_count,
        "filters": {
            "severities": sorted(severities) if severities else [],
            "codes": sorted(codes) if codes else [],
        },
        "warning_count": warning_count,
        "error_count": error_count,
        "status_counts": status_counts,
        "metadata_state_counts": metadata_state_counts,
        "issue_groups": issue_groups,
        "issues": issues,
    }


def records_doctor_example_payload(kind: str = "warning_grouped") -> dict[str, Any]:
    status_counts = {status: 0 for status in VALID_STATUSES}
    empty_metadata_counts = {"available": 0, "missing": 0, "unavailable": 0}
    if kind == "ok":
        return _doctor_payload_from_parts(
            records_checked=0,
            status_counts=status_counts,
            metadata_state_counts=empty_metadata_counts,
            issues=[],
        )
    if kind == "warning_grouped":
        status_counts["completed"] = 1
        issues = [
            _doctor_issue(
                {
                    "task_id": "task-1",
                    "target_path": "/downloads/missing-run.bin",
                    "status": "completed",
                },
                code="run_id_missing",
                severity="warning",
                message="Record is missing run_id.",
            ),
            _doctor_issue(
                {
                    "run_id": "run-2",
                    "task_id": "task-2",
                    "target_path": "/downloads/unknown-status.bin",
                    "status": "unknown",
                },
                code="invalid_status",
                severity="warning",
                message="Record status is missing or not supported by the records schema.",
            ),
            _doctor_issue(
                {
                    "run_id": "run-2",
                    "task_id": "task-2",
                    "target_path": "/downloads/unknown-status.bin",
                    "status": "unknown",
                },
                code="url_missing",
                severity="info",
                message="Record is missing url, so metadata locator cannot be derived.",
            ),
        ]
        return _doctor_payload_from_parts(
            records_checked=2,
            status_counts=status_counts,
            metadata_state_counts={"available": 0, "missing": 0, "unavailable": 2},
            issues=issues,
        )
    if kind == "filtered_warning":
        full = records_doctor_example_payload("warning_grouped")
        severities = {"warning"}
        codes = {"invalid_status"}
        filtered = _filter_doctor_issues(
            full["issues"],
            severities=severities,
            codes=codes,
        )
        return _doctor_payload_from_parts(
            records_checked=full["records_checked"],
            status_counts=full["status_counts"],
            metadata_state_counts=full["metadata_state_counts"],
            issues=filtered,
            total_issue_count=full["total_issue_count"],
            severities=severities,
            codes=codes,
        )
    raise ValueError(f"Unknown records doctor example kind: {kind}")


def records_doctor_exit_code(payload: dict[str, Any], fail_on: str = "never") -> int:
    if fail_on == "never":
        return 0
    status = payload.get("status")
    if fail_on == "warning" and status in {"warning", "error"}:
        return 1
    if fail_on == "error" and status == "error":
        return 1
    return 0


def format_records_doctor(payload: dict[str, Any]) -> str:
    lines = [
        "Records doctor:",
        f"  status: {payload.get('status')}",
        f"  records_checked: {payload.get('records_checked')}",
        f"  issue_count: {payload.get('issue_count')}",
    ]
    status_counts = payload.get("status_counts") or {}
    metadata_counts = payload.get("metadata_state_counts") or {}
    lines.append(
        "  status_counts: "
        + ", ".join(f"{key}={value}" for key, value in status_counts.items())
    )
    lines.append(
        "  metadata_state_counts: "
        + ", ".join(f"{key}={value}" for key, value in metadata_counts.items())
    )
    groups = payload.get("issue_groups") or []
    if groups:
        lines.append("  issue_groups:")
        for group in groups:
            lines.append(
                f"    {group.get('severity')} {group.get('code')} count={group.get('count')}"
            )
    issues = payload.get("issues") or []
    if not issues:
        lines.append("  issues: none")
        return "\n".join(lines)
    lines.append("  issues:")
    for issue in issues:
        identity = f"{issue.get('run_id') or '-'}/{issue.get('task_id') or '-'}"
        lines.append(
            f"    {issue.get('severity')} {issue.get('code')} {identity}: "
            f"{issue.get('message')}"
        )
    return "\n".join(lines)


def records_schema_payload(surface: str = "all") -> dict[str, Any]:
    commands = {
        "list": {
            "introduced_in": "0.8.0",
            "outputs": ["readable", "json", "jsonl"],
            "filters": {
                "status": ["completed", "skipped", "failed"],
                "url": "exact",
                "target": "exact_target_path_or_filepath",
                "run_id": "exact",
                "limit": "recent_count_after_filters; zero means unlimited",
                "last": "compatibility alias for limit",
            },
            "json_shape": {
                "records": "list[record_summary]",
                "count": "int",
            },
            "record_summary_fields": list(record_summary({}).keys()),
        },
        "metadata": {
            "introduced_in": "0.8.2",
            "outputs": ["readable", "json", "jsonl"],
            "selectors": ["url", "target", "run_id"],
            "selector_mode": "exactly_one_required",
            "json_shape": {
                "query": {
                    "url": "str|null",
                    "target_path": "str|null",
                    "run_id": "str|null",
                },
                "matches": "list[metadata_match]",
                "count": "int",
            },
            "metadata_match_fields": [
                "run_id",
                "task_id",
                "url",
                "target_path",
                "metadata",
            ],
        },
        "doctor": {
            "introduced_in": "0.8.7",
            "outputs": ["readable", "json", "jsonl"],
            "filters": {
                "limit": "recent_count; zero means unlimited",
                "fail_on": "never|warning|error; controls CLI exit code only",
                "severity": "info|warning|error; repeatable issue filter",
                "code": "exact issue code; repeatable issue filter",
            },
            "json_shape": {
                "schema_version": "int",
                "status": "ok|warning|error",
                "records_checked": "int",
                "issue_count": "int",
                "total_issue_count": "int",
                "filters": "active issue filters",
                "status_counts": "dict[str,int]",
                "metadata_state_counts": "dict[str,int]",
                "issue_groups": "list[doctor_issue_group]",
                "issues": "list[doctor_issue]",
            },
            "examples": ["ok", "warning_grouped", "filtered_warning"],
        },
        "show": {
            "introduced_in": "0.8.3",
            "outputs": ["readable", "json"],
            "selectors": ["run_id", "task_id"],
            "selector_mode": "both_required",
            "json_shape": {
                "record_summary": "record_summary fields at top level",
                "error": ["reason", "reason_code", "error"],
                "metadata": "metadata_locator",
                "suggested_debug": "list[debug_action]",
                "suggested_commands": "list[str]; compatibility shell commands",
            },
        },
    }
    if surface != "all":
        commands = {surface: commands[surface]}
    return {
        "schema_version": 1,
        "surface": surface,
        "commands": commands,
        "shared_payloads": {
            "metadata_locator": {
                "resume": ["path", "exists", "source", "status", "reason"],
                "dynamic_ranges": ["path", "exists", "source", "status", "reason"],
            },
            "debug_action": [
                "kind",
                "metadata_key",
                "metadata_path",
                "source",
                "reason",
                "argv",
                "command",
            ],
            "doctor_issue": [
                "code",
                "severity",
                "message",
                "impact",
                "suggested_action",
                "run_id",
                "task_id",
                "url",
                "target_path",
            ],
            "doctor_issue_group": [
                "code",
                "severity",
                "count",
                "impact",
                "suggested_action",
                "sample_records",
            ],
        },
        "non_goals": [
            "database_index_engine",
            "full_metadata_embedding",
            "metadata_validation",
            "metadata_repair",
            "download_or_queue_mutation",
        ],
    }


def format_records_schema(payload: dict[str, Any]) -> str:
    lines = [
        "Records schema:",
        f"  schema_version: {payload.get('schema_version')}",
        f"  surface: {payload.get('surface')}",
        "  commands:",
    ]
    for name, contract in (payload.get("commands") or {}).items():
        outputs = ", ".join(contract.get("outputs") or [])
        introduced = contract.get("introduced_in") or "-"
        lines.append(f"    {name}: introduced={introduced} outputs={outputs}")
    lines.append("  shared_payloads: metadata_locator, debug_action, doctor_issue")
    non_goals = ", ".join(payload.get("non_goals") or [])
    lines.append(f"  non_goals: {non_goals}")
    return "\n".join(lines)


def _query_metadata_records(
    cache_dir: str | None,
    *,
    url: str | None = None,
    target: str | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    return query_records(
        cache_dir,
        limit=0,
        url=url,
        target=target,
        run_id=run_id,
    )


def _metadata_match(
    record: dict[str, Any],
    cache_dir: str | None,
    *,
    url_override: str | None = None,
) -> dict[str, Any] | None:
    url = url_override or record.get("url")
    if not isinstance(url, str) or not url:
        return None
    return {
        "run_id": record.get("run_id"),
        "task_id": record.get("task_id"),
        "url": url,
        "target_path": _record_target(record),
        "metadata": metadata_locator(cache_dir, url),
    }


def _metadata_skip(record: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "run_id": record.get("run_id"),
        "task_id": record.get("task_id"),
        "target_path": _record_target(record),
        "reason": reason,
    }


def records_metadata_payload(
    cache_dir: str | None = None,
    *,
    url: str | None = None,
    target: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    query = {
        "url": url,
        "target_path": target,
        "run_id": run_id,
    }
    records = _query_metadata_records(
        cache_dir,
        url=url,
        target=target,
        run_id=run_id,
    )
    matches: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for record in records:
        match = _metadata_match(record, cache_dir)
        if match is not None:
            matches.append(match)
        else:
            skipped.append(_metadata_skip(record, "url_missing"))
    if url is not None and not matches:
        synthetic = _metadata_match({}, cache_dir, url_override=url)
        if synthetic is not None:
            matches.append(synthetic)
    return {
        "query": query,
        "matches": matches,
        "count": len(matches),
        "skipped": skipped,
        "skipped_count": len(skipped),
    }


def _shell_command(argv: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in argv)


def _debug_action_for_metadata(
    label: str,
    item: dict[str, Any],
) -> dict[str, Any] | None:
    path = item.get("path")
    if not item.get("exists") or not isinstance(path, str) or not path:
        return None
    if label == "resume":
        argv = ["pdman", "debug", "resume", "--metadata", path]
        kind = "resume_metadata"
    elif label == "dynamic_ranges":
        argv = ["pdman", "debug", "ranges", path]
        kind = "dynamic_ranges"
    else:
        return None
    return {
        "kind": kind,
        "metadata_key": label,
        "metadata_path": path,
        "source": item.get("source") or "cache",
        "reason": "metadata_exists",
        "argv": argv,
        "command": _shell_command(argv),
    }


def suggested_debug_actions(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for label in ("resume", "dynamic_ranges"):
        action = _debug_action_for_metadata(label, metadata.get(label) or {})
        if action is not None:
            actions.append(action)
    return actions


def suggested_debug_commands(metadata: dict[str, Any]) -> list[str]:
    return [action["command"] for action in suggested_debug_actions(metadata)]


def _record_error_payload(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "reason": record.get("reason"),
        "reason_code": record.get("reason_code"),
        "error": record.get("error"),
    }


def records_show_payload(
    cache_dir: str | None,
    *,
    run_id: str,
    task_id: str,
) -> dict[str, Any] | None:
    matches = [
        record
        for record in query_records(cache_dir, limit=0, run_id=run_id)
        if record.get("task_id") == task_id
    ]
    if not matches:
        return None
    record = matches[-1]
    payload = record_summary(record)
    payload["error"] = _record_error_payload(record)
    url = record.get("url")
    url_present = isinstance(url, str) and bool(url)
    metadata = metadata_locator(cache_dir, url) if url_present else unavailable_metadata_locator("url_missing")
    actions = suggested_debug_actions(metadata)
    payload["metadata"] = metadata
    payload["suggested_debug"] = actions
    payload["suggested_commands"] = [action["command"] for action in actions]
    payload["diagnostics"] = {
        "record_found": True,
        "url_present": url_present,
        "metadata_locator": "derived_from_url" if url_present else "unavailable_url_missing",
        "suggested_debug_count": len(actions),
    }
    return payload


def _format_metadata_file(label: str, item: dict[str, Any]) -> str:
    state = "exists" if item.get("exists") else "missing"
    return f"    {label}: {state} {item.get('path') or '-'}"


def format_records_metadata(payload: dict[str, Any]) -> str:
    query = payload.get("query") or {}
    query_parts = [
        f"{key}={value}"
        for key, value in query.items()
        if value is not None
    ]
    query_text = " ".join(query_parts) if query_parts else "-"
    lines = ["Records metadata:", f"  query: {query_text}", f"  count: {payload.get('count', 0)}"]
    matches = payload.get("matches") or []
    if not matches:
        return "\n".join(lines)
    for match in matches:
        run_id = match.get("run_id") or "-"
        task_id = match.get("task_id") or "-"
        url = match.get("url") or "-"
        target = match.get("target_path") or "-"
        lines.append(f"  {run_id}/{task_id} url={url} target={target}")
        metadata = match.get("metadata") or {}
        lines.append(_format_metadata_file("resume", metadata.get("resume") or {}))
        lines.append(
            _format_metadata_file(
                "dynamic_ranges", metadata.get("dynamic_ranges") or {}
            )
        )
    return "\n".join(lines)


def format_record_show(payload: dict[str, Any]) -> str:
    identity = f"{payload.get('run_id') or '-'}/{payload.get('task_id') or '-'}"
    lines = [
        f"Record: {identity}",
        f"  status: {payload.get('status') or '-'}",
        f"  url: {payload.get('url') or '-'}",
        f"  filename: {payload.get('filename') or '-'}",
        f"  target: {payload.get('target_path') or '-'}",
        f"  file_size: {format_bytes(payload.get('file_size'))}",
        f"  created_at: {payload.get('created_at') or '-'}",
        f"  completed_at: {payload.get('completed_at') or '-'}",
    ]
    error = payload.get("error") or {}
    if any(error.get(key) for key in ("reason", "reason_code", "error")):
        lines.extend(
            [
                "",
                "Error:",
                f"  reason: {error.get('reason') or '-'}",
                f"  reason_code: {error.get('reason_code') or '-'}",
                f"  error: {error.get('error') or '-'}",
            ]
        )
    resume = payload.get("resume_rejection") or {}
    lines.extend(
        [
            "",
            "Resume:",
            f"  present: {resume.get('present', False)}",
            f"  code: {resume.get('code') or '-'}",
            f"  reason: {resume.get('reason') or '-'}",
        ]
    )
    probe = payload.get("header_probe") or {}
    lines.extend(
        [
            "",
            "Probe:",
            f"  method: {probe.get('method') or '-'}",
            f"  fallback_used: {probe.get('fallback_used', False)}",
            f"  fallback_reason: {probe.get('fallback_reason') or '-'}",
        ]
    )
    network = payload.get("network_error") or {}
    lines.extend(
        [
            "",
            "Network:",
            f"  present: {network.get('present', False)}",
            f"  phase: {network.get('phase') or '-'}",
            f"  kind: {network.get('kind') or '-'}",
            f"  http_status: {network.get('http_status') if network.get('http_status') is not None else '-'}",
        ]
    )
    metadata = payload.get("metadata") or {}
    lines.extend(
        [
            "",
            "Metadata:",
            _format_metadata_file("resume", metadata.get("resume") or {}),
            _format_metadata_file("dynamic_ranges", metadata.get("dynamic_ranges") or {}),
        ]
    )
    commands = payload.get("suggested_commands") or []
    lines.extend(["", "Next:"])
    if commands:
        lines.extend(f"  {command}" for command in commands)
    else:
        lines.append("  No debug metadata found.")
    return "\n".join(lines)


def _record_name(record: dict[str, Any]) -> str:
    return str(record.get("filename") or record.get("url") or "-")


def _record_identity(record: dict[str, Any]) -> str:
    run_id = record.get("run_id") or "-"
    task_id = record.get("task_id") or "-"
    return f"{run_id}/{task_id}"


def _diagnostic_suffix(record: dict[str, Any]) -> str:
    parts: list[str] = []
    resume = record.get("resume_rejection") or {}
    if resume.get("present"):
        parts.append(f"resume={resume.get('code') or '-'}")
    probe = record.get("header_probe") or {}
    if probe.get("fallback_used"):
        parts.append(
            f"probe={probe.get('method') or '-'} fallback={probe.get('fallback_reason') or '-'}"
        )
    network = record.get("network_error") or {}
    if network.get("present"):
        network_text = f"network={network.get('phase') or '-'}/{network.get('kind') or '-'}"
        if network.get("http_status") is not None:
            network_text += f"/{network.get('http_status')}"
        parts.append(network_text)
    return " " + " ".join(parts) if parts else ""


def format_records(records: list[dict[str, Any]]) -> str:
    if not records:
        return "No records found."
    payload = records_payload(records)
    lines = ["Records:"]
    for record in payload["records"]:
        timestamp = record.get("completed_at") or record.get("created_at") or "-"
        status = str(record.get("status") or "unknown")
        name = _record_name(record)
        size = format_bytes(record.get("file_size"))
        target = record.get("target_path") or "-"
        identity = _record_identity(record)
        lines.append(
            f"  {timestamp} {status:<9} {identity} {name} size={size} target={target}"
            f"{_diagnostic_suffix(record)}"
        )
    return "\n".join(lines)


__all__ = [
    "format_record_show",
    "format_records",
    "format_records_doctor",
    "format_records_metadata",
    "format_records_schema",
    "metadata_locator",
    "query_records",
    "record_summary",
    "records_doctor_example_payload",
    "records_doctor_exit_code",
    "records_doctor_payload",
    "records_metadata_payload",
    "records_payload",
    "records_schema_payload",
    "records_show_payload",
    "suggested_debug_actions",
    "suggested_debug_commands",
    "unavailable_metadata_locator",
]
