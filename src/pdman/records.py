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
    lines.append("  shared_payloads: metadata_locator, debug_action")
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
    "format_records_metadata",
    "format_records_schema",
    "metadata_locator",
    "query_records",
    "record_summary",
    "records_metadata_payload",
    "records_payload",
    "records_schema_payload",
    "records_show_payload",
    "suggested_debug_actions",
    "suggested_debug_commands",
    "unavailable_metadata_locator",
]
