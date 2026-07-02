#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
CLI 入口：提供命令行解析并调用 PDManager。
安装后可通过 console_scripts 生成可执行命令。
"""

import os
import sys
import argparse
import asyncio
from importlib.metadata import PackageNotFoundError, version as package_version

from .history import (
    format_history,
    format_run_detail,
    format_runs,
    list_runs,
    load_run,
    query_history,
)
from .manager import Manager
from .output import (
    history_records_payload,
    print_json,
    run_detail_payload,
    print_jsonl,
    queue_add_payload,
    queue_clear_payload,
    queue_records_payload,
    queue_recover_payload,
    queue_remove_payload,
    queue_repair_payload,
    validation_report_payload,
)
from .queue import append_queue, clear_queue, create_queue_records, finish_queue_records
from .queue import format_queue, format_queue_validation, load_queue, query_queue
from .queue import recover_running, remove_queue_records, repair_queue
from .queue import retry_failed_candidates, start_queue_records, validate_queue
from .records import (
    format_record_show,
    format_records,
    format_records_doctor,
    format_records_metadata,
    format_records_schema,
    query_records,
    records_doctor_exit_code,
    records_doctor_payload,
    records_metadata_payload,
    records_payload,
    records_schema_payload,
    records_show_payload,
)
from .range_metadata_inspect import (
    RangeMetadataError,
    find_latest_range_metadata_diagnostics,
    filter_ranges,
    format_range_metadata,
    load_range_metadata,
    range_metadata_summary,
)
from .resume_metadata import ResumeMetadataError, format_resume_rejection
from .resume_metadata_inspect import (
    find_latest_resume_metadata_diagnostics,
    format_resume_metadata_summary,
    resume_metadata_summary,
)
from .runtime import default_cache_root, default_system_tmp_root
from .task_input import TaskInput, load_task_input


def get_version() -> str:
    try:
        return package_version("pdman")
    except PackageNotFoundError:
        return "unknown"


def handle_history_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman history")
    parser.add_argument("--last", type=int, default=20)
    parser.add_argument(
        "--status",
        choices=("completed", "skipped", "failed"),
        default=None,
    )
    parser.add_argument("--failed", action="store_true")
    parser.add_argument("--run-id", default=None)
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--json", action="store_true")
    output_group.add_argument("--jsonl", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    status = "failed" if args.failed else args.status
    records = query_history(
        args.cache_dir,
        last=args.last,
        status=status,
        run_id=args.run_id,
    )
    payload = history_records_payload(records)
    if args.json:
        print_json(payload)
        return 0
    if args.jsonl:
        print_jsonl(payload["records"])
        return 0
    print(format_history(records))
    return 0


def handle_runs_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman runs")
    parser.add_argument("--last", type=int, default=20)
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    print(format_runs(list_runs(args.cache_dir, last=args.last)))
    return 0


def handle_run_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman run")
    parser.add_argument("run_id")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    run = load_run(args.run_id, args.cache_dir)
    if run is None:
        print(f"Run not found: {args.run_id}")
        return 1
    tasks = query_history(
        args.cache_dir,
        last=0,
        run_id=args.run_id,
    )
    if args.json:
        print_json(run_detail_payload(run, tasks))
        return 0
    print(format_run_detail(run, tasks))
    return 0


def handle_records_list_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman records list")
    parser.add_argument("--last", type=int, default=20)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--status",
        choices=("completed", "skipped", "failed"),
        default=None,
    )
    parser.add_argument("--url", default=None)
    parser.add_argument("--target", default=None)
    parser.add_argument("--run-id", default=None)
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--json", action="store_true")
    output_group.add_argument("--jsonl", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    limit = args.limit if args.limit is not None else args.last
    records = query_records(
        args.cache_dir,
        limit=limit,
        status=args.status,
        url=args.url,
        target=args.target,
        run_id=args.run_id,
    )
    payload = records_payload(records)
    if args.json:
        print_json(payload)
        return 0
    if args.jsonl:
        print_jsonl(payload["records"])
        return 0
    print(format_records(records))
    return 0


def handle_records_doctor_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman records doctor")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--fail-on",
        choices=("never", "warning", "error"),
        default="never",
    )
    parser.add_argument(
        "--severity",
        action="append",
        choices=("info", "warning", "error"),
        default=[],
    )
    parser.add_argument("--code", action="append", default=[])
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--json", action="store_true")
    output_group.add_argument("--jsonl", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    payload = records_doctor_payload(
        args.cache_dir,
        limit=args.limit,
        severities=set(args.severity) or None,
        codes=set(args.code) or None,
    )
    exit_code = records_doctor_exit_code(payload, args.fail_on)
    if args.json:
        print_json(payload)
        return exit_code
    if args.jsonl:
        print_jsonl(payload["issues"])
        return exit_code
    print(format_records_doctor(payload))
    return exit_code


def handle_records_schema_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman records schema")
    parser.add_argument(
        "--surface",
        choices=("all", "doctor", "list", "metadata", "show"),
        default="all",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    payload = records_schema_payload(surface=args.surface)
    if args.json:
        print_json(payload)
        return 0
    print(format_records_schema(payload))
    return 0


def handle_records_show_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman records show")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    payload = records_show_payload(
        args.cache_dir,
        run_id=args.run_id,
        task_id=args.task_id,
    )
    if payload is None:
        error_payload = {
            "error": {
                "code": "record_not_found",
                "message": f"Record not found: {args.run_id}/{args.task_id}",
                "run_id": args.run_id,
                "task_id": args.task_id,
            }
        }
        if args.json:
            print_json(error_payload)
            return 1
        print(error_payload["error"]["message"])
        return 1
    if args.json:
        print_json(payload)
        return 0
    print(format_record_show(payload))
    return 0


def handle_records_metadata_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman records metadata")
    query_group = parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument("--url", default=None)
    query_group.add_argument("--target", default=None)
    query_group.add_argument("--run-id", default=None)
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--json", action="store_true")
    output_group.add_argument("--jsonl", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    payload = records_metadata_payload(
        args.cache_dir,
        url=args.url,
        target=args.target,
        run_id=args.run_id,
    )
    if args.json:
        print_json(payload)
        return 0
    if args.jsonl:
        print_jsonl(payload["matches"])
        return 0
    print(format_records_metadata(payload))
    return 0


def handle_records_command(argv=None) -> int:
    argv = list(argv or [])
    if not argv:
        print("Records command required: doctor, list, metadata, schema, or show")
        return 1
    command, rest = argv[0], argv[1:]
    if command == "doctor":
        return handle_records_doctor_command(rest)
    if command == "list":
        return handle_records_list_command(rest)
    if command == "metadata":
        return handle_records_metadata_command(rest)
    if command == "schema":
        return handle_records_schema_command(rest)
    if command == "show":
        return handle_records_show_command(rest)
    print(f"Unknown records command: {command}")
    return 1


def handle_queue_add_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman queue add")
    parser.add_argument("urls", nargs="*")
    parser.add_argument("-i", "--input-file", action="append", default=[])
    parser.add_argument("-d", "--dir", dest="dir_path", default=None)
    parser.add_argument("--file-name", default=None)
    parser.add_argument("--md5", default=None)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)

    tasks = []
    for input_file in args.input_file:
        tasks.extend(load_task_input(input_file))
    for index, url in enumerate(args.urls):
        file_name = args.file_name if len(args.urls) == 1 and index == 0 else None
        tasks.append(
            TaskInput(
                url=url,
                file_name=file_name,
                dir_path=args.dir_path,
                md5=args.md5,
            )
        )
    if args.dir_path:
        for task in tasks:
            if task.dir_path is None:
                task.dir_path = args.dir_path
    records = create_queue_records(tasks)
    append_queue(records, args.cache_dir)
    if args.json:
        print_json(queue_add_payload(records))
    else:
        print(f"Added {len(records)} queue record(s).")
    return 0


def handle_queue_list_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman queue list")
    parser.add_argument("--status", choices=("pending", "running", "completed", "skipped", "failed"), default=None)
    parser.add_argument("--last", type=int, default=20)
    parser.add_argument("--attempts-ge", type=int, default=None)
    parser.add_argument("--attempts-lt", type=int, default=None)
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--json", action="store_true")
    output_group.add_argument("--jsonl", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    records = query_queue(
        args.cache_dir,
        status=args.status,
        last=args.last,
        attempts_ge=args.attempts_ge,
        attempts_lt=args.attempts_lt,
    )
    if args.json:
        print_json(queue_records_payload(records))
        return 0
    if args.jsonl:
        print_jsonl(queue_records_payload(records)["records"])
        return 0
    print(format_queue(records))
    return 0


def add_queue_run_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("-N", "--max-downloads", type=int, default=4)
    parser.add_argument("-x", "--max-concurrent-downloads", type=int, default=5)
    parser.add_argument("--tmp", default=None)
    parser.add_argument("--tmp-policy", choices=("auto", "system", "target"), default="auto")
    parser.add_argument("--keep-tmp", action="store_true")
    parser.add_argument("--segment-mode", choices=("static", "dynamic", "auto"), default="static")
    parser.add_argument("--retry", type=int, default=3)
    parser.add_argument("--retry-wait", type=int, default=5)


def add_retry_policy_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--max-attempts", type=int, default=None)
    parser.add_argument("--error-contains", default=None)


def run_queue_records(args, *, status: str, empty_message: str) -> int:
    manager = Manager(
        max_downloads=args.max_downloads,
        max_concurrent_downloads=args.max_concurrent_downloads,
        tmp_dir=args.tmp,
        tmp_policy=args.tmp_policy,
        cache_dir=args.cache_dir,
        keep_tmp=args.keep_tmp,
        retry=args.retry,
        segment_mode=args.segment_mode,
        retry_wait=args.retry_wait,
    )
    selected = start_queue_records(
        cache_dir=args.cache_dir,
        status=status,
        limit=args.limit,
        run_id=manager.run_id,
        max_attempts=getattr(args, "max_attempts", None),
        error_contains=getattr(args, "error_contains", None),
    )
    if not selected:
        print(empty_message)
        return 0
    for record in selected:
        manager.append(
            record.url,
            md5=record.md5,
            file_name=record.file_name,
            dir_path=record.dir_path or os.getcwd(),
        )
    asyncio.run(manager.download())
    finish_queue_records(
        cache_dir=args.cache_dir,
        selected_records=selected,
        results=manager.results,
        run_id=manager.run_id,
    )
    return manager.exit_code


def handle_queue_start_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman queue start")
    add_queue_run_args(parser)
    parser.add_argument("--status", choices=("pending", "failed"), default="pending")
    args = parser.parse_args(argv)
    return run_queue_records(
        args,
        status=args.status,
        empty_message="No queue records to start.",
    )


def handle_queue_retry_failed_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman queue retry-failed")
    add_queue_run_args(parser)
    add_retry_policy_args(parser)
    parser.add_argument("--dry-run", action="store_true")
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--json", action="store_true")
    output_group.add_argument("--jsonl", action="store_true")
    args = parser.parse_args(argv)
    if (args.json or args.jsonl) and not args.dry_run:
        print(
            "--json/--jsonl for retry-failed only supports preview mode. "
            "Use --dry-run with --json/--jsonl, or omit --json/--jsonl "
            "to execute retries with human-readable output."
        )
        return 1
    if args.dry_run:
        candidates = retry_failed_candidates(
            cache_dir=args.cache_dir,
            limit=args.limit,
            max_attempts=args.max_attempts,
            error_contains=args.error_contains,
        )
        payload = queue_records_payload(candidates, key="candidates")
        payload["dry_run"] = True
        if args.json:
            print_json(payload)
        elif args.jsonl:
            print_jsonl(payload["candidates"])
        elif not candidates:
            print("No failed queue records to retry.")
        else:
            print(format_queue(candidates, title="Retry candidates:"))
        return 0
    return run_queue_records(
        args,
        status="failed",
        empty_message="No failed queue records to retry.",
    )


def handle_queue_validate_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman queue validate")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    report = validate_queue(args.cache_dir)
    if args.json:
        print_json(validation_report_payload(report))
    else:
        print(format_queue_validation(report))
    return 0 if report.ok else 1


def handle_queue_repair_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman queue repair")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    stats = repair_queue(args.cache_dir)
    if args.json:
        print_json(queue_repair_payload(stats))
    else:
        print("Repaired queue:")
        for key in (
            "kept",
            "dropped_malformed",
            "dropped_invalid",
            "dropped_unsupported_schema",
            "fixed",
        ):
            print(f"  {key}: {stats[key]}")
    return 0


def handle_queue_recover_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman queue recover")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    recovered = recover_running(args.cache_dir)
    if args.json:
        print_json(queue_recover_payload(recovered))
    else:
        print(f"Recovered {recovered} running queue record(s).")
    return 0


def handle_queue_remove_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman queue remove")
    parser.add_argument("queue_ids", nargs="+")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    removed = remove_queue_records(args.queue_ids, args.cache_dir)
    if args.json:
        print_json(queue_remove_payload(args.queue_ids, removed))
    else:
        print(f"Removed {removed} queue record(s).")
    return 0


def handle_queue_clear_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman queue clear")
    parser.add_argument("--status", choices=("pending", "running", "completed", "skipped", "failed"), default=None)
    parser.add_argument("--all", dest="all_records", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--cache-dir", default=None)
    args = parser.parse_args(argv)
    if not args.all_records and args.status is None:
        print("queue clear requires --status STATUS or --all")
        return 1
    cleared = clear_queue(
        status=args.status,
        all_records=args.all_records,
        cache_dir=args.cache_dir,
    )
    if args.json:
        print_json(queue_clear_payload(cleared, args.status, args.all_records))
    else:
        print(f"Cleared {cleared} queue record(s).")
    return 0


def handle_queue_command(argv=None) -> int:
    argv = list(argv or [])
    if not argv:
        print("Queue command required: add, list, or start")
        return 1
    command, rest = argv[0], argv[1:]
    if command == "add":
        return handle_queue_add_command(rest)
    if command == "list":
        return handle_queue_list_command(rest)
    if command == "start":
        return handle_queue_start_command(rest)
    if command == "retry-failed":
        return handle_queue_retry_failed_command(rest)
    if command == "validate":
        return handle_queue_validate_command(rest)
    if command == "repair":
        return handle_queue_repair_command(rest)
    if command == "recover":
        return handle_queue_recover_command(rest)
    if command == "remove":
        return handle_queue_remove_command(rest)
    if command == "clear":
        return handle_queue_clear_command(rest)
    print(f"Unknown queue command: {command}")
    return 1


def _debug_range_search_roots(args) -> list[str]:
    if args.search_root:
        return list(args.search_root)
    cache_root = str(default_cache_root() if args.cache_dir is None else args.cache_dir)
    return [cache_root]


def _format_latest_search(search) -> str:
    lines = ["Latest search:"]
    for root in search.roots:
        lines.append(f"  root: {root}")
    lines.append(f"  valid: {search.valid_count}")
    lines.append(f"  skipped_invalid: {search.skipped_invalid_count}")
    return "\n".join(lines)


def _format_searched_roots(roots: list[str]) -> str:
    return "Searched:\n" + "\n".join(f"  {root}" for root in roots)


def handle_debug_ranges_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman debug ranges")
    parser.add_argument("metadata_file", nargs="?", default=None)
    parser.add_argument("--latest", action="store_true")
    parser.add_argument("--search-root", action="append", default=[])
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument(
        "--state",
        choices=("pending", "active", "completed", "failed", "unknown"),
        default=None,
    )
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--json", action="store_true")
    output_group.add_argument("--jsonl", action="store_true")
    args = parser.parse_args(argv)

    latest_search = None
    if args.latest:
        latest_search = find_latest_range_metadata_diagnostics(
            _debug_range_search_roots(args)
        )
        metadata_path = latest_search.selected_path
        if metadata_path is None:
            print("No dynamic range metadata found.")
            print(_format_searched_roots(latest_search.roots))
            return 1
    else:
        if args.metadata_file is None:
            print("metadata file required, or use --latest")
            return 1
        metadata_path = args.metadata_file

    try:
        payload = load_range_metadata(metadata_path)
    except RangeMetadataError as exc:
        print(f"Error: {exc}")
        return 1

    if args.json:
        json_payload = range_metadata_summary(payload, state=args.state)
        json_payload["source_path"] = str(metadata_path)
        print_json(json_payload)
        return 0
    if args.jsonl:
        print_jsonl(filter_ranges(payload, state=args.state))
        return 0
    if latest_search is not None:
        print(_format_latest_search(latest_search))
    print(format_range_metadata(payload, state=args.state, source_path=metadata_path))
    return 0


def handle_debug_resume_command(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pdman debug resume")
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument("--metadata", default=None)
    source_group.add_argument("--latest", action="store_true")
    parser.add_argument("--search-root", action="append", default=[])
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument(
        "--state",
        choices=("completed", "partial", "pending", "failed"),
        default=None,
    )
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--json", action="store_true")
    output_group.add_argument("--jsonl", action="store_true")
    args = parser.parse_args(argv)

    latest_search = None
    if args.latest:
        latest_search = find_latest_resume_metadata_diagnostics(
            _debug_range_search_roots(args)
        )
        metadata_path = latest_search.selected_path
        if metadata_path is None:
            print("No resume metadata found.")
            print(_format_searched_roots(latest_search.roots))
            return 1
    else:
        if args.metadata is None:
            print("--metadata is required, or use --latest")
            return 1
        metadata_path = args.metadata

    try:
        summary = resume_metadata_summary(metadata_path, state=args.state)
    except ResumeMetadataError as exc:
        print(f"Error: {format_resume_rejection(exc)}")
        return 1

    if args.json:
        print_json(summary)
        return 0
    if args.jsonl:
        print_jsonl(summary["segments"])
        return 0
    if latest_search is not None:
        print(_format_latest_search(latest_search))
    print(format_resume_metadata_summary(summary))
    return 0


def handle_debug_command(argv=None) -> int:
    argv = list(argv or [])
    if not argv:
        print("Debug command required: ranges or resume")
        return 1
    command, rest = argv[0], argv[1:]
    if command == "ranges":
        return handle_debug_ranges_command(rest)
    if command == "resume":
        return handle_debug_resume_command(rest)
    print(f"Unknown debug command: {command}")
    return 1


def handle_subcommand(argv=None) -> int:
    argv = list(argv or [])
    if not argv:
        return 1
    command, rest = argv[0], argv[1:]
    if command == "history":
        return handle_history_command(rest)
    if command == "runs":
        return handle_runs_command(rest)
    if command == "run":
        return handle_run_command(rest)
    if command == "queue":
        return handle_queue_command(rest)
    if command == "debug":
        return handle_debug_command(rest)
    if command == "records":
        return handle_records_command(rest)
    return 1


def main(argv=None):
    argv = list(argv) if argv is not None else sys.argv[1:]
    if argv and argv[0] in {"history", "runs", "run", "queue", "debug", "records"}:
        return handle_subcommand(argv)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"PythonDownloadManager(PDMAN) version {get_version()}",
        help="Print the version number and exit.",
    )
    parser.add_argument(
        "-l",
        "--log",
        type=str,
        required=False,
        default=None,
        help="The file name of the log file. If '-' is specified, log is written to stdout.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode with verbose logging.",
    )
    parser.add_argument(
        "-d",
        "--dir",
        type=str,
        default=os.path.join(os.getcwd(), "pdman"),
        help="The directory to store the downloaded file.",
    )
    parser.add_argument(
        "-o",
        "--out",
        type=str,
        default=None,
        help="The file name of the downloaded file. It is always relative to the directory given in -d option. When the -Z option is used, this option will be ignored.",
    )
    parser.add_argument(
        "-V",
        "--check-integrity",
        action="store_true",
        help="Check file integrity by validating piece hashes or a hash of the entire file.",
    )
    parser.add_argument(
        "-c",
        "--continue",
        dest="continue_download",
        action="store_true",
        help="Continue downloading a partially downloaded file.",
    )
    parser.add_argument(
        "-i",
        "--input-file",
        type=str,
        default=[],
        action="append",
        help="Downloads URIs found in FILE(s). Supports JSON, YAML, or plain text.",
    )
    parser.add_argument(
        "-x",
        "--max-concurrent-downloads",
        type=int,
        default=5,
        help="Set maximum number of parallel downloads for each URL or task.",
    )
    parser.add_argument(
        "--chunk-retry-speed",
        default="",
        help="If the chunk speed falls below SIZE bytes/second, restart that chunk. Append K/M.",
    )
    parser.add_argument(
        "-r",
        "--retry",
        type=int,
        default=3,
        help="Number of times to retry downloading a URL upon failure.",
    )
    parser.add_argument(
        "-W",
        "--retry-wait",
        type=int,
        default=5,
        help="Maximum wait time in seconds between retries.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout in seconds for each download request.",
    )
    parser.add_argument(
        "--chunk-timeout",
        type=int,
        default=None,
        help="Timeout in seconds for each chunk download request.",
    )
    parser.add_argument(
        "-N",
        "--max-downloads",
        type=int,
        default=4,
        help="The maximum number of concurrent downloads.",
    )
    parser.add_argument(
        "--no-auto-file-renaming",
        action="store_false",
        help="Disable auto renaming when target file exists.",
    )
    parser.add_argument(
        "-Z",
        "--force-sequential",
        action="store_true",
        help="Fetch URIs sequentially.",
    )
    parser.add_argument(
        "-k",
        "--min-split-size",
        type=str,
        default="1M",
        help="Minimum split size. Append K/M.",
    )
    parser.add_argument(
        "--tmp",
        type=str,
        default=None,
        help="Temporary directory for chunk files.",
    )
    parser.add_argument(
        "--tmp-policy",
        choices=("auto", "system", "target"),
        default="auto",
        help="Temporary directory policy. auto uses system tmp by default, target keeps legacy .pdman directories beside the output file.",
    )
    parser.add_argument(
        "--segment-mode",
        choices=("static", "dynamic", "auto"),
        default="static",
        help="Segmented download mode. static preserves legacy chunk slicing; dynamic uses the experimental range allocator; auto selects dynamic only when eligible.",
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default=None,
        help="Directory for pdman runtime metadata and history. Defaults to ~/.cache/pdman.",
    )
    parser.add_argument(
        "--keep-tmp",
        action="store_true",
        help="Keep runtime tmp files when a run fails or is interrupted.",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=4,
        help="Alias of max-downloads (deprecated).",
    )
    parser.add_argument(
        "-ua",
        "--user-agent",
        type=str,
        default="PDMAN-Downloader/1.0",
        help="The User-Agent string to use for HTTP requests.",
    )
    # === 认证与 Cookie ===
    parser.add_argument(
        "--http-auth",
        type=str,
        default=None,
        help="HTTP authentication credentials in user:pass format.",
    )
    parser.add_argument(
        "--cookie-file",
        type=str,
        default=None,
        help="Load cookies from a Netscape/Mozilla format file.",
    )
    # === 限速 ===
    parser.add_argument(
        "--max-download-limit",
        type=str,
        default=None,
        help="Max download speed per task (bytes/sec). Append K or M.",
    )
    parser.add_argument(
        "--max-overall-download-limit",
        type=str,
        default=None,
        help="Max overall download speed (bytes/sec). Append K or M.",
    )
    # === 代理 ===
    parser.add_argument(
        "--proxy",
        type=str,
        default=None,
        help="HTTP/HTTPS proxy URL (e.g., http://127.0.0.1:8080).",
    )
    parser.add_argument(
        "--proxy-auth",
        type=str,
        default=None,
        help="Proxy authentication credentials in user:pass format.",
    )
    # === 请求头与超时 ===
    parser.add_argument(
        "--header",
        type=str,
        default=[],
        action="append",
        help="Add custom HTTP header (Key: Value). Repeatable.",
    )
    parser.add_argument(
        "--connect-timeout",
        type=int,
        default=30,
        help="Connection timeout in seconds before skipping the URL.",
    )
    parser.add_argument(
        "--connect-progress-delay",
        type=float,
        default=5.0,
        help="Seconds to wait before showing an indeterminate connection progress indicator.",
    )
    parser.add_argument(
        "--max-connection-per-server",
        type=int,
        default=0,
        help="Max connections per server. 0 means unlimited.",
    )
    parser.add_argument(
        "--referer",
        type=str,
        default=None,
        help="Set HTTP Referer header.",
    )
    # === 回调 ===
    parser.add_argument(
        "--on-download-complete",
        type=str,
        default=None,
        help="Shell command to run after download completes. Supports {filename}/{filepath}/{url}/{dir}/{size} placeholders.",
    )
    # === SSL ===
    parser.add_argument(
        "--no-check-certificate",
        dest="check_certificate",
        action="store_false",
        help="Do not verify SSL certificates.",
    )
    parser.add_argument(
        "--ca-certificate",
        type=str,
        default=None,
        help="Path to custom CA certificate file.",
    )
    # === 其他 ===
    parser.add_argument(
        "--conf-path",
        type=str,
        default=None,
        help="Path to config file (JSON or YAML).",
    )
    parser.add_argument(
        "-q",
        "--quit",
        dest="quit_if_exists",
        action="store_true",
        help="Quit if the target file already exists.",
    )
    parser.add_argument(
        "--summary-interval",
        type=float,
        default=1.0,
        help="Progress summary output interval in seconds.",
    )
    parser.add_argument(
        "urls",
        type=str,
        nargs="*",
        default=None,
        help="The URL(s) to download.",
    )

    args = parser.parse_args(argv)
    if args.log == "-":
        args.log = sys.stdout
    if args.force_sequential and args.out is not None:
        args.out = None

    pdman = Manager(
        max_downloads=args.max_downloads,
        log_path=args.log,
        debug=args.debug,
        continue_download=args.continue_download,
        max_concurrent_downloads=args.max_concurrent_downloads,
        min_split_size=args.min_split_size,
        force_sequential=args.force_sequential,
        tmp_dir=args.tmp,
        tmp_policy=args.tmp_policy,
        cache_dir=args.cache_dir,
        keep_tmp=args.keep_tmp,
        check_integrity=args.check_integrity,
        segment_mode=args.segment_mode,
        user_agent=args.user_agent,
        chunk_retry_speed=args.chunk_retry_speed,
        retry=args.retry,
        retry_wait=args.retry_wait,
        timeout=args.timeout,
        chunk_timeout=args.chunk_timeout,
        auto_file_renaming=args.no_auto_file_renaming,
        out_dir=args.dir,
        # 认证与 Cookie
        http_auth=args.http_auth,
        cookie_file=args.cookie_file,
        # 限速
        max_download_limit=args.max_download_limit,
        max_overall_download_limit=args.max_overall_download_limit,
        # 代理
        proxy=args.proxy,
        proxy_auth=args.proxy_auth,
        # 请求头与超时
        headers=args.header if args.header else None,
        connect_timeout=args.connect_timeout,
        connect_progress_delay=args.connect_progress_delay,
        max_connection_per_server=args.max_connection_per_server,
        referer=args.referer,
        # 回调
        on_download_complete=args.on_download_complete,
        # SSL
        check_certificate=args.check_certificate,
        ca_certificate=args.ca_certificate,
        # 其他
        conf_path=args.conf_path,
        quit_if_exists=args.quit_if_exists,
        summary_interval=args.summary_interval,
    )

    if args.urls and len(args.urls) == 1 and args.out is not None:
        pdman.append(args.urls[0], file_name=args.out, dir_path=args.dir)
    else:
        if args.out is not None:
            pass  # ignore --out when multiple urls
        pdman.add_urls(args.urls or [])

    if args.input_file:
        for file in args.input_file:
            if os.path.exists(file):
                pdman.load_input_file(file)
    try:
        asyncio.run(pdman.download())
        return pdman.exit_code
    except KeyboardInterrupt:
        print("\033[31mDownload interrupted by user.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
