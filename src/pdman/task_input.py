from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

import yaml


_TASK_FIELDS = {"url", "file_name", "dir_path", "md5", "log_path"}
_RESERVED_SCHEMA_V2_FIELDS = {"version", "defaults", "groups", "tasks"}


class TaskInputError(ValueError):
    def __init__(self, code: str, message: str, *, field: str | None = None):
        self.code = code
        self.message = message
        self.field = field
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        payload = {"code": self.code, "message": self.message}
        if self.field is not None:
            payload["field"] = self.field
        return payload


def task_input_error_payload(exc: ValueError) -> dict[str, Any]:
    if isinstance(exc, TaskInputError):
        return exc.to_dict()
    return {"code": "task_input_invalid", "message": str(exc)}


def format_task_input_error(exc: ValueError) -> str:
    payload = task_input_error_payload(exc)
    return f"[{payload['code']}] {payload['message']}"


def _task_input_error(code: str, message: str, *, field: str | None = None) -> TaskInputError:
    return TaskInputError(code, message, field=field)


@dataclass
class TaskInput:
    url: str
    file_name: str | None = None
    dir_path: str | None = None
    md5: str | None = None
    log_path: str | None = None
    group: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "url": self.url,
            "file_name": self.file_name,
            "dir_path": self.dir_path,
            "md5": self.md5,
            "log_path": self.log_path,
        }
        if self.group is not None:
            payload["group"] = self.group
        if self.options:
            payload["options"] = self.options
        return payload


def _task_from_mapping(data: dict[str, Any], *, group: str | None = None) -> TaskInput:
    url = data.get("url")
    if not url:
        raise _task_input_error(
            "task_mapping_url_missing",
            "Task mapping in a list must include a url field",
            field="url",
        )
    options = {
        key: value
        for key, value in data.items()
        if key not in _TASK_FIELDS and key not in _RESERVED_SCHEMA_V2_FIELDS
    }
    return TaskInput(
        url=str(url),
        file_name=data.get("file_name"),
        dir_path=data.get("dir_path"),
        md5=data.get("md5"),
        log_path=data.get("log_path"),
        group=group,
        options=options,
    )


def _from_mapping(data: dict[str, Any]) -> list[TaskInput]:
    tasks: list[TaskInput] = []
    for url, value in data.items():
        if value is None:
            value = {}
        if not isinstance(value, dict):
            raise _task_input_error(
                "task_options_not_mapping",
                f"Task options for {url} must be a mapping",
            )
        options = {
            key: option_value
            for key, option_value in value.items()
            if key not in _TASK_FIELDS and key not in _RESERVED_SCHEMA_V2_FIELDS
        }
        tasks.append(
            TaskInput(
                url=str(url),
                file_name=value.get("file_name"),
                dir_path=value.get("dir_path"),
                md5=value.get("md5"),
                log_path=value.get("log_path"),
                options=options,
            )
        )
    return tasks


def _from_sequence(data: list[Any], *, group: str | None = None) -> list[TaskInput]:
    tasks: list[TaskInput] = []
    for value in data:
        if isinstance(value, str):
            tasks.append(TaskInput(url=value, group=group))
        elif isinstance(value, dict):
            tasks.append(_task_from_mapping(value, group=group))
        else:
            raise _task_input_error(
                "task_entry_invalid_type",
                "Task list entries must be strings or mappings",
            )
    return tasks


def _is_schema_v2(data: dict[str, Any]) -> bool:
    version = data.get("version")
    return version == 2 or version == "2"


def _has_schema_v2_shape(data: dict[str, Any]) -> bool:
    return any(key in data for key in ("defaults", "groups", "tasks"))


def _mapping_or_empty(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        code_field = field_name.split(".")[-1]
        if code_field == "defaults":
            code = "schema_v2_defaults_not_mapping"
        elif code_field == "groups":
            code = "schema_v2_groups_not_mapping"
        else:
            code = "schema_v2_group_not_mapping"
        raise _task_input_error(
            code,
            f"schema v2 {field_name} must be a mapping",
            field=field_name,
        )
    return value


def _tasks_sequence(value: Any, *, field_name: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        code = "schema_v2_group_tasks_not_list" if "." in field_name else "schema_v2_tasks_not_list"
        raise _task_input_error(
            code,
            f"schema v2 {field_name} must be a list",
            field=field_name,
        )
    return value


def _merge_task_options(
    defaults: dict[str, Any],
    group_defaults: dict[str, Any],
    task_data: Any,
) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    resolved.update(defaults)
    resolved.update(group_defaults)
    if isinstance(task_data, str):
        resolved["url"] = task_data
        return resolved
    if isinstance(task_data, dict):
        resolved.update(task_data)
        return resolved
    raise _task_input_error(
        "schema_v2_task_entry_invalid_type",
        "schema v2 task entries must be strings or mappings",
    )


def _from_schema_v2(data: dict[str, Any], *, group: str | None = None) -> list[TaskInput]:
    defaults = _mapping_or_empty(data.get("defaults"), field_name="defaults")
    groups = _mapping_or_empty(data.get("groups"), field_name="groups")
    tasks: list[TaskInput] = []

    if group is not None:
        if group not in groups:
            raise _task_input_error(
                "schema_v2_group_not_found",
                f"schema v2 group not found: {group}",
                field="group",
            )
        group_data = _mapping_or_empty(groups[group], field_name=f"groups.{group}")
        group_defaults = {
            key: value for key, value in group_data.items() if key != "tasks"
        }
        for task_data in _tasks_sequence(
            group_data.get("tasks"), field_name=f"groups.{group}.tasks"
        ):
            tasks.append(
                _task_from_mapping(
                    _merge_task_options(defaults, group_defaults, task_data),
                    group=group,
                )
            )
        return tasks

    for task_data in _tasks_sequence(data.get("tasks"), field_name="tasks"):
        tasks.append(_task_from_mapping(_merge_task_options(defaults, {}, task_data)))

    for group_name, group_data_value in groups.items():
        group_data = _mapping_or_empty(group_data_value, field_name=f"groups.{group_name}")
        group_defaults = {
            key: value for key, value in group_data.items() if key != "tasks"
        }
        for task_data in _tasks_sequence(
            group_data.get("tasks"), field_name=f"groups.{group_name}.tasks"
        ):
            tasks.append(
                _task_from_mapping(
                    _merge_task_options(defaults, group_defaults, task_data),
                    group=str(group_name),
                )
            )
    return tasks


def parse_task_data(data: Any, *, group: str | None = None) -> list[TaskInput]:
    if data is None:
        return []
    if isinstance(data, dict):
        if _is_schema_v2(data):
            return _from_schema_v2(data, group=group)
        if _has_schema_v2_shape(data):
            raise _task_input_error(
                "schema_v2_missing_version",
                "schema v2 task input must set version: 2",
                field="version",
            )
        if group is not None:
            raise _task_input_error(
                "group_requires_schema_v2",
                "--group requires schema v2 task input",
                field="group",
            )
        return _from_mapping(data)
    if isinstance(data, list):
        if group is not None:
            raise _task_input_error(
                "group_requires_schema_v2",
                "--group requires schema v2 task input",
                field="group",
            )
        return _from_sequence(data)
    raise _task_input_error(
        "task_input_invalid_type",
        "Task input data must be a mapping or list",
    )


def list_task_groups_from_data(data: Any) -> list[str]:
    if not isinstance(data, dict) or not _is_schema_v2(data):
        return []
    groups = _mapping_or_empty(data.get("groups"), field_name="groups")
    return [str(group_name) for group_name in groups]


def _load_structured_file(input_file: str) -> Any:
    with open(input_file, "r") as f:
        content = f.read()
    suffix = os.path.splitext(input_file)[1].lower()
    if suffix == ".json":
        return json.loads(content)
    if suffix in (".yaml", ".yml"):
        return yaml.safe_load(content)
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    return lines


def load_task_input(input_file: str, *, group: str | None = None) -> list[TaskInput]:
    return parse_task_data(_load_structured_file(input_file), group=group)


def load_task_groups(input_file: str) -> list[str]:
    return list_task_groups_from_data(_load_structured_file(input_file))


def task_input_schema_payload() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "surface": "task_input",
        "introduced_in": "0.8.19",
        "commands": {
            "input schema": {
                "introduced_in": "0.8.19",
                "outputs": ["readable", "json"],
                "purpose": "Describe task input formats and YAML schema v2 boundaries without reading task files or downloading.",
            }
        },
        "legacy_formats": {
            "plain_text": "one URL per non-empty line",
            "json_mapping": "{url: task_options}",
            "json_list": "list[str|task_mapping]",
            "yaml_mapping": "{url: task_options}",
            "yaml_list": "list[str|task_mapping]",
        },
        "schema_v2": {
            "version": 2,
            "top_level_fields": {
                "version": "required literal 2",
                "defaults": "optional mapping applied to every task",
                "tasks": "optional top-level list[str|task_mapping]",
                "groups": "optional mapping of group name to group config",
            },
            "group_fields": {
                "tasks": "optional list[str|task_mapping]",
                "other_fields": "group defaults merged between global defaults and task fields",
            },
            "task_fields": {
                "url": "required for task mappings; string task entries are treated as url",
                "file_name": "mapped to TaskInput.file_name and current download/queue records",
                "dir_path": "mapped to TaskInput.dir_path and current download/queue records",
                "md5": "mapped to TaskInput.md5 and current download/queue records",
                "log_path": "mapped to TaskInput.log_path for direct downloads",
                "other_fields": "preserved in TaskInput.options for dry-run and future contract expansion",
            },
            "precedence": ["task", "group", "defaults"],
            "validation_errors": {
                "schema_v2_missing_version": "schema-like input must explicitly set version: 2",
                "schema_v2_defaults_not_mapping": "defaults must be a mapping",
                "schema_v2_groups_not_mapping": "groups must be a mapping",
                "schema_v2_group_not_mapping": "each group value must be a mapping",
                "schema_v2_tasks_not_list": "top-level tasks must be a list",
                "schema_v2_group_tasks_not_list": "group tasks must be a list",
                "task_mapping_url_missing": "task mapping must include url",
                "schema_v2_group_not_found": "requested group does not exist",
                "group_requires_schema_v2": "--group requires schema v2 input",
            },
            "selection": {
                "main_cli": "pdman -i tasks.yaml --group NAME",
                "queue_add": "pdman queue add -i tasks.yaml --group NAME",
                "list_groups": [
                    "pdman -i tasks.yaml --list-groups",
                    "pdman queue add -i tasks.yaml --list-groups",
                ],
                "dry_run": [
                    "pdman -i tasks.yaml --group NAME --dry-run",
                    "pdman queue add -i tasks.yaml --group NAME --dry-run",
                ],
            },
        },
        "mapped_fields": ["url", "file_name", "dir_path", "md5", "log_path"],
        "preserved_option_fields": "schema v2 fields other than mapped fields and reserved structural fields are preserved in TaskInput.options",
        "non_goals": [
            "does not map retry, headers, max_connections, or connect_timeout to per-task downloader behavior",
            "does not change queue record schema v1",
            "does not write TaskInput.options to queue records",
            "does not start downloads or inspect task files",
            "does not introduce database or index storage",
        ],
    }


def format_task_input_schema(payload: dict[str, Any]) -> str:
    schema_v2 = payload["schema_v2"]
    lines = [
        "Task input schema:",
        f"  schema_version: {payload['schema_version']}",
        f"  surface: {payload['surface']}",
        f"  introduced_in: {payload['introduced_in']}",
        "  legacy_formats:",
    ]
    for name, description in payload["legacy_formats"].items():
        lines.append(f"    {name}: {description}")
    lines.extend(
        [
            "  schema_v2:",
            f"    version: {schema_v2['version']}",
            "    top_level_fields:",
        ]
    )
    for name, description in schema_v2["top_level_fields"].items():
        lines.append(f"      {name}: {description}")
    lines.append("    group_fields:")
    for name, description in schema_v2["group_fields"].items():
        lines.append(f"      {name}: {description}")
    lines.append("    task_fields:")
    for name, description in schema_v2["task_fields"].items():
        lines.append(f"      {name}: {description}")
    lines.append("    validation_errors:")
    for code, description in schema_v2["validation_errors"].items():
        lines.append(f"      {code}: {description}")
    lines.append("    precedence: " + " > ".join(schema_v2["precedence"]))
    lines.append("  mapped_fields: " + ", ".join(payload["mapped_fields"]))
    lines.append(f"  preserved_option_fields: {payload['preserved_option_fields']}")
    lines.append("  non_goals:")
    for item in payload["non_goals"]:
        lines.append(f"    - {item}")
    return "\n".join(lines)
