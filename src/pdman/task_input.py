from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

import yaml


_TASK_FIELDS = {"url", "file_name", "dir_path", "md5", "log_path"}
_RESERVED_SCHEMA_V2_FIELDS = {"version", "defaults", "groups", "tasks"}


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
        raise ValueError("Task mapping in a list must include a url field")
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
            raise ValueError(f"Task options for {url} must be a mapping")
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
            raise ValueError("Task list entries must be strings or mappings")
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
        raise ValueError(f"schema v2 {field_name} must be a mapping")
    return value


def _tasks_sequence(value: Any, *, field_name: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"schema v2 {field_name} must be a list")
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
    raise ValueError("schema v2 task entries must be strings or mappings")


def _from_schema_v2(data: dict[str, Any], *, group: str | None = None) -> list[TaskInput]:
    defaults = _mapping_or_empty(data.get("defaults"), field_name="defaults")
    groups = _mapping_or_empty(data.get("groups"), field_name="groups")
    tasks: list[TaskInput] = []

    if group is not None:
        if group not in groups:
            raise ValueError(f"schema v2 group not found: {group}")
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
            raise ValueError("schema v2 task input must set version: 2")
        if group is not None:
            raise ValueError("--group requires schema v2 task input")
        return _from_mapping(data)
    if isinstance(data, list):
        if group is not None:
            raise ValueError("--group requires schema v2 task input")
        return _from_sequence(data)
    raise ValueError("Task input data must be a mapping or list")


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
