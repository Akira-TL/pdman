from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import yaml


@dataclass
class TaskInput:
    url: str
    file_name: str | None = None
    dir_path: str | None = None
    md5: str | None = None
    log_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "file_name": self.file_name,
            "dir_path": self.dir_path,
            "md5": self.md5,
            "log_path": self.log_path,
        }


def _from_mapping(data: dict[str, Any]) -> list[TaskInput]:
    tasks: list[TaskInput] = []
    for url, value in data.items():
        if value is None:
            value = {}
        if not isinstance(value, dict):
            raise ValueError(f"Task options for {url} must be a mapping")
        tasks.append(
            TaskInput(
                url=str(url),
                file_name=value.get("file_name"),
                dir_path=value.get("dir_path"),
                md5=value.get("md5"),
                log_path=value.get("log_path"),
            )
        )
    return tasks


def _from_sequence(data: list[Any]) -> list[TaskInput]:
    tasks: list[TaskInput] = []
    for value in data:
        if isinstance(value, str):
            tasks.append(TaskInput(url=value))
        elif isinstance(value, dict):
            url = value.get("url")
            if not url:
                raise ValueError("Task mapping in a list must include a url field")
            tasks.append(
                TaskInput(
                    url=str(url),
                    file_name=value.get("file_name"),
                    dir_path=value.get("dir_path"),
                    md5=value.get("md5"),
                    log_path=value.get("log_path"),
                )
            )
        else:
            raise ValueError("Task list entries must be strings or mappings")
    return tasks


def parse_task_data(data: Any) -> list[TaskInput]:
    if data is None:
        return []
    if isinstance(data, dict):
        return _from_mapping(data)
    if isinstance(data, list):
        return _from_sequence(data)
    raise ValueError("Task input data must be a mapping or list")


def load_task_input(input_file: str) -> list[TaskInput]:
    with open(input_file, "r") as f:
        content = f.read()
    suffix = os.path.splitext(input_file)[1].lower()
    if suffix == ".json":
        return parse_task_data(json.loads(content))
    if suffix in (".yaml", ".yml"):
        return parse_task_data(yaml.safe_load(content))
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    return [TaskInput(url=line) for line in lines]
