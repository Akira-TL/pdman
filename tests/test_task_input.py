import json

from pdman.task_input import TaskInput, load_task_groups, load_task_input, parse_task_data


def test_parse_mapping_task_data():
    tasks = parse_task_data(
        {
            "https://example.com/a.bin": {
                "file_name": "a.bin",
                "dir_path": "/data",
                "md5": "0" * 32,
            }
        }
    )

    assert tasks == [
        TaskInput(
            url="https://example.com/a.bin",
            file_name="a.bin",
            dir_path="/data",
            md5="0" * 32,
        )
    ]


def test_parse_sequence_task_data():
    tasks = parse_task_data(
        [
            "https://example.com/a.bin",
            {"url": "https://example.com/b.bin", "file_name": "b.bin"},
        ]
    )

    assert [task.url for task in tasks] == [
        "https://example.com/a.bin",
        "https://example.com/b.bin",
    ]
    assert tasks[1].file_name == "b.bin"


def test_load_plain_text_task_input(tmp_path):
    path = tmp_path / "urls.txt"
    path.write_text("https://example.com/a.bin\n\nhttps://example.com/b.bin\n")

    assert [task.url for task in load_task_input(str(path))] == [
        "https://example.com/a.bin",
        "https://example.com/b.bin",
    ]


def test_load_json_task_input(tmp_path):
    path = tmp_path / "tasks.json"
    path.write_text(
        json.dumps(
            {
                "https://example.com/a.bin": {
                    "file_name": "a.bin",
                    "dir_path": "/data",
                }
            }
        )
    )

    tasks = load_task_input(str(path))

    assert tasks[0].url == "https://example.com/a.bin"
    assert tasks[0].file_name == "a.bin"
    assert tasks[0].dir_path == "/data"


def test_load_yaml_task_input(tmp_path):
    path = tmp_path / "tasks.yaml"
    path.write_text(
        "\n".join(
            [
                "https://example.com/a.bin:",
                "  file_name: a.bin",
                "  dir_path: /data",
            ]
        )
    )

    tasks = load_task_input(str(path))

    assert tasks[0].url == "https://example.com/a.bin"
    assert tasks[0].file_name == "a.bin"
    assert tasks[0].dir_path == "/data"


def test_parse_yaml_schema_v2_defaults_groups_and_task_overrides():
    tasks = parse_task_data(
        {
            "version": 2,
            "defaults": {
                "dir_path": "/data/default",
                "retry": 5,
                "headers": {"User-Agent": "PDMAN"},
            },
            "groups": {
                "nt-db": {
                    "dir_path": "/data/nt",
                    "tasks": [
                        {
                            "url": "https://example.com/nt.171.tar.gz",
                            "file_name": "nt.171.tar.gz",
                            "md5": "https://example.com/nt.171.tar.gz.md5",
                        },
                        {
                            "url": "https://example.com/nt.172.tar.gz",
                            "dir_path": "/override",
                        },
                    ],
                }
            },
        }
    )

    assert [task.url for task in tasks] == [
        "https://example.com/nt.171.tar.gz",
        "https://example.com/nt.172.tar.gz",
    ]
    assert tasks[0].group == "nt-db"
    assert tasks[0].dir_path == "/data/nt"
    assert tasks[0].file_name == "nt.171.tar.gz"
    assert tasks[0].md5 == "https://example.com/nt.171.tar.gz.md5"
    assert tasks[0].options == {
        "retry": 5,
        "headers": {"User-Agent": "PDMAN"},
    }
    assert tasks[1].dir_path == "/override"


def test_parse_yaml_schema_v2_can_select_one_group():
    tasks = parse_task_data(
        {
            "version": 2,
            "defaults": {"dir_path": "/data"},
            "groups": {
                "a": {"tasks": [{"url": "https://example.com/a.bin"}]},
                "b": {"tasks": [{"url": "https://example.com/b.bin"}]},
            },
        },
        group="b",
    )

    assert [task.url for task in tasks] == ["https://example.com/b.bin"]
    assert tasks[0].dir_path == "/data"
    assert tasks[0].group == "b"


def test_parse_yaml_schema_v2_rejects_unknown_group():
    try:
        parse_task_data({"version": 2, "groups": {}}, group="missing")
    except ValueError as exc:
        assert "group not found" in str(exc)
    else:
        raise AssertionError("expected unknown schema v2 group to be rejected")


def test_load_yaml_schema_v2_groups(tmp_path):
    path = tmp_path / "tasks.yaml"
    path.write_text(
        "\n".join(
            [
                "version: 2",
                "groups:",
                "  nt-db:",
                "    tasks:",
                "      - url: https://example.com/nt.tar.gz",
                "  refseq:",
                "    tasks:",
                "      - url: https://example.com/refseq.tar.gz",
            ]
        )
    )

    assert load_task_groups(str(path)) == ["nt-db", "refseq"]
    assert [task.url for task in load_task_input(str(path), group="refseq")] == [
        "https://example.com/refseq.tar.gz"
    ]
