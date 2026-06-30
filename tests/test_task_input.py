import json

from pdman.task_input import TaskInput, load_task_input, parse_task_data


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
