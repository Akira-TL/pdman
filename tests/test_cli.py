import json

import pytest

import pdman.cli as cli
from pdman.output_modes import OutputMode


class StubManager:
    last_instance = None

    def __init__(self, *args, **kwargs):
        StubManager.last_instance = self
        self.args = args
        self.kwargs = kwargs
        self.output_mode = kwargs.get("output_mode")
        self.exit_code = 1
        self.added_urls = []
        self.loaded_inputs = []

    def append(self, url, file_name=None, dir_path=None):
        self.added_urls.append((url, file_name, dir_path))

    def add_urls(self, urls):
        self.added_urls.extend((url, None, None) for url in urls)

    def load_input_file(self, path, *, group=None):
        self.loaded_inputs.append((path, group))

    async def download(self):
        return None


def test_cli_returns_zero_when_no_tasks():
    assert cli.main([]) == 0


def test_cli_returns_manager_exit_code(monkeypatch):
    monkeypatch.setattr(cli, "Manager", StubManager)

    exit_code = cli.main(["https://example.com/file.bin"])

    assert exit_code == 1


def test_cli_output_mode_is_passed_to_manager(monkeypatch):
    monkeypatch.setattr(cli, "Manager", StubManager)

    exit_code = cli.main(["--output", "jsonl", "https://example.com/file.bin"])

    assert exit_code == 1
    assert StubManager.last_instance.output_mode is OutputMode.JSONL


def test_cli_output_mode_defaults_to_plain_for_non_tty(monkeypatch):
    monkeypatch.setattr(cli, "Manager", StubManager)
    monkeypatch.setattr(cli.sys.stdout, "isatty", lambda: False)

    cli.main(["https://example.com/file.bin"])

    assert StubManager.last_instance.output_mode is OutputMode.PLAIN


def test_cli_output_mode_rejects_invalid_value():
    with pytest.raises(SystemExit):
        cli.main(["--output", "xml", "https://example.com/file.bin"])


def test_cli_output_json_help_smoke(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["--output", "json", "--help"])

    output = capsys.readouterr().out
    assert exc_info.value.code == 0
    assert "--output" in output
    assert "{rich,plain,json,jsonl}" in output


def test_cli_output_jsonl_help_smoke(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["--output", "jsonl", "--help"])

    output = capsys.readouterr().out
    assert exc_info.value.code == 0
    assert "--output" in output
    assert "{rich,plain,json,jsonl}" in output


def test_cli_output_schema_json(capsys):
    exit_code = cli.main(["output", "schema", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["surface"] == "output"
    assert payload["modes"]["jsonl"]["structured"] is True
    assert payload["json"]["kind"] == "download_summary"
    assert payload["jsonl"]["event_kinds"] == [
        "run_started",
        "task_finished",
        "run_finished",
    ]


def test_cli_output_schema_readable(capsys):
    exit_code = cli.main(["output", "schema"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Output schema:" in output
    assert "default_resolution:" in output
    assert "event_kinds: run_started, task_finished, run_finished" in output


def test_cli_output_requires_schema(capsys):
    exit_code = cli.main(["output"])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Output command required: schema" in output


def test_cli_input_schema_json(capsys):
    exit_code = cli.main(["input", "schema", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["surface"] == "task_input"
    assert payload["schema_v2"]["precedence"] == ["task", "group", "defaults"]
    assert payload["mapped_fields"] == ["url", "file_name", "dir_path", "md5", "log_path"]


def test_cli_input_schema_readable(capsys):
    exit_code = cli.main(["input", "schema"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Task input schema:" in output
    assert "precedence: task > group > defaults" in output
    assert "does not write TaskInput.options to queue records" in output


def test_cli_input_examples_json(capsys):
    exit_code = cli.main(["input", "examples", "minimal", "invalid_defaults", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["count"] == 2
    assert payload["examples"][0]["kind"] == "minimal"
    assert payload["examples"][0]["valid"] is True
    assert payload["examples"][1]["kind"] == "invalid_defaults"
    assert payload["examples"][1]["valid"] is False
    assert payload["examples"][1]["error"]["code"] == "schema_v2_defaults_not_mapping"


def test_cli_input_examples_readable(capsys):
    exit_code = cli.main(["input", "examples", "minimal", "invalid_defaults"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Task input examples:" in output
    assert "minimal: valid count=1 group=-" in output
    assert "invalid_defaults: invalid:schema_v2_defaults_not_mapping" in output


def test_cli_input_examples_unknown_kind_json(capsys):
    exit_code = cli.main(["input", "examples", "missing", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload == {
        "error": {
            "code": "task_input_example_not_found",
            "message": "task input example not found: missing",
            "field": "kind",
        }
    }


def test_cli_input_requires_schema_or_examples(capsys):
    exit_code = cli.main(["input"])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Input command required: schema or examples" in output


def test_cli_list_groups_for_schema_v2_input(tmp_path, capsys):
    path = tmp_path / "tasks.yaml"
    path.write_text(
        "\n".join(
            [
                "version: 2",
                "groups:",
                "  nt-db:",
                "    tasks: []",
                "  refseq:",
                "    tasks: []",
            ]
        )
    )

    exit_code = cli.main(["-i", str(path), "--list-groups"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Groups:" in output
    assert "  nt-db" in output
    assert "  refseq" in output


def test_cli_dry_run_resolves_schema_v2_group_without_downloading(tmp_path, capsys):
    path = tmp_path / "tasks.yaml"
    path.write_text(
        "\n".join(
            [
                "version: 2",
                "defaults:",
                "  dir_path: /data/default",
                "  retry: 5",
                "groups:",
                "  nt-db:",
                "    dir_path: /data/nt",
                "    tasks:",
                "      - url: https://example.com/nt.tar.gz",
                "        file_name: nt.tar.gz",
                "  refseq:",
                "    tasks:",
                "      - url: https://example.com/refseq.tar.gz",
            ]
        )
    )

    exit_code = cli.main(["-i", str(path), "--group", "nt-db", "--dry-run"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Resolved tasks:" in output
    assert "https://example.com/nt.tar.gz" in output
    assert "file_name=nt.tar.gz" in output
    assert "dir_path=/data/nt" in output
    assert "group=nt-db" in output
    assert "retry=5" in output
    assert "refseq" not in output


def test_cli_list_groups_json_for_schema_v2_input(tmp_path, capsys):
    path = tmp_path / "tasks.yaml"
    path.write_text(
        "\n".join(
            [
                "version: 2",
                "groups:",
                "  nt-db:",
                "    tasks: []",
                "  refseq:",
                "    tasks: []",
            ]
        )
    )

    exit_code = cli.main(["-i", str(path), "--list-groups", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload == {"groups": ["nt-db", "refseq"], "count": 2}


def test_cli_dry_run_json_resolves_schema_v2_group_without_downloading(tmp_path, capsys):
    path = tmp_path / "tasks.yaml"
    path.write_text(
        "\n".join(
            [
                "version: 2",
                "defaults:",
                "  dir_path: /data/default",
                "  retry: 5",
                "groups:",
                "  nt-db:",
                "    dir_path: /data/nt",
                "    tasks:",
                "      - url: https://example.com/nt.tar.gz",
                "        file_name: nt.tar.gz",
                "  refseq:",
                "    tasks:",
                "      - url: https://example.com/refseq.tar.gz",
            ]
        )
    )

    exit_code = cli.main(["-i", str(path), "--group", "nt-db", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["dry_run"] is True
    assert payload["count"] == 1
    assert payload["group"] == "nt-db"
    assert payload["tasks"] == [
        {
            "url": "https://example.com/nt.tar.gz",
            "file_name": "nt.tar.gz",
            "dir_path": "/data/nt",
            "md5": None,
            "log_path": None,
            "group": "nt-db",
            "options": {"retry": 5},
        }
    ]


def test_cli_dry_run_json_resolves_direct_urls(tmp_path, capsys):
    exit_code = cli.main(
        [
            "--dry-run",
            "--json",
            "-d",
            str(tmp_path / "downloads"),
            "-o",
            "file.bin",
            "https://example.com/file.bin",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["dry_run"] is True
    assert payload["count"] == 1
    assert payload["group"] is None
    assert payload["tasks"] == [
        {
            "url": "https://example.com/file.bin",
            "file_name": "file.bin",
            "dir_path": str(tmp_path / "downloads"),
            "md5": None,
            "log_path": None,
        }
    ]


def test_cli_dry_run_json_reports_schema_v2_error_code(tmp_path, capsys):
    path = tmp_path / "tasks.yaml"
    path.write_text("version: 2\ndefaults: []\n")

    exit_code = cli.main(["-i", str(path), "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload == {
        "error": {
            "code": "schema_v2_defaults_not_mapping",
            "message": "schema v2 defaults must be a mapping",
            "field": "defaults",
        }
    }


def test_cli_dry_run_reports_schema_v2_error_code(tmp_path, capsys):
    path = tmp_path / "tasks.yaml"
    path.write_text("version: 2\ndefaults: []\n")

    exit_code = cli.main(["-i", str(path), "--dry-run"])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Failed to resolve input tasks:" in output
    assert "[schema_v2_defaults_not_mapping]" in output
    assert "defaults must be a mapping" in output


def test_cli_passes_group_to_manager_input_loading(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "Manager", StubManager)
    path = tmp_path / "tasks.yaml"
    path.write_text("version: 2\ngroups:\n  nt-db:\n    tasks: []\n")

    exit_code = cli.main(["-i", str(path), "--group", "nt-db"])

    assert exit_code == 1
    assert StubManager.last_instance.loaded_inputs == [(str(path), "nt-db")]
