import json

import pdman.cli as cli


class StubManager:
    last_instance = None

    def __init__(self, *args, **kwargs):
        StubManager.last_instance = self
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


def test_cli_input_requires_schema(capsys):
    exit_code = cli.main(["input"])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "Input command required: schema" in output


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
