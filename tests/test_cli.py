import pdman.cli as cli


class StubManager:
    def __init__(self, *args, **kwargs):
        self.exit_code = 1
        self.added_urls = []
        self.loaded_inputs = []

    def append(self, url, file_name=None):
        self.added_urls.append((url, file_name))

    def add_urls(self, urls):
        self.added_urls.extend((url, None) for url in urls)

    def load_input_file(self, path):
        self.loaded_inputs.append(path)

    async def download(self):
        return None


def test_cli_returns_zero_when_no_tasks():
    assert cli.main([]) == 0


def test_cli_returns_manager_exit_code(monkeypatch):
    monkeypatch.setattr(cli, "Manager", StubManager)

    exit_code = cli.main(["https://example.com/file.bin"])

    assert exit_code == 1
