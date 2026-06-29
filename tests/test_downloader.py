import asyncio

from pdman.chunk import Chunk
from pdman.downloader import Downloader
from pdman.manager import Manager
from pdman.status import TaskReason, TaskStatus


def test_refresh_downloaded_bytes_uses_existing_chunk_sizes(tmp_path):
    manager = Manager(log_path=None)
    downloader = Downloader(manager, "https://example.com/file.bin", str(tmp_path))

    first = Chunk(downloader, 0, 99, str(tmp_path / "file.0"))
    second = Chunk(downloader, 100, 199, str(tmp_path / "file.100"))
    first.size = 40
    second.size = 25
    first.next = second
    downloader.chunk_root = first

    assert downloader.refresh_downloaded_bytes() == 65
    assert downloader.downloaded_bytes == 65


def test_quit_if_exists_skips_named_file_before_parse_config(tmp_path):
    async def run_case():
        existing_file = tmp_path / "already-there.bin"
        existing_file.write_bytes(b"existing")
        manager = Manager(quit_if_exists=True, log_path=None)
        downloader = Downloader(
            manager,
            "https://example.com/should-not-be-requested.bin",
            str(tmp_path),
            filename=existing_file.name,
        )

        async def fail_if_called():
            raise AssertionError("parse_config should not run for an existing named target")

        downloader.parse_config = fail_if_called

        result = await downloader.start_download()

        assert result.url == downloader.url
        assert result.status == TaskStatus.SKIPPED
        assert result.reason_code == TaskReason.TARGET_EXISTS
        assert existing_file.read_bytes() == b"existing"
        assert downloader._done is True

    asyncio.run(run_case())
