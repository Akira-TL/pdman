from pdman.chunk import Chunk
from pdman.downloader import Downloader
from pdman.manager import Manager


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
