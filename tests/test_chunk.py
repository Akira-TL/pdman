from pdman.chunk import Chunk, STREAM_CHUNK_SIZE


def test_stream_chunk_size_is_100_kib():
    assert STREAM_CHUNK_SIZE == 100 * 1024


def test_chunk_download_ensures_parent_dir(tmp_path):
    chunk_path = tmp_path / "missing" / "nested" / "file.bin.0"
    chunk = Chunk(parent=None, start=0, end=9, chunk_path=str(chunk_path))

    chunk._ensure_chunk_parent_dir()

    assert chunk_path.parent.is_dir()
