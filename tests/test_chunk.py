from pdman.chunk import STREAM_CHUNK_SIZE


def test_stream_chunk_size_is_100_kib():
    assert STREAM_CHUNK_SIZE == 100 * 1024
