from pdman.range_allocator import RangeAllocator


def test_range_allocator_covers_full_file(tmp_path):
    allocator = RangeAllocator(
        file_size=10,
        range_size=4,
        tmp_dir=tmp_path,
        filename="file.bin",
    )

    assert [(task.start, task.end) for task in allocator.ranges] == [
        (0, 3),
        (4, 7),
        (8, 9),
    ]
    assert [task.expected_size for task in allocator.ranges] == [4, 4, 2]


def test_range_allocator_claims_until_empty(tmp_path):
    allocator = RangeAllocator(
        file_size=8,
        range_size=4,
        tmp_dir=tmp_path,
        filename="file.bin",
    )

    first = allocator.claim_next()
    second = allocator.claim_next()

    assert first is not None
    assert second is not None
    assert first.start == 0
    assert second.start == 4
    assert allocator.claim_next() is None


def test_range_allocator_tracks_completed_ranges(tmp_path):
    allocator = RangeAllocator(
        file_size=8,
        range_size=4,
        tmp_dir=tmp_path,
        filename="file.bin",
    )

    first = allocator.claim_next()
    assert first is not None
    first.path.write_bytes(b"1234")
    allocator.mark_completed(first)

    assert allocator.completed == [first]
    assert allocator.completed_bytes == 4


def test_range_allocator_requeues_failed_range_until_retry_limit(tmp_path):
    allocator = RangeAllocator(
        file_size=4,
        range_size=4,
        tmp_dir=tmp_path,
        filename="file.bin",
        max_retries=1,
    )

    first = allocator.claim_next()
    assert first is not None
    assert first.attempts == 1
    assert allocator.mark_failed(first, "temporary failure") is True

    retry = allocator.claim_next()
    assert retry is first
    assert retry.attempts == 2
    assert allocator.mark_failed(retry, "permanent failure") is False

    assert allocator.claim_next() is None
    assert allocator.has_failures
    assert allocator.failed == [first]
    assert first.last_error == "permanent failure"


def test_range_task_detects_existing_complete_file(tmp_path):
    allocator = RangeAllocator(
        file_size=4,
        range_size=4,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    task = allocator.ranges[0]
    task.path.write_bytes(b"1234")

    assert task.is_complete
    assert task.existing_size() == 4
