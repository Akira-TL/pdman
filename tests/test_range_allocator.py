from pdman.range_allocator import DYNAMIC_RANGE_ALIGNMENT, RangeAllocator, choose_dynamic_range_size


def test_choose_dynamic_range_size_keeps_at_least_min_split_size():
    assert choose_dynamic_range_size(
        file_size=10 * 1024,
        min_split_size=1024,
        worker_count=4,
    ) == 1024


def test_choose_dynamic_range_size_scales_large_files_and_aligns():
    range_size = choose_dynamic_range_size(
        file_size=1024 * 1024 * 1024,
        min_split_size=1024 * 1024,
        worker_count=4,
    )

    assert range_size > 1024 * 1024
    assert range_size % DYNAMIC_RANGE_ALIGNMENT == 0
    assert range_size == 64 * 1024 * 1024


def test_choose_dynamic_range_size_rejects_invalid_values():
    invalid_cases = [
        {"file_size": 0, "min_split_size": 1, "worker_count": 1},
        {"file_size": 1, "min_split_size": 0, "worker_count": 1},
        {"file_size": 1, "min_split_size": 1, "worker_count": 0},
        {
            "file_size": 1,
            "min_split_size": 1,
            "worker_count": 1,
            "target_ranges_per_worker": 0,
        },
    ]

    for kwargs in invalid_cases:
        try:
            choose_dynamic_range_size(**kwargs)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError for {kwargs}")


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

    assert allocator.total_ranges == 2
    assert allocator.pending_count == 2
    assert allocator.active_count == 0
    assert allocator.completed_count == 0
    assert allocator.failed_count == 0

    first = allocator.claim_next()
    assert first is not None
    assert allocator.pending_count == 1
    assert allocator.active_count == 1
    first.path.write_bytes(b"1234")
    allocator.mark_completed(first)

    assert allocator.completed == [first]
    assert allocator.completed_bytes == 4
    assert allocator.active_count == 0
    assert allocator.completed_count == 1


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
    assert allocator.requeue_count == 1

    retry = allocator.claim_next()
    assert retry is first
    assert retry.attempts == 2
    assert allocator.mark_failed(retry, "permanent failure") is False

    assert allocator.claim_next() is None
    assert allocator.has_failures
    assert allocator.failed_count == 1
    assert allocator.retried_count == 1
    assert allocator.failed == [first]
    assert first.last_error == "permanent failure"


def test_range_task_reports_remaining_split_state(tmp_path):
    allocator = RangeAllocator(
        file_size=10,
        range_size=10,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    task = allocator.ranges[0]
    task.path.write_bytes(b"abc")

    assert task.next_start == 3
    assert task.remaining_size == 7
    assert task.can_split(1)


def test_range_allocator_split_remaining_renames_partial_and_creates_child(tmp_path):
    allocator = RangeAllocator(
        file_size=10,
        range_size=10,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    task = allocator.claim_next()
    assert task is not None
    task.path.write_bytes(b"abc")
    original_path = task.path

    child = allocator.split_remaining(task, min_size=1)

    assert child is not None
    assert task.start == 0
    assert task.end == 2
    assert task.path.name == "file.bin.range.0-2"
    assert task.path.read_bytes() == b"abc"
    assert not original_path.exists()
    assert child.start == 3
    assert child.end == 9
    assert child.path.name == "file.bin.range.3-9"
    assert allocator.completed == [task]
    assert allocator.pending_count == 1
    assert allocator.active_count == 0
    assert allocator.total_ranges == 2
    assert allocator.split_count == 1
    claimed_child = allocator.claim_next()
    assert claimed_child is child
    assert claimed_child.attempts == 1


def test_range_allocator_split_remaining_returns_none_without_partial(tmp_path):
    allocator = RangeAllocator(
        file_size=10,
        range_size=10,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    task = allocator.claim_next()
    assert task is not None

    assert allocator.split_remaining(task, min_size=1) is None
    assert allocator.completed_count == 0
    assert allocator.pending_count == 0
    assert allocator.active_count == 1


def test_range_allocator_split_remaining_returns_none_when_remaining_too_small(tmp_path):
    allocator = RangeAllocator(
        file_size=10,
        range_size=10,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    task = allocator.claim_next()
    assert task is not None
    task.path.write_bytes(b"abcdefghi")

    assert allocator.split_remaining(task, min_size=1) is None
    assert allocator.completed_count == 0
    assert allocator.pending_count == 0
    assert allocator.active_count == 1


def test_range_task_discard_partial_removes_file_and_resets_counter(tmp_path):
    allocator = RangeAllocator(
        file_size=8,
        range_size=4,
        tmp_dir=tmp_path,
        filename="file.bin",
    )
    task = allocator.ranges[0]
    task.path.write_bytes(b"123")
    task.downloaded_bytes = 3

    removed = task.discard_partial()

    assert removed == 3
    assert task.downloaded_bytes == 0
    assert not task.path.exists()


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
