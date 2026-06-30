import os
import threading
import time

import pytest

from pdman.filelock import (
    AtomicDirectoryFileLock,
    FileLock,
    FileLockTimeout,
    PosixFileLock,
    default_lock_backend,
    platform_lock_backend_name,
)


def test_filelock_acquire_and_release(tmp_path):
    lock_path = tmp_path / "queue.lock"

    with FileLock(lock_path):
        assert lock_path.exists() or (tmp_path / "queue.lock.d").exists()

    with FileLock(lock_path):
        assert lock_path.exists() or (tmp_path / "queue.lock.d").exists()


def test_atomic_directory_lock_acquire_release_and_timeout(tmp_path):
    lock_path = tmp_path / "queue.lock"
    first = AtomicDirectoryFileLock(lock_path, timeout=1, poll_interval=0.01)
    first.acquire()
    try:
        second = AtomicDirectoryFileLock(lock_path, timeout=0.05, poll_interval=0.01)
        with pytest.raises(FileLockTimeout):
            second.acquire()
    finally:
        first.release()

    with AtomicDirectoryFileLock(lock_path, timeout=0.05, poll_interval=0.01):
        assert (tmp_path / "queue.lock.d").exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX-only fcntl lock")
def test_posix_file_lock_blocks_second_lock_until_release(tmp_path):
    lock_path = tmp_path / "queue.lock"
    first = PosixFileLock(lock_path, timeout=1, poll_interval=0.01)
    first.acquire()
    try:
        second = PosixFileLock(lock_path, timeout=0.05, poll_interval=0.01)
        with pytest.raises(FileLockTimeout):
            second.acquire()
    finally:
        first.release()

    with PosixFileLock(lock_path, timeout=0.05, poll_interval=0.01):
        assert lock_path.exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows-only msvcrt lock")
def test_windows_backend_is_selected_on_windows():
    assert default_lock_backend().__name__ == "WindowsFileLock"


def test_platform_backend_name_is_stable():
    name = platform_lock_backend_name()
    assert name in {
        "posix-fcntl",
        "windows-msvcrt",
    } or name.startswith("atomic-directory-fallback:")


def test_filelock_timeout_waits_for_release(tmp_path):
    lock_path = tmp_path / "queue.lock"
    events = []

    def worker():
        with FileLock(lock_path, timeout=1, poll_interval=0.01):
            events.append("worker-acquired")

    with FileLock(lock_path, timeout=1, poll_interval=0.01):
        thread = threading.Thread(target=worker)
        thread.start()
        time.sleep(0.05)
        assert events == []
    thread.join(timeout=1)

    assert events == ["worker-acquired"]
