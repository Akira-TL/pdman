from __future__ import annotations

import os
import shutil
import sys
import time
from pathlib import Path
from typing import BinaryIO


class FileLockTimeout(TimeoutError):
    pass


class BaseFileLock:
    def __init__(
        self,
        path: str | os.PathLike[str],
        timeout: float | None = 10.0,
        poll_interval: float = 0.1,
    ):
        self.path = Path(path).expanduser()
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._acquired = False

    def acquire(self) -> "BaseFileLock":
        raise NotImplementedError

    def release(self) -> None:
        raise NotImplementedError

    def __enter__(self) -> "BaseFileLock":
        return self.acquire()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

    def _deadline(self) -> float | None:
        return None if self.timeout is None else time.monotonic() + self.timeout

    def _timed_out(self, deadline: float | None) -> bool:
        return deadline is not None and time.monotonic() >= deadline

    def _sleep_or_timeout(self, deadline: float | None) -> None:
        if self._timed_out(deadline):
            raise FileLockTimeout(f"Timed out waiting for lock: {self.path}")
        if deadline is None:
            time.sleep(self.poll_interval)
            return
        remaining = max(0.0, deadline - time.monotonic())
        time.sleep(min(self.poll_interval, remaining))


class PosixFileLock(BaseFileLock):
    def __init__(
        self,
        path: str | os.PathLike[str],
        timeout: float | None = 10.0,
        poll_interval: float = 0.1,
    ):
        super().__init__(path, timeout, poll_interval)
        self._file: BinaryIO | None = None

    def acquire(self) -> "PosixFileLock":
        import fcntl

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a+b")
        deadline = self._deadline()
        while True:
            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._acquired = True
                return self
            except BlockingIOError:
                self._sleep_or_timeout(deadline)

    def release(self) -> None:
        import fcntl

        if self._file is None:
            return
        try:
            if self._acquired:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._acquired = False
            self._file.close()
            self._file = None


class WindowsFileLock(BaseFileLock):
    LOCK_SIZE = 1

    def __init__(
        self,
        path: str | os.PathLike[str],
        timeout: float | None = 10.0,
        poll_interval: float = 0.1,
    ):
        super().__init__(path, timeout, poll_interval)
        self._file: BinaryIO | None = None

    def acquire(self) -> "WindowsFileLock":
        import msvcrt

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a+b")
        deadline = self._deadline()
        while True:
            try:
                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, self.LOCK_SIZE)
                self._acquired = True
                return self
            except OSError:
                self._sleep_or_timeout(deadline)

    def release(self) -> None:
        import msvcrt

        if self._file is None:
            return
        try:
            if self._acquired:
                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, self.LOCK_SIZE)
        finally:
            self._acquired = False
            self._file.close()
            self._file = None


class AtomicDirectoryFileLock(BaseFileLock):
    def __init__(
        self,
        path: str | os.PathLike[str],
        timeout: float | None = 10.0,
        poll_interval: float = 0.1,
    ):
        super().__init__(path, timeout, poll_interval)
        self.lock_dir = Path(str(self.path) + ".d")

    def acquire(self) -> "AtomicDirectoryFileLock":
        self.lock_dir.parent.mkdir(parents=True, exist_ok=True)
        deadline = self._deadline()
        while True:
            try:
                self.lock_dir.mkdir()
                self._acquired = True
                return self
            except FileExistsError:
                self._sleep_or_timeout(deadline)

    def release(self) -> None:
        if not self._acquired:
            return
        try:
            shutil.rmtree(self.lock_dir)
        finally:
            self._acquired = False


def default_lock_backend():
    if os.name == "posix":
        return PosixFileLock
    if os.name == "nt":
        return WindowsFileLock
    return AtomicDirectoryFileLock


class FileLock:
    def __init__(
        self,
        path: str | os.PathLike[str],
        timeout: float | None = 10.0,
        poll_interval: float = 0.1,
        backend: type[BaseFileLock] | None = None,
    ):
        backend = backend or default_lock_backend()
        self.backend_name = backend.__name__
        self._lock = backend(path, timeout=timeout, poll_interval=poll_interval)

    @property
    def path(self) -> Path:
        return self._lock.path

    def acquire(self) -> BaseFileLock:
        return self._lock.acquire()

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> BaseFileLock:
        return self.acquire()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


def platform_lock_backend_name() -> str:
    if os.name == "posix":
        return "posix-fcntl"
    if os.name == "nt":
        return "windows-msvcrt"
    return f"atomic-directory-fallback:{sys.platform}"
