from dataclasses import dataclass
from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "pending"
    CONNECTING = "connecting"
    HEADER_CHECKING = "header_checking"
    DOWNLOADING = "downloading"
    MERGING = "merging"
    VERIFYING = "verifying"
    RETRYING = "retrying"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"


class TaskReason(str, Enum):
    TARGET_EXISTS = "target_exists"
    HTTP_STATUS = "http_status"
    CONNECTION_TIMEOUT = "connection_timeout"
    CONNECTION_FAILED = "connection_failed"
    INTEGRITY_MISMATCH = "integrity_mismatch"
    MERGE_FAILED = "merge_failed"
    FILESYSTEM_ERROR = "filesystem_error"
    TMP_SPACE_INSUFFICIENT = "tmp_space_insufficient"
    TMP_DIR_CREATE_FAILED = "tmp_dir_create_failed"
    UNEXPECTED_ERROR = "unexpected_error"


@dataclass
class TaskResult:
    url: str
    filename: str | None
    status: TaskStatus
    reason: str | None = None
    reason_code: TaskReason | None = None
    error: str | None = None
    downloaded_bytes: int = 0
    total_bytes: int | None = None

    @property
    def failed(self) -> bool:
        return self.status == TaskStatus.FAILED

    @property
    def skipped(self) -> bool:
        return self.status == TaskStatus.SKIPPED
