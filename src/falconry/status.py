from enum import Enum


class FalconryStatus(Enum):
    UNKNOWN = 0
    IDLE = 1
    RUNNING = 2
    REMOVED = 3
    COMPLETE = 4
    HELD = 5
    TRANSPORTING = 6
    SUSPENDED = 7
    SKIPPED = 8
    NOT_SUBMITTED = 9
    LOG_FILE_MISSING = 10
    ABNORMAL_TERMINATION = 11
    ABORTED_BY_USER = 12
    FAILED = 13
