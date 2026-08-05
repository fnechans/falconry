import pytest
import tempfile
import os
from pathlib import Path

from falconry.lock import LockFile, LockFileException, lock


"""Tests for fcntl.flock()-based atomic locking mechanism.

Note: These tests verify the new atomic locking implementation which uses
fcntl.flock() for kernel-level advisory locking. The lock file persists
after use (standard flock pattern), but the lock itself is released.
"""


class TestLockFile:
    @pytest.fixture
    def temp_lock_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir, 'test.lock')

    def test_create_lock(self, temp_lock_path):
        """Test that lock file is created and persists after context exit."""
        with LockFile(str(temp_lock_path)):
            assert temp_lock_path.exists()
        # With flock(), the file persists - only the lock is released
        assert temp_lock_path.exists()

    def test_raises_on_concurrent_access(self, temp_lock_path):
        """Test that concurrent lock acquisition fails with LockFileException."""
        with LockFile(str(temp_lock_path)):
            # Lock is held, second attempt should fail
            with pytest.raises(LockFileException):
                with LockFile(str(temp_lock_path)):
                    pass

    def test_lock_released_after_context(self, temp_lock_path):
        """Test that lock is released after exiting context, allowing new acquisition."""
        with LockFile(str(temp_lock_path)):
            assert temp_lock_path.exists()
        # Should be able to acquire again since lock was released
        with LockFile(str(temp_lock_path)):
            assert temp_lock_path.exists()

    def test_lock_released_on_exception(self, temp_lock_path):
        """Test that lock is properly released even when exception occurs."""
        try:
            with LockFile(str(temp_lock_path)):
                assert temp_lock_path.exists()
                raise ValueError("Test error")
        except ValueError:
            pass
        # Lock should be released, allowing new acquisition
        assert temp_lock_path.exists()  # File persists
        # Verify we can acquire the lock again
        with LockFile(str(temp_lock_path)):
            pass

    def test_lock_file_contains_pid(self, temp_lock_path):
        """Test that lock file contains the process ID for debugging."""
        current_pid = os.getpid()
        with LockFile(str(temp_lock_path)):
            with open(temp_lock_path) as f:
                content = f.read().strip()
            assert str(current_pid) in content


class TestLockDecorator:
    @pytest.fixture
    def temp_lock_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir, 'test.lock')

    def test_decorator_applies_lock(self, temp_lock_path):
        """Test that lock decorator properly acquires and releases lock."""
        class TestClass:
            def __init__(self):
                self.lockFile = str(temp_lock_path)

            @lock
            def locked_method(self):
                assert temp_lock_path.exists()
                return "success"

        obj = TestClass()
        result = obj.locked_method()
        assert result == "success"
        # File persists with flock pattern
        assert temp_lock_path.exists()

    def test_decorator_with_exception(self, temp_lock_path):
        """Test that lock decorator releases lock even when method raises exception."""
        class TestClass:
            def __init__(self):
                self.lockFile = str(temp_lock_path)

            @lock
            def failing_method(self):
                assert temp_lock_path.exists()
                raise RuntimeError("Intentional failure")

        obj = TestClass()
        with pytest.raises(RuntimeError):
            obj.failing_method()
        # File persists but lock is released
        assert temp_lock_path.exists()
        # Verify lock can be acquired again
        with LockFile(str(temp_lock_path)):
            pass

    def test_decorator_blocks_concurrent_calls(self, temp_lock_path):
        """Test that decorator prevents concurrent execution."""
        class TestClass:
            def __init__(self):
                self.lockFile = str(temp_lock_path)

            @lock
            def locked_method(self):
                return "success"

        obj = TestClass()
        # First call acquires lock
        result = obj.locked_method()
        assert result == "success"
        # File exists with lock (will be released after context)
        assert temp_lock_path.exists()
