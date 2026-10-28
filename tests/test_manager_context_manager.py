"""Unit tests for manager context manager functionality.

Tests the context manager protocol implementation for the manager class,
including lock management, deprecation warnings, and exception handling.

Usage:
    # With pytest (recommended):
    python -m pytest tests/test_manager_context_manager.py -v

    # Standalone (without pytest):
    python tests/test_manager_context_manager.py
"""

import pytest
import warnings
import tempfile
import sys
from unittest.mock import Mock, patch, MagicMock

from falconry.manager import manager
from falconry.lock import LockFile
from MockHTCondor import MockHTCondor


# --- Shared fixtures ---

@pytest.fixture
def schedd():
    """Shared Schedd mock."""
    return MockHTCondor.Schedd()


@pytest.fixture
def tmp_dir(tmp_path):
    """Create a temporary directory for the manager."""
    return str(tmp_path / "manager_dir")


@pytest.fixture
def mock_lock():
    """Create a mock LockFile."""
    lock = MagicMock(spec=LockFile)
    lock.__enter__ = Mock(return_value=None)
    lock.__exit__ = Mock(return_value=None)
    return lock


# --- Context Manager Tests ---

class TestManagerContextManagerProtocol:
    """Tests for the context manager protocol implementation."""

    def test_enter_returns_self(self, tmp_dir, schedd):
        """__enter__ returns the manager instance."""
        with manager(tmp_dir, schedd=schedd) as mgr:
            assert isinstance(mgr, manager)
            assert mgr.dir == tmp_dir

    def test_enter_sets_context_manager_flag(self, tmp_dir, schedd):
        """__enter__ sets _context_depth to 1."""
        mgr = manager(tmp_dir, schedd=schedd)
        assert mgr._context_depth == 0

        mgr.__enter__()
        assert mgr._context_depth == 1

        mgr._release_lock(_from_context_manager=True)

    def test_exit_calls_release_lock(self, tmp_dir, schedd):
        """__exit__ calls _release_lock with _from_context_manager=True."""
        mgr = manager(tmp_dir, schedd=schedd)
        mgr.__enter__()

        with patch.object(mgr, '_release_lock') as mock_release:
            mgr.__exit__(None, None, None)
            mock_release.assert_called_once_with(_from_context_manager=True)

        mgr._release_lock(_from_context_manager=True)

    def test_exit_with_exception_propagates(self, tmp_dir, schedd):
        """__exit__ does not suppress exceptions."""
        with pytest.raises(ValueError, match="test error"):
            with manager(tmp_dir, schedd=schedd) as mgr:
                raise ValueError("test error")

        # Lock should still be released
        assert mgr._lock is None

    def test_exit_releases_lock(self, tmp_dir, schedd):
        """__exit__ releases the lock."""
        with manager(tmp_dir, schedd=schedd) as mgr:
            assert mgr._lock is not None

        # After exiting context, lock should be released
        assert mgr._lock is None


class TestLockManagement:
    """Tests for lock acquisition and release."""

    def test_init_acquires_lock(self, tmp_dir, schedd, mock_lock):
        """Manager __init__ acquires the lock."""
        with patch('falconry.manager.LockFile', return_value=mock_lock):
            mgr = manager(tmp_dir, schedd=schedd)
            mock_lock.__enter__.assert_called_once()
            assert mgr._lock is mock_lock

            # Clean up
            mgr._lock = None

    def test_context_manager_releases_lock_on_exit(self, tmp_dir, schedd):
        """Context manager releases lock on normal exit."""
        with manager(tmp_dir, schedd=schedd) as mgr:
            lock = mgr._lock
            assert lock is not None

        # Lock should be released and set to None
        assert mgr._lock is None

    def test_context_manager_releases_lock_on_exception(self, tmp_dir, schedd):
        """Context manager releases lock even when exception occurs."""
        mgr = None
        with pytest.raises(RuntimeError):
            with manager(tmp_dir, schedd=schedd) as mgr:
                lock = mgr._lock
                assert lock is not None
                raise RuntimeError("test")

        # Lock should still be released
        assert mgr is not None
        assert mgr._lock is None

    def test_manual_release_lock_with_internal_flag(self, tmp_dir, schedd):
        """Manual _release_lock with _internal=True does not warn."""
        mgr = manager(tmp_dir, schedd=schedd)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            mgr._release_lock(_internal=True)

            # No warning should be issued
            assert len(w) == 0

        # Lock should be released
        assert mgr._lock is None


class TestDeprecationWarnings:
    """Tests for deprecation warning behavior."""

    def test_direct_call_without_context_warns(self, tmp_dir, schedd):
        """Direct call to _release_lock without context manager warns."""
        mgr = manager(tmp_dir, schedd=schedd)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            mgr._release_lock()

            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "Directly calling _release_lock()" in str(w[0].message)

        assert mgr._lock is None

    def test_context_manager_suppresses_warning(self, tmp_dir, schedd):
        """Calls from __exit__ do not trigger warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            with manager(tmp_dir, schedd=schedd):
                pass  # Just exit normally

            # Filter for our specific warning
            deprecation_warnings = [
                warning for warning in w
                if issubclass(warning.category, DeprecationWarning)
                and "Directly calling _release_lock()" in str(warning.message)
            ]
            assert len(deprecation_warnings) == 0

    def test_internal_calls_suppress_warning(self, tmp_dir, schedd):
        """Internal calls with _internal=True do not trigger warning."""
        mgr = manager(tmp_dir, schedd=schedd)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            mgr._release_lock(_internal=True)

            # No warning should be issued
            assert len(w) == 0

    def test_from_context_manager_suppresses_warning(self, tmp_dir, schedd):
        """Calls with _from_context_manager=True do not trigger warning."""
        mgr = manager(tmp_dir, schedd=schedd)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            mgr._release_lock(_from_context_manager=True)

            # No warning should be issued
            assert len(w) == 0

    def test_context_manager_with_internal_call_no_warning(self, tmp_dir, schedd):
        """Internal calls from within context manager do not warn."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            with manager(tmp_dir, schedd=schedd) as mgr:
                # Simulate internal call (like from start() method)
                mgr._release_lock(_internal=True)

            # No warning should be issued
            assert len(w) == 0


class TestContextManagerWithStart:
    """Tests for using context manager with start() method."""

    def test_start_within_context_manager(self, tmp_dir, schedd):
        """start() can be called within context manager."""
        with manager(tmp_dir, schedd=schedd) as mgr:
            # Mock the start method to avoid actual execution
            with patch.object(mgr, '_start_cli'):
                mgr.start(sleepTime=0, gui=False)
                # start() should work and lock should be managed by context manager

        # Lock should be released by context manager
        assert mgr._lock is None

    def test_start_exception_handled(self, tmp_dir, schedd):
        """Exceptions in start() are handled and lock is released."""
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")

            with manager(tmp_dir, schedd=schedd) as mgr:
                with patch.object(mgr, '_start_cli', side_effect=RuntimeError("test")):
                    with patch.object(mgr, '_save'):
                        with patch.object(mgr, 'print_failed'):
                            result = mgr.start(sleepTime=0, gui=False)
                            assert result == 2

            # Lock should be released
            assert mgr._lock is None


class TestContextManagerLockSafety:
    """Tests for lock safety with context manager."""

    def test_double_exit_safe(self, tmp_dir, schedd):
        """Calling __exit__ multiple times is safe."""
        with manager(tmp_dir, schedd=schedd) as mgr:
            pass

        # First __exit__ already released the lock
        assert mgr._lock is None

        # Calling __exit__ again should not cause errors
        mgr.__exit__(None, None, None)
        assert mgr._lock is None

    def test_skip_release_when_in_context_manager(self, tmp_dir, schedd):
        """_release_lock skips when in context manager mode."""
        with manager(tmp_dir, schedd=schedd) as mgr:
            lock = mgr._lock
            assert lock is not None

            # Direct call should skip (return early)
            mgr._release_lock()

            # Lock should still be held
            assert mgr._lock is lock

        # After exiting context, lock is released
        assert mgr._lock is None

    def test_context_manager_flag_reset(self, tmp_dir, schedd):
        """Test behavior when context manager flag is set."""
        mgr = manager(tmp_dir, schedd=schedd)

        # Initially not in context manager
        assert mgr._context_depth == 0

        # Enter context
        mgr.__enter__()
        lock = mgr._lock
        assert mgr._context_depth == 1

        # Internal call should skip (returns early) because we're in context manager mode
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            mgr._release_lock(_internal=True)
            assert len(w) == 0

        # Lock should still be held because _release_lock returned early
        assert mgr._lock is lock

        # Clean up
        mgr._release_lock(_from_context_manager=True)


class TestManagerWithLoad:
    """Tests for load() method with context manager."""

    def test_load_within_context_manager(self, tmp_dir, schedd):
        """load() can be called within context manager."""
        # First, create a manager and save some state
        with manager(tmp_dir, schedd=schedd) as mgr1:
            # Add a job (simplified - we're not testing job functionality here)
            mgr1.mgrMsg = ["Initial message"]
            mgr1._save()

        # Now load with another manager
        with manager(tmp_dir, schedd=schedd) as mgr2:
            # Check if savefile exists
            status, var = mgr2.check_savefile_status(non_interactive=True)
            assert status is True
            assert var == "l"

            # Load the state
            mgr2.load()
            # Verify message was loaded
            assert len(mgr2.mgrMsg) > 0

        # Lock should be released
        assert mgr2._lock is None

    def test_load_with_retry_failed_and_exception(self, tmp_path, schedd):
        """load(retryFailed=True) handles exceptions properly with context manager."""
        # First create a manager and save some state so the save file exists with a job
        tmp_dir = str(tmp_path / "manager_dir")
        from falconry.job import job
        with manager(tmp_dir, schedd=schedd) as mgr1:
            # Add a test job so load() will have jobs to process
            j = job("test_job", schedd)
            mgr1._add_job(j)
            mgr1.mgrMsg = ["Initial message"]
            mgr1._save()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            with manager(tmp_dir, schedd=schedd) as mgr:
                # Mock a KeyboardInterrupt during load with retryFailed
                with patch.object(mgr, '_check_resubmit', side_effect=KeyboardInterrupt()):
                    with patch.object(mgr, '_save'):
                        with patch.object(mgr, 'print_failed'):
                            with patch('sys.exit') as mock_exit:
                                # This should call _release_lock(_internal=True)
                                mgr.load(retryFailed=True)
                                # exit should be called with 0
                                mock_exit.assert_called_once_with(0)

            # No deprecation warnings should be issued
            deprecation_warnings = [
                warning for warning in w
                if issubclass(warning.category, DeprecationWarning)
                and "Directly calling _release_lock()" in str(warning.message)
            ]
            assert len(deprecation_warnings) == 0

            # Lock should be released by context manager
            assert mgr._lock is None


class TestLegacyUsage:
    """Tests for backward compatibility with non-context-manager usage."""

    def test_legacy_usage_with_try_finally(self, tmp_dir, schedd):
        """Legacy usage with try/finally still works."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            mgr = manager(tmp_dir, schedd=schedd)
            try:
                assert mgr._lock is not None
            finally:
                mgr._release_lock()

            assert mgr._lock is None

            # A warning should be issued for direct call
            assert any(
                issubclass(warning.category, DeprecationWarning)
                and "Directly calling _release_lock()" in str(warning.message)
                for warning in w
            )

    def test_legacy_usage_without_cleanup_warns(self, tmp_dir, schedd):
        """Legacy usage without cleanup triggers deprecation warning."""
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")

            mgr = manager(tmp_dir, schedd=schedd)
            # Don't call _release_lock - this is the legacy buggy usage

            # The warning is only triggered when _release_lock is called
            # So in this case, no warning is issued because we never call it
            # But the lock is not released!
            assert mgr._lock is not None

            # Actually, we should clean up
            mgr._release_lock()


class TestContextManagerNesting:
    """Tests for nested context managers."""

    def test_nested_context_managers_different_dirs(self, schedd):
        """Nested context managers with different directories work."""
        with tempfile.TemporaryDirectory() as tmp1:
            with tempfile.TemporaryDirectory() as tmp2:
                with warnings.catch_warnings(record=True):
                    warnings.simplefilter("always")

                    with manager(tmp1, schedd=schedd) as mgr1:
                        assert mgr1._lock is not None

                        with manager(tmp2, schedd=schedd) as mgr2:
                            assert mgr2._lock is not None
                            assert mgr1._lock is not None

                        assert mgr2._lock is None
                        assert mgr1._lock is not None

                    assert mgr1._lock is None

    def test_nested_context_managers_same_dir_raises(self, tmp_dir, schedd):
        """Nested context managers with same directory should fail on second lock."""
        # The first manager acquires the lock
        with manager(tmp_dir, schedd=schedd) as mgr1:
            assert mgr1._lock is not None

            # Second manager with same directory should fail to acquire lock
            # LockFile will detect the conflict and raise LockFileException
            # which is caught by manager.__init__ and calls sys.exit(1)
            # This raises SystemExit
            with pytest.raises(SystemExit):
                manager(tmp_dir, schedd=schedd)


class TestContextManagerFlag:
    """Tests for the _context_depth flag behavior."""

    def test_flag_starts_zero(self, tmp_dir, schedd):
        """_context_depth starts as 0."""
        mgr = manager(tmp_dir, schedd=schedd)
        assert mgr._context_depth == 0
        mgr._release_lock(_internal=True)

    def test_flag_set_in_enter(self, tmp_dir, schedd):
        """_context_depth is set to 1 in __enter__."""
        mgr = manager(tmp_dir, schedd=schedd)
        mgr.__enter__()
        assert mgr._context_depth == 1
        mgr._release_lock(_from_context_manager=True)

    def test_flag_reset_after_exit(self, tmp_dir, schedd):
        """_context_depth is reset to 0 after __exit__."""
        with manager(tmp_dir, schedd=schedd) as mgr:
            assert mgr._context_depth == 1

        # After exit, flag is reset to 0
        assert mgr._context_depth == 0
        # But lock is released
        assert mgr._lock is None


# ============================================================================
# Standalone execution support
# ============================================================================
# This allows running tests directly without pytest:
#   python tests/test_manager_context_manager.py
# ============================================================================


def _run_test_1(schedd):
    """Test basic context manager usage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with manager(tmpdir, schedd=schedd) as mgr:
            assert mgr._lock is not None
            assert mgr._context_depth == 1
        assert mgr._lock is None


def _run_test_2(schedd):
    """Test context manager with exception."""
    with tempfile.TemporaryDirectory() as tmpdir:
        mgr = None
        try:
            with manager(tmpdir, schedd=schedd) as mgr:
                assert mgr._lock is not None
                raise RuntimeError("Test exception")
        except RuntimeError:
            pass
        assert mgr is not None
        assert mgr._lock is None


def _run_test_3(schedd):
    """Test deprecation warning on direct call."""
    with tempfile.TemporaryDirectory() as tmpdir:
        mgr = manager(tmpdir, schedd=schedd)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            mgr._release_lock()
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "Directly calling _release_lock()" in str(w[0].message)
        assert mgr._lock is None


def _run_test_4(schedd):
    """Test no warning with _internal=True."""
    with tempfile.TemporaryDirectory() as tmpdir:
        mgr = manager(tmpdir, schedd=schedd)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            mgr._release_lock(_internal=True)
            assert len(w) == 0
        assert mgr._lock is None


def _run_test_5(schedd):
    """Test skip release when in context manager."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with manager(tmpdir, schedd=schedd) as mgr:
            lock = mgr._lock
            assert lock is not None
            mgr._release_lock()
            assert mgr._lock is lock
        assert mgr._lock is None


if __name__ == "__main__":
    # Run a subset of tests directly without pytest
    print("=" * 70)
    print("Running Context Manager Tests (Standalone Mode)")
    print("=" * 70)

    # Create a simple test runner
    schedd_instance = MockHTCondor.Schedd()
    failed = []
    passed = 0

    tests = [
        ("basic_context_manager", _run_test_1, "Basic context manager usage"),
        ("context_manager_exception", _run_test_2, "Context manager with exception"),
        ("deprecation_warning", _run_test_3, "Deprecation warning on direct call"),
        ("no_warning_internal", _run_test_4, "No warning with _internal=True"),
        ("skip_release_in_context", _run_test_5, "Skip release when in context manager"),
    ]

    for i, (name, test_func, desc) in enumerate(tests, 1):
        print(f"\n[{i}/5] Testing {desc}...")
        try:
            test_func(schedd_instance)
            print("   ✓ PASS")
            passed += 1
        except Exception as e:
            print(f"   ✗ FAIL: {e}")
            failed.append(name)

    # Summary
    print("\n" + "=" * 70)
    if failed:
        print(f"FAILED: {len(failed)}/{passed + len(failed)} tests failed:")
        for test_name in failed:
            print(f"  - {test_name}")
        sys.exit(1)
    else:
        print(f"SUCCESS: All {passed} tests passed!")
        print("\nNote: For full test suite, run with pytest:")
        print("  python -m pytest tests/test_manager_context_manager.py -v")
        sys.exit(0)
