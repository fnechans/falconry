from pathlib import Path
from contextlib import nullcontext

import pytest

from pmux import __main__ as pmux_main
from pmux import ssh_check


def test_main_allows_empty_command_for_existing_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.setattr(pmux_main, "HOSTFILE_DIR", tmp_path)
    monkeypatch.setattr(pmux_main.os, "uname", lambda: type("U", (), {"nodename": "localnode"})())

    # Pretend an existing hostfile/session mapping already exists.
    (tmp_path / "job1.host").write_text("remote-node\n")

    attached = {"called": False, "job_id": None, "node": None}

    def fake_attach(job_id: str, node: str):
        attached["called"] = True
        attached["job_id"] = job_id
        attached["node"] = node
        return pmux_main.DummyReturn(0, "", "")

    monkeypatch.setattr(pmux_main, "attach_to_session", fake_attach)
    monkeypatch.setattr(pmux_main, "start_session", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("start_session should not be called")))

    monkeypatch.setattr(pmux_main.sys, "argv", ["pmux", "job1"])

    with pytest.raises(SystemExit) as exc_info:
        pmux_main.main()

    assert exc_info.value.code == 0
    assert attached["called"] is True
    assert attached["job_id"] == "job1"
    assert attached["node"] == "remote-node"


def test_attach_to_session_nonzero_when_remote_exitstatus_missing(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pmux_main.os, "uname", lambda: type("U", (), {"nodename": "localnode"})())

    class FakeChild:
        def __init__(self):
            self.exitstatus = None
            self.signalstatus = 15
            self.before = "remote out"
            self.after = "remote err"

        def expect(self, _pattern: str):
            return 0

        def interact(self):
            return None

    monkeypatch.setattr(pmux_main.pexpect, "spawn", lambda *args, **kwargs: FakeChild())

    result = pmux_main.attach_to_session("job1", "remote-node")

    assert result.returncode == 143
    assert result.stdout == "remote out"
    assert result.stderr == "remote err"


def test_attach_to_session_local_uses_terminal_subprocess(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pmux_main.os, "uname", lambda: type("U", (), {"nodename": "localnode"})())
    monkeypatch.setattr(pmux_main, "tmux_has_session", lambda _job_id: True)

    calls = []

    class FakeCompletedProcess:
        def __init__(self, returncode=0):
            self.returncode = returncode

    def fake_subprocess_run(command):
        calls.append(command)
        return FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(pmux_main.subprocess, "run", fake_subprocess_run)

    result = pmux_main.attach_to_session("job1", "localnode")

    assert calls == [["tmux", "attach", "-t", "job1"]]
    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""


def test_start_session_sets_trap_and_cleanup_hook(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    hostfile_dir = tmp_path / "hosts"
    logfile_dir = tmp_path / "logs"
    hostfile_dir.mkdir(parents=True)

    monkeypatch.setattr(pmux_main, "HOSTFILE_DIR", hostfile_dir)
    monkeypatch.setattr(pmux_main, "LOGFILE_DIR", logfile_dir)
    monkeypatch.setattr(pmux_main.os, "uname", lambda: type("U", (), {"nodename": "localnode"})())
    monkeypatch.setattr(pmux_main, "chdir", lambda _path: nullcontext())

    calls = []

    class FakeResult:
        def __init__(self, returncode=0, stdout="", stderr=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run_command_local(command):
        calls.append(command)
        return FakeResult(returncode=0)

    monkeypatch.setattr(pmux_main, "run_command_local", fake_run_command_local)

    assert pmux_main.start_session("job1", "echo hello", verbose=False) is True

    assert len(calls) >= 6
    new_session_cmd = calls[0]
    session_opt_cmd = calls[1]
    show_hook_cmd = calls[2]
    hook_cmd = calls[3]
    send_keys_cmd = calls[4]
    pipe_cmd = calls[5]

    assert new_session_cmd[:3] == ["tmux", "new-session", "-d"]

    assert session_opt_cmd[:4] == ["tmux", "set-option", "-t", "job1"]
    assert session_opt_cmd[4] == "@pmux_hostfile"
    assert session_opt_cmd[5].endswith("job1.host")

    assert show_hook_cmd == ["tmux", "show-hooks", "-g", "session-closed"]

    assert hook_cmd[:3] == ["tmux", "set-hook", "-ag"]
    assert hook_cmd[3] == "session-closed"
    assert "@pmux_hostfile" in hook_cmd[4]

    assert send_keys_cmd[:4] == ["tmux", "send-keys", "-t", "job1"]
    assert "trap" in send_keys_cmd[4]
    assert str(hostfile_dir / "job1.host") in send_keys_cmd[4]
    assert "EXIT HUP INT TERM" in send_keys_cmd[4]
    assert "echo hello" in send_keys_cmd[4]
    assert send_keys_cmd[5] == "C-m"

    assert pipe_cmd[:4] == ["tmux", "pipe-pane", "-t", "job1"]
    assert str(logfile_dir / "job1.log") in pipe_cmd[4]


def test_start_session_cleans_up_when_send_keys_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    hostfile_dir = tmp_path / "hosts"
    logfile_dir = tmp_path / "logs"
    hostfile_dir.mkdir(parents=True)

    monkeypatch.setattr(pmux_main, "HOSTFILE_DIR", hostfile_dir)
    monkeypatch.setattr(pmux_main, "LOGFILE_DIR", logfile_dir)
    monkeypatch.setattr(pmux_main.os, "uname", lambda: type("U", (), {"nodename": "localnode"})())
    monkeypatch.setattr(pmux_main, "chdir", lambda _path: nullcontext())

    calls = []

    class FakeResult:
        def __init__(self, returncode=0, stdout="", stderr=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run_command_local(command):
        calls.append(command)
        if command[:3] == ["tmux", "send-keys", "-t"]:
            return FakeResult(returncode=1, stderr="send failed")
        return FakeResult(returncode=0)

    monkeypatch.setattr(pmux_main, "run_command_local", fake_run_command_local)

    assert pmux_main.start_session("job1", "echo hello", verbose=False) is False

    assert ["tmux", "kill-session", "-t", "job1"] in calls
    assert not (hostfile_dir / "job1.host").exists()


def test_start_session_cleans_up_when_pipe_pane_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    hostfile_dir = tmp_path / "hosts"
    logfile_dir = tmp_path / "logs"
    hostfile_dir.mkdir(parents=True)

    monkeypatch.setattr(pmux_main, "HOSTFILE_DIR", hostfile_dir)
    monkeypatch.setattr(pmux_main, "LOGFILE_DIR", logfile_dir)
    monkeypatch.setattr(pmux_main.os, "uname", lambda: type("U", (), {"nodename": "localnode"})())
    monkeypatch.setattr(pmux_main, "chdir", lambda _path: nullcontext())

    calls = []

    class FakeResult:
        def __init__(self, returncode=0, stdout="", stderr=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run_command_local(command):
        calls.append(command)
        if command[:4] == ["tmux", "pipe-pane", "-t", "job1"]:
            return FakeResult(returncode=1, stderr="pipe failed")
        return FakeResult(returncode=0)

    monkeypatch.setattr(pmux_main, "run_command_local", fake_run_command_local)

    assert pmux_main.start_session("job1", "echo hello", verbose=False) is False

    assert ["tmux", "kill-session", "-t", "job1"] in calls
    assert not (hostfile_dir / "job1.host").exists()


# ============================================================================
# New tests for fixed issues
# ============================================================================


def test_command_parsing_with_remainder(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Test that command parsing uses REMAINDER to capture all remaining args."""
    hostfile_dir = tmp_path / "hosts"
    hostfile_dir.mkdir(parents=True)
    
    monkeypatch.setattr(pmux_main, "HOSTFILE_DIR", hostfile_dir)
    monkeypatch.setattr(pmux_main.os, "uname", lambda: type("U", (), {"nodename": "localnode"})())
    monkeypatch.setattr(pmux_main, "chdir", lambda _path: nullcontext())
    
    calls = []
    
    class FakeResult:
        def __init__(self, returncode=0, stdout="", stderr=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr
    
    def fake_run_command_local(command):
        calls.append(command)
        return FakeResult(returncode=0)
    
    monkeypatch.setattr(pmux_main, "run_command_local", fake_run_command_local)
    monkeypatch.setattr(pmux_main, "start_session", lambda *args, **kwargs: True)
    monkeypatch.setattr(pmux_main, "attach_to_session", lambda *args, **kwargs: pmux_main.DummyReturn(0, "", ""))
    
    # Test with command containing flags
    monkeypatch.setattr(pmux_main.sys, "argv", ["pmux", "job1", "--", "echo", "hello", "-f", "file.txt"])
    
    with pytest.raises(SystemExit) as exc_info:
        pmux_main.main()
    
    assert exc_info.value.code == 0


def test_get_hostfile_validation():
    """Test that get_hostfile validates job_id format."""
    # Valid job IDs
    assert pmux_main.get_hostfile("job1").name == "job1.host"
    assert pmux_main.get_hostfile("job-2").name == "job-2.host"
    assert pmux_main.get_hostfile("job_3").name == "job_3.host"
    assert pmux_main.get_hostfile("Job4").name == "Job4.host"
    assert pmux_main.get_hostfile("job_5-6").name == "job_5-6.host"
    
    # Invalid job IDs
    with pytest.raises(ValueError):
        pmux_main.get_hostfile("invalid@job")
    
    with pytest.raises(ValueError):
        pmux_main.get_hostfile("job id")  # space
    
    with pytest.raises(ValueError):
        pmux_main.get_hostfile("job/id")  # slash
    
    with pytest.raises(ValueError):
        pmux_main.get_hostfile("")  # empty


# ============================================================================
# Tests for ssh_check module
# ============================================================================


def test_validate_hostfile_filename():
    """Test hostfile filename validation."""
    assert ssh_check.validate_hostfile_filename("/path/to/job1.host", "job1") is True
    assert ssh_check.validate_hostfile_filename("/path/to/job1.host", "job2") is False
    assert ssh_check.validate_hostfile_filename("/path/to/job-2.host", "job-2") is True
    assert ssh_check.validate_hostfile_filename("/path/to/job1.host", "wrong_id") is False


def test_check_session_invalid_job_id(tmp_path: Path):
    """Test check_session with invalid job_id format."""
    # Job ID with special characters should be rejected
    result = ssh_check.check_session(str(tmp_path / "bad@id.host"), "bad@id", "localnode", debug=False)
    assert result == 1
    
    # Job ID with space should be rejected
    result = ssh_check.check_session(str(tmp_path / "bad id.host"), "bad id", "localnode", debug=False)
    assert result == 1


def test_read_hostfile_node(tmp_path: Path):
    """Test reading node from hostfile."""
    # Valid hostfile
    hostfile = tmp_path / "job1.host"
    hostfile.write_text("node1.cern.ch\n")
    assert ssh_check.read_hostfile_node(str(hostfile)) == "node1.cern.ch"
    
    # Empty hostfile
    hostfile2 = tmp_path / "job2.host"
    hostfile2.write_text("")
    assert ssh_check.read_hostfile_node(str(hostfile2)) is None
    
    # Non-existent hostfile
    assert ssh_check.read_hostfile_node(str(tmp_path / "nonexistent.host")) is None
    
    # Whitespace only
    hostfile3 = tmp_path / "job3.host"
    hostfile3.write_text("   \n\t\n   ")
    assert ssh_check.read_hostfile_node(str(hostfile3)) is None


def test_check_session_valid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test check_session with valid hostfile and matching node."""
    hostfile = tmp_path / "job1.host"
    hostfile.write_text("localnode\n")
    
    # Mock local node
    monkeypatch.setattr(ssh_check.os, "uname", lambda: type("U", (), {"nodename": "localnode"})())
    
    # Mock tmux has-session to return success
    def fake_run(command, **kwargs):
        if "has-session" in command:
            return type("Result", (), {"returncode": 0})()
        return type("Result", (), {"returncode": 1})()
    
    monkeypatch.setattr(ssh_check.subprocess, "run", fake_run)
    
    result = ssh_check.check_session(str(hostfile), "job1", "localnode", debug=False)
    assert result == 0


def test_check_session_mismatched_node(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test check_session with mismatched node."""
    hostfile = tmp_path / "job1.host"
    hostfile.write_text("remote-node\n")
    
    monkeypatch.setattr(ssh_check.os, "uname", lambda: type("U", (), {"nodename": "localnode"})())
    
    result = ssh_check.check_session(str(hostfile), "job1", "localnode", debug=False)
    assert result == 1


def test_check_session_stale_hostfile_removed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test that stale hostfile is removed when session doesn't exist."""
    hostfile = tmp_path / "job1.host"
    hostfile.write_text("localnode\n")
    
    monkeypatch.setattr(ssh_check.os, "uname", lambda: type("U", (), {"nodename": "localnode"})())
    
    # Mock tmux has-session to return failure (session doesn't exist)
    def fake_run(command, **kwargs):
        if "has-session" in command:
            return type("Result", (), {"returncode": 1})()
        return type("Result", (), {"returncode": 0})()
    
    monkeypatch.setattr(ssh_check.subprocess, "run", fake_run)
    
    result = ssh_check.check_session(str(hostfile), "job1", "localnode", debug=False)
    assert result == 0
    assert not hostfile.exists()  # Should be removed


def test_check_session_invalid_hostfile_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test check_session with invalid hostfile name."""
    hostfile = tmp_path / "invalid_name.host"
    hostfile.write_text("localnode\n")
    
    result = ssh_check.check_session(str(hostfile), "job1", "localnode", debug=False)
    assert result == 1


def test_check_session_empty_hostfile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test check_session with empty hostfile."""
    hostfile = tmp_path / "job1.host"
    hostfile.write_text("")
    
    result = ssh_check.check_session(str(hostfile), "job1", "localnode", debug=False)
    assert result == 1


def test_check_session_nonexistent_hostfile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test check_session with non-existent hostfile."""
    result = ssh_check.check_session(str(tmp_path / "job1.host"), "job1", "localnode", debug=False)
    assert result == 0  # Non-existent hostfile is OK (session not started yet)
