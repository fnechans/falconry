"""Tests for __main__.py utility functions.

Focused on bugs fixed in recent changes:
- get_name() edge cases (empty, long names, shell operators)
- parse_job() validation (empty lines, missing commands)
"""
import pytest
from falconry.__main__ import get_name, parse_job


class TestGetName:
    """Tests for get_name() - covers edge case bugs that were fixed."""

    def test_empty_string_returns_unnamed_job(self):
        """Empty string returns 'unnamed_job' (bug fix)."""
        with pytest.raises(ValueError, match="Command cannot be empty"):
            get_name("")

    def test_whitespace_only_returns_unnamed_job(self):
        """Whitespace-only returns 'unnamed_job' (bug fix)."""
        with pytest.raises(ValueError, match="Command cannot be empty"):
            get_name("   ")
            get_name("\t\n")

    def test_all_underscores_becomes_unnamed_job(self):
        """String of only underscores becomes 'unnamed_job' (bug fix)."""
        with pytest.raises(ValueError, match="Job name empty after sanitization"):
            get_name("___")

    def test_long_name_truncated(self):
        """Names > 100 chars are truncated with '_trunc' (new feature)."""
        long_name = "a" * 150
        result = get_name(long_name)
        assert len(result) == 106
        assert result.endswith("_trunc")
        assert result.startswith("a" * 100)

    def test_exactly_100_chars_not_truncated(self):
        """Exactly 100 char names not truncated."""
        name = "a" * 100
        assert get_name(name) == name

    def test_shell_operators_sanitized(self):
        """Shell operators are properly sanitized (new feature)."""
        assert get_name("cmd && other") == "cmd_other"
        assert get_name("cmd | other") == "cmd_other"
        assert get_name("cmd > file") == "cmd_file"
        assert get_name("cmd; other") == "cmd_other"


class TestParseJob:
    """Tests for parse_job() - covers validation bugs that were fixed."""

    def test_empty_line_raises(self):
        """Empty line raises ValueError (new validation)."""
        with pytest.raises(ValueError, match="Empty line"):
            parse_job("")

    def test_whitespace_only_raises(self):
        """Whitespace-only line raises ValueError (new validation)."""
        with pytest.raises(ValueError, match="Empty line"):
            parse_job("   ")

    def test_brackets_without_command_raises(self):
        """Brackets without command raises ValueError (new validation)."""
        with pytest.raises(ValueError, match="No command found"):
            parse_job("[my_job]")

    def test_empty_name_generates_from_command(self):
        """Empty name in brackets generates from command (new feature)."""
        name, cmd = parse_job("[] my_script.sh")
        assert name == "my_script_sh"
        assert cmd == "my_script.sh"

    def test_simple_command(self):
        """Simple command without brackets works."""
        name, cmd = parse_job("my_script.sh arg1")
        assert cmd == "my_script.sh arg1"
        assert name == "my_script_sh_arg1"

    def test_command_with_explicit_name(self):
        """Command with explicit name in brackets works."""
        name, cmd = parse_job("[my_job] my_script.sh")
        assert name == "my_job"
        assert cmd == "my_script.sh"
