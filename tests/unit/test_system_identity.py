"""Unit tests for system identity helpers."""
import os
from unittest.mock import patch, mock_open


class TestGetHostBootId:
    """Tests for get_host_boot_id() helper."""

    def test_env_override_takes_precedence(self):
        """CRYPTOLAKE_TEST_BOOT_ID env var should override /proc lookup."""
        from src.common.system_identity import get_host_boot_id

        with patch.dict(os.environ, {"CRYPTOLAKE_TEST_BOOT_ID": "test-boot-id-123"}):
            result = get_host_boot_id()
        assert result == "test-boot-id-123"

    def test_reads_proc_boot_id(self):
        """Should read /proc/sys/kernel/random/boot_id on Linux."""
        from src.common.system_identity import get_host_boot_id

        fake_boot_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890\n"
        with patch.dict(os.environ, {}, clear=True):
            # Remove the test env var if present
            os.environ.pop("CRYPTOLAKE_TEST_BOOT_ID", None)
            with patch("builtins.open", mock_open(read_data=fake_boot_id)):
                result = get_host_boot_id()
        assert result == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

    def test_strips_whitespace_from_boot_id(self):
        """Boot ID from /proc may have trailing newline; should be stripped."""
        from src.common.system_identity import get_host_boot_id

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("CRYPTOLAKE_TEST_BOOT_ID", None)
            with patch("builtins.open", mock_open(read_data="  boot-id-with-spaces  \n")):
                result = get_host_boot_id()
        assert result == "boot-id-with-spaces"

    def test_fallback_when_proc_unavailable(self):
        """On non-Linux (e.g., macOS), should fall back to 'unknown'."""
        from src.common.system_identity import get_host_boot_id

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("CRYPTOLAKE_TEST_BOOT_ID", None)
            with patch("builtins.open", side_effect=FileNotFoundError):
                result = get_host_boot_id()
        assert result == "unknown"

    def test_fallback_on_permission_error(self):
        """If /proc is unreadable, should fall back to 'unknown'."""
        from src.common.system_identity import get_host_boot_id

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("CRYPTOLAKE_TEST_BOOT_ID", None)
            with patch("builtins.open", side_effect=PermissionError):
                result = get_host_boot_id()
        assert result == "unknown"

    def test_return_type_is_str(self):
        """get_host_boot_id() should always return a string."""
        from src.common.system_identity import get_host_boot_id

        with patch.dict(os.environ, {"CRYPTOLAKE_TEST_BOOT_ID": "abc"}):
            result = get_host_boot_id()
        assert isinstance(result, str)

    def test_env_override_strips_whitespace(self):
        """Env var value should also be stripped."""
        from src.common.system_identity import get_host_boot_id

        with patch.dict(os.environ, {"CRYPTOLAKE_TEST_BOOT_ID": "  trimmed  "}):
            result = get_host_boot_id()
        assert result == "trimmed"
