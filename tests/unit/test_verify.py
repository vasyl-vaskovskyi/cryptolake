from __future__ import annotations

from click.testing import CliRunner


def test_verify_cli_help() -> None:
    from src.cli.verify import cli

    result = CliRunner().invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "Usage:" in result.output
    assert "Verify CryptoLake data captures." in result.output
