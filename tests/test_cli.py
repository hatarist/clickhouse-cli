import pytest

from clickhouse_cli.cli import run_cli


def test_main_help():
    # Call with the --help option as a basic sanity check.
    with pytest.raises(SystemExit) as exinfo:
        run_cli(["--help", ])
    assert exinfo.value.code == 0
