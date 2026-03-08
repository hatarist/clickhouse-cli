import pytest
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from clickhouse_cli.cli import run_cli
from clickhouse_cli.clickhouse.client import Client


def test_main_help():
    # Call with the --help option as a basic sanity check.
    with pytest.raises(SystemExit) as exinfo:
        run_cli(["--help", ])
    assert exinfo.value.code == 0


def test_custom_headers_sent_with_request():
    """tests -H --header arguments on CLI"""
    runner = CliRunner()

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.elapsed.total_seconds.return_value = 0.1
    mock_response.text = ""

    captured = {}

    def fake_request(method, url, **kwargs):
        captured["headers"] = kwargs.get("headers", {})
        return mock_response

    with patch("requests.Session.request", side_effect=fake_request):
        runner.invoke(run_cli, [
            "-h", "localhost",
            "-p", "8123",
            "-H", "X-My-Header: test-value",
            "-H", "X-Another: hello",
            "-q", "SELECT 1",
        ])

    assert captured["headers"].get("X-My-Header") == "test-value"
    assert captured["headers"].get("X-Another") == "hello"


def test_custom_headers_stored_on_client():
    """tests headers suppliet for client object"""
    client = Client(
        url="http://localhost:8123/",
        user="default",
        password="",
        database="default",
        cookie=None,
        headers={"X-Custom": "value"},
    )
    assert client.headers == {"X-Custom": "value"}

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.elapsed.total_seconds.return_value = 0.0
    mock_response.text = ""

    captured = {}

    def fake_request(method, url, **kwargs):
        captured["headers"] = kwargs.get("headers", {})
        return mock_response

    with patch("requests.Session.request", side_effect=fake_request):
        client._query("GET", "SELECT 1", {}, fmt="Null", stream=False)

    assert captured["headers"].get("X-Custom") == "value"


def test_custom_headers_override_defaults():
    """user supplied headers override defaults"""
    client = Client(
        url="http://localhost:8123/",
        user="default",
        password="",
        database="default",
        cookie=None,
        headers={"User-Agent": "my-custom-agent"},
    )

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.elapsed.total_seconds.return_value = 0.0
    mock_response.text = ""

    captured = {}

    def fake_request(method, url, **kwargs):
        captured["headers"] = kwargs.get("headers", {})
        return mock_response

    with patch("requests.Session.request", side_effect=fake_request):
        client._query("GET", "SELECT 1", {}, fmt="Null", stream=False)

    assert captured["headers"]["User-Agent"] == "my-custom-agent"
