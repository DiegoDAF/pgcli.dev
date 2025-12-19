"""Tests for pgcli_dump and pgcli_dumpall wrappers."""

import os
import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from pgcli.dump import (
    cli as dump_cli,
    find_pg_dump,
    parse_connection_args,
    build_tunneled_args,
)
from pgcli.dumpall import (
    cli as dumpall_cli,
    find_pg_dumpall,
    parse_connection_args as parse_connection_args_dumpall,
    build_tunneled_args as build_tunneled_args_dumpall,
)


class TestParseConnectionArgs:
    """Tests for parse_connection_args function."""

    def test_parse_host_short_option(self):
        """Test parsing -h option."""
        args = ["-h", "myhost.com", "-d", "mydb"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "myhost.com"
        assert has_host is True
        assert has_port is False

    def test_parse_host_long_option(self):
        """Test parsing --host option."""
        args = ["--host", "myhost.com", "-d", "mydb"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "myhost.com"
        assert has_host is True

    def test_parse_host_equals_format(self):
        """Test parsing --host=value format."""
        args = ["--host=myhost.com", "-d", "mydb"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "myhost.com"
        assert has_host is True

    def test_parse_port_short_option(self):
        """Test parsing -p option."""
        args = ["-h", "myhost.com", "-p", "5433"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert port == 5433
        assert has_port is True

    def test_parse_port_long_option(self):
        """Test parsing --port option."""
        args = ["--port", "5433"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert port == 5433
        assert has_port is True

    def test_parse_port_equals_format(self):
        """Test parsing --port=value format."""
        args = ["--port=5433"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert port == 5433
        assert has_port is True

    def test_default_values(self):
        """Test default values when no host/port specified."""
        args = ["-d", "mydb", "-U", "myuser"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == os.environ.get("PGHOST", "localhost")
        assert port == int(os.environ.get("PGPORT", 5432))
        assert has_host is False
        assert has_port is False

    def test_connection_string_in_dbname(self):
        """Test parsing connection string in --dbname."""
        args = ["-d", "host=db.example.com port=5433 dbname=mydb"]
        host, port, remaining, has_host, has_port = parse_connection_args(args)
        assert host == "db.example.com"
        assert port == 5433
        assert has_host is True
        assert has_port is True


class TestBuildTunneledArgs:
    """Tests for build_tunneled_args function."""

    def test_replace_host_short(self):
        """Test replacing -h with tunnel host."""
        args = ["-h", "original.host", "-d", "mydb"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "original.host", 5432, True, False)
        assert "-h" in result
        idx = result.index("-h")
        assert result[idx + 1] == "127.0.0.1"

    def test_replace_port_short(self):
        """Test replacing -p with tunnel port."""
        args = ["-h", "host", "-p", "5432"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "host", 5432, True, True)
        assert "-p" in result
        idx = result.index("-p")
        assert result[idx + 1] == "12345"

    def test_add_host_port_when_missing(self):
        """Test adding host/port when not in original args."""
        args = ["-d", "mydb"]
        result = build_tunneled_args(args, "127.0.0.1", 12345, "host", 5432, False, False)
        assert "-h" in result
        assert "127.0.0.1" in result
        assert "-p" in result
        assert "12345" in result


class TestFindExecutables:
    """Tests for finding pg_dump and pg_dumpall executables."""

    def test_find_pg_dump_in_path(self):
        """Test that pg_dump can be found."""
        # This test just verifies the function doesn't crash
        result = find_pg_dump()
        assert result.endswith("pg_dump")

    def test_find_pg_dumpall_in_path(self):
        """Test that pg_dumpall can be found."""
        result = find_pg_dumpall()
        assert result.endswith("pg_dumpall")


class TestDumpCli:
    """Tests for pgcli_dump CLI."""

    def test_help(self):
        """Test --help option."""
        runner = CliRunner()
        result = runner.invoke(dump_cli, ["--help"])
        assert result.exit_code == 0
        assert "pg_dump wrapper with SSH tunnel support" in result.output
        assert "--ssh-tunnel" in result.output

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_passthrough_args(self, mock_config, mock_run):
        """Test that pg_dump args are passed through."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-h", "localhost", "-d", "mydb", "-F", "c"])

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "-F" in cmd
        assert "c" in cmd

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    @patch("pgcli.dump.get_tunnel_manager_from_config")
    def test_with_ssh_tunnel_option(self, mock_tunnel_manager, mock_config, mock_run):
        """Test --ssh-tunnel option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 12345)
        mock_tunnel_manager.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(
            dump_cli,
            ["--ssh-tunnel", "user@bastion", "-h", "db.internal", "-d", "mydb"],
        )

        mock_tunnel_manager.assert_called_once()
        mock_manager.start_tunnel.assert_called_once()

    @patch("pgcli.dump.subprocess.run")
    @patch("pgcli.dump.get_config")
    def test_exit_code_passthrough(self, mock_config, mock_run):
        """Test that exit code is passed through from pg_dump."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=1)

        runner = CliRunner()
        result = runner.invoke(dump_cli, ["-d", "nonexistent"])

        assert result.exit_code == 1


class TestDumpallCli:
    """Tests for pgcli_dumpall CLI."""

    def test_help(self):
        """Test --help option."""
        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["--help"])
        assert result.exit_code == 0
        assert "pg_dumpall wrapper with SSH tunnel support" in result.output
        assert "--ssh-tunnel" in result.output

    @patch("pgcli.dumpall.subprocess.run")
    @patch("pgcli.dumpall.get_config")
    def test_passthrough_args(self, mock_config, mock_run):
        """Test that pg_dumpall args are passed through."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["-h", "localhost", "-g", "-f", "globals.sql"])

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "-g" in cmd
        assert "-f" in cmd
        assert "globals.sql" in cmd

    @patch("pgcli.dumpall.subprocess.run")
    @patch("pgcli.dumpall.get_config")
    @patch("pgcli.dumpall.get_tunnel_manager_from_config")
    def test_with_ssh_tunnel_option(self, mock_tunnel_manager, mock_config, mock_run):
        """Test --ssh-tunnel option."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)
        mock_manager = MagicMock()
        mock_manager.start_tunnel.return_value = ("127.0.0.1", 12345)
        mock_tunnel_manager.return_value = mock_manager

        runner = CliRunner()
        result = runner.invoke(
            dumpall_cli,
            ["--ssh-tunnel", "user@bastion", "-h", "db.internal", "-g"],
        )

        mock_tunnel_manager.assert_called_once()
        mock_manager.start_tunnel.assert_called_once()

    @patch("pgcli.dumpall.subprocess.run")
    @patch("pgcli.dumpall.get_config")
    def test_globals_only_option(self, mock_config, mock_run):
        """Test -g/--globals-only option passthrough."""
        mock_config.return_value = {}
        mock_run.return_value = MagicMock(returncode=0)

        runner = CliRunner()
        result = runner.invoke(dumpall_cli, ["-h", "localhost", "-g"])

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "-g" in cmd
