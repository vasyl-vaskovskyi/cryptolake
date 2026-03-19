from __future__ import annotations

import json
import os
from pathlib import Path

import yaml

from src.common.config import load_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read_text(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text()


def _read_yaml(relative_path: str) -> dict:
    return yaml.safe_load(_read_text(relative_path))


class TestDockerIgnore:
    def test_dockerignore_excludes_non_build_directories(self) -> None:
        content = _read_text(".dockerignore")
        for excluded in (".git", "tests", "docs", "infra", "__pycache__", ".venv", ".env"):
            assert excluded in content, f"{excluded} should be excluded in .dockerignore"


class TestDockerfiles:
    def test_service_dockerfiles_use_uv_and_non_root_runtime(self) -> None:
        collector = _read_text("Dockerfile.collector")
        writer = _read_text("Dockerfile.writer")

        assert "FROM python:3.12.7-slim AS base" in collector
        assert "RUN pip install uv" in collector
        assert 'CMD ["/app/.venv/bin/python", "-m", "src.collector.main"]' in collector
        assert "USER cryptolake" in collector

        assert "FROM python:3.12.7-slim AS base" in writer
        assert "RUN pip install uv" in writer
        assert 'CMD ["/app/.venv/bin/python", "-m", "src.writer.main"]' in writer
        assert "USER cryptolake" in writer


class TestRuntimeConfigs:
    def test_runtime_configs_load_expected_profiles(self) -> None:
        production = load_config(PROJECT_ROOT / "config" / "config.yaml")
        example = load_config(PROJECT_ROOT / "config" / "config.example.yaml")
        test = load_config(PROJECT_ROOT / "config" / "config.test.yaml")

        assert production.database.url.startswith("postgresql://")
        assert example.database.url.startswith("postgresql://")
        assert production.redpanda.brokers == ["redpanda:9092"]
        assert production.exchanges.binance.symbols == ["btcusdt"]
        assert example.exchanges.binance.symbols == ["btcusdt"]

        assert test.exchanges.binance.symbols == ["btcusdt"]
        assert test.exchanges.binance.depth.snapshot_interval == "30s"
        assert test.exchanges.binance.open_interest.poll_interval == "30s"
        assert test.writer.flush_messages == 100
        assert test.writer.flush_interval_seconds == 10


class TestComposeStacks:
    def test_main_compose_wires_full_stack_with_isolated_networks(self) -> None:
        compose = _read_yaml("docker-compose.yml")
        services = compose["services"]

        assert set(services) == {
            "postgres",
            "redpanda",
            "collector",
            "writer",
            "prometheus",
            "grafana",
            "alertmanager",
        }

        assert services["collector"]["build"]["context"] == "."
        assert services["collector"]["build"]["dockerfile"] == "Dockerfile.collector"
        assert set(services["collector"]["networks"]) == {"cryptolake_internal", "collector_egress"}

        assert services["writer"]["build"]["context"] == "."
        assert services["writer"]["build"]["dockerfile"] == "Dockerfile.writer"
        assert set(services["writer"]["networks"]) == {"cryptolake_internal", "host_access"}
        assert services["writer"]["depends_on"]["postgres"]["condition"] == "service_healthy"
        assert "${HOST_DATA_DIR:-/data}:${HOST_DATA_DIR:-/data}" in services["writer"]["volumes"]
        assert "HOST_DATA_DIR=${HOST_DATA_DIR:-/data}" in services["writer"]["environment"]

        assert compose["networks"]["cryptolake_internal"]["internal"] is True
        assert "host_access" in compose["networks"]
        assert "collector_egress" in compose["networks"]
        assert "alertmanager_egress" in compose["networks"]

        assert set(compose["volumes"]) == {
            "postgres_data",
            "redpanda_data",
            "prometheus_data",
        }

        # All services must have health checks
        for name in services:
            assert "healthcheck" in services[name], f"{name} is missing a healthcheck"

    def test_test_compose_uses_fast_config_and_timeout_wrappers(self) -> None:
        compose = _read_yaml("docker-compose.test.yml")
        services = compose["services"]

        for name in ("postgres", "redpanda", "collector", "writer", "prometheus", "grafana", "alertmanager"):
            assert services[name]["extends"]["file"] == "docker-compose.yml"
            assert services[name]["extends"]["service"] == name

        # Verify explicit network assignments (extends does not inherit networks)
        assert set(services["collector"]["networks"]) == {"cryptolake_internal", "collector_egress"}
        assert set(services["writer"]["networks"]) == {"cryptolake_internal"}
        assert set(services["alertmanager"]["networks"]) == {"cryptolake_internal", "alertmanager_egress"}
        assert "${TEST_DATA_DIR:-/data}:${TEST_DATA_DIR:-/data}" in services["writer"]["volumes"]
        assert "HOST_DATA_DIR=${TEST_DATA_DIR:-/data}" in services["writer"]["environment"]

        collector_env = set(services["collector"]["environment"])
        writer_env = set(services["writer"]["environment"])
        assert "CONFIG_PATH=/app/config/config.test.yaml" in collector_env
        assert "CONFIG_PATH=/app/config/config.test.yaml" in writer_env
        assert any("TEST_DURATION_SECONDS" in item for item in collector_env)
        assert any("TEST_DURATION_SECONDS" in item for item in writer_env)
        assert "timeout" in " ".join(services["collector"]["command"])
        assert "timeout" in " ".join(services["writer"]["command"])
        assert "NET_ADMIN" in services["collector"].get("cap_add", [])


class TestObservabilityAssets:
    def test_prometheus_alertmanager_and_dashboard_assets_cover_expected_signals(self) -> None:
        prometheus = _read_yaml("infra/prometheus/prometheus.yml")
        alert_rules = _read_yaml("infra/prometheus/alert_rules.yml")
        alertmanager = _read_yaml("infra/alertmanager/alertmanager.yml")
        datasources = _read_yaml("infra/grafana/provisioning/datasources/datasources.yml")
        dashboards = _read_yaml("infra/grafana/provisioning/dashboards/dashboards.yml")
        dashboard = json.loads(_read_text("infra/grafana/dashboards/cryptolake.json"))

        job_names = {job["job_name"] for job in prometheus["scrape_configs"]}
        assert job_names == {"collector", "writer", "redpanda"}
        assert prometheus["alerting"]["alertmanagers"][0]["static_configs"][0]["targets"] == ["alertmanager:9093"]

        rules = alert_rules["groups"][0]["rules"]
        alert_names = {rule["alert"] for rule in rules}
        assert alert_names == {
            "GapDetected",
            "ConnectionLost",
            "WriterLagging",
            "WriterLagCritical",
            "SnapshotsFailing",
            "DiskAlmostFull",
            "DiskCritical",
            "HighLatency",
            "NTPDrift",
            "RedpandaBufferHigh",
            "MessagesDropped",
        }

        assert datasources["datasources"][0]["url"] == "http://prometheus:9090"
        assert dashboards["providers"][0]["options"]["path"] == "/var/lib/grafana/dashboards"

        panel_titles = {panel["title"] for panel in dashboard["panels"]}
        assert panel_titles == {
            "Message Throughput",
            "Exchange Latency Heatmap",
            "Consumer Lag",
            "Connection Status",
            "Gap Timeline",
            "Disk Usage",
            "Snapshot Health",
            "Compression Efficiency",
        }

        receiver = alertmanager["receivers"][0]["webhook_configs"][0]
        assert receiver["url"] == "${WEBHOOK_URL}"
        assert receiver["send_resolved"] is True


class TestAwsDeploymentAssets:
    def test_cloudformation_template_bootstrap_is_safe_and_architecture_consistent(self) -> None:
        template = _read_text("infra/aws/cryptolake-ec2.yaml")

        assert "InstanceArchitecture:" in template
        assert "al2023-ami-kernel-default-arm64" in template
        assert "al2023-ami-kernel-default-x86_64" in template
        assert "AvailabilityZone: !Select [0, !GetAZs '']" in template
        assert "!GetAtt EC2Instance.AvailabilityZone" not in template
        assert "<< 'ENVFILE'" in template
        assert 'docker inspect --format \'{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}\'' in template
        assert 'ERROR: Services failed to become healthy in time' in template

    def test_aws_docs_avoid_plaintext_secrets_and_match_architecture_support(self) -> None:
        readme = _read_text("infra/aws/README.md")
        spec = _read_text("docs/superpowers/specs/2026-03-18-aws-ec2-deployment-design.md")

        assert "read -s POSTGRES_PASSWORD" in readme
        assert "read -s GRAFANA_PASSWORD" in readme
        assert "PostgresPassword=\"$POSTGRES_PASSWORD\"" in readme
        assert "GrafanaPassword=\"$GRAFANA_PASSWORD\"" in readme
        assert "Graviton (ARM) compatible" not in spec
        assert "Architecture parameter selects x86_64 or arm64 AMIs" in spec


class TestChaosScripts:
    def test_chaos_scripts_exist_are_executable_and_target_expected_failures(self) -> None:
        scripts = {
            "tests/chaos/3_kill_writer.sh": ["docker kill", "WRITER_CONTAINER", "setup_stack", "teardown_stack", "print_test_report"],
            "tests/chaos/1_kill_ws_connection.sh": ["docker kill", "COLLECTOR_CONTAINER", "restart_gap", "setup_stack", "teardown_stack", "print_test_report"],
            "tests/chaos/5_fill_disk.sh": ["dd if=/dev/zero", "fill_disk.tmp", "docker compose", "setup_stack", "teardown_stack", "print_test_report"],
            "tests/chaos/6_depth_reconnect_inflight.sh": ["depth", "COLLECTOR_CONTAINER", "restart_gap", "setup_stack", "teardown_stack", "print_test_report"],
            "tests/chaos/2_buffer_overflow_recovery.sh": ["redpanda", "buffer_overflow", "setup_stack", "teardown_stack", "print_test_report"],
            "tests/chaos/4_writer_crash_before_commit.sh": ["docker kill -s KILL", "WRITER_CONTAINER", "check_integrity", "setup_stack", "teardown_stack", "print_test_report"],
            "tests/chaos/8_host_reboot_restart_gap.sh": ["host", "reboot", "restart_gap", "BOOT_ID", "setup_stack", "teardown_stack", "print_test_report"],
            "tests/chaos/9_ws_disconnect.sh": ["ws_disconnect", "block_egress", "unblock_egress", "setup_stack", "teardown_stack", "print_test_report"],
            "tests/chaos/10_snapshot_poll_miss.sh": ["snapshot_poll_miss", "iptables", "setup_stack", "teardown_stack", "print_test_report"],
            "tests/chaos/11_planned_collector_restart.sh": ["collector", "mark-maintenance", "planned", "restart_gap", "setup_stack", "teardown_stack", "print_test_report"],
        }

        for relative_path, expected_snippets in scripts.items():
            path = PROJECT_ROOT / relative_path
            text = path.read_text()

            assert text.startswith("#!/usr/bin/env bash")
            assert "set -euo pipefail" in text
            assert os.access(path, os.X_OK)
            for snippet in expected_snippets:
                assert snippet in text

    def test_common_helper_exists_with_setup_and_teardown(self) -> None:
        path = PROJECT_ROOT / "tests/chaos/common.sh"
        text = path.read_text()
        assert "setup_stack" in text
        assert "teardown_stack" in text
        assert "down -v --rmi local" in text
        assert "count_gaps" in text
        assert "check_integrity" in text
        assert "print_archive_stats" in text
        assert "print_gap_details" in text
        assert "print_test_report" in text
        assert "preflight_checks" in text
