// ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_create_gap_envelope
// ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_create_restart_gap_envelope
// ports:
// tests/unit/test_envelope.py::TestEnvelopeCreation::test_restart_gap_envelope_optional_fields_omitted
// ports:
// tests/unit/test_envelope.py::TestEnvelopeCreation::test_restart_gap_envelope_partial_optional_fields
// ports:
// tests/unit/test_envelope.py::TestEnvelopeCreation::test_non_restart_gap_ignores_extra_fields
// ports:
// tests/unit/test_envelope.py::TestEnvelopeCreation::test_create_gap_envelope_with_checkpoint_lost
// ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_gap_invalid_reason_raises
package com.cryptolake.common.envelope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.cryptolake.common.util.Clocks;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GapEnvelopeTest {

  @Test
  void createGapEnvelope() {
    // ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_create_gap_envelope
    GapEnvelope env =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "depth",
            "test_2026-01-01T00:00:00Z",
            100L,
            1_000_000_000_000_000_000L,
            1_000_000_005_000_000_000L,
            "ws_disconnect",
            "WebSocket closed after 2h, reconnected in 1.2s",
            Clocks.systemNanoClock());

    assertThat(env.v()).isEqualTo(1);
    assertThat(env.type()).isEqualTo("gap");
    assertThat(env.exchange()).isEqualTo("binance");
    assertThat(env.symbol()).isEqualTo("btcusdt");
    assertThat(env.stream()).isEqualTo("depth");
    assertThat(env.gapStartTs()).isEqualTo(1_000_000_000_000_000_000L);
    assertThat(env.gapEndTs()).isEqualTo(1_000_000_005_000_000_000L);
    assertThat(env.reason()).isEqualTo("ws_disconnect");
    assertThat(env.detail()).isEqualTo("WebSocket closed after 2h, reconnected in 1.2s");
    // Optional restart metadata must be null (omitted from JSON via @JsonInclude(NON_NULL))
    assertThat(env.component()).isNull();
    assertThat(env.cause()).isNull();
    assertThat(env.planned()).isNull();
    assertThat(env.classifier()).isNull();
    assertThat(env.evidence()).isNull();
    assertThat(env.maintenanceId()).isNull();
  }

  @Test
  void restartMetadataIncluded() {
    // ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_create_restart_gap_envelope
    GapEnvelope env =
        GapEnvelope.createWithRestartMetadata(
            "binance",
            "btcusdt",
            "depth",
            "test_2026-01-01T00:00:00Z",
            100L,
            1_000_000_000_000_000_000L,
            1_000_000_005_000_000_000L,
            "restart_gap",
            "collector restarted for upgrade",
            Clocks.systemNanoClock(),
            "ws_collector",
            "upgrade",
            Boolean.TRUE,
            "rule:scheduled_restart",
            Map.of("exit_code", 0, "uptime_seconds", 86400),
            "maint-2026-01-15-001");

    assertThat(env.reason()).isEqualTo("restart_gap");
    assertThat(env.component()).isEqualTo("ws_collector");
    assertThat(env.cause()).isEqualTo("upgrade");
    assertThat(env.planned()).isTrue();
    assertThat(env.classifier()).isEqualTo("rule:scheduled_restart");
    assertThat(env.evidence()).containsEntry("exit_code", 0);
    assertThat(env.maintenanceId()).isEqualTo("maint-2026-01-15-001");
  }

  @Test
  void optionalFieldsOmittedWhenNull() {
    // ports:
    // tests/unit/test_envelope.py::TestEnvelopeCreation::test_restart_gap_envelope_optional_fields_omitted
    GapEnvelope env =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "depth",
            "s",
            0L,
            0L,
            1L,
            "restart_gap",
            "bare restart gap",
            Clocks.fixed(0L));

    assertThat(env.component()).isNull();
    assertThat(env.cause()).isNull();
    assertThat(env.planned()).isNull();
    assertThat(env.classifier()).isNull();
    assertThat(env.evidence()).isNull();
    assertThat(env.maintenanceId()).isNull();
  }

  @Test
  void partialOptionalFields() {
    // ports:
    // tests/unit/test_envelope.py::TestEnvelopeCreation::test_restart_gap_envelope_partial_optional_fields
    GapEnvelope env =
        GapEnvelope.createWithRestartMetadata(
            "binance",
            "btcusdt",
            "depth",
            "s",
            0L,
            0L,
            1L,
            "restart_gap",
            "partial metadata",
            Clocks.fixed(0L),
            "snapshot_poller",
            null,
            Boolean.FALSE,
            null,
            null,
            null);

    assertThat(env.component()).isEqualTo("snapshot_poller");
    assertThat(env.planned()).isFalse();
    assertThat(env.cause()).isNull();
    assertThat(env.classifier()).isNull();
    assertThat(env.evidence()).isNull();
    assertThat(env.maintenanceId()).isNull();
  }

  @Test
  void nonRestartGapOmitsMetadata() {
    // ports:
    // tests/unit/test_envelope.py::TestEnvelopeCreation::test_non_restart_gap_ignores_extra_fields
    GapEnvelope env =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "trades",
            "s",
            0L,
            0L,
            1L,
            "ws_disconnect",
            "normal disconnect",
            Clocks.fixed(0L));

    assertThat(env.reason()).isEqualTo("ws_disconnect");
    assertThat(env.component()).isNull();
    assertThat(env.cause()).isNull();
    assertThat(env.planned()).isNull();
    assertThat(env.classifier()).isNull();
    assertThat(env.evidence()).isNull();
    assertThat(env.maintenanceId()).isNull();
  }

  @Test
  void createWithCheckpointLost() {
    // ports:
    // tests/unit/test_envelope.py::TestEnvelopeCreation::test_create_gap_envelope_with_checkpoint_lost
    GapEnvelope env =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "trades",
            "test-session",
            -1L,
            1000L,
            2000L,
            "checkpoint_lost",
            "No durable checkpoint; recovered gap bounds from archive",
            Clocks.fixed(0L));

    assertThat(env.reason()).isEqualTo("checkpoint_lost");
    assertThat(env.type()).isEqualTo("gap");
  }

  @Test
  void invalidReasonThrows() {
    // ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_gap_invalid_reason_raises
    assertThatThrownBy(
            () ->
                GapEnvelope.create(
                    "binance",
                    "btcusdt",
                    "trades",
                    "s",
                    0L,
                    0L,
                    1L,
                    "invalid_reason",
                    "test",
                    Clocks.fixed(0L)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid_reason");
  }
}
