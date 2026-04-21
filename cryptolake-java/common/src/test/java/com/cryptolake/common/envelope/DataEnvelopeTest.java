// ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_create_data_envelope
// ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_data_envelope_raw_sha256_integrity
// ports:
// tests/unit/test_envelope.py::TestEnvelopeCreation::test_envelope_received_at_is_nanoseconds
package com.cryptolake.common.envelope;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.util.Clocks;
import com.cryptolake.common.util.Sha256;
import org.junit.jupiter.api.Test;

class DataEnvelopeTest {

  @Test
  void createDataEnvelope() {
    // ports: tests/unit/test_envelope.py::TestEnvelopeCreation::test_create_data_envelope
    String rawText = "{\"e\":\"aggTrade\",\"E\":1741689600120}";
    DataEnvelope env =
        DataEnvelope.create(
            "binance",
            "btcusdt",
            "trades",
            rawText,
            1741689600120L,
            "test_2026-01-01T00:00:00Z",
            1L,
            Clocks.systemNanoClock());

    assertThat(env.v()).isEqualTo(1);
    assertThat(env.type()).isEqualTo("data");
    assertThat(env.exchange()).isEqualTo("binance");
    assertThat(env.symbol()).isEqualTo("btcusdt");
    assertThat(env.stream()).isEqualTo("trades");
    assertThat(env.rawText()).isEqualTo(rawText);
    assertThat(env.rawSha256()).isEqualTo(Sha256.hexDigestUtf8(rawText));
    assertThat(env.collectorSessionId()).isEqualTo("test_2026-01-01T00:00:00Z");
    assertThat(env.sessionSeq()).isEqualTo(1L);
    assertThat(env.exchangeTs()).isEqualTo(1741689600120L);
    assertThat(env.receivedAt()).isGreaterThan(0L);
  }

  @Test
  void rawSha256IsOverRawTextBytes() {
    // ports:
    // tests/unit/test_envelope.py::TestEnvelopeCreation::test_data_envelope_raw_sha256_integrity
    String raw = "{\"key\": \"value\", \"num\": 0.00100000}";
    DataEnvelope env =
        DataEnvelope.create("binance", "btcusdt", "trades", raw, 100L, "s", 0L, Clocks.fixed(42L));

    String expectedHash = Sha256.hexDigestUtf8(raw);
    assertThat(env.rawSha256()).isEqualTo(expectedHash);
  }

  @Test
  void receivedAtIsNanoseconds() {
    // ports:
    // tests/unit/test_envelope.py::TestEnvelopeCreation::test_envelope_received_at_is_nanoseconds
    long before = System.currentTimeMillis() * 1_000_000L;
    DataEnvelope env =
        DataEnvelope.create(
            "binance", "btcusdt", "trades", "{}", 100L, "s", 0L, Clocks.systemNanoClock());
    long after = (System.currentTimeMillis() + 1) * 1_000_000L;

    // receivedAt must be in nanosecond range
    assertThat(env.receivedAt()).isBetween(before, after);
  }
}
