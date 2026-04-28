package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.common.util.Clocks;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.buffer.FlushResult;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.failover.FailoverController;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RecordHandler} gap/heartbeat routing.
 *
 * <p>Verifies Bug C fix: collector-emitted gap envelopes arriving in the Kafka topic are
 * deserialized as {@link GapEnvelope} (not as {@link com.cryptolake.common.envelope.DataEnvelope})
 * so all 12 mandatory gap fields are preserved in the archive (Tier 1 §5; GAP_ENVELOPE_FIELDS).
 */
class RecordHandlerGapRoutingTest {

  private EnvelopeCodec codec;
  private BufferManager buffers;
  private WriterMetrics metrics;
  private CoverageFilter coverage;
  private FailoverController failover;
  private RecordHandler handler;

  @BeforeEach
  void setUp() {
    codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    metrics = new WriterMetrics(registry);
    AtomicLong fakeClock = new AtomicLong(1_000_000_000_000L);
    ClockSupplier clock = fakeClock::get;
    buffers = new BufferManager("/tmp/rh-gap-test", 1000, 60, codec);
    coverage = new CoverageFilter(1.0, 1.0, metrics, clock);
    // Minimal FailoverController — only resetSilenceTimer() is called in the gap path.
    failover =
        new FailoverController(
            () -> null, // backup factory — not activated in this test
            List.of("binance.trades"),
            "backup_",
            Duration.ofSeconds(30),
            coverage,
            metrics,
            clock);

    handler =
        new RecordHandler(
            codec,
            null, // sessionDetector — not reached in gap path
            null, // depthFilter — not reached in gap path
            coverage,
            failover,
            null, // recovery — not reached in gap path
            buffers,
            null, // gaps — not reached in gap path
            metrics,
            "backup_");
  }

  /**
   * Bug C fix: a gap envelope emitted by the collector and received via Kafka must be deserialized
   * as {@link GapEnvelope} (preserving gap_start_ts, gap_end_ts, reason, detail) and buffered.
   */
  @Test
  void handle_collectorGapEnvelope_preservesAllGapFields() {
    // Build a gap envelope as the collector would emit it (all 12 required fields present).
    GapEnvelope collectorGap =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "trades",
            "binance-collector-01_2026-04-27T21:41:23Z",
            296L, // session_seq from the collector (NOT -1)
            1_777_326_113_000_000_000L, // gap_start_ts
            1_777_326_115_000_000_000L, // gap_end_ts
            "ws_disconnect",
            "WebSocket closed unexpectedly",
            Clocks.fixed(1_777_326_115_437_720_798L));

    // Serialize as the collector's KafkaProducerBridge would: codec.toJsonBytes(gap)
    byte[] gapBytes = codec.toJsonBytes(collectorGap);

    // Create a Kafka record from the "primary" topic (offset = 95061 — a real, positive offset)
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>("binance.trades", 0, 95061L, null, gapBytes);

    handler.handle(record, false);

    // The gap should have been added to the buffer (coverage filter is disabled — only 1 source).
    List<FlushResult> results = buffers.flushAll();
    assertThat(results).isNotEmpty();

    // The flushed result must contain exactly one line — the gap envelope.
    FlushResult result = results.get(0);
    assertThat(result.count()).isEqualTo(1);

    // Deserialize the buffered line back to verify ALL gap fields are preserved.
    byte[] bufferedLine = result.lines().get(0);
    // Strip the trailing broker-coordinate fields by re-parsing the JSON node.
    com.fasterxml.jackson.databind.JsonNode node = codec.readTree(bufferedLine);

    assertThat(node.path("type").asText()).isEqualTo("gap");
    assertThat(node.path("gap_start_ts").asLong()).isEqualTo(1_777_326_113_000_000_000L);
    assertThat(node.path("gap_end_ts").asLong()).isEqualTo(1_777_326_115_000_000_000L);
    assertThat(node.path("reason").asText()).isEqualTo("ws_disconnect");
    assertThat(node.path("detail").asText()).isEqualTo("WebSocket closed unexpectedly");
    assertThat(node.path("session_seq").asLong()).isEqualTo(296L);
    // Broker coordinates must also be appended.
    assertThat(node.path("_topic").asText()).isEqualTo("binance.trades");
    assertThat(node.path("_offset").asLong()).isEqualTo(95061L);
  }

  /**
   * Bug C fix: after fix, a gap envelope must NOT be deserialized as DataEnvelope (which would
   * lose gap_start_ts, gap_end_ts, reason, detail and emit a broken archive record).
   */
  @Test
  void handle_collectorGapEnvelope_doesNotWriteDataEnvelopeFields() {
    GapEnvelope collectorGap =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "trades",
            "binance-collector-01_2026-04-27T21:41:23Z",
            296L,
            1_777_326_113_000_000_000L,
            1_777_326_115_000_000_000L,
            "pu_chain_break",
            "pu chain broken: expected pu=100 got pu=99",
            Clocks.fixed(1_777_326_115_437_720_798L));

    byte[] gapBytes = codec.toJsonBytes(collectorGap);
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>("binance.trades", 0, 95062L, null, gapBytes);

    handler.handle(record, false);

    List<FlushResult> results = buffers.flushAll();
    assertThat(results).isNotEmpty();

    byte[] bufferedLine = results.get(0).lines().get(0);
    com.fasterxml.jackson.databind.JsonNode node = codec.readTree(bufferedLine);

    // DataEnvelope-shape fields must NOT be present (exchange_ts, raw_text, raw_sha256).
    assertThat(node.has("exchange_ts")).isFalse();
    assertThat(node.has("raw_text")).isFalse();
    assertThat(node.has("raw_sha256")).isFalse();
  }

  /**
   * Heartbeat envelopes must be silently dropped — no archive write.
   */
  @Test
  void handle_heartbeatEnvelope_notWrittenToBuffer() {
    // Build a minimal heartbeat envelope JSON (matches HeartbeatEnvelope serialization).
    String heartbeatJson =
        "{\"v\":1,\"type\":\"heartbeat\",\"exchange\":\"binance\","
            + "\"symbol\":\"btcusdt\",\"stream\":\"trades\","
            + "\"received_at\":1777326113915016714,"
            + "\"collector_session_id\":\"binance-collector-01_2026-04-27T21:41:23Z\","
            + "\"emitted_at_ns\":1777326113915016714,"
            + "\"last_session_seq\":837,\"status\":\"alive\"}";
    byte[] heartbeatBytes = heartbeatJson.getBytes();

    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>("binance.trades", 0, 95063L, null, heartbeatBytes);

    handler.handle(record, false);

    List<FlushResult> results = buffers.flushAll();
    // Buffer should be empty — heartbeats are not archived.
    assertThat(results).isEmpty();
  }
}
