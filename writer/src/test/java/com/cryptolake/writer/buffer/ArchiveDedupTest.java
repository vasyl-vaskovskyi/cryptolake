package com.cryptolake.writer.buffer;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.BrokerCoordinates;
import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for archive-layer dedup in {@link BufferManager} (Tier 1 §4 exactly-once).
 *
 * <p>Verifies Bug 1 fix: the same Kafka (topic, partition, offset) must never be archived twice.
 * The second occurrence must be dropped silently, and {@code writer_duplicates_dropped_total} must
 * be incremented.
 */
class ArchiveDedupTest {

  private static final long TS_NS = 1_705_329_600_000_000_000L; // 2024-01-15 14:00:00 UTC

  private EnvelopeCodec codec;
  private PrometheusMeterRegistry registry;
  private WriterMetrics metrics;
  private BufferManager manager;

  @BeforeEach
  void setUp() {
    codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    metrics = new WriterMetrics(registry);
    // threshold=1000 (high so auto-flush doesn't trigger), interval=3600s
    manager = new BufferManager("/archive", 1000, 3600, codec, metrics);
  }

  private DataEnvelope makeEnvelope() {
    return new DataEnvelope(
        1,
        "data",
        "binance",
        "btcusdt",
        "bookticker",
        TS_NS,
        TS_NS,
        "collector_session_id",
        1L,
        "{}",
        "abc123");
  }

  /** Tier 1 §4: two envelopes with the same broker coordinates — only the first is archived. */
  @Test
  void add_duplicateBrokerCoordinates_secondIsDropped() {
    DataEnvelope env1 = makeEnvelope();
    DataEnvelope env2 = makeEnvelope();
    BrokerCoordinates coords = new BrokerCoordinates("binance.bookticker", 0, 643L);

    // First add: accepted
    Optional<List<FlushResult>> r1 = manager.add(env1, coords, "primary");
    assertThat(r1).isEmpty(); // below threshold, no auto-flush

    // Second add with SAME coords: must be dropped
    Optional<List<FlushResult>> r2 = manager.add(env2, coords, "primary");
    assertThat(r2).isEmpty();

    // Flush: should contain exactly ONE record, not two
    List<FlushResult> results = manager.flushAll();
    assertThat(results).hasSize(1);
    assertThat(results.get(0).count()).isEqualTo(1);
  }

  /** Tier 1 §4: duplicate drop must increment writer_duplicates_dropped_total. */
  @Test
  void add_duplicateBrokerCoordinates_incrementsDuplicatesDroppedMetric() {
    BrokerCoordinates coords = new BrokerCoordinates("binance.bookticker", 0, 573L);

    manager.add(makeEnvelope(), coords, "primary");
    manager.add(makeEnvelope(), coords, "primary"); // duplicate

    Counter counter = metrics.duplicatesDropped();
    assertThat(counter.count()).isEqualTo(1.0);
  }

  /** Multiple different offsets from the same topic+partition are all accepted. */
  @Test
  void add_distinctOffsets_allAccepted() {
    for (long offset = 573L; offset <= 649L; offset++) {
      BrokerCoordinates coords = new BrokerCoordinates("binance.bookticker", 0, offset);
      manager.add(makeEnvelope(), coords, "primary");
    }

    List<FlushResult> results = manager.flushAll();
    assertThat(results).hasSize(1);
    assertThat(results.get(0).count()).isEqualTo(77); // 649 - 573 + 1
  }

  /**
   * Synthetic offsets (-1L) must bypass dedup — they are gap records, never duplicated by Kafka
   * (Tier 5 M9).
   */
  @Test
  void add_syntheticOffset_bypassesDedup() {
    BrokerCoordinates synth1 = new BrokerCoordinates("binance.bookticker", 0, -1L);
    BrokerCoordinates synth2 = new BrokerCoordinates("binance.bookticker", 0, -1L);

    manager.add(makeEnvelope(), synth1, "primary");
    manager.add(makeEnvelope(), synth2, "primary");

    // Both synthetic records accepted (dedup bypassed)
    List<FlushResult> results = manager.flushAll();
    assertThat(results).hasSize(1);
    assertThat(results.get(0).count()).isEqualTo(2);
    // Counter not incremented (no dedup for synthetic)
    assertThat(metrics.duplicatesDropped().count()).isEqualTo(0.0);
  }

  /** Same offset on different partitions are NOT duplicates — both must be accepted. */
  @Test
  void add_sameOffsetDifferentPartition_bothAccepted() {
    BrokerCoordinates p0 = new BrokerCoordinates("binance.bookticker", 0, 100L);
    BrokerCoordinates p1 = new BrokerCoordinates("binance.bookticker", 1, 100L);

    manager.add(makeEnvelope(), p0, "primary");
    manager.add(makeEnvelope(), p1, "primary");

    List<FlushResult> results = manager.flushAll();
    // The two envelopes map to the same FileTarget (same stream+hour) so one FlushResult
    // with count=2
    assertThat(results.stream().mapToInt(FlushResult::count).sum()).isEqualTo(2);
    assertThat(metrics.duplicatesDropped().count()).isEqualTo(0.0);
  }

  /** Dedup drop returns empty optional (no auto-flush side-effect from dropped record). */
  @Test
  void add_duplicate_returnsEmptyOptional() {
    BrokerCoordinates coords = new BrokerCoordinates("binance.bookticker", 0, 200L);
    manager.add(makeEnvelope(), coords, "primary");
    Optional<List<FlushResult>> dropped = manager.add(makeEnvelope(), coords, "primary");
    assertThat(dropped).isEmpty();
  }
}
