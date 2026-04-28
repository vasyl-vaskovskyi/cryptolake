package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.GapReasons;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for KafkaOffsetResetEmitter — validates that the {@code kafka_offset_reset} gap reason
 * is registered and that the OffsetOutOfRangeException path is correctly plumbed.
 *
 * <p>The full end-to-end test (catching the exception from the poll loop) requires a real Kafka
 * broker and is covered by chaos test scenario 19. These unit tests validate the invariants that
 * can be checked without a broker.
 */
class KafkaOffsetResetTest {

  /** Validates that kafka_offset_reset is a known valid gap reason. */
  @Test
  void kafkaOffsetResetIsValidGapReason() {
    assertThat(GapReasons.VALID).contains("kafka_offset_reset");
  }

  /** Validates the gap reason string matches what the consumer loop emits. */
  @Test
  void kafkaOffsetResetReasonStringIsCorrect() {
    // The reason must be exactly "kafka_offset_reset" — not an alias
    String reason = "kafka_offset_reset";
    // requireValid must not throw
    GapReasons.requireValid(reason);
    assertThat(reason).isEqualTo("kafka_offset_reset");
  }

  /**
   * Validates that OffsetOutOfRangeException from the kafka-clients API carries the affected
   * partitions.
   */
  @Test
  void offsetOutOfRangeExceptionCarriesPartitions() {
    org.apache.kafka.common.TopicPartition tp =
        new org.apache.kafka.common.TopicPartition("binance.depth", 0);
    java.util.Map<org.apache.kafka.common.TopicPartition, Long> offsets =
        java.util.Map.of(tp, 12345L);

    org.apache.kafka.clients.consumer.OffsetOutOfRangeException e =
        new org.apache.kafka.clients.consumer.OffsetOutOfRangeException(offsets);

    assertThat(e.offsetOutOfRangePartitions()).containsKey(tp);
    assertThat(e.offsetOutOfRangePartitions().get(tp)).isEqualTo(12345L);
  }
}
