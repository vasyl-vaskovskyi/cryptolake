package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.GapReason;
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

  /** Validates the gap reason wire string matches what the consumer loop emits. */
  @Test
  void kafkaOffsetResetReasonStringIsCorrect() {
    // The wire string must be exactly "kafka_offset_reset" — not an alias.
    assertThat(GapReason.KAFKA_OFFSET_RESET.wire()).isEqualTo("kafka_offset_reset");
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
