package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;

/**
 * Smoke test for {@link BackupTailConsumer} — verifies the wrapper subscribes the underlying
 * consumer on {@code start()} and returns empty {@link ConsumerRecords} from {@code poll()} when
 * no records are available.
 */
class BackupTailConsumerTest {

  @Test
  void poll_returnsEmptyWhenNoRecords() {
    MockConsumer<byte[], byte[]> mock = new MockConsumer<>(OffsetResetStrategy.LATEST);
    BackupTailConsumer tail = new BackupTailConsumer(mock, List.of("backup.binance.bookticker"));
    tail.start();

    ConsumerRecords<byte[], byte[]> records = tail.poll(Duration.ofMillis(10));

    assertThat(records.isEmpty()).isTrue();
  }
}
