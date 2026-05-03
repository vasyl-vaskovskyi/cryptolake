package com.cryptolake.writer.consumer;

import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Long-lived tail consumer for the backup topic prefix. Owns its own KafkaConsumer with a
 * distinct group.id and {@code auto.offset.reset=latest}. Does NOT commit offsets. Used purely
 * to feed liveness signals into {@link com.cryptolake.writer.failover.CoverageFilter} so the
 * coverage decision has up-to-date data for the backup source even when the writer is currently
 * archiving from primary.
 */
public final class BackupTailConsumer {

  private static final Logger log = LoggerFactory.getLogger(BackupTailConsumer.class);

  private final Consumer<byte[], byte[]> consumer;
  private final List<String> topics;

  /** Volatile: T1 polls; SIGTERM shutdown hook (T3) calls close(). */
  private volatile boolean started = false;

  public BackupTailConsumer(Consumer<byte[], byte[]> consumer, List<String> topics) {
    this.consumer = consumer;
    this.topics = List.copyOf(topics);
  }

  public void start() {
    if (started) {
      return;
    }
    consumer.subscribe(topics);
    started = true;
    log.info("backup_tail_started", "topics", topics.toString());
  }

  public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
    if (!started) {
      return ConsumerRecords.empty();
    }
    return consumer.poll(timeout);
  }

  public void close() {
    if (!started) {
      return;
    }
    try {
      consumer.close(Duration.ofSeconds(5));
    } catch (Exception ignored) {
      // best-effort
    }
    started = false;
    log.info("backup_tail_closed");
  }
}
