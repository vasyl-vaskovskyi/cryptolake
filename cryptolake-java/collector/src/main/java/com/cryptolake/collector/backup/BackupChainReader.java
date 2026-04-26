package com.cryptolake.collector.backup;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.logging.StructuredLogger;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * One-shot Kafka consumer that reads the most recent depth update ID from a backup topic.
 *
 * <p>Ports {@code BackupChainReader} from {@code src/collector/backup_chain_reader.py}. Creates a
 * dedicated consumer per call (ephemeral, with a unique group ID) so it does not interfere with
 * production consumers.
 *
 * <p>Thread safety: stateless — one consumer per call, closed in a {@code finally} block.
 */
public final class BackupChainReader {

  private static final StructuredLogger log = StructuredLogger.of(BackupChainReader.class);

  private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
  private static final String BACKUP_PREFIX = "backup.";

  private final EnvelopeCodec codec;

  public BackupChainReader(EnvelopeCodec codec) {
    this.codec = codec;
  }

  /**
   * Derives the "other" depth topic (primary ↔ backup swap).
   *
   * <p>If {@code ownPrefix} is {@code ""} (primary), returns the backup topic. If {@code ownPrefix}
   * is {@code "backup."}, returns the primary topic.
   */
  public static String otherDepthTopic(String ownPrefix, String exchange) {
    String otherPrefix = ownPrefix.isEmpty() ? BACKUP_PREFIX : "";
    return otherPrefix + exchange + ".depth";
  }

  /**
   * Reads the most recent depth update ID ({@code u} field) from the given topic for the specified
   * symbol within the {@code maxAge} window. Returns {@link Optional#empty()} if no recent data was
   * found.
   *
   * @param brokers Kafka brokers
   * @param topic topic name (e.g. {@code "backup.binance.depth"})
   * @param symbol symbol to filter for (lowercase)
   * @param maxAge maximum age of messages to consider
   */
  public Optional<Long> readLastDepthUpdateId(
      List<String> brokers, String topic, String symbol, Duration maxAge) {

    // Unique group ID per call to avoid rebalance interference (design §2.9)
    String groupId = "depth-chain-reader-" + Instant.now().toEpochMilli();
    Properties props = buildConsumerConfig(brokers, groupId);

    KafkaConsumer<byte[], byte[]> consumer = null;
    try {
      consumer = new KafkaConsumer<>(props);

      // Discover partitions
      List<TopicPartition> partitions;
      try {
        partitions = new ArrayList<>();
        for (var info : consumer.partitionsFor(topic)) {
          partitions.add(new TopicPartition(info.topic(), info.partition()));
        }
      } catch (Exception e) {
        log.warn("backup_chain_topic_missing", "topic", topic, "error", e.getMessage());
        return Optional.empty();
      }

      if (partitions.isEmpty()) return Optional.empty();

      consumer.assign(partitions);

      // Seek to the start of the maxAge window
      long startTimestampMs = Instant.now().minusMillis(maxAge.toMillis()).toEpochMilli();
      Map<TopicPartition, Long> timestamps = new HashMap<>();
      for (TopicPartition tp : partitions) {
        timestamps.put(tp, startTimestampMs);
      }
      Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestamps);
      for (TopicPartition tp : partitions) {
        OffsetAndTimestamp ots = offsets.get(tp);
        if (ots != null) {
          consumer.seek(tp, ots.offset());
        } else {
          // No data in the window for this partition — seek to end
          Map<TopicPartition, Long> endOffsets = consumer.endOffsets(List.of(tp));
          Long endOffset = endOffsets.get(tp);
          if (endOffset != null) consumer.seek(tp, endOffset);
        }
      }

      // Poll up to POLL_TIMEOUT, track the latest u per symbol
      long latestU = Long.MIN_VALUE;
      long pollEndNs = System.nanoTime() + POLL_TIMEOUT.toNanos();

      while (System.nanoTime() < pollEndNs) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        if (records.isEmpty()) break; // no more recent data

        for (ConsumerRecord<byte[], byte[]> rec : records) {
          try {
            JsonNode envNode = codec.readTree(rec.value());
            String envType = envNode.path("type").asText();
            if (!"data".equals(envType)) continue;
            String envSymbol = envNode.path("symbol").asText();
            if (!symbol.equals(envSymbol)) continue;

            // Extract u from raw_text
            JsonNode rawNode = codec.readTree(
                envNode.path("raw_text").asText().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            JsonNode uNode = rawNode.path("u");
            if (uNode.isMissingNode()) continue;
            long u = uNode.asLong(); // never .asInt() — Tier 5 E1
            if (u > latestU) latestU = u;
          } catch (Exception e) {
            // Corrupt or unexpected envelope — skip (Tier 5 G4)
          }
        }
      }

      if (latestU == Long.MIN_VALUE) {
        log.info("backup_chain_no_recent_data", "topic", topic, "symbol", symbol);
        return Optional.empty();
      }

      log.info("backup_chain_found_last_u", "topic", topic, "symbol", symbol, "u", latestU);
      return Optional.of(latestU);

    } finally {
      if (consumer != null) {
        try {
          consumer.close();
        } catch (Exception ignored) {
          // best-effort close (Tier 5 G1)
        }
      }
    }
  }

  private static Properties buildConsumerConfig(List<String> brokers, String groupId) {
    Properties p = new Properties();
    p.put("bootstrap.servers", String.join(",", brokers));
    p.put("group.id", groupId);
    p.put("auto.offset.reset", "latest");
    p.put("enable.auto.commit", false);
    p.put("session.timeout.ms", 10_000);
    p.put("key.deserializer", ByteArrayDeserializer.class.getName());
    p.put("value.deserializer", ByteArrayDeserializer.class.getName());
    return p;
  }
}
