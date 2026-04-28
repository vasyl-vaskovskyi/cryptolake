package com.cryptolake.collector.durability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.OptionalLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-record on-disk durability journal for Kafka producer outages.
 *
 * <p>Persists the outage start timestamp at {@code ${dataDir}/cryptolake/${collectorId}/
 * kafka_outage.json} so that a collector restart after a Kafka outage can still emit the correct
 * bridging {@code kafka_producer_outage} gap envelope.
 *
 * <p>The file contains a single JSON object: {@code {"outage_started_at_ns": <long>}}. {@code
 * FileChannel.force(true)} is called on every write to guarantee durability.
 *
 * <p>Thread safety: the collector's producer bridge runs on a single virtual thread; all three
 * methods are called from that thread only. No synchronization needed.
 */
public final class KafkaOutageJournal {

  private static final Logger log = LoggerFactory.getLogger(KafkaOutageJournal.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Path journalPath;

  /**
   * Constructs a {@code KafkaOutageJournal}.
   *
   * @param dataDir host data directory (e.g. {@code /data})
   * @param collectorId identifies which collector owns this journal
   */
  public KafkaOutageJournal(Path dataDir, String collectorId) {
    this.journalPath =
        dataDir.resolve("cryptolake").resolve(collectorId).resolve("kafka_outage.json");
  }

  /** Returns the full path to the journal file (for tests and readers). */
  public Path journalPath() {
    return journalPath;
  }

  /**
   * Records the start of a Kafka producer outage.
   *
   * <p>Overwrites any previously recorded outage (last-writer-wins semantics).
   *
   * @param tsNs nanoseconds since Unix epoch marking the outage start
   */
  public void recordOutageStart(long tsNs) {
    try {
      Files.createDirectories(journalPath.getParent());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    ObjectNode node = MAPPER.createObjectNode();
    node.put("outage_started_at_ns", tsNs);
    try {
      byte[] jsonBytes = MAPPER.writeValueAsBytes(node);
      try (FileChannel fc =
          FileChannel.open(
              journalPath,
              StandardOpenOption.WRITE,
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING)) {
        fc.write(ByteBuffer.wrap(jsonBytes));
        fc.force(true); // fsync data + metadata
      }
      log.info("kafka_outage_started", "ts_ns", tsNs);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize kafka outage journal", e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Reads the recorded outage start timestamp.
   *
   * @return {@link OptionalLong} containing the timestamp if the file exists and is valid; {@link
   *     OptionalLong#empty()} otherwise
   */
  public OptionalLong readOutageStart() {
    if (!Files.exists(journalPath)) {
      return OptionalLong.empty();
    }
    try {
      String content = Files.readString(journalPath, StandardCharsets.UTF_8).strip();
      if (content.isEmpty()) return OptionalLong.empty();
      ObjectNode node = (ObjectNode) MAPPER.readTree(content);
      if (!node.has("outage_started_at_ns")) return OptionalLong.empty();
      return OptionalLong.of(node.get("outage_started_at_ns").asLong());
    } catch (Exception e) {
      log.warn("kafka_outage_journal_read_failed", "error", e.getMessage());
      return OptionalLong.empty();
    }
  }

  /**
   * Truncates (deletes) the outage journal, indicating the outage has been resolved.
   *
   * <p>Idempotent — does nothing if the file does not exist.
   */
  public void truncate() {
    try {
      Files.deleteIfExists(journalPath);
      log.info("kafka_outage_journal_truncated");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
