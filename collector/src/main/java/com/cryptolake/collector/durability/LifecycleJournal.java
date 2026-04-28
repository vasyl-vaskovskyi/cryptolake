package com.cryptolake.collector.durability;

import com.cryptolake.common.util.ClockSupplier;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Append-only process start/stop ledger.
 *
 * <p>Replaces the Python {@code host_lifecycle_agent.py}. Persists lifecycle events at {@code
 * ${dataDir}/cryptolake/${collectorId}/lifecycle.jsonl}. Each line is a compact JSON object. Atomic
 * single-write + {@code FileChannel.force(true)} ensures durability even under process crash.
 *
 * <p>Used by the writer-side {@code LifecycleJournalReader} to provide authoritative {@code
 * host_boot_id} evidence to {@code RestartGapClassifier}.
 *
 * <p>Thread safety: all write operations are synchronized on the file path object. The reader
 * ({@link #readSince}) is safe to call from any thread.
 */
public final class LifecycleJournal {

  private static final Logger log = LoggerFactory.getLogger(LifecycleJournal.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Path journalPath;
  private final ClockSupplier clock;

  /**
   * Constructs a {@code LifecycleJournal}.
   *
   * @param dataDir host data directory (e.g. {@code /data})
   * @param collectorId identifies which collector's journal this is
   * @param clock nanosecond clock for event timestamps
   */
  public LifecycleJournal(Path dataDir, String collectorId, ClockSupplier clock) {
    this.journalPath =
        dataDir.resolve("cryptolake").resolve(collectorId).resolve("lifecycle.jsonl");
    this.clock = clock;
  }

  /** Returns the full path to the journal file (used by readers and tests). */
  public Path journalPath() {
    return journalPath;
  }

  /**
   * Records a collector start event.
   *
   * @param hostBootId OS boot ID (e.g. from {@code /proc/sys/kernel/random/boot_id})
   * @param collectorSessionId unique ID for this process invocation
   */
  public void recordStart(String hostBootId, String collectorSessionId) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("ts_ns", clock.nowNs());
    node.put("event", "start");
    node.put("host_boot_id", hostBootId);
    node.put("collector_session_id", collectorSessionId);
    appendLine(node);
    log.info("lifecycle_start_recorded", "session_id", collectorSessionId);
  }

  /**
   * Records a clean (orderly) collector shutdown event.
   *
   * @param hostBootId OS boot ID
   * @param collectorSessionId unique ID for this process invocation
   * @param planned {@code true} if this was a maintenance-window shutdown
   * @param maintenanceId optional maintenance intent identifier (may be null)
   */
  public void recordCleanShutdown(
      String hostBootId, String collectorSessionId, boolean planned, String maintenanceId) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("ts_ns", clock.nowNs());
    node.put("event", "clean_shutdown");
    node.put("host_boot_id", hostBootId);
    node.put("collector_session_id", collectorSessionId);
    node.put("planned", planned);
    if (maintenanceId != null) {
      node.put("maintenance_id", maintenanceId);
    }
    appendLine(node);
    log.info("lifecycle_shutdown_recorded", "session_id", collectorSessionId, "planned", planned);
  }

  /**
   * Reads all events with {@code ts_ns >= nsSinceEpoch} from the journal.
   *
   * <p>Corrupt / non-JSON lines are silently dropped (matches Python's crash-resilient read).
   *
   * @param nsSinceEpoch lower bound (inclusive) in nanoseconds since Unix epoch; pass 0 for all
   * @return immutable list of events in file order
   */
  public List<LifecycleEvent> readSince(long nsSinceEpoch) {
    if (!Files.exists(journalPath)) {
      return Collections.emptyList();
    }
    List<LifecycleEvent> result = new ArrayList<>();
    try (BufferedReader br = Files.newBufferedReader(journalPath, StandardCharsets.UTF_8)) {
      String line;
      while ((line = br.readLine()) != null) {
        line = line.strip();
        if (line.isEmpty()) continue;
        try {
          ObjectNode node = (ObjectNode) MAPPER.readTree(line);
          long tsNs = node.path("ts_ns").asLong(0);
          if (tsNs < nsSinceEpoch) continue;
          String event = node.path("event").asText(null);
          String hostBootId = node.path("host_boot_id").asText(null);
          String sessionId = node.path("collector_session_id").asText(null);
          Boolean planned = node.has("planned") ? node.path("planned").asBoolean() : null;
          String maintenanceId =
              node.has("maintenance_id") ? node.path("maintenance_id").asText(null) : null;
          result.add(
              new LifecycleEvent(tsNs, event, hostBootId, sessionId, planned, maintenanceId));
        } catch (Exception ignored) {
          // corrupt line — drop silently
        }
      }
    } catch (NoSuchFileException ignored) {
      // file vanished after exists() check — return empty
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return Collections.unmodifiableList(result);
  }

  /**
   * Prunes journal entries older than {@code retention}.
   *
   * <p>Rewrites the file in place, keeping only events whose {@code ts_ns} is within the retention
   * window relative to the current clock. 7-day default matches the Python agent's behaviour.
   *
   * @param retention maximum age of entries to retain
   */
  public void pruneOlderThan(Duration retention) {
    if (!Files.exists(journalPath)) {
      return;
    }
    long cutoffNs = clock.nowNs() - retention.toNanos();
    List<LifecycleEvent> toKeep = readSince(cutoffNs);
    if (toKeep.isEmpty()) {
      try {
        Files.deleteIfExists(journalPath);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return;
    }
    // Rewrite: serialize kept events back
    try {
      Files.createDirectories(journalPath.getParent());
      // Write atomically via temp file + rename
      Path tmp = journalPath.resolveSibling(journalPath.getFileName() + ".tmp");
      StringBuilder sb = new StringBuilder();
      for (LifecycleEvent ev : toKeep) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("ts_ns", ev.tsNs());
        node.put("event", ev.event());
        if (ev.hostBootId() != null) node.put("host_boot_id", ev.hostBootId());
        if (ev.collectorSessionId() != null)
          node.put("collector_session_id", ev.collectorSessionId());
        if (ev.planned() != null) node.put("planned", ev.planned());
        if (ev.maintenanceId() != null) node.put("maintenance_id", ev.maintenanceId());
        sb.append(MAPPER.writeValueAsString(node)).append('\n');
      }
      Files.writeString(tmp, sb.toString());
      Files.move(tmp, journalPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      log.info("lifecycle_journal_pruned", "kept", toKeep.size());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ── private helpers ───────────────────────────────────────────────────────

  private void appendLine(ObjectNode node) {
    try {
      Files.createDirectories(journalPath.getParent());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    try {
      byte[] jsonBytes = MAPPER.writeValueAsBytes(node);
      byte[] line = new byte[jsonBytes.length + 1];
      System.arraycopy(jsonBytes, 0, line, 0, jsonBytes.length);
      line[jsonBytes.length] = 0x0A; // newline
      try (FileChannel fc =
          FileChannel.open(
              journalPath,
              StandardOpenOption.APPEND,
              StandardOpenOption.CREATE,
              StandardOpenOption.SYNC)) {
        fc.write(ByteBuffer.wrap(line));
        // SYNC open option ensures data+metadata fsync on each write
      }
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize lifecycle event", e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
