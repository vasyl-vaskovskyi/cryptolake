package com.cryptolake.writer.durability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads lifecycle journals from both the primary and backup collector data directories.
 *
 * <p>Replaces the Python {@code HostLifecycleReader} for the Java writer. Scans each collector's
 * {@code lifecycle.jsonl} (written by the collector's {@code LifecycleJournal}) and exposes the
 * results as a {@code Map<sessionId, JournalEntry>} for use by {@code RestartGapClassifier}.
 *
 * <p>Discovery: both {@code ${dataDir}/cryptolake/binance-collector-01/lifecycle.jsonl} and {@code
 * ${dataDir}/cryptolake/binance-collector-backup/lifecycle.jsonl} are read if they exist. Missing
 * files are silently skipped.
 *
 * <p>Format: each line is a compact JSON object with fields {@code ts_ns}, {@code event}, {@code
 * host_boot_id}, {@code collector_session_id}, optional {@code planned}, optional {@code
 * maintenance_id}.
 *
 * <p>Thread safety: stateless; all methods are static.
 */
public final class LifecycleJournalReader {

  private static final Logger log = LoggerFactory.getLogger(LifecycleJournalReader.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private LifecycleJournalReader() {}

  /**
   * Immutable record representing one parsed lifecycle event from the JSONL journal.
   *
   * @param tsNs nanoseconds since Unix epoch
   * @param event event type ({@code "start"} or {@code "clean_shutdown"})
   * @param hostBootId OS boot identifier at the time of this event
   * @param collectorSessionId unique session identifier for the collector process
   * @param planned whether the shutdown was planned (null for non-shutdown events)
   * @param maintenanceId optional maintenance intent identifier
   */
  public record JournalEntry(
      long tsNs,
      String event,
      String hostBootId,
      String collectorSessionId,
      Boolean planned,
      String maintenanceId) {}

  /**
   * Reads lifecycle events from both collector journal files and returns a map keyed by {@code
   * collectorSessionId}.
   *
   * <p>Events from both collectors are merged; if the same session ID appears in both files (should
   * not happen in practice), the later entry wins.
   *
   * @param dataDir host data directory (e.g. {@code /data})
   * @param primaryCollectorId collector ID of the primary (e.g. {@code "binance-collector-01"})
   * @param backupCollectorId collector ID of the backup (e.g. {@code "binance-collector-backup"})
   * @param sinceNs lower bound: only return events with {@code ts_ns >= sinceNs}; pass 0 for all
   * @return immutable map from {@code collector_session_id} → most-recent {@link JournalEntry} for
   *     that session; entries appear in file-chronological order
   */
  public static Map<String, JournalEntry> readSince(
      Path dataDir, String primaryCollectorId, String backupCollectorId, long sinceNs) {
    Map<String, JournalEntry> result = new LinkedHashMap<>();
    for (String collectorId : new String[] {primaryCollectorId, backupCollectorId}) {
      Path journalPath =
          dataDir.resolve("cryptolake").resolve(collectorId).resolve("lifecycle.jsonl");
      readJournalFile(journalPath, sinceNs, result);
    }
    return Collections.unmodifiableMap(result);
  }

  /**
   * Convenience overload that reads all events (since epoch=0) from both collectors.
   *
   * @param dataDir host data directory
   * @param primaryCollectorId collector ID of the primary
   * @param backupCollectorId collector ID of the backup
   * @return map from session ID → most-recent entry (see {@link #readSince})
   */
  public static Map<String, JournalEntry> readAll(
      Path dataDir, String primaryCollectorId, String backupCollectorId) {
    return readSince(dataDir, primaryCollectorId, backupCollectorId, 0L);
  }

  /**
   * Returns {@code true} if the most-recent event for {@code sessionId} in the map is {@code
   * "clean_shutdown"}.
   *
   * @param journalEntries map returned by {@link #readSince} or {@link #readAll}
   * @param sessionId session to check
   * @return whether the session ended cleanly according to the journal
   */
  public static boolean wasCleanShutdown(
      Map<String, JournalEntry> journalEntries, String sessionId) {
    JournalEntry ev = journalEntries.get(sessionId);
    return ev != null && "clean_shutdown".equals(ev.event());
  }

  // ── private helpers ───────────────────────────────────────────────────────

  private static void readJournalFile(
      Path journalPath, long sinceNs, Map<String, JournalEntry> target) {
    if (!Files.exists(journalPath)) {
      return;
    }
    try (BufferedReader br = Files.newBufferedReader(journalPath, StandardCharsets.UTF_8)) {
      String line;
      int count = 0;
      while ((line = br.readLine()) != null) {
        line = line.strip();
        if (line.isEmpty()) continue;
        try {
          ObjectNode node = (ObjectNode) MAPPER.readTree(line);
          long tsNs = node.path("ts_ns").asLong(0);
          if (tsNs < sinceNs) continue;
          String event = node.path("event").asText(null);
          String hostBootId = node.path("host_boot_id").asText(null);
          String sessionId = node.path("collector_session_id").asText(null);
          if (sessionId == null) continue;
          Boolean planned = node.has("planned") ? node.path("planned").asBoolean() : null;
          String maintenanceId =
              node.has("maintenance_id") ? node.path("maintenance_id").asText(null) : null;
          target.put(
              sessionId,
              new JournalEntry(tsNs, event, hostBootId, sessionId, planned, maintenanceId));
          count++;
        } catch (Exception ignored) {
          // corrupt line — drop silently
        }
      }
      if (count > 0) {
        log.debug("lifecycle_journal_read", "path", journalPath, "events", count);
      }
    } catch (NoSuchFileException ignored) {
      // file removed between exists() check and read — ok
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
