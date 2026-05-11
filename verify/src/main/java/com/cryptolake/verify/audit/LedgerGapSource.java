package com.cryptolake.verify.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads {@link GapRecord}s from per-collector lifecycle ledger files.
 *
 * <p>Ledger files live at {@code ${dataDir}/cryptolake/<collectorId>/lifecycle.jsonl} (written by
 * the collector's {@code LifecycleJournal}). Each line is a compact JSON object with fields {@code
 * ts_ns}, {@code event}, {@code host_boot_id}, {@code collector_session_id}, optional {@code
 * planned}, and optional {@code maintenance_id}.
 *
 * <p>For every {@code clean_shutdown} event whose timestamp overlaps the {@link AuditScope} and
 * whose {@code collector_session_id} matches a preceding {@code start} event in the same file, one
 * {@link GapRecord} is emitted with {@code source="ledger"}.
 *
 * <p>Reason classification:
 *
 * <ul>
 *   <li>{@code planned=true} → {@code reason="collector_restart"}
 *   <li>{@code planned=false} or absent → {@code reason="restart_gap"}
 * </ul>
 *
 * <p>Graceful degradation: if the {@code ${dataDir}/cryptolake/} directory does not exist, or any
 * individual journal file fails to parse, the source returns an empty list (or partial results)
 * without throwing.
 */
public final class LedgerGapSource implements GapSource {

  private static final Logger log = LoggerFactory.getLogger(LedgerGapSource.class);

  private static final String SOURCE_LABEL = "ledger";

  private final Path dataDir;
  private final ObjectMapper mapper;

  /**
   * Constructs a {@code LedgerGapSource}.
   *
   * @param dataDir host data directory (e.g. {@code /data}); ledgers live at {@code
   *     ${dataDir}/cryptolake/<collectorId>/lifecycle.jsonl}
   * @param mapper shared {@link ObjectMapper}
   */
  public LedgerGapSource(Path dataDir, ObjectMapper mapper) {
    this.dataDir = dataDir;
    this.mapper = mapper;
  }

  @Override
  public String name() {
    return "LedgerGapSource";
  }

  @Override
  public List<GapRecord> read(AuditScope scope) {
    Path cryptolakeDir = dataDir.resolve("cryptolake");
    if (!Files.exists(cryptolakeDir)) {
      return List.of();
    }

    List<GapRecord> result = new ArrayList<>();
    List<Path> collectorDirs = new ArrayList<>();
    try (var stream = Files.list(cryptolakeDir)) {
      stream.filter(Files::isDirectory).forEach(collectorDirs::add);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to list cryptolake dirs under " + cryptolakeDir, e);
    }

    for (Path collectorDir : collectorDirs) {
      String collectorId = collectorDir.getFileName().toString();
      Path journalPath = collectorDir.resolve("lifecycle.jsonl");
      readJournalRecords(journalPath, collectorId, scope, result);
    }

    return result;
  }

  // ── private helpers ───────────────────────────────────────────────────────

  private void readJournalRecords(
      Path journalPath, String collectorId, AuditScope scope, List<GapRecord> result) {
    if (!Files.exists(journalPath)) {
      return;
    }

    // Accumulate start events by session id as we read the file in order.
    // Only the *last* start per session id is kept (matching LifecycleJournal semantics).
    Map<String, Long> startNsBySession = new HashMap<>();
    Map<String, String> hostBootBySession = new HashMap<>();

    try (BufferedReader br = Files.newBufferedReader(journalPath, StandardCharsets.UTF_8)) {
      String line;
      while ((line = br.readLine()) != null) {
        line = line.strip();
        if (line.isEmpty()) {
          continue;
        }
        try {
          ObjectNode node = (ObjectNode) mapper.readTree(line);
          long tsNs = node.path("ts_ns").asLong(0);
          String event = node.path("event").asText(null);
          String sessionId = node.path("collector_session_id").asText(null);
          String hostBootId = node.path("host_boot_id").asText(null);

          if ("start".equals(event) && sessionId != null) {
            startNsBySession.put(sessionId, tsNs);
            hostBootBySession.put(sessionId, hostBootId);
          } else if ("clean_shutdown".equals(event) && sessionId != null) {
            long shutdownMs = tsNs / 1_000_000L;

            // Only emit if the shutdown timestamp is within scope
            if (shutdownMs < scope.startMs() || shutdownMs > scope.endMs()) {
              continue;
            }

            Long startNs = startNsBySession.get(sessionId);
            if (startNs == null) {
              // No matching start — skip
              continue;
            }

            long startMs = startNs / 1_000_000L;
            boolean planned = node.has("planned") && node.path("planned").asBoolean(false);
            String maintenanceId =
                node.has("maintenance_id") ? node.path("maintenance_id").asText(null) : null;
            String bootId = hostBootBySession.getOrDefault(sessionId, hostBootId);

            String reason = planned ? "collector_restart" : "restart_gap";
            String detail =
                "collector_id="
                    + collectorId
                    + "; collector_session_id="
                    + sessionId
                    + "; host_boot_id="
                    + (bootId != null ? bootId : "-")
                    + "; maintenance_id="
                    + (maintenanceId != null ? maintenanceId : "-");

            result.add(
                new GapRecord(SOURCE_LABEL, "", "", "", startMs, shutdownMs, reason, detail));
          }
        } catch (Exception e) {
          // Corrupt line — drop silently (matches LifecycleJournal's crash-resilient read)
          log.debug(
              "ledger_gap_source_skipped_line",
              "collector_id",
              collectorId,
              "reason",
              e.getMessage());
        }
      }
    } catch (NoSuchFileException ignored) {
      // file vanished after exists() check — treat as empty
    } catch (IOException e) {
      log.warn(
          "ledger_gap_source_read_failed", "path", journalPath.toString(), "error", e.getMessage());
    }
  }
}
