package com.cryptolake.writer.failover;

import com.cryptolake.common.jsonl.JsonlReader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads and filters host lifecycle events from the JSONL ledger.
 *
 * <p>Ports Python's {@code host_lifecycle_reader.py:load_host_evidence} (design §2.7; design §4.7).
 *
 * <p>Uses {@link JsonlReader#readAll(Path, ObjectMapper)} + stream-filter by ISO timestamps.
 * Tolerates {@code +00:00} / {@code Z} / naive timestamps (Tier 5 F2).
 *
 * <p>Thread safety: stateless; all methods are static.
 */
public final class HostLifecycleReader {

  private static final Logger log = LoggerFactory.getLogger(HostLifecycleReader.class);

  /** Default ledger path (overridable via {@code LIFECYCLE_LEDGER_PATH} env var). */
  public static final Path DEFAULT_LEDGER_PATH =
      Path.of("/data/.cryptolake/lifecycle/events.jsonl");

  private HostLifecycleReader() {}

  /**
   * Loads host lifecycle evidence from the ledger file, filtering events to the window {@code
   * [windowStartIso, windowEndIso)}.
   *
   * <p>Ports {@code load_host_evidence(ledger_path, window_start_iso, window_end_iso)}.
   *
   * @param ledgerPath path to the lifecycle JSONL ledger
   * @param windowStartIso ISO-8601 start (inclusive); if null, no lower bound
   * @param windowEndIso ISO-8601 end (exclusive); if null, no upper bound
   * @param mapper Jackson ObjectMapper for parsing
   * @return {@link HostLifecycleEvidence} (may be empty if no events in window)
   */
  public static HostLifecycleEvidence load(
      Path ledgerPath, String windowStartIso, String windowEndIso, ObjectMapper mapper) {
    List<JsonNode> all = JsonlReader.readAll(ledgerPath, mapper);
    if (all.isEmpty()) {
      return new HostLifecycleEvidence(List.of());
    }

    Instant windowStart = parseIsoOrNull(windowStartIso);
    Instant windowEnd = parseIsoOrNull(windowEndIso);

    List<JsonNode> filtered = new ArrayList<>();
    for (JsonNode ev : all) {
      JsonNode tsNode = ev.path("timestamp");
      if (tsNode.isMissingNode()) {
        filtered.add(ev); // events without timestamp pass through
        continue;
      }
      String tsStr = tsNode.asText();
      Instant ts = parseIsoOrNull(tsStr);
      if (ts == null) {
        log.debug("lifecycle_event_bad_timestamp", "ts", tsStr);
        continue; // malformed timestamp — skip (matches Python's except: continue)
      }
      if (windowStart != null && ts.isBefore(windowStart)) continue;
      if (windowEnd != null && !ts.isBefore(windowEnd)) continue;
      filtered.add(ev);
    }
    return new HostLifecycleEvidence(filtered);
  }

  /**
   * Parses an ISO-8601 timestamp string tolerating {@code Z}, {@code +00:00}, and naive
   * (timezone-free) forms (Tier 5 F2). Returns {@code null} on parse error.
   */
  public static Instant parseIsoOrNull(String iso) {
    if (iso == null || iso.isBlank()) return null;
    // Try as OffsetDateTime first (handles both Z and +00:00 — Tier 5 F2)
    try {
      return OffsetDateTime.parse(iso).toInstant();
    } catch (DateTimeParseException e1) {
      // Try naive timestamp — interpret as UTC (Tier 5 F2)
      try {
        return java.time.LocalDateTime.parse(iso).toInstant(ZoneOffset.UTC);
      } catch (DateTimeParseException e2) {
        return null;
      }
    }
  }
}
