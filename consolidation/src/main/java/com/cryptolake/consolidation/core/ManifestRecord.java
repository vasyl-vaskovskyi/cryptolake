package com.cryptolake.consolidation.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import java.util.Map;

/**
 * Manifest record for a consolidated daily archive.
 *
 * <p>Field order is LOAD-BEARING (Tier 5 B1; Tier 3 §21) — matches Python's manifest schema.
 *
 * <p>Tier 2 §12 — record (immutable, no setters).
 */
@JsonPropertyOrder({
  "version",
  "exchange",
  "symbol",
  "stream",
  "date",
  "consolidated_at",
  "daily_file",
  "daily_file_sha256",
  "total_records",
  "data_records",
  "gap_records",
  "hours",
  "missing_hours",
  "source_files"
})
public record ManifestRecord(
    @JsonProperty("version") int version,
    @JsonProperty("exchange") String exchange,
    @JsonProperty("symbol") String symbol,
    @JsonProperty("stream") String stream,
    @JsonProperty("date") String date,
    @JsonProperty("consolidated_at") String consolidatedAt,
    @JsonProperty("daily_file") String dailyFile,
    @JsonProperty("daily_file_sha256") String dailyFileSha256,
    @JsonProperty("total_records") long totalRecords,
    @JsonProperty("data_records") long dataRecords,
    @JsonProperty("gap_records") long gapRecords,
    @JsonProperty("hours") Map<String, HourSummary> hours,
    @JsonProperty("missing_hours") List<Integer> missingHours,
    @JsonProperty("source_files") List<String> sourceFiles) {

  /**
   * Per-hour summary within the manifest.
   *
   * <p>Field order matches Python's manifest schema (Tier 5 B1).
   */
  @JsonPropertyOrder({"status", "data_records", "gap_records", "sources"})
  public record HourSummary(
      @JsonProperty("status") String status,
      @JsonProperty("data_records") long dataRecords,
      @JsonProperty("gap_records") long gapRecords,
      @JsonProperty("sources") List<String> sources) {}
}
