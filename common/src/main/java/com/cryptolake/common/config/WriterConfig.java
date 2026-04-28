package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;

/**
 * Writer service configuration.
 *
 * <p>Ports Python's {@code WriterConfig}. Default {@code baseDir} is resolved from the {@code
 * HOST_DATA_DIR} environment variable (Tier 5 J4 watch-out). Immutable record (Tier 2 §12).
 */
public record WriterConfig(
    @JsonProperty("base_dir") String baseDir,
    @JsonProperty("rotation") String rotation,
    @JsonProperty("compression") String compression,
    @JsonProperty("compression_level") int compressionLevel,
    @JsonProperty("checksum") String checksum,
    @JsonProperty("flush_messages") int flushMessages,
    @JsonProperty("flush_interval_seconds") int flushIntervalSeconds,
    @JsonProperty("gap_filter") @Valid GapFilterConfig gapFilter) {

  public WriterConfig {
    if (baseDir == null) baseDir = YamlConfigLoader.defaultArchiveDir();
    if (rotation == null) rotation = "hourly";
    if (compression == null) compression = "zstd";
    if (compressionLevel == 0) compressionLevel = 3;
    if (checksum == null) checksum = "sha256";
    if (flushMessages == 0) flushMessages = 10_000;
    if (flushIntervalSeconds == 0) flushIntervalSeconds = 30;
    if (gapFilter == null) gapFilter = new GapFilterConfig();
  }

  /** Default constructor with all defaults applied. */
  public WriterConfig() {
    this(null, null, null, 0, null, 0, 0, null);
  }
}
