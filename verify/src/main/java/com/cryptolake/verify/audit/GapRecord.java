package com.cryptolake.verify.audit;

import com.cryptolake.common.envelope.GapReason;

public record GapRecord(
    String source, // "file.envelope", "file.missing_hour", "pg.component_runtime", "ledger", ...
    String exchange,
    String symbol,
    String stream,
    long startMs,
    long endMs,
    GapReason reason,
    String detail) {

  public GapRecord {
    java.util.Objects.requireNonNull(reason, "reason");
  }

  /** Diff key — what makes two records "equal" for the gating comparison. */
  public DiffKey diffKey() {
    return new DiffKey(exchange, symbol, stream, startMs, endMs, reason);
  }

  public record DiffKey(
      String exchange, String symbol, String stream, long startMs, long endMs, GapReason reason) {}
}
