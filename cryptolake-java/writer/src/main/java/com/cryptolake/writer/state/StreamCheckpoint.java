package com.cryptolake.writer.state;

/**
 * Durable checkpoint for a stream: the last-written envelope metadata so writer can resume from
 * the correct Kafka offset on restart (Tier 1 §6 — replay over reconstruction).
 *
 * <p>Ports Python's {@code StreamCheckpoint} (design §6.6). PG table {@code stream_checkpoint},
 * PK {@code (exchange, symbol, stream)}.
 *
 * <p>Timestamp fields stored as ISO-8601 strings to match Python's {@code datetime.isoformat()}
 * round-trip (Tier 5 F1, F2). {@code StateManager} parses via {@code OffsetDateTime.parse}
 * tolerating {@code Z} / {@code +00:00}. {@code lastGapReason} is nullable.
 *
 * <p>Immutable record (Tier 2 §12).
 */
public record StreamCheckpoint(
    String exchange,
    String symbol,
    String stream,
    String lastReceivedAt, // ISO-8601 string (Tier 5 F1, F2)
    String lastCollectorSessionId,
    String lastGapReason) {} // nullable
