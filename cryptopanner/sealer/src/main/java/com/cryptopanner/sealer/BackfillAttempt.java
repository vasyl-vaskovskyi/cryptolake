package com.cryptopanner.sealer;

/**
 * One entry in the manifest's {@code backfill_attempts[]} array — describes a single REST call made
 * to fill a sequence-ID gap (master spec §10.d). One attempt per gap for now; once §9.b.3's
 * 3-attempts-with-backoff loop is wired the {@code attempts} count grows above 1.
 */
public record BackfillAttempt(
    String endpoint,
    long fromId,
    long toId,
    int attempts,
    int httpStatus,
    int recordsInserted,
    RestBackfiller.Outcome outcome,
    String error /* nullable */) {}
