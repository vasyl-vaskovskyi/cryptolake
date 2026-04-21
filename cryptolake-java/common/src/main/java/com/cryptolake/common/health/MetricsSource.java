package com.cryptolake.common.health;

import java.util.function.Supplier;

/**
 * Functional interface for the {@code /metrics} endpoint.
 *
 * <p>Returns Prometheus text-format bytes (e.g., from {@code
 * PrometheusMeterRegistry.scrape().getBytes(UTF_8)}). The bytes are served verbatim — {@link
 * HealthServer} does NOT transform them (design §9; Tier 3 §18).
 *
 * <p>Implementations MUST be thread-safe.
 */
@FunctionalInterface
public interface MetricsSource extends Supplier<byte[]> {}
