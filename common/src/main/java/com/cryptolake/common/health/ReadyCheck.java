package com.cryptolake.common.health;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Functional interface for the {@code /ready} endpoint readiness check.
 *
 * <p>Returns a map from check name to boolean status. If all values are {@code true}, the endpoint
 * returns HTTP 200; otherwise HTTP 503.
 *
 * <p>Implementations MUST be thread-safe — they are called from arbitrary virtual threads serving
 * HTTP requests.
 */
@FunctionalInterface
public interface ReadyCheck extends Supplier<Map<String, Boolean>> {}
