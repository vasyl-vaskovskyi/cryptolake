package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

/**
 * Database connection configuration.
 *
 * <p>Ports Python's {@code DatabaseConfig}. Immutable record (Tier 2 §12).
 */
public record DatabaseConfig(@JsonProperty("url") @NotBlank String url) {}
