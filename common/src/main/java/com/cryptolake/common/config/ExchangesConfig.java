package com.cryptolake.common.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/**
 * Top-level exchanges configuration.
 *
 * <p>Ports Python's {@code ExchangesConfig}. Immutable record (Tier 2 §12).
 */
public record ExchangesConfig(
    @JsonProperty("binance") @NotNull @Valid BinanceExchangeConfig binance) {}
