package com.cryptolake.common.envelope;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Carrier for Kafka broker coordinates appended to an envelope after produce/consume.
 *
 * <p>Field order: {@code _topic}, {@code _partition}, {@code _offset} — matches Python's
 * {@code add_broker_coordinates} dict-mutation order (Tier 5 B1; design §6.1).
 *
 * <p>{@code offset = -1L} is a sentinel meaning "synthetic record, no Kafka offset" (Tier 5 M9).
 * {@code partition} is an {@code int} (never exceeds 2^15 in practice); {@code offset} is a {@code
 * long} (routinely exceeds 2^31 — Tier 5 M8).
 *
 * <p>Immutable record — no setters (Tier 2 §12).
 */
@JsonPropertyOrder({"_topic", "_partition", "_offset"})
public record BrokerCoordinates(
    @JsonProperty("_topic") String topic,
    @JsonProperty("_partition") int partition,
    @JsonProperty("_offset") long offset) {}
