package com.cryptolake.common.envelope;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

/**
 * Single {@link ObjectMapper} owner for the envelope codec path.
 *
 * <p>One instance per service — callers receive it via constructor injection (Tier 2 §14; Tier 5
 * B6). The static factory {@link #newMapper()} lets each service's {@code Main} create exactly one
 * mapper and pass it everywhere.
 *
 * <p>Broker-coordinates serialization uses Option A from design §6.1: thin wrapper records with
 * {@code @JsonUnwrapped} so the three broker fields appear at the end of the JSON object, matching
 * Python's post-mutation {@code orjson.dumps()} output.
 *
 * <p>Thread safety: {@link ObjectMapper} is thread-safe after configuration; this instance is
 * immutable post-construction.
 */
public final class EnvelopeCodec {

  private final ObjectMapper mapper;

  /** Constructs a codec backed by the supplied pre-configured mapper. */
  public EnvelopeCodec(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  // ── Wrapper records for broker-coordinates serialization (Option A, design §6.1) ──

  /** Wraps a {@link DataEnvelope} with broker coordinates for serialization. */
  private record DataWithBroker(
      @JsonUnwrapped DataEnvelope env, @JsonUnwrapped BrokerCoordinates coords) {}

  /** Wraps a {@link GapEnvelope} with broker coordinates for serialization. */
  private record GapWithBroker(
      @JsonUnwrapped GapEnvelope env, @JsonUnwrapped BrokerCoordinates coords) {}

  // ── Static mapper factories (Tier 5 B6; design §6.2) ──

  /**
   * Creates a configured JSON {@link ObjectMapper} (design §6.2).
   *
   * <ul>
   *   <li>{@code SORT_PROPERTIES_ALPHABETICALLY=false} — {@code @JsonPropertyOrder} governs order
   *       (Tier 5 B1)
   *   <li>{@code ORDER_MAP_ENTRIES_BY_KEYS=false} — insertion order for maps
   *   <li>{@code INDENT_OUTPUT=false} — compact JSON matching orjson defaults (Tier 5 B2)
   *   <li>{@code WRITE_DATES_AS_TIMESTAMPS=false}
   *   <li>{@code FAIL_ON_UNKNOWN_PROPERTIES=false} — forward-compatibility
   *   <li>No global naming strategy — explicit {@code @JsonProperty} everywhere (Tier 5 J1)
   * </ul>
   */
  public static ObjectMapper newMapper() {
    ObjectMapper m = new ObjectMapper();
    m.disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
    m.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, false);
    m.disable(SerializationFeature.INDENT_OUTPUT);
    m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return m;
  }

  /**
   * Creates a configured YAML {@link ObjectMapper} using the same feature set (Tier 5 B7). The same
   * {@code @JsonProperty} annotations work for both YAML config and JSON envelopes.
   */
  public static ObjectMapper newYamlMapper() {
    ObjectMapper m = new ObjectMapper(new YAMLFactory());
    m.disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
    m.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, false);
    m.disable(SerializationFeature.INDENT_OUTPUT);
    m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return m;
  }

  // ── Serialize ──

  /**
   * Serializes an envelope (or any object) to compact JSON bytes (Tier 5 B2). No trailing newline —
   * caller appends {@code 0x0A} if needed.
   */
  public byte[] toJsonBytes(Object envelope) {
    try {
      return mapper.writeValueAsBytes(envelope);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Appends a newline byte ({@code 0x0A}) to the supplied JSON bytes (Tier 5 B2). */
  public byte[] appendNewline(byte[] jsonBytes) {
    byte[] out = Arrays.copyOf(jsonBytes, jsonBytes.length + 1);
    out[jsonBytes.length] = 0x0A;
    return out;
  }

  // ── Deserialize ──

  /** Deserializes bytes into a {@link DataEnvelope} (Tier 5 B3). */
  public DataEnvelope readData(byte[] bytes) {
    try {
      return mapper.readValue(bytes, DataEnvelope.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Deserializes bytes into a {@link GapEnvelope} (Tier 5 B3). */
  public GapEnvelope readGap(byte[] bytes) {
    try {
      return mapper.readValue(bytes, GapEnvelope.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Parses bytes into a {@link JsonNode} for lightweight routing (e.g., inspecting the {@code
   * "stream"} field without full deserialization — Tier 5 B3).
   */
  public JsonNode readTree(byte[] bytes) {
    try {
      return mapper.readTree(bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ── Broker coordinates ──

  /**
   * Returns a serialization wrapper that appends {@link BrokerCoordinates} fields at the end of the
   * JSON object, matching Python's post-mutation {@code orjson.dumps()} output (design §6.1 Option
   * A; Tier 2 §12 — records are immutable, no in-place mutation).
   *
   * <p>Usage: {@code codec.toJsonBytes(codec.withBrokerCoordinates(env, coords))}
   */
  public static DataWithBroker withBrokerCoordinates(DataEnvelope env, BrokerCoordinates coords) {
    return new DataWithBroker(env, coords);
  }

  /**
   * Returns a serialization wrapper that appends {@link BrokerCoordinates} fields at the end of the
   * JSON object (see {@link #withBrokerCoordinates(DataEnvelope, BrokerCoordinates)}).
   */
  public static GapWithBroker withBrokerCoordinates(GapEnvelope env, BrokerCoordinates coords) {
    return new GapWithBroker(env, coords);
  }
}
