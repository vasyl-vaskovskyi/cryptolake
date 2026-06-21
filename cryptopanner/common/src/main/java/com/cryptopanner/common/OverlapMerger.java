package com.cryptopanner.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Merges two minute-segment files (primary + shadow/candidate) for one (symbol, stream, minute)
 * into a single ordered list of capture-envelope lines (design doc §3.2). The §4.3 equivalence
 * check is the gate that decides whether the merge runs; this only produces the union.
 *
 * <ul>
 *   <li><b>ID streams</b> ({@code trade}→{@code t}, {@code aggTrade}→{@code a}, {@code
 *       depth@100ms}→{@code u}): union by ID, output sorted by ID ascending. A shared ID with
 *       byte-identical payload keeps one copy; a shared ID with diverging bytes keeps the
 *       <b>primary</b> copy and records a {@code merger_divergent_id} note.
 *   <li><b>Non-ID streams</b>: union of the {@code (server_event_time, raw_sha256)} multiset,
 *       output sorted by {@code (event_time ascending, raw_sha256 lexicographic)}.
 * </ul>
 *
 * <p>Operates on decompressed lines; the durable file write (tmp→fsync→rename, §3.2) is a separate
 * concern layered on top.
 */
public final class OverlapMerger {

  private final ObjectMapper mapper;

  public OverlapMerger(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  /** Merged lines (ordered) plus any divergence notes (shared ID, differing bytes). */
  public record Merged(List<String> lines, List<String> divergences) {}

  public Merged merge(String stream, List<String> primary, List<String> candidate) {
    String idField = idField(stream);
    return idField != null
        ? mergeIdStream(idField, primary, candidate)
        : mergeNonIdStream(stream, primary, candidate);
  }

  private static String idField(String stream) {
    String base = stream;
    int at = stream.indexOf('@');
    if (at >= 0) {
      base = stream.substring(0, at);
    }
    return switch (base) {
      case "trade" -> "t";
      case "aggTrade" -> "a";
      case "depth" -> "u";
      default -> null;
    };
  }

  private Merged mergeIdStream(String idField, List<String> primary, List<String> candidate) {
    // Sorted by ID; primary wins on a shared ID. Track divergences when bytes differ.
    TreeMap<Long, String> byId = new TreeMap<>();
    List<String> divergences = new ArrayList<>();
    for (String l : primary) {
      Long id = idOf(l, idField);
      if (id != null) {
        byId.putIfAbsent(id, l);
      }
    }
    for (String l : candidate) {
      Long id = idOf(l, idField);
      if (id == null) {
        continue;
      }
      String existing = byId.get(id);
      if (existing == null) {
        byId.put(id, l); // candidate-only record: include unconditionally
      } else if (!rawOf(existing).equals(rawOf(l))) {
        divergences.add(
            "merger_divergent_id id="
                + id
                + " primary="
                + hashOf(existing)
                + " candidate="
                + hashOf(l));
        // keep primary's bytes (already in byId)
      }
    }
    return new Merged(new ArrayList<>(byId.values()), divergences);
  }

  private Merged mergeNonIdStream(String stream, List<String> primary, List<String> candidate) {
    // Multiset union keyed by (event_time, raw_sha256); per key keep max(count) copies of a
    // representative line, output sorted by (event_time asc, hash lexicographic).
    Map<String, Integer> pCount = new LinkedHashMap<>();
    Map<String, String> repLine = new LinkedHashMap<>();
    countKeys(stream, primary, pCount, repLine);
    Map<String, Integer> cCount = new LinkedHashMap<>();
    countKeys(stream, candidate, cCount, repLine);

    TreeMap<String, Integer> unionCount = new TreeMap<>();
    for (var e : pCount.entrySet()) {
      unionCount.merge(e.getKey(), e.getValue(), Math::max);
    }
    for (var e : cCount.entrySet()) {
      unionCount.merge(e.getKey(), e.getValue(), Math::max);
    }

    List<String> out = new ArrayList<>();
    for (var e : unionCount.entrySet()) {
      for (int i = 0; i < e.getValue(); i++) {
        out.add(repLine.get(e.getKey()));
      }
    }
    return new Merged(out, List.of());
  }

  /**
   * Builds sort-stable keys {@code <19-digit-zero-padded-eventMillis>|<hash>} → count + rep line.
   */
  private void countKeys(
      String stream, List<String> lines, Map<String, Integer> counts, Map<String, String> repLine) {
    for (String l : lines) {
      JsonNode env = parse(l);
      if (env == null) {
        continue;
      }
      JsonNode data = unwrapData(env);
      String hash = env.path("raw_sha256").asText(null);
      if (data == null || hash == null) {
        continue;
      }
      var instant = EventTime.bucketInstant(stream, data);
      if (instant.isEmpty()) {
        continue;
      }
      String key = String.format("%019d|%s", instant.get().toEpochMilli(), hash);
      counts.merge(key, 1, Integer::sum);
      repLine.putIfAbsent(key, l);
    }
  }

  private Long idOf(String line, String idField) {
    JsonNode data = data(line);
    if (data != null && data.path(idField).isIntegralNumber()) {
      return data.get(idField).asLong();
    }
    return null;
  }

  private String rawOf(String line) {
    JsonNode env = parse(line);
    return env == null ? line : env.path("raw").asText("");
  }

  private String hashOf(String line) {
    JsonNode env = parse(line);
    return env == null ? "" : env.path("raw_sha256").asText("");
  }

  private JsonNode data(String line) {
    JsonNode env = parse(line);
    return env == null ? null : unwrapData(env);
  }

  private JsonNode unwrapData(JsonNode env) {
    try {
      JsonNode inner = CaptureEnvelope.unwrap(mapper, env);
      JsonNode data = inner.path("data");
      return data.isObject() ? data : null;
    } catch (Exception e) {
      return null;
    }
  }

  private JsonNode parse(String line) {
    try {
      return mapper.readTree(line);
    } catch (Exception e) {
      return null;
    }
  }
}
