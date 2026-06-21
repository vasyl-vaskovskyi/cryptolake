package com.cryptopanner.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Compares two minute-segment files (primary + candidate/shadow) for a single (symbol, stream,
 * minute) during a deploy or WS rotation overlap, returning PASS/FAIL with a diff detail. Shared by
 * both hot-swap variants (design doc §4.3).
 *
 * <p>Rules:
 *
 * <ul>
 *   <li><b>ID-bearing streams</b> ({@code trade}→{@code t}, {@code aggTrade}→{@code a}, {@code
 *       depth@100ms}→{@code u}): compare the sets of IDs. The symmetric difference is allowed only
 *       at the union edges — at most one straddling frame at the min edge and one at the max edge.
 *       Any differing ID in the interior is a real divergence → FAIL.
 *   <li><b>Non-ID streams</b>: compare the multiset of {@code (server_event_time, raw_sha256)}
 *       tuples. With {@code E}-field bucketing the two sides must match exactly (diff 0) → else
 *       FAIL.
 * </ul>
 *
 * <p>Operates on the decompressed capture-envelope lines; file/zstd reading is the caller's job.
 */
public final class EquivalenceChecker {

  private final ObjectMapper mapper;

  public EquivalenceChecker(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  /** PASS/FAIL plus a human-readable detail describing the divergence (empty when PASS). */
  public record Result(boolean pass, String detail) {}

  public Result check(String stream, List<String> primaryLines, List<String> candidateLines) {
    String idField = idField(stream);
    return idField != null
        ? checkIdStream(idField, primaryLines, candidateLines)
        : checkNonIdStream(stream, primaryLines, candidateLines);
  }

  /** {@code trade}→{@code t}, {@code aggTrade}→{@code a}, {@code depth}→{@code u}; else non-ID. */
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

  private Result checkIdStream(String idField, List<String> primary, List<String> candidate) {
    TreeSet<Long> p = idSet(idField, primary);
    TreeSet<Long> c = idSet(idField, candidate);
    if (p.isEmpty() && c.isEmpty()) {
      return new Result(true, "");
    }
    TreeSet<Long> union = new TreeSet<>(p);
    union.addAll(c);
    long min = union.first();
    long max = union.last();

    List<Long> interiorDiffs = new ArrayList<>();
    for (Long id : union) {
      boolean onBoth = p.contains(id) && c.contains(id);
      if (!onBoth && id != min && id != max) {
        interiorDiffs.add(id); // a non-edge ID present on only one side → real divergence
      }
    }
    if (interiorDiffs.isEmpty()) {
      return new Result(true, "");
    }
    return new Result(false, "interior ID divergence (only at edges allowed): " + interiorDiffs);
  }

  private Result checkNonIdStream(String stream, List<String> primary, List<String> candidate) {
    Map<String, Integer> p = timeHashMultiset(stream, primary);
    Map<String, Integer> c = timeHashMultiset(stream, candidate);
    if (p.equals(c)) {
      return new Result(true, "");
    }
    return new Result(false, "non-ID (event_time, payload_hash) multiset mismatch: " + diff(p, c));
  }

  private TreeSet<Long> idSet(String idField, List<String> lines) {
    TreeSet<Long> ids = new TreeSet<>();
    for (String l : lines) {
      JsonNode data = data(l);
      if (data != null && data.path(idField).isIntegralNumber()) {
        ids.add(data.get(idField).asLong());
      }
    }
    return ids;
  }

  private Map<String, Integer> timeHashMultiset(String stream, List<String> lines) {
    Map<String, Integer> counts = new HashMap<>();
    for (String l : lines) {
      JsonNode env = parse(l);
      if (env == null) {
        continue;
      }
      JsonNode data = unwrapData(env);
      if (data == null) {
        continue;
      }
      var instant = EventTime.bucketInstant(stream, data);
      String hash = env.path("raw_sha256").asText(null);
      if (instant.isEmpty() || hash == null) {
        continue;
      }
      String key = instant.get().toEpochMilli() + "|" + hash;
      counts.merge(key, 1, Integer::sum);
    }
    return counts;
  }

  /** Returns the inner frame's {@code data} node, or null if the line isn't a usable ws frame. */
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

  private static String diff(Map<String, Integer> p, Map<String, Integer> c) {
    int onlyP = 0;
    int onlyC = 0;
    for (var e : p.entrySet()) {
      if (!e.getValue().equals(c.get(e.getKey()))) onlyP++;
    }
    for (var e : c.entrySet()) {
      if (!e.getValue().equals(p.get(e.getKey()))) onlyC++;
    }
    return onlyP + " differing on primary, " + onlyC + " differing on candidate";
  }
}
