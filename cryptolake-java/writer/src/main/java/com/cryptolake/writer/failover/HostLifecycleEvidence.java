package com.cryptolake.writer.failover;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Indexed accessors over host lifecycle events loaded from the JSONL ledger.
 *
 * <p>Ports Python's {@code HostLifecycleEvidence} class (design §2.7). Immutable after
 * construction (internal maps made unmodifiable).
 *
 * <p>Events are indexed by component and event type for O(1) lookup. Thread safety: immutable
 * after construction.
 */
public final class HostLifecycleEvidence {

  private final List<JsonNode> events;
  /** Map from component name to the die events for that component. */
  private final Map<String, List<JsonNode>> dieEvents;
  /** Map from component name to stop events. */
  private final Map<String, Boolean> hasStop;
  /** Whether there is at least one maintenance_intent event. */
  private final boolean hasMaintenanceIntentEvent;

  /**
   * Constructs evidence from the given list of events.
   *
   * @param events list of lifecycle event JsonNodes (from ledger JSONL)
   */
  public HostLifecycleEvidence(List<JsonNode> events) {
    this.events = List.copyOf(events);

    Map<String, List<JsonNode>> dieMap = new HashMap<>();
    Map<String, Boolean> stopMap = new HashMap<>();
    boolean hasIntent = false;

    for (JsonNode ev : events) {
      JsonNode typeNode = ev.path("type");
      if (typeNode.isMissingNode()) continue;
      String type = typeNode.asText();
      if ("maintenance_intent".equals(type)) {
        hasIntent = true;
      } else if ("component_die".equals(type)) {
        String component = ev.path("component").asText("unknown");
        dieMap.computeIfAbsent(component, k -> new ArrayList<>()).add(ev);
      } else if ("component_stop".equals(type)) {
        String component = ev.path("component").asText("unknown");
        stopMap.put(component, true);
      }
    }

    // Make internal maps unmodifiable
    for (Map.Entry<String, List<JsonNode>> entry : dieMap.entrySet()) {
      dieMap.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
    }
    this.dieEvents = Collections.unmodifiableMap(dieMap);
    this.hasStop = Collections.unmodifiableMap(stopMap);
    this.hasMaintenanceIntentEvent = hasIntent;
  }

  /** Returns {@code true} if there is at least one {@code component_die} event for the given component. */
  public boolean hasComponentDie(String component) {
    return dieEvents.containsKey(component) && !dieEvents.get(component).isEmpty();
  }

  /**
   * Returns the clean-exit flag for the most recent {@code component_die} event for the given
   * component, or {@code Optional.empty()} if no die event exists.
   *
   * <p>Ports Python's {@code HostLifecycleEvidence.component_clean_exit()} → {@code Optional<Boolean>}.
   * The Java translation maps Python's {@code bool | None} to {@code Optional<Boolean>}.
   */
  public Optional<Boolean> componentCleanExit(String component) {
    List<JsonNode> dies = dieEvents.get(component);
    if (dies == null || dies.isEmpty()) return Optional.empty();
    // Most recent is last (events are ordered by timestamp from ledger)
    JsonNode last = dies.get(dies.size() - 1);
    JsonNode exitCodeNode = last.path("clean_exit");
    if (exitCodeNode.isMissingNode()) return Optional.empty();
    return Optional.of(exitCodeNode.asBoolean(false));
  }

  /** Returns {@code true} if there is a {@code component_stop} event for the given component. */
  public boolean hasComponentStop(String component) {
    return hasStop.containsKey(component);
  }

  /** Returns {@code true} if there is at least one {@code maintenance_intent} event. */
  public boolean hasMaintenanceIntent() {
    return hasMaintenanceIntentEvent;
  }

  /** Returns {@code true} if no events were loaded. */
  public boolean isEmpty() {
    return events.isEmpty();
  }
}
