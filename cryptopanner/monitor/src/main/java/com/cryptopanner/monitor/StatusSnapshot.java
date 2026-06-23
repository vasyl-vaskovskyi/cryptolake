package com.cryptopanner.monitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Typed model of the Node Agent's {@code GET /status} JSON snapshot (master spec §11.c). The
 * Monitor scrapes this every {@code scrape_interval_s} and feeds it to the alert evaluator and the
 * dashboard.
 *
 * <p>Parsed by tree-walking the {@link JsonNode} rather than data-binding so the model stays robust
 * against the shared mapper's naming strategy and tolerates the nullable/nested fields (a {@code
 * down} component reports {@code pid: null, heartbeat_age_s: null}). Unknown fields are ignored.
 */
public record StatusSnapshot(
    String node,
    String scrapedAt,
    Map<String, Component> components,
    String activeSlot,
    String fsHeavyLockHeldBy,
    String deployState,
    String rotationState,
    Double currentConnectionAgeS,
    Map<String, Double> diskPercentByMount) {

  /** One component's state line from {@code /status.components}. */
  public record Component(String state, Integer pid, Double heartbeatAgeS, Long uptimeS) {}

  public static StatusSnapshot parse(String json, ObjectMapper mapper) throws IOException {
    JsonNode root = mapper.readTree(json);

    Map<String, Component> components = new LinkedHashMap<>();
    JsonNode comps = root.path("components");
    comps
        .fieldNames()
        .forEachRemaining(
            name -> {
              JsonNode c = comps.get(name);
              components.put(
                  name,
                  new Component(
                      text(c, "state"),
                      intOrNull(c, "pid"),
                      doubleOrNull(c, "heartbeat_age_s"),
                      longOrNull(c, "uptime_s")));
            });

    Map<String, Double> disk = new LinkedHashMap<>();
    JsonNode disks = root.path("vps").path("disk");
    disks
        .fieldNames()
        .forEachRemaining(mount -> disk.put(mount, doubleOrNull(disks.get(mount), "percent")));

    return new StatusSnapshot(
        text(root, "node"),
        text(root, "scraped_at"),
        Map.copyOf(components),
        text(root, "active_slot"),
        text(root.path("fs_heavy_lock"), "held_by"),
        text(root.path("deploy"), "state"),
        text(root.path("rotation"), "state"),
        doubleOrNull(root.path("rotation"), "current_connection_age_s"),
        Map.copyOf(disk));
  }

  /** Disk-usage percent for a mount (e.g. {@code "/data"}), or {@code null} if not reported. */
  public Double diskPercent(String mount) {
    return diskPercentByMount.get(mount);
  }

  /** The collector component name for the currently active slot, e.g. {@code collector@a}. */
  public String activeCollectorComponent() {
    return "cryptopanner-collector@" + activeSlot;
  }

  private static String text(JsonNode n, String field) {
    JsonNode v = n.get(field);
    return v == null || v.isNull() ? null : v.asText();
  }

  private static Integer intOrNull(JsonNode n, String field) {
    JsonNode v = n.get(field);
    return v == null || v.isNull() ? null : v.asInt();
  }

  private static Long longOrNull(JsonNode n, String field) {
    JsonNode v = n.get(field);
    return v == null || v.isNull() ? null : v.asLong();
  }

  private static Double doubleOrNull(JsonNode n, String field) {
    JsonNode v = n.get(field);
    return v == null || v.isNull() ? null : v.asDouble();
  }
}
