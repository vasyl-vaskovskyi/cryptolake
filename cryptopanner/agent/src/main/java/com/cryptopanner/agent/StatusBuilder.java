package com.cryptopanner.agent;

import com.cryptopanner.common.Heartbeat;
import com.cryptopanner.common.SlotManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Builds the {@code GET /status} JSON snapshot (master spec §11.c) from per-component heartbeat
 * files + the systemd active flag + the active slot. The per-component state is derived via {@link
 * HeartbeatState}; a component with no/expired heartbeat reports {@code heartbeat_age_s: null}.
 */
public final class StatusBuilder {

  private final ObjectMapper mapper;
  private final String nodeId;
  private final Duration degradedThreshold;
  private final Duration stuckThreshold;

  public StatusBuilder(
      ObjectMapper mapper, String nodeId, Duration degradedThreshold, Duration stuckThreshold) {
    this.mapper = mapper;
    this.nodeId = nodeId;
    this.degradedThreshold = degradedThreshold;
    this.stuckThreshold = stuckThreshold;
  }

  /** One monitored component: its name, heartbeat file, and whether systemd reports it active. */
  public record Component(String name, Path heartbeat, boolean systemdActive) {}

  public String build(List<Component> components, SlotManager.Slot activeSlot, Instant now)
      throws IOException {
    ObjectNode root = mapper.createObjectNode();
    root.put("node", nodeId);
    root.put("scraped_at", now.toString());
    ObjectNode comps = root.putObject("components");
    for (Component c : components) {
      Optional<Duration> age = Heartbeat.age(c.heartbeat(), now);
      ComponentState state =
          HeartbeatState.classify(
              age.orElse(null), c.systemdActive(), degradedThreshold, stuckThreshold);
      ObjectNode cn = comps.putObject(c.name());
      cn.put("state", state.name().toLowerCase(java.util.Locale.ROOT));
      if (age.isPresent()) {
        cn.put("heartbeat_age_s", age.get().toMillis() / 1000.0);
      } else {
        cn.putNull("heartbeat_age_s");
      }
    }
    root.put("active_slot", activeSlot.token());
    try {
      return mapper.writeValueAsString(root);
    } catch (Exception e) {
      throw new IOException("status serialization failed", e);
    }
  }
}
