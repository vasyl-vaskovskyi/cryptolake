package com.cryptopanner.monitor;

import java.time.Instant;

/**
 * One alert condition the Monitor has detected for a node (master spec §13.a). The
 * dedup/correlation pipeline (§13.e) keys on {@code (node, component, type)}; {@code component} is
 * {@code null} for node-level conditions (unreachable, disk, deploy, rotation). Severity is
 * intrinsic to the {@link AlertType}.
 */
public record Alert(
    Severity severity,
    String node,
    String component,
    AlertType type,
    String message,
    Instant firedAt) {

  public enum Severity {
    WARNING,
    CRITICAL
  }

  /** The §13.a conditions. Severity is fixed per type so dedup keys stay stable across firings. */
  public enum AlertType {
    COMPONENT_DEGRADED(Severity.WARNING),
    COMPONENT_STUCK(Severity.WARNING),
    COMPONENT_DOWN(Severity.CRITICAL),
    NODE_UNREACHABLE(Severity.CRITICAL),
    DISK_PRESSURE_WARNING(Severity.WARNING),
    DISK_PRESSURE_CRITICAL(Severity.CRITICAL),
    DEPLOY_STUCK(Severity.WARNING),
    ACTIVE_SLOT_MISMATCH(Severity.CRITICAL),
    WS_CONNECTION_AGE_CLIFF(Severity.CRITICAL),
    UPLOAD_BACKLOG(Severity.WARNING),
    CIRCUIT_BREAKER_TRIPPED(Severity.CRITICAL);

    private final Severity severity;

    AlertType(Severity severity) {
      this.severity = severity;
    }

    public Severity severity() {
      return severity;
    }
  }

  public static Alert of(
      String node, String component, AlertType type, String message, Instant firedAt) {
    return new Alert(type.severity(), node, component, type, message, firedAt);
  }

  /** Stable dedup key (§13.e): {@code node|component|type}. */
  public String dedupKey() {
    return node + "|" + (component == null ? "-" : component) + "|" + type;
  }
}
