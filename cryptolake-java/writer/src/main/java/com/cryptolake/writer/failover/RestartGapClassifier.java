package com.cryptolake.writer.failover;

import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.state.MaintenanceIntent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Pure-function restart gap classifier.
 *
 * <p>Ports Python's {@code restart_gap_classifier.py:classify_restart_gap} (design §2.7; design
 * §4.8). Returns a {@link Classification} record describing the restart cause, component, and
 * supporting evidence.
 *
 * <p>Classifier version: {@code "writer_recovery_v1"}. Promotable components: {@code
 * ["redpanda","postgres","writer"]}. Ordering identical to Python.
 *
 * <p>Thread safety: stateless pure function; all methods are static.
 */
public final class RestartGapClassifier {

  private static final String CLASSIFIER_VERSION = "writer_recovery_v1";
  private static final List<String> PROMOTABLE_COMPONENTS =
      List.of("redpanda", "postgres", "writer");

  private RestartGapClassifier() {}

  /**
   * Classification result — equivalent to Python's returned dict of restart-metadata fields.
   *
   * <p>Serialized into {@code GapEnvelope} optional restart-metadata fields (design §6.9).
   */
  public record Classification(
      String component, // host | system | collector | writer | redpanda | postgres
      String cause, // operator_shutdown | host_reboot | unclean_exit | unknown
      boolean planned,
      String classifier, // "writer_recovery_v1"
      List<String> evidence, // ordered list of string evidence tokens
      String maintenanceId) {} // nullable

  /**
   * Classifies a writer restart gap.
   *
   * <p>Ports {@code classify_restart_gap()} — exact Python logic preserved.
   *
   * @param previousBootId previous host boot ID (nullable)
   * @param currentBootId current host boot ID
   * @param previousSessionId previous collector session ID (nullable)
   * @param currentSessionId current collector session ID (nullable)
   * @param collectorCleanShutdown whether collector had a clean shutdown previously
   * @param systemCleanShutdown whether writer had a clean shutdown previously
   * @param maintenanceIntent active maintenance intent (nullable)
   * @param hostEvidence host lifecycle evidence (nullable)
   * @param clock for intent-expiry wall-clock check
   */
  public static Classification classify(
      String previousBootId,
      String currentBootId,
      String previousSessionId,
      String currentSessionId,
      boolean collectorCleanShutdown,
      boolean systemCleanShutdown,
      MaintenanceIntent maintenanceIntent,
      HostLifecycleEvidence hostEvidence,
      ClockSupplier clock) {

    List<String> evidence = new ArrayList<>();
    String maintenanceId = null;

    // Phase 1: Determine the primary cause
    boolean bootIdChanged = previousBootId != null && !previousBootId.equals(currentBootId);
    boolean sessionChanged =
        previousSessionId != null
            && currentSessionId != null
            && !previousSessionId.equals(currentSessionId);

    if (bootIdChanged) {
      evidence.add("host_boot_id_changed");
    }
    if (!bootIdChanged && previousBootId != null && previousBootId.equals(currentBootId)) {
      evidence.add("host_boot_id_unchanged");
    }
    if (sessionChanged) {
      evidence.add("collector_session_changed");
    }
    if (collectorCleanShutdown) {
      evidence.add("collector_clean_exit");
    }
    if (systemCleanShutdown) {
      evidence.add("writer_clean_exit");
    }

    // Check for valid maintenance intent
    boolean intentValid = isIntentValid(maintenanceIntent, clock);
    if (intentValid) {
      evidence.add("maintenance_intent_valid");
      maintenanceId = maintenanceIntent.maintenanceId();
    } else if (maintenanceIntent != null) {
      evidence.add("maintenance_intent_expired");
    }

    // Classify component and cause
    String component;
    String cause;
    boolean planned;

    if (bootIdChanged) {
      // Host reboot
      component = "host";
      if (intentValid) {
        cause = "operator_shutdown";
        planned = true;
      } else {
        cause = "host_reboot";
        planned = false;
      }
    } else if (hostEvidence != null && hostEvidence.hasMaintenanceIntent() && intentValid) {
      // Planned operator action without host reboot
      component = "system";
      cause = "operator_shutdown";
      planned = true;
    } else if (collectorCleanShutdown && intentValid) {
      component = "collector";
      cause = "operator_shutdown";
      planned = true;
    } else if (collectorCleanShutdown) {
      component = "collector";
      cause = "operator_shutdown";
      planned = false; // clean exit but no maintenance intent
    } else if (!collectorCleanShutdown && sessionChanged) {
      component = "collector";
      cause = "unclean_exit";
      planned = false;
    } else if (systemCleanShutdown && intentValid) {
      component = "writer";
      cause = "operator_shutdown";
      planned = true;
    } else if (systemCleanShutdown) {
      component = "writer";
      cause = "operator_shutdown";
      planned = false;
    } else {
      component = "unknown";
      cause = "unknown";
      planned = false;
      evidence.add("no_clear_signal");
    }

    // Phase 2: Promote to infrastructure component if die events found (bootId unchanged only)
    if (!bootIdChanged && hostEvidence != null) {
      for (String promotable : PROMOTABLE_COMPONENTS) {
        if (hostEvidence.hasComponentDie(promotable)) {
          boolean cleanExit = hostEvidence.componentCleanExit(promotable).orElse(false);
          if (!cleanExit) {
            component = promotable;
            cause = "unclean_exit";
            planned = false;
            evidence.add("component_die:" + promotable);
            break;
          }
        }
      }
    }

    return new Classification(
        component, cause, planned, CLASSIFIER_VERSION, List.copyOf(evidence), maintenanceId);
  }

  /**
   * Checks whether a maintenance intent is valid (not expired) at the current time.
   *
   * <p>Ports Python's {@code _is_intent_valid(intent)} — tolerates both {@code Z} and {@code
   * +00:00} (Tier 5 F2).
   */
  private static boolean isIntentValid(MaintenanceIntent intent, ClockSupplier clock) {
    if (intent == null) return false;
    if (intent.expiresAt() == null) return true; // no expiry = always valid
    Instant expiresAt = HostLifecycleReader.parseIsoOrNull(intent.expiresAt());
    if (expiresAt == null) return false;
    Instant now =
        Instant.ofEpochSecond(clock.nowNs() / 1_000_000_000L, clock.nowNs() % 1_000_000_000L);
    return now.isBefore(expiresAt);
  }
}
