package com.cryptolake.writer.failover;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.metrics.WriterMetrics;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the failover lifecycle: primary silence detection → backup activation → switchback.
 *
 * <p>Ports Python's {@code FailoverManager} (design §2.7; design §4.6). After plan
 * 2026-05-03-continuous-dual-source-tailing (Task 4), this class is STATE-ONLY: it tracks the
 * activation/deactivation hysteresis state machine and the switchback boundary filter. The backup
 * topic is now tailed continuously by {@link com.cryptolake.writer.consumer.BackupTailConsumer} —
 * the on-demand backup consumer (along with the seek-to-replay rebalance listener) was removed in
 * the same task.
 *
 * <p>Thread safety: consume-loop thread only (T1). {@code _isActive} is {@code volatile} so the
 * Ready thread reads a coherent value for {@code isConnected()} checks (design §3.3).
 */
public final class FailoverController {

  private static final Logger log = LoggerFactory.getLogger(FailoverController.class);

  private final String backupPrefix;
  private final Duration silenceTimeout;
  private final Duration recoveryStabilityWindow;
  private final WriterMetrics metrics;
  private final ClockSupplier clock;

  /** Volatile so the Ready thread reads a coherent value. */
  private volatile boolean isActive = false;

  private long lastPrimaryRecordNs = -1L;
  private long activationStartNs = -1L;

  /** Nanos timestamp of the first primary record observed since the most recent activate(). */
  private long firstRecoveryRecordNs = -1L;

  /** Per-stream last natural key seen from primary (for switchback filter). */
  private final Map<String, Long> lastPrimaryKey = new HashMap<>();

  /** Per-stream last natural key seen from backup (for switchback filter). */
  private final Map<String, Long> lastBackupKey = new HashMap<>();

  private boolean switchbackInProgress = false;

  public FailoverController(
      String backupPrefix,
      Duration silenceTimeout,
      Duration recoveryStabilityWindow,
      WriterMetrics metrics,
      ClockSupplier clock) {
    this.backupPrefix = backupPrefix;
    this.silenceTimeout = silenceTimeout;
    this.recoveryStabilityWindow = recoveryStabilityWindow;
    this.metrics = metrics;
    this.clock = clock;
    this.lastPrimaryRecordNs = clock.nowNs(); // Initialize to now (no silence at startup)
  }

  // ── Silence timer ─────────────────────────────────────────────────────────────────────────────

  /** Resets the silence timer when a primary record is received. */
  public void resetSilenceTimer() {
    this.lastPrimaryRecordNs = clock.nowNs();
  }

  /** Returns {@code true} if the silence timeout has elapsed since the last primary record. */
  public boolean shouldActivate() {
    if (isActive || lastPrimaryRecordNs < 0) return false;
    long silentNs = clock.nowNs() - lastPrimaryRecordNs;
    return silentNs >= silenceTimeout.toNanos();
  }

  // ── Activation / deactivation ─────────────────────────────────────────────────────────────────

  /**
   * Marks failover ACTIVE. State-only: the backup topic is tailed continuously by {@link
   * com.cryptolake.writer.consumer.BackupTailConsumer} regardless of this flag; this method only
   * controls whether the writer ARCHIVES backup records (via {@code RecordHandler}'s failover
   * check) and whether the LIFECYCLE log lines fire.
   */
  public void activate() {
    if (isActive) return;
    log.info("failover_activated", "backup_prefix", backupPrefix);
    log.info(
        "LIFECYCLE MAIN_FAILURE_DETECTED: Main collector stopped delivering data."
            + " Silence timeout of {}s reached; failing over to backup.",
        silenceTimeout.toSeconds());
    log.info(
        "LIFECYCLE WRITER_NOW_ARCHIVING_FROM=BACKUP: Writer is now archiving data from"
            + " the BACKUP collector (failover active). backup_prefix={}",
        backupPrefix);
    isActive = true;
    firstRecoveryRecordNs = -1L;
    activationStartNs = clock.nowNs();
    metrics.setFailoverActive(true);
    metrics.failoverTotal().increment();
  }

  /** Marks failover INACTIVE and records failover duration. */
  public void deactivate() {
    if (!isActive) return;
    long durationNs = clock.nowNs() - activationStartNs;
    double durationSec = durationNs / 1_000_000_000.0;
    log.info("failover_deactivated", "duration_seconds", durationSec);
    log.info(
        "LIFECYCLE MAIN_RECOVERED: Main collector is delivering data again after {}s;"
            + " writer is switching back from backup to main.",
        durationSec);
    log.info(
        "LIFECYCLE WRITER_NOW_ARCHIVING_FROM=MAIN: Writer is back to archiving data from"
            + " the MAIN collector (failover deactivated).");
    isActive = false;
    firstRecoveryRecordNs = -1L;
    metrics.setFailoverActive(false);
    metrics.failoverDurationSeconds().record(durationSec);
    metrics.switchbackTotal().increment();
    switchbackInProgress = false;
  }

  /** Returns {@code true} if backup consumer is active. */
  public boolean isActive() {
    return isActive;
  }

  // ── Record tracking ───────────────────────────────────────────────────────────────────────────

  /** Tracks a data envelope from primary for switchback filter. */
  public void trackRecord(DataEnvelope env) {
    // No natural key tracking here; that's done by NaturalKeyExtractor in RecordHandler
  }

  /**
   * Returns {@code true} if this backup envelope should be filtered out (primary already provided
   * this data — switchback filter).
   */
  public boolean shouldFilter(DataEnvelope env) {
    if (!switchbackInProgress) return false;
    // If primary has caught up past this backup record's natural key, filter it
    Long primaryKey = lastPrimaryKey.get(streamId(env));
    if (primaryKey == null) return false;
    // Allow backup records only if their natural key hasn't been seen from primary
    return primaryKey > 0;
  }

  /** Initiates the switchback (backup → primary) transition. */
  public void beginSwitchback() {
    switchbackInProgress = true;
    log.info("switchback_initiated");
  }

  /** Returns {@code true} if the switchback filter should drop this backup envelope. */
  public boolean checkSwitchbackFilter(DataEnvelope env) {
    return shouldFilter(env);
  }

  // ── Hysteresis state machine (bug B) ─────────────────────────────────────────────────────────

  /**
   * Records that a primary record was delivered (called by KafkaConsumerLoop after each primary
   * record processed). If failover is active and this is the first primary record since activation,
   * starts the recovery observation window. If primary has been silent longer than {@code
   * silenceTimeout} (symmetric with {@link #shouldActivate()}), resets the recovery window — the
   * controller wants continuous (not flapping) primary delivery before declaring recovery.
   *
   * <p>Always updates {@code lastPrimaryRecordNs} (used by {@code shouldActivate}). Uses the
   * controller's injected clock — symmetric with {@link #resetSilenceTimer()}; ensures all
   * FailoverController state lives in a single clock domain.
   */
  public void markPrimaryDelivered() {
    long nowNs = clock.nowNs();
    if (isActive) {
      if (firstRecoveryRecordNs < 0 || (nowNs - lastPrimaryRecordNs) > silenceTimeout.toNanos()) {
        // First record post-activate, or primary went silent again — restart the window.
        firstRecoveryRecordNs = nowNs;
      }
    }
    lastPrimaryRecordNs = nowNs;
  }

  /**
   * Returns {@code true} if failover is active AND primary has been delivering continuously for at
   * least {@code recoveryStabilityWindow}. The caller is responsible for invoking {@link
   * #deactivate()} when this returns true.
   */
  public boolean shouldDeactivate() {
    if (!isActive || firstRecoveryRecordNs < 0) {
      return false;
    }
    long now = clock.nowNs();
    long recoveredFor = now - firstRecoveryRecordNs;
    long sinceLast = now - lastPrimaryRecordNs;
    return recoveredFor >= recoveryStabilityWindow.toNanos()
        && sinceLast < silenceTimeout.toNanos();
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────────────────────

  private static String streamId(DataEnvelope env) {
    return env.exchange() + "|" + env.symbol() + "|" + env.stream();
  }
}
