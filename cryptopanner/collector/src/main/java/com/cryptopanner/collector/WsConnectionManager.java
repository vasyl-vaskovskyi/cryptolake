package com.cryptopanner.collector;

import com.cryptopanner.common.DurableSegment;
import com.cryptopanner.common.EquivalenceChecker;
import com.cryptopanner.common.FsHeavyLock;
import com.cryptopanner.common.OverlapMerger;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Drives the WS-rotation overlap lifecycle (design doc §5.2 / §5.5): open a shadow connection,
 * probe its liveness, wait for sealed overlap minutes, verify primary-vs-shadow equivalence, and
 * cut over (merge + promote + log) under the node {@code .fs-heavy.lock}. The pure decisions are
 * delegated to {@link RotationDecider}; the soak-only WS plumbing is delegated to the injected
 * {@link ShadowSession} seam, so this conductor is fully unit-testable.
 *
 * <p>One {@link #rotate} call is one full attempt. The Collector's per-minute scheduler re-invokes
 * it on the next eligible minute when an attempt defers or retries.
 */
public final class WsConnectionManager {

  /** Tunable knobs (defaults from §5.1/§5.2/§5.4). */
  public record Config(
      String lockHolder, Duration lockTimeout, int maxFails, Duration minOperatorAge) {
    public static Config defaults(String lockHolder) {
      return new Config(lockHolder, Duration.ofSeconds(60), 3, Duration.ofMinutes(5));
    }
  }

  /** Terminal status of a rotation attempt. */
  public enum Status {
    /** Cut over: merged + promoted + logged. */
    COMPLETED,
    /** Shadow was half-open; closed and will be retried at the next eligible minute. */
    RETRY_SHADOW,
    /** Could not acquire {@code .fs-heavy.lock} within the timeout; cutover deferred. */
    DEFERRED,
    /** Both connections dropped mid-overlap; rotation abandoned. */
    ABORTED,
    /** Operator trigger refused (connection too young). */
    REFUSED,
    /** Another rotation is already running. */
    IN_PROGRESS
  }

  public record RotateOutcome(
      Status status, String rotationId, String verifyResult, int minutesMerged) {
    static RotateOutcome of(Status s) {
      return new RotateOutcome(s, null, null, 0);
    }
  }

  private final Supplier<ShadowSession> shadowOpener;
  private final ObjectMapper mapper;
  private final EquivalenceChecker equivalence;
  private final OverlapMerger merger;
  private final Path fsHeavyLock;
  private final Path rotationsLog;
  private final Supplier<Optional<Duration>> connectionAge;
  private final Supplier<String> rotationIdGen;
  private final Supplier<Instant> clock;
  private final Config config;
  private final AtomicBoolean inProgress = new AtomicBoolean(false);

  public WsConnectionManager(
      Supplier<ShadowSession> shadowOpener,
      ObjectMapper mapper,
      Path fsHeavyLock,
      Path rotationsLog,
      Supplier<Optional<Duration>> connectionAge,
      Supplier<String> rotationIdGen,
      Supplier<Instant> clock,
      Config config) {
    this.shadowOpener = shadowOpener;
    this.mapper = mapper;
    this.equivalence = new EquivalenceChecker(mapper);
    this.merger = new OverlapMerger(mapper);
    this.fsHeavyLock = fsHeavyLock;
    this.rotationsLog = rotationsLog;
    this.connectionAge = connectionAge;
    this.rotationIdGen = rotationIdGen;
    this.clock = clock;
    this.config = config;
  }

  /** Runs one rotation attempt for {@code reason} (SCHEDULED / EMERGENCY / OPERATOR_TRIGGERED). */
  public RotateOutcome rotate(String reason) throws IOException {
    if (!inProgress.compareAndSet(false, true)) {
      return RotateOutcome.of(Status.IN_PROGRESS);
    }
    try {
      if ("OPERATOR_TRIGGERED".equals(reason) && tooYoungForOperatorTrigger()) {
        return RotateOutcome.of(Status.REFUSED);
      }
      ShadowSession session = shadowOpener.get();
      if (session == null || !session.probeLiveness()) {
        if (session != null) {
          session.close();
        }
        return RotateOutcome.of(Status.RETRY_SHADOW);
      }
      return runOverlap(reason, session);
    } finally {
      inProgress.set(false);
    }
  }

  private RotateOutcome runOverlap(String reason, ShadowSession session) throws IOException {
    int fails = 0;
    while (true) {
      Optional<OverlapMinute> next = session.awaitSealedMinute();
      if (next.isEmpty()) {
        session.close();
        return RotateOutcome.of(Status.ABORTED);
      }
      OverlapMinute minute = next.get();
      boolean pass = aggregateEquivalencePass(minute);
      RotationDecider.Action action =
          RotationDecider.decideCutover(
              pass, fails, config.maxFails(), session.primaryDropped(), session.bothDropped());
      switch (action) {
        case ABORT -> {
          session.close();
          return RotateOutcome.of(Status.ABORTED);
        }
        case WAIT -> fails++;
        case CUTOVER -> {
          return cutover(reason, "PASS", session, minute);
        }
        case FORCE_CUTOVER -> {
          return cutover(reason, "FORCED", session, minute);
        }
        case EARLY_CUTOVER -> {
          return cutover(reason, "EARLY", session, minute);
        }
        case RETRY_SHADOW -> {
          session.close();
          return RotateOutcome.of(Status.RETRY_SHADOW);
        }
      }
    }
  }

  /**
   * Merge + promote + log under the {@code .fs-heavy.lock}; defer if the lock can't be acquired.
   */
  private RotateOutcome cutover(
      String reason, String verifyResult, ShadowSession session, OverlapMinute minute)
      throws IOException {
    String rotationId = rotationIdGen.get();
    try (FsHeavyLock ignored =
        FsHeavyLock.acquire(fsHeavyLock, config.lockHolder(), config.lockTimeout())) {
      for (SegmentPair pair : minute.pairs()) {
        mergePair(pair);
      }
      session.promote();
      RotationsLog.append(
          rotationsLog,
          mapper,
          new RotationsLog.RotationEvent(
              rotationId,
              reason,
              ageHours(),
              clock.get(),
              List.of(minute.minuteOfHour()),
              verifyResult));
      return new RotateOutcome(Status.COMPLETED, rotationId, verifyResult, 1);
    } catch (TimeoutException lockBusy) {
      session.close();
      return RotateOutcome.of(Status.DEFERRED);
    }
  }

  private void mergePair(SegmentPair pair) throws IOException {
    List<String> primaryLines = DurableSegment.readLines(pair.primaryFile());
    List<String> shadowLines = DurableSegment.readLines(pair.shadowFile());
    OverlapMerger.Merged merged = merger.merge(pair.stream(), primaryLines, shadowLines);
    DurableSegment.writeLines(pair.primaryFile(), merged.lines());
    Files.deleteIfExists(pair.shadowFile());
    Files.deleteIfExists(
        pair.shadowFile().resolveSibling(pair.shadowFile().getFileName() + ".sha256"));
  }

  private boolean aggregateEquivalencePass(OverlapMinute minute) throws IOException {
    for (SegmentPair pair : minute.pairs()) {
      List<String> primaryLines = DurableSegment.readLines(pair.primaryFile());
      List<String> shadowLines = DurableSegment.readLines(pair.shadowFile());
      if (!equivalence.check(pair.stream(), primaryLines, shadowLines).pass()) {
        return false;
      }
    }
    return true;
  }

  private boolean tooYoungForOperatorTrigger() {
    Optional<Duration> age = connectionAge.get();
    return age.isPresent() && age.get().compareTo(config.minOperatorAge()) < 0;
  }

  private double ageHours() {
    return connectionAge.get().map(d -> d.toMinutes() / 60.0).orElse(0.0);
  }
}
