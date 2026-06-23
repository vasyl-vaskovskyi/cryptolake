package com.cryptopanner.collector;

import java.util.Optional;

/**
 * One open WS-rotation overlap (design doc §5.2). The genuinely soak-only WS plumbing — open a
 * second "shadow" connection with a distinct User-Agent, subscribe to the same streams, write its
 * frames to {@code .shadow} minute segments, detect a sealed minute, and promote the shadow to
 * primary — lives behind this seam so the rotation <em>lifecycle</em> in {@link
 * WsConnectionManager} is fully unit-testable with a fake.
 *
 * <p>{@link LiveShadowSession} is the production implementation; {@code make dev-up} soak (§14.e)
 * exercises it end-to-end against a real second connection.
 */
public interface ShadowSession extends AutoCloseable {

  /**
   * Liveness probe (§5.2 step 2): {@code true} iff the shadow received at least one high-frequency
   * frame within the probe window. A half-open shadow returns {@code false}.
   */
  boolean probeLiveness();

  /**
   * Blocks until the shadow's next full minute has sealed on disk (§5.2 step 3) and returns that
   * overlap minute's file pairs. Returns empty once the session has terminated (no more minutes —
   * both connections dropped).
   */
  Optional<OverlapMinute> awaitSealedMinute();

  /** The old primary WS connection dropped during the overlap (§5.5 — early cutover). */
  boolean primaryDropped();

  /** Both connections dropped during the overlap (§5.5 — abort). */
  boolean bothDropped();

  /**
   * Cutover (§5.2 step 5): make the shadow the new primary and close the old primary connection
   * cleanly. Subsequent frames route to the primary-named segments.
   */
  void promote();

  /**
   * Close the shadow without promoting (failed/aborted rotation); the old primary keeps running.
   */
  @Override
  void close();
}
