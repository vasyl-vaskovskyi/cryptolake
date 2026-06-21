package com.cryptopanner.agent;

/**
 * Liveness state the Node Agent derives for each component from its heartbeat mtime plus the
 * systemd active/inactive signal (master spec §11.b).
 */
public enum ComponentState {
  /** Heartbeat fresh (mtime ≤ degraded threshold). */
  RUNNING,
  /** Main loop slow (degraded < mtime ≤ stuck threshold) — early warning, not yet restarted. */
  DEGRADED,
  /** Alive per systemd but not progressing (mtime > stuck threshold). */
  STUCK,
  /** systemd reports the unit inactive or failed. */
  DOWN
}
