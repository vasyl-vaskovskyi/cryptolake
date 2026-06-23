package com.cryptopanner.monitor;

import com.cryptopanner.common.config.MonitorConfig;

/**
 * Issues a restart of one component on a node. The real implementation ({@link AgentRestartClient})
 * makes the authenticated {@code POST /restart/<component>} call; tests inject a fake to assert the
 * {@link RestartOrchestrator}'s gate/backoff/breaker decisions without real HTTP.
 */
public interface RestartClient {

  /** Returns true if the agent accepted the restart (2xx), false on any failure. */
  boolean restart(MonitorConfig.Node node, String component);
}
