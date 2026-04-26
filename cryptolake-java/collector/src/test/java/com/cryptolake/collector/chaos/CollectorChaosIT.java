package com.cryptolake.collector.chaos;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Chaos/integration tests for the collector service.
 *
 * <p>These tests require a running docker-compose stack and are intentionally disabled by default.
 * The {@code @Tag("chaos")} annotation allows selective execution.
 *
 * <p>Design §8.1 test plan mapping:
 */
@Tag("chaos")
@Disabled("requires docker-compose stack — skeleton only")
class CollectorChaosIT {

  @Test
  // ports: (chaos 1) ws disconnect slow reconnect emits single gap
  void wsDisconnectSlowReconnectEmitsSingleGap() {
    // TODO: start collector, kill WebSocket, verify exactly one ws_disconnect gap emitted
  }

  @Test
  // ports: (chaos 2) pu chain break triggers resync
  void puChainBreakTriggersResync() {
    // TODO: inject a depth diff with incorrect pu, verify resync + pu_chain_break gap
  }

  @Test
  // ports: (chaos 3) snapshot poll miss gap emitted
  void snapshotPollMissGapEmitted() {
    // TODO: block REST endpoint, verify snapshot_poll_miss gap after 3 retries
  }

  @Test
  // ports: (chaos 4) buffer overflow per stream isolation
  void bufferOverflowPerStreamIsolation() {
    // TODO: fill depth stream buffer, verify liquidations still produce
  }

  @Test
  // ports: (chaos 5) producer unhealthy aborts resync
  void producerUnhealthyAbortsResync() {
    // TODO: make producer unhealthy, trigger resync, verify it aborts after 60s
  }

  @Test
  // ports: (chaos 6) backup chain reader skips snapshot when recent
  void backupChainReaderSkipsSnapshotWhenRecent() {
    // TODO: pre-seed backup topic, trigger resync, verify no REST snapshot call
  }
}
