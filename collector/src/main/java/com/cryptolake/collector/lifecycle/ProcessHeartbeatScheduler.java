package com.cryptolake.collector.lifecycle;

import com.cryptolake.common.logging.StructuredLogger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Sends process-level heartbeats to the database every 30 seconds.
 *
 * <p>Distinct from {@code StreamHeartbeatEmitter} which sends per-(symbol, stream) liveness
 * heartbeats to Kafka. This class only updates the PG row that shows the process is alive (design
 * §2.10).
 *
 * <p>Uses {@link CountDownLatch#await(long, TimeUnit)} for interruptible waits (Tier 5 A3).
 */
public final class ProcessHeartbeatScheduler {

  private static final StructuredLogger log = StructuredLogger.of(ProcessHeartbeatScheduler.class);

  private static final long INTERVAL_SECONDS = 30L;

  private final LifecycleStateManager stateManager;
  private final ComponentRuntimeState state;
  private final CountDownLatch stopLatch = new CountDownLatch(1);
  private volatile Thread thread;

  public ProcessHeartbeatScheduler(
      LifecycleStateManager stateManager, ComponentRuntimeState state) {
    this.stateManager = stateManager;
    this.state = state;
  }

  /** Starts the heartbeat loop on a virtual thread. */
  public void start() {
    thread =
        Thread.ofVirtual()
            .name("process-heartbeat")
            .start(
                () -> {
                  log.info("process_heartbeat_started");
                  while (true) {
                    try {
                      // Tier 5 A3: await(timeout) = true means stop was signalled
                      if (stopLatch.await(INTERVAL_SECONDS, TimeUnit.SECONDS)) {
                        break; // stop requested
                      }
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      break;
                    }
                    stateManager.heartbeat(state);
                  }
                  log.info("process_heartbeat_stopped");
                });
  }

  /** Stops the heartbeat loop. */
  public void stop() {
    stopLatch.countDown();
  }
}
