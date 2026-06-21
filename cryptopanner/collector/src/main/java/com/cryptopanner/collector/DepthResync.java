package com.cryptopanner.collector;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * On-demand depth snapshot resync (master spec §7.b.1 / §8). Watches each symbol's {@code depth}
 * diff stream for {@code pu}-chain breaks via {@link DepthChainTracker}; on a break it triggers a
 * fresh {@code /fapi/v1/depth} snapshot so a consumer can rebuild the order book (latest snapshot +
 * subsequent diffs).
 *
 * <p>Detection runs inline on the WS read thread (cheap); the trigger — which performs the network
 * fetch and writes the snapshot — runs on {@code executor} so frame processing never blocks. The
 * trigger is injected (in production a depth-snapshot {@code RestPoller.pollOnce}) so the snapshot
 * is written with the same {@code rest_response} envelope as the periodic baseline poll.
 */
public final class DepthResync {

  private final Consumer<String> resyncTrigger;
  private final Executor executor;
  private final Map<String, DepthChainTracker> trackers = new ConcurrentHashMap<>();
  private final AtomicLong resyncs = new AtomicLong();

  public DepthResync(Consumer<String> resyncTrigger, Executor executor) {
    this.resyncTrigger = resyncTrigger;
    this.executor = executor;
  }

  /** Feed one depth diff frame's {@code data} node; triggers a resync on a chain break. */
  public void onDepthFrame(String symbol, JsonNode data) {
    JsonNode u = data.get("u");
    JsonNode pu = data.get("pu");
    if (u == null || pu == null) {
      return; // not a depth diff (no continuity fields)
    }
    boolean broke =
        trackers
            .computeIfAbsent(symbol, k -> new DepthChainTracker())
            .isBreak(u.asLong(), pu.asLong());
    if (!broke) {
      return;
    }
    executor.execute(
        () -> {
          try {
            resyncTrigger.accept(symbol);
            resyncs.incrementAndGet();
          } catch (Exception e) {
            System.err.println(
                "[collector] depth resync failed for " + symbol + ": " + e.getMessage());
          }
        });
  }

  /** Count of resyncs triggered by chain breaks. */
  public long resyncs() {
    return resyncs.get();
  }
}
