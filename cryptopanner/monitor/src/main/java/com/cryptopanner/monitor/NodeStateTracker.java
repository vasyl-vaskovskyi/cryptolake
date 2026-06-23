package com.cryptopanner.monitor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-node temporal memory the {@link AlertEvaluator} reads to evaluate the duration-gated §13.a
 * conditions: consecutive scrape failures (node-unreachable), how long a component has been {@code
 * degraded}, how long a deploy has sat in one non-IDLE state, and how long an upload backlog has
 * been non-empty. {@link #update} is called once per scrape before {@link AlertEvaluator#evaluate};
 * all timestamps are the injected scrape {@code now}, so the whole pipeline is deterministic in
 * tests.
 */
public final class NodeStateTracker {

  private static final class NodeState {
    int consecutiveFailures;
    final Map<String, Instant> degradedSince = new HashMap<>();
    String lastDeployState;
    Instant deploySince;
    Instant backlogSince;
  }

  private final Map<String, NodeState> byNode = new ConcurrentHashMap<>();

  private NodeState state(String node) {
    return byNode.computeIfAbsent(node, k -> new NodeState());
  }

  /** Fold one scrape result into the node's temporal memory. */
  public void update(String node, ScrapeResult r, Instant now) {
    NodeState st = state(node);
    if (!r.ok()) {
      st.consecutiveFailures++;
      return;
    }
    st.consecutiveFailures = 0;
    StatusSnapshot s = r.status();

    for (var e : s.components().entrySet()) {
      if ("degraded".equals(e.getValue().state())) {
        st.degradedSince.putIfAbsent(e.getKey(), now);
      } else {
        st.degradedSince.remove(e.getKey());
      }
    }

    String deploy = s.deployState();
    if (deploy == null || "IDLE".equals(deploy)) {
      st.deploySince = null;
      st.lastDeployState = deploy;
    } else if (!deploy.equals(st.lastDeployState)) {
      st.deploySince = now;
      st.lastDeployState = deploy;
    }

    var pending =
        r.metrics() == null
            ? java.util.OptionalDouble.empty()
            : r.metrics().sealedFilesPendingUpload();
    if (pending.isPresent() && pending.getAsDouble() > 0) {
      if (st.backlogSince == null) {
        st.backlogSince = now;
      }
    } else {
      st.backlogSince = null;
    }
  }

  public int consecutiveFailures(String node) {
    return state(node).consecutiveFailures;
  }

  public Instant degradedSince(String node, String component) {
    return state(node).degradedSince.get(component);
  }

  public Instant deploySince(String node) {
    return state(node).deploySince;
  }

  public Instant backlogSince(String node) {
    return state(node).backlogSince;
  }
}
