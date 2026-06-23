package com.cryptopanner.monitor;

import com.cryptopanner.common.config.MonitorConfig;
import com.cryptopanner.monitor.Alert.AlertType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One scrape cycle of the Monitor, composing every unit (master spec §11–§13). For each configured
 * node it scrapes {@code /status}+{@code /metrics}, folds the result into the tracker, evaluates
 * the §13.a alert conditions, applies the §13.b/d restart policy, then runs the whole cycle's
 * alerts through the §13.e dispatcher to the channels, publishes a fresh {@link NodeView} list for
 * the dashboard, and ticks the §13.c dead-man's switch.
 *
 * <p>Time is the injected {@code now} so a cycle is deterministic; {@code Main} schedules {@code
 * runCycle} every {@code scrape_interval_s}.
 */
public final class MonitorCore {

  private final List<MonitorConfig.Node> nodes;
  private final NodeScraper scraper;
  private final NodeStateTracker tracker;
  private final AlertEvaluator evaluator;
  private final RestartOrchestrator orchestrator;
  private final AlertDispatcher dispatcher;
  private final List<AlertChannel> channels;
  private final DeadMansSwitch deadMan;
  private final AtomicReference<List<NodeView>> view;

  public MonitorCore(
      List<MonitorConfig.Node> nodes,
      NodeScraper scraper,
      NodeStateTracker tracker,
      AlertEvaluator evaluator,
      RestartOrchestrator orchestrator,
      AlertDispatcher dispatcher,
      List<AlertChannel> channels,
      DeadMansSwitch deadMan,
      AtomicReference<List<NodeView>> view) {
    this.nodes = nodes;
    this.scraper = scraper;
    this.tracker = tracker;
    this.evaluator = evaluator;
    this.orchestrator = orchestrator;
    this.dispatcher = dispatcher;
    this.channels = channels;
    this.deadMan = deadMan;
    this.view = view;
  }

  public void runCycle(Instant now) {
    List<NodeView> views = new ArrayList<>();
    List<Alert> cycleAlerts = new ArrayList<>();

    for (MonitorConfig.Node node : nodes) {
      ScrapeResult r = scraper.poll(node);
      tracker.update(node.id(), r, now);

      List<Alert> alerts = new ArrayList<>(evaluator.evaluate(node.id(), r, tracker, now));
      boolean tripped = false;
      if (r.ok()) {
        List<Alert> breaker = orchestrator.reconcile(node, r.status(), now);
        alerts.addAll(breaker);
        tripped = breaker.stream().anyMatch(a -> a.type() == AlertType.CIRCUIT_BREAKER_TRIPPED);
      }
      cycleAlerts.addAll(alerts);

      List<String> alertTypes = alerts.stream().map(a -> a.type().name()).distinct().toList();
      views.add(
          r.ok()
              ? NodeView.of(node.id(), r.status(), tripped, alertTypes)
              : NodeView.unreachable(node.id(), r.error()));
    }

    view.set(List.copyOf(views));

    for (DispatchedMessage m : dispatcher.dispatch(cycleAlerts, now)) {
      for (AlertChannel ch : channels) {
        if (ch.enabled()) {
          ch.send(m);
        }
      }
    }

    deadMan.tick(now);
  }
}
