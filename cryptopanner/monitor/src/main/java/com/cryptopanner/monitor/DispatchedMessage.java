package com.cryptopanner.monitor;

import com.cryptopanner.monitor.Alert.AlertType;
import com.cryptopanner.monitor.Alert.Severity;
import java.util.List;

/**
 * A message the {@link AlertDispatcher} decided to send to the alert channels (§13.e): a single
 * alert, a correlated group spanning several nodes, or a one-shot recovery notice. The channels
 * render {@code title} + {@code body}; {@code nodes} lists the affected nodes for a correlated
 * message.
 */
public record DispatchedMessage(
    Kind kind, Severity severity, AlertType type, String title, String body, List<String> nodes) {

  public enum Kind {
    ALERT,
    CORRELATED,
    RECOVERED,
    SELF_TEST
  }

  /** The §13.c daily "alert path healthy" self-test sent on every channel. */
  public static DispatchedMessage selfTest(String text) {
    return new DispatchedMessage(
        Kind.SELF_TEST, Severity.WARNING, null, "alert path healthy", text, List.of());
  }

  static DispatchedMessage single(Alert a) {
    String where = a.component() == null ? a.node() : a.node() + "/" + a.component();
    return new DispatchedMessage(
        Kind.ALERT,
        a.severity(),
        a.type(),
        a.severity() + " " + a.type(),
        where + ": " + a.message(),
        List.of(a.node()));
  }

  static DispatchedMessage correlated(AlertType type, List<Alert> alerts) {
    List<String> nodes = alerts.stream().map(Alert::node).distinct().toList();
    return new DispatchedMessage(
        Kind.CORRELATED,
        type.severity(),
        type,
        type.severity() + " " + type + " on " + nodes.size() + " nodes",
        type + " affecting: " + String.join(", ", nodes),
        nodes);
  }

  static DispatchedMessage recovered(Alert a) {
    String where = a.component() == null ? a.node() : a.node() + "/" + a.component();
    return new DispatchedMessage(
        Kind.RECOVERED,
        a.severity(),
        a.type(),
        "RECOVERED " + a.type(),
        where + ": condition cleared",
        List.of(a.node()));
  }
}
