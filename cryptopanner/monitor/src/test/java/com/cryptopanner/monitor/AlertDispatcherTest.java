package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.monitor.Alert.AlertType;
import com.cryptopanner.monitor.DispatchedMessage.Kind;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** §13.e — dedup, correlation, and recovery over the evaluator's alerts. */
class AlertDispatcherTest {

  private static final Instant T0 = Instant.parse("2026-06-23T00:00:00Z");
  private AlertDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    dispatcher = new AlertDispatcher(Duration.ofHours(1), 3, Duration.ofMinutes(1));
  }

  private Alert alert(String node, AlertType type) {
    return Alert.of(node, null, type, type + " on " + node, T0);
  }

  @Test
  void firstAlertIsDispatched() {
    List<DispatchedMessage> out =
        dispatcher.dispatch(List.of(alert("n1", AlertType.COMPONENT_DOWN)), T0);
    assertEquals(1, out.size());
    assertEquals(Kind.ALERT, out.get(0).kind());
  }

  @Test
  void duplicateWithinTtlIsSuppressed() {
    dispatcher.dispatch(List.of(alert("n1", AlertType.COMPONENT_DOWN)), T0);
    List<DispatchedMessage> out =
        dispatcher.dispatch(
            List.of(alert("n1", AlertType.COMPONENT_DOWN)), T0.plus(Duration.ofMinutes(30)));
    assertTrue(out.isEmpty());
  }

  @Test
  void duplicateAfterTtlIsReDispatched() {
    dispatcher.dispatch(List.of(alert("n1", AlertType.COMPONENT_DOWN)), T0);
    List<DispatchedMessage> out =
        dispatcher.dispatch(
            List.of(alert("n1", AlertType.COMPONENT_DOWN)), T0.plus(Duration.ofMinutes(61)));
    assertEquals(1, out.size());
    assertEquals(Kind.ALERT, out.get(0).kind());
  }

  @Test
  void recoveryEmittedWhenConditionClears() {
    dispatcher.dispatch(List.of(alert("n1", AlertType.COMPONENT_DOWN)), T0);
    List<DispatchedMessage> out = dispatcher.dispatch(List.of(), T0.plusSeconds(5));
    assertEquals(1, out.size());
    assertEquals(Kind.RECOVERED, out.get(0).kind());
  }

  @Test
  void recoveryEmittedOnlyOnce() {
    dispatcher.dispatch(List.of(alert("n1", AlertType.COMPONENT_DOWN)), T0);
    dispatcher.dispatch(List.of(), T0.plusSeconds(5));
    assertTrue(dispatcher.dispatch(List.of(), T0.plusSeconds(10)).isEmpty());
  }

  @Test
  void recoveryClearsDedupSoReoccurrenceFiresImmediately() {
    dispatcher.dispatch(List.of(alert("n1", AlertType.COMPONENT_DOWN)), T0);
    dispatcher.dispatch(List.of(), T0.plusSeconds(5)); // recovered
    List<DispatchedMessage> out =
        dispatcher.dispatch(List.of(alert("n1", AlertType.COMPONENT_DOWN)), T0.plusSeconds(10));
    assertEquals(1, out.size());
    assertEquals(Kind.ALERT, out.get(0).kind());
  }

  @Test
  void threeNodesSameTypeAreCorrelatedIntoOneMessage() {
    List<DispatchedMessage> out =
        dispatcher.dispatch(
            List.of(
                alert("n1", AlertType.NODE_UNREACHABLE),
                alert("n2", AlertType.NODE_UNREACHABLE),
                alert("n3", AlertType.NODE_UNREACHABLE)),
            T0);
    assertEquals(1, out.size());
    assertEquals(Kind.CORRELATED, out.get(0).kind());
    assertEquals(3, out.get(0).nodes().size());
  }

  @Test
  void belowCorrelationThresholdStaysIndividual() {
    List<DispatchedMessage> out =
        dispatcher.dispatch(
            List.of(
                alert("n1", AlertType.NODE_UNREACHABLE), alert("n2", AlertType.NODE_UNREACHABLE)),
            T0);
    assertEquals(2, out.size());
    assertTrue(out.stream().allMatch(m -> m.kind() == Kind.ALERT));
  }
}
