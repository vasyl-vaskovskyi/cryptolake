package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ReasonCausedByTest {

  @Test
  void collectorRestartExplainsWsDisconnect() {
    assertThat(ReasonCausedBy.explains("collector_restart", "ws_disconnect")).isTrue();
  }

  @Test
  void collectorRestartExplainsItself() {
    assertThat(ReasonCausedBy.explains("collector_restart", "collector_restart")).isTrue();
  }

  @Test
  void collectorRestartExplainsRecoveryDepthAnchor() {
    assertThat(ReasonCausedBy.explains("collector_restart", "recovery_depth_anchor")).isTrue();
  }

  @Test
  void missingHourDoesNotExplainWsDisconnect() {
    assertThat(ReasonCausedBy.explains("missing_hour", "ws_disconnect")).isFalse();
  }

  @Test
  void missingHourOnlyExplainsItself() {
    assertThat(ReasonCausedBy.explains("missing_hour", "missing_hour")).isTrue();
    assertThat(ReasonCausedBy.explains("missing_hour", "restart_gap")).isFalse();
  }

  @Test
  void unknownStateReasonExplainsNothing() {
    assertThat(ReasonCausedBy.explains("unknown_reason", "ws_disconnect")).isFalse();
  }

  @Test
  void restartGapExplainsPuChainBreak() {
    assertThat(ReasonCausedBy.explains("restart_gap", "pu_chain_break")).isTrue();
  }

  @Test
  void kafkaProducerOutageExplainsWriteError() {
    assertThat(ReasonCausedBy.explains("kafka_producer_outage", "write_error")).isTrue();
  }

  @Test
  void kafkaProducerOutageDoesNotExplainWsDisconnect() {
    assertThat(ReasonCausedBy.explains("kafka_producer_outage", "ws_disconnect")).isFalse();
  }
}
