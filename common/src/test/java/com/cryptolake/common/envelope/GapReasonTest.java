package com.cryptolake.common.envelope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class GapReasonTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void serializesToWireString() throws Exception {
    assertThat(mapper.writeValueAsString(GapReason.WS_DISCONNECT)).isEqualTo("\"ws_disconnect\"");
  }

  @Test
  void deserializesFromWireString() throws Exception {
    GapReason r = mapper.readValue("\"collector_restart\"", GapReason.class);
    assertThat(r).isEqualTo(GapReason.COLLECTOR_RESTART);
  }

  @Test
  void unknownWireStringThrows() {
    assertThatThrownBy(() -> GapReason.fromWire("not_a_real_reason"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not_a_real_reason");
  }

  @Test
  void unknownWireStringThrowsViaJackson() {
    assertThatThrownBy(() -> mapper.readValue("\"bogus\"", GapReason.class))
        .hasRootCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void classificationPartitionsAllValues() {
    for (GapReason r : GapReason.values()) {
      assertThat(r.isPersistent() ^ r.isRuntimeOnly())
          .as("reason %s must be in exactly one bucket", r)
          .isTrue();
    }
  }

  @Test
  void persistentSetMatchesSpec() {
    assertThat(java.util.Arrays.stream(GapReason.values()).filter(GapReason::isPersistent))
        .containsExactlyInAnyOrder(
            GapReason.COLLECTOR_RESTART,
            GapReason.RESTART_GAP,
            GapReason.KAFKA_PRODUCER_OUTAGE,
            GapReason.MISSING_HOUR);
  }

  @Test
  void collectorRestartExplainsTransientReasons() {
    assertThat(GapReason.COLLECTOR_RESTART.explains(GapReason.WS_DISCONNECT)).isTrue();
    assertThat(GapReason.COLLECTOR_RESTART.explains(GapReason.PU_CHAIN_BREAK)).isTrue();
    assertThat(GapReason.COLLECTOR_RESTART.explains(GapReason.SESSION_SEQ_SKIP)).isTrue();
    assertThat(GapReason.COLLECTOR_RESTART.explains(GapReason.RECOVERY_DEPTH_ANCHOR)).isTrue();
    assertThat(GapReason.COLLECTOR_RESTART.explains(GapReason.RESTART_GAP)).isTrue();
  }

  @Test
  void kafkaProducerOutageExplainsRelatedTransients() {
    assertThat(GapReason.KAFKA_PRODUCER_OUTAGE.explains(GapReason.KAFKA_DELIVERY_FAILED)).isTrue();
    assertThat(GapReason.KAFKA_PRODUCER_OUTAGE.explains(GapReason.KAFKA_OFFSET_RESET)).isTrue();
    assertThat(GapReason.KAFKA_PRODUCER_OUTAGE.explains(GapReason.WRITE_ERROR)).isTrue();
  }

  @Test
  void missingHourExplainsOnlyItself() {
    assertThat(GapReason.MISSING_HOUR.explains(GapReason.MISSING_HOUR)).isTrue();
    assertThat(GapReason.MISSING_HOUR.explains(GapReason.WS_DISCONNECT)).isFalse();
  }

  @Test
  void runtimeOnlyExplainsNothing() {
    assertThat(GapReason.WS_DISCONNECT.explains(GapReason.WS_DISCONNECT)).isFalse();
    assertThat(GapReason.PU_CHAIN_BREAK.explains(GapReason.SESSION_SEQ_SKIP)).isFalse();
  }

  @Test
  void collectorRestartExplainsItself() {
    assertThat(GapReason.COLLECTOR_RESTART.explains(GapReason.COLLECTOR_RESTART)).isTrue();
  }

  @Test
  void restartGapExplainsTransients() {
    assertThat(GapReason.RESTART_GAP.explains(GapReason.RESTART_GAP)).isTrue();
    assertThat(GapReason.RESTART_GAP.explains(GapReason.WS_DISCONNECT)).isTrue();
    assertThat(GapReason.RESTART_GAP.explains(GapReason.PU_CHAIN_BREAK)).isTrue();
    assertThat(GapReason.RESTART_GAP.explains(GapReason.SESSION_SEQ_SKIP)).isTrue();
    assertThat(GapReason.RESTART_GAP.explains(GapReason.RECOVERY_DEPTH_ANCHOR)).isTrue();
  }

  @Test
  void kafkaProducerOutageDoesNotExplainUnrelatedTransients() {
    assertThat(GapReason.KAFKA_PRODUCER_OUTAGE.explains(GapReason.WS_DISCONNECT)).isFalse();
    assertThat(GapReason.KAFKA_PRODUCER_OUTAGE.explains(GapReason.PU_CHAIN_BREAK)).isFalse();
  }

  @Test
  void allKnownWireStringsRoundTrip() throws Exception {
    for (GapReason r : GapReason.values()) {
      String json = mapper.writeValueAsString(r);
      assertThat(mapper.readValue(json, GapReason.class)).isEqualTo(r);
    }
  }

  @Test
  void explainsThrowsOnNull() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> GapReason.COLLECTOR_RESTART.explains(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("fileReason");
  }
}
