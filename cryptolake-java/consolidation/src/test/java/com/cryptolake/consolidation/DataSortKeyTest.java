package com.cryptolake.consolidation;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.consolidation.core.DataSortKey;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

/** new — DataSortKey correctness tests (Tier 5 E1) */
class DataSortKeyTest {

  private final ObjectMapper mapper = EnvelopeCodec.newMapper();

  private ObjectNode buildEnv(String stream, String rawText, long exchangeTs) {
    ObjectNode node = mapper.createObjectNode();
    node.put("stream", stream);
    node.put("exchange_ts", exchangeTs);
    node.put("raw_text", rawText);
    return node;
  }

  @Test
  void tradesUsesAggregateTradeId() {
    // new — Tier 5 E1
    ObjectNode env = buildEnv("trades", "{\"a\":999999999999}", 100L);
    assertThat(DataSortKey.of(env, "trades", mapper)).isEqualTo(999999999999L);
  }

  @Test
  void depthUsesUpdateId() {
    // new — Tier 5 E1
    ObjectNode env = buildEnv("depth", "{\"u\":888888}", 100L);
    assertThat(DataSortKey.of(env, "depth", mapper)).isEqualTo(888888L);
  }

  @Test
  void otherStreamsUseExchangeTs() {
    // new
    ObjectNode env = buildEnv("bookticker", "{\"u\":777}", 42L);
    assertThat(DataSortKey.of(env, "bookticker", mapper)).isEqualTo(42L);
  }

  @Test
  void noLossOnIdsAbove2Pow31() {
    // new — Tier 5 E1 watch-out: IDs > 2^31 must not overflow
    long bigId = 3_000_000_000L; // exceeds Integer.MAX_VALUE
    ObjectNode env = buildEnv("trades", "{\"a\":" + bigId + "}", 100L);
    assertThat(DataSortKey.of(env, "trades", mapper)).isEqualTo(bigId);
  }
}
