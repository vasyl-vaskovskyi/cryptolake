package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.cryptopanner.common.EnvelopeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DepthResyncTest {

  private static final ObjectMapper MAPPER = EnvelopeCodec.newMapper();

  private static JsonNode depth(String json) throws IOException {
    return MAPPER.readTree(json);
  }

  @Test
  void triggersResyncOnlyOnChainBreaks() throws IOException {
    AtomicInteger triggers = new AtomicInteger();
    DepthResync resync = new DepthResync(sym -> triggers.incrementAndGet(), Runnable::run);

    resync.onDepthFrame("btcusdt", depth("{\"u\":5,\"pu\":0}")); // first → break
    resync.onDepthFrame("btcusdt", depth("{\"u\":8,\"pu\":5}")); // contiguous → no break
    resync.onDepthFrame("btcusdt", depth("{\"u\":15,\"pu\":10}")); // gap → break

    assertEquals(2, triggers.get(), "two breaks → two resync triggers");
    assertEquals(2, resync.resyncs());
  }

  @Test
  void ignoresFramesWithoutDepthFields() throws IOException {
    AtomicInteger triggers = new AtomicInteger();
    DepthResync resync = new DepthResync(sym -> triggers.incrementAndGet(), Runnable::run);

    resync.onDepthFrame("btcusdt", depth("{\"E\":1}")); // no u/pu → not a depth diff
    assertEquals(0, resync.resyncs());
    assertEquals(0, triggers.get());
  }

  @Test
  void tracksChainPerSymbolIndependently() throws IOException {
    AtomicInteger triggers = new AtomicInteger();
    DepthResync resync = new DepthResync(sym -> triggers.incrementAndGet(), Runnable::run);

    resync.onDepthFrame("btcusdt", depth("{\"u\":5,\"pu\":0}")); // break (first btc)
    resync.onDepthFrame("ethusdt", depth("{\"u\":9,\"pu\":0}")); // break (first eth)
    resync.onDepthFrame("btcusdt", depth("{\"u\":8,\"pu\":5}")); // btc contiguous → no break
    resync.onDepthFrame("ethusdt", depth("{\"u\":12,\"pu\":9}")); // eth contiguous → no break

    assertEquals(2, resync.resyncs(), "only the two first-frame breaks");
  }
}
