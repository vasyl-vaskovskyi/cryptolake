package com.cryptopanner.collector;

import com.cryptopanner.common.CaptureEnvelope;
import com.cryptopanner.common.EventTime;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Routes each raw WebSocket frame to its {@code (symbol, stream)} writer, wraps it in a {@code
 * ws_frame} capture envelope (§8.c), and places it by server event time. A frame whose event
 * timestamp is missing/non-numeric falls back to receive-time bucketing and is counted in {@link
 * #unparseableFrames()} — kept, never discarded.
 *
 * <p>Writers are keyed {@code "<symbol>@<stream>"}; the {@code !forceOrder@arr} broadcast is routed
 * to the liquidated symbol via {@code data.o.s}.
 */
public final class FrameRouter {

  /** On-disk stream name for the {@code !forceOrder@arr} broadcast (master spec §7.a item 8). */
  public static final String FORCE_ORDER_STREAM = "forceOrder";

  public static final String FORCE_ORDER_BROADCAST = "!forceOrder@arr";

  private final ObjectMapper mapper;
  private final Map<String, MinuteSegmentWriter> writers;
  private final AtomicLong framesWritten = new AtomicLong();
  private final AtomicLong unparseableFrames = new AtomicLong();

  public FrameRouter(ObjectMapper mapper, Map<String, MinuteSegmentWriter> writers) {
    this.mapper = mapper;
    this.writers = writers;
  }

  /** Handle one raw frame delivered by a WS client, with its receive instant. */
  public void handle(String rawText, Instant receivedAt) {
    try {
      JsonNode root = mapper.readTree(rawText);
      String streamName = root.get("stream").asText();
      JsonNode data = root.path("data");

      MinuteSegmentWriter w;
      String streamType;
      if (FORCE_ORDER_BROADCAST.equals(streamName)) {
        JsonNode sym = data.path("o").path("s");
        if (sym.isMissingNode() || !sym.isTextual()) {
          System.err.println("[collector] forceOrder frame missing data.o.s; dropped");
          return;
        }
        w = writers.get(sym.asText().toLowerCase(Locale.ROOT) + "@" + FORCE_ORDER_STREAM);
        if (w == null) {
          return; // symbol not in our config (production: top-20 only)
        }
        streamType = FORCE_ORDER_STREAM;
      } else {
        w = writers.get(streamName);
        if (w == null) {
          System.err.println("[collector] unknown stream in wrapper: " + streamName);
          return;
        }
        streamType = streamTypeOf(streamName);
      }

      Instant bucket =
          EventTime.bucketInstant(streamType, data)
              .orElseGet(
                  () -> {
                    unparseableFrames.incrementAndGet();
                    return receivedAt;
                  });
      String line = CaptureEnvelope.wsFrame(mapper, rawText, receivedAt);
      w.accept((line + "\n").getBytes(StandardCharsets.UTF_8), bucket);
      framesWritten.incrementAndGet();
    } catch (Exception e) {
      System.err.println("[collector] frame handling error: " + e.getMessage());
    }
  }

  public long framesWritten() {
    return framesWritten.get();
  }

  public long unparseableFrames() {
    return unparseableFrames.get();
  }

  /**
   * Stream type is the part after the symbol's {@code @} (e.g. {@code btcusdt@trade} → {@code
   * trade}).
   */
  static String streamTypeOf(String streamName) {
    int at = streamName.indexOf('@');
    return at >= 0 ? streamName.substring(at + 1) : streamName;
  }
}
