package com.cryptolake.collector.capture;

import com.cryptolake.collector.adapter.BinanceAdapter;
import com.cryptolake.collector.adapter.StreamKey;
import com.cryptolake.collector.connection.BackpressureGate;
import com.cryptolake.collector.gap.DisconnectGapCoalescer;
import com.cryptolake.collector.gap.GapEmitter;
import com.cryptolake.collector.streams.StreamHandler;
import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.common.util.ClockSupplier;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

/**
 * Single pre-parse capture point (Tier 1 §1, §2; Tier 5 B4).
 *
 * <p>Every WebSocket frame enters via {@link #onFrame(String, String)}, which:
 *
 * <ol>
 *   <li>Checks {@link BackpressureGate} (pause if full).
 *   <li>Routes via {@link BinanceAdapter#routeStream(String)}.
 *   <li>Looks up the handler — null drops silently (Tier 1 §3: disabled streams emit nothing).
 *   <li>Extracts exchange_ts; records latency.
 *   <li>Allocates session_seq.
 *   <li>Clears disconnect-gap flag ({@link DisconnectGapCoalescer#onData}).
 *   <li>Updates {@code lastReceivedAt}.
 *   <li>Calls {@code handler.handle(...)} under MDC (Tier 5 H3).
 * </ol>
 *
 * <p>Thread safety: {@code lastReceivedAt} is a {@link ConcurrentHashMap} safe for unsynchronized
 * reads from the {@code FirstFrameWatchdog} thread. All other fields are immutable
 * post-construction or owned by the listener thread.
 */
public final class RawFrameCapture {

  private static final StructuredLogger log = StructuredLogger.of(RawFrameCapture.class);

  private final String exchange;
  private final BinanceAdapter adapter;
  private final Map<String, StreamHandler> handlers;
  private final SessionSeqAllocator seqAllocator;
  private final BackpressureGate backpressureGate;
  private final DisconnectGapCoalescer disconnectGapCoalescer;
  private final ExchangeLatencyRecorder latencyRecorder;
  private final GapEmitter gapEmitter;
  final ClockSupplier clock; // package-visible for WebSocketSupervisor
  private final List<String> enabledStreams;

  /**
   * Cross-thread readable map: {@code (symbol + NUL + stream) -> received_at_ns}. Read by {@code
   * FirstFrameWatchdog} concurrently.
   */
  public final ConcurrentHashMap<String, Long> lastReceivedAt = new ConcurrentHashMap<>();

  public RawFrameCapture(
      String exchange,
      BinanceAdapter adapter,
      Map<String, StreamHandler> handlers,
      SessionSeqAllocator seqAllocator,
      BackpressureGate backpressureGate,
      DisconnectGapCoalescer disconnectGapCoalescer,
      ExchangeLatencyRecorder latencyRecorder,
      GapEmitter gapEmitter,
      ClockSupplier clock,
      List<String> enabledStreams) {
    this.exchange = exchange;
    this.adapter = adapter;
    this.handlers = handlers;
    this.seqAllocator = seqAllocator;
    this.backpressureGate = backpressureGate;
    this.disconnectGapCoalescer = disconnectGapCoalescer;
    this.latencyRecorder = latencyRecorder;
    this.gapEmitter = gapEmitter;
    this.clock = clock;
    this.enabledStreams = enabledStreams;
  }

  /**
   * Processes one WebSocket frame (text).
   *
   * <p>Backpressure: when gate is paused, we park briefly before proceeding (not Thread.sleep —
   * Tier 2 §10).
   */
  public void onFrame(String socketName, String rawFrame) {
    // (1) Backpressure check
    if (backpressureGate.shouldPause()) {
      LockSupport.parkNanos(100_000L); // 0.1ms — virtual-thread-friendly (Tier 2 §10)
    }

    // (2) Route via balanced-brace extractor (Tier 1 §1, Tier 5 B4)
    FrameRoute route = adapter.routeStream(rawFrame);
    if (route == null) {
      // Subscription ack or unknown frame — drop silently
      return;
    }

    String streamType = route.streamType();
    String symbol = route.symbol();
    String rawText = route.rawText();

    // (3) Handler lookup — null means stream is disabled (Tier 1 §3)
    StreamHandler handler = handlers.get(streamType);
    if (handler == null) {
      return; // disabled stream: emit nothing
    }

    // (4) Exchange timestamp + latency recording
    Long exchangeTs = adapter.extractExchangeTs(streamType, rawText);
    long receivedAtNs = clock.nowNs();
    if (exchangeTs != null) {
      latencyRecorder.record(symbol, streamType, exchangeTs, receivedAtNs);
    }

    // (5) Allocate session seq
    long sessionSeq = seqAllocator.next(symbol, streamType);

    // (6) Clear disconnect-gap flag (data arrived)
    disconnectGapCoalescer.onData(symbol, streamType);

    // (7) Update lastReceivedAt (read by FirstFrameWatchdog on a separate thread)
    lastReceivedAt.put(tupleKey(symbol, streamType), receivedAtNs);

    // (8) Dispatch to handler under MDC (Tier 5 H3)
    try (MDCCloseable c1 = MDC.putCloseable("symbol", symbol);
        MDCCloseable c2 = MDC.putCloseable("stream", streamType)) {
      handler.handle(symbol, rawText, exchangeTs, sessionSeq);
    } catch (Exception e) {
      log.error(
          "handler_error", e, "symbol", symbol, "stream", streamType, "error", e.getMessage());
      // Emit a handler_error gap so the consumer sees the hole (design §6.2 new reasons)
      gapEmitter.emit(
          symbol, streamType, sessionSeq, "handler_error", "handler threw: " + e.getMessage());
    }
  }

  /**
   * Called when the WebSocket disconnects. Emits one {@code ws_disconnect} gap per enabled {@code
   * (symbol, stream)} pair that has not already had one emitted since the last data frame.
   *
   * <p>Gap-start fallback ordering (design §6.2; Python commit 30348b2): {@code
   * lastReceivedAt[(sym, st)] ?? subscribeAckAt ?? now}. The middle fallback covers cold-boot
   * half-open reconnects.
   *
   * @param subscribeAckAtNs timestamp when the SUBSCRIBE ack was received; {@code 0} if not known
   */
  public void onDisconnect(String socketName, List<String> symbols, long subscribeAckAtNs) {
    long now = clock.nowNs();
    for (String symbol : symbols) {
      for (String stream : enabledStreams) {
        if (StreamKey.REST_ONLY_STREAMS.contains(stream)) continue;
        if (disconnectGapCoalescer.tryMark(symbol, stream)) {
          long gapStart =
              lastReceivedAt.getOrDefault(
                  tupleKey(symbol, stream), subscribeAckAtNs > 0 ? subscribeAckAtNs : now);
          long sessionSeq = seqAllocator.current(symbol, stream);
          gapEmitter.emitWithTimestamps(
              symbol,
              stream,
              sessionSeq,
              "ws_disconnect",
              "WebSocket disconnected on socket " + socketName,
              gapStart,
              now);
        }
      }
    }
  }

  /** Returns the clock (accessible to the supervisor for subscribeAckAt tracking). */
  public ClockSupplier getClock() {
    return clock;
  }

  /** Utility: builds the CHM key for a {@code (symbol, stream)} tuple. */
  public static String tupleKey(String symbol, String stream) {
    return symbol + '\0' + stream;
  }
}
