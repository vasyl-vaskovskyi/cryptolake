package com.cryptolake.collector.streams;

/**
 * Contract for per-stream message handlers.
 *
 * <p>All implementations are called synchronously on the WebSocket listener's virtual thread. No
 * async wrapping — blocking is free on virtual threads (Tier 5 A2).
 *
 * <p>Sealed so the compiler enforces the known set of implementations.
 */
public sealed interface StreamHandler permits SimpleStreamHandler, DepthStreamHandler {

  /**
   * Handles a received frame.
   *
   * @param symbol lowercase symbol (e.g. {@code "btcusdt"})
   * @param rawText raw data payload — byte-for-byte passthrough (Tier 1 §1)
   * @param exchangeTs exchange timestamp in ms; may be {@code null} if not present in the payload
   * @param sessionSeq monotonic sequence number allocated by the session
   */
  void handle(String symbol, String rawText, Long exchangeTs, long sessionSeq);

  /**
   * Invoked by {@link com.cryptolake.collector.capture.RawFrameCapture#onDisconnect} when the
   * underlying WebSocket disconnects. Default is no-op; stateful handlers (e.g. {@link
   * DepthStreamHandler} with its pu-chain detector and pending-diff buffer) should reset per-symbol
   * state here so that diffs received on the NEXT connection are buffered until the post-reconnect
   * snapshot resync establishes a fresh sync point — instead of being validated against the stale
   * {@code lastU} from the previous connection and emitting a false {@code pu_chain_break} gap.
   *
   * @param symbols the symbols active on this connection
   */
  default void onDisconnect(java.util.List<String> symbols) {}
}
