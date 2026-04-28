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
}
