package com.cryptolake.collector.gap;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DisconnectGapCoalescer}.
 *
 * <p>Ports {@code test_disconnect_gap_coalescing.py} tests.
 */
class DisconnectGapCoalescerTest {

  private DisconnectGapCoalescer coalescer;

  @BeforeEach
  void setUp() {
    coalescer = new DisconnectGapCoalescer();
  }

  @Test
  // ports: tests/unit/collector/test_disconnect_gap_coalescing.py::test_first_disconnect_emits_one_gap_per_public_stream
  void firstDisconnectEmitsOneGapPerPublicStream() {
    // First call for a given (symbol, stream) pair returns true (emit gap)
    assertThat(coalescer.tryMark("btcusdt", "trades")).isTrue();
    assertThat(coalescer.tryMark("btcusdt", "depth")).isTrue();
    assertThat(coalescer.tryMark("ethusdt", "trades")).isTrue();
  }

  @Test
  // ports: tests/unit/collector/test_disconnect_gap_coalescing.py::test_repeated_disconnect_calls_coalesce
  void repeatedDisconnectCallsCoalesce() {
    assertThat(coalescer.tryMark("btcusdt", "trades")).isTrue();
    // Second call for same pair: already marked — suppress
    assertThat(coalescer.tryMark("btcusdt", "trades")).isFalse();
    assertThat(coalescer.tryMark("btcusdt", "trades")).isFalse();
  }

  @Test
  // ports: tests/unit/collector/test_disconnect_gap_coalescing.py::test_data_arrival_clears_emitted_flag
  void dataArrivalClearsEmittedFlag() {
    assertThat(coalescer.tryMark("btcusdt", "trades")).isTrue();
    coalescer.onData("btcusdt", "trades");
    // After data arrival, next disconnect should be emitted
    assertThat(coalescer.tryMark("btcusdt", "trades")).isTrue();
  }
}
