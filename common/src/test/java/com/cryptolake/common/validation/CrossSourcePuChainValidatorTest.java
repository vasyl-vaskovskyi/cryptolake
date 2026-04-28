package com.cryptolake.common.validation;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.util.Clocks;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for CrossSourcePuChainValidator. */
class CrossSourcePuChainValidatorTest {

  record Break(String exchange, String symbol, String detail, long gapStart, long gapEnd) {}

  private List<Break> breaks;
  private CrossSourcePuChainValidator validator;
  private AtomicLong clock;

  @BeforeEach
  void setUp() {
    breaks = new ArrayList<>();
    clock = new AtomicLong(1_000_000_000L);
    validator =
        new CrossSourcePuChainValidator(
            (exchange, symbol, detail, gapStart, gapEnd) ->
                breaks.add(new Break(exchange, symbol, detail, gapStart, gapEnd)));
  }

  private DataEnvelope makeDepth(String symbol, long bigU, long u, long pu) {
    clock.addAndGet(1_000_000L); // advance clock by 1ms per call
    String rawText = "{\"U\":" + bigU + ",\"u\":" + u + ",\"pu\":" + pu + ",\"e\":\"depthUpdate\"}";
    return DataEnvelope.create(
        "binance", symbol, "depth", rawText, clock.get(), "s1", 1L, clock::get);
  }

  private DataEnvelope makeTrade(String symbol) {
    return DataEnvelope.create(
        "binance", symbol, "trades", "{\"t\":1}", 0L, "s1", 1L, Clocks.fixed(1L));
  }

  // ── test 1: clean chain — no break ────────────────────────────────────────

  @Test
  void cleanChainNoBreak() {
    // First envelope: U=100, u=110, pu=99 (chain starts at pu=99 — initial state)
    // Second envelope: U=111, u=120, pu=110 — pu matches last u
    boolean r1 = validator.handle(makeDepth("btcusdt", 100, 110, 99));
    boolean r2 = validator.handle(makeDepth("btcusdt", 111, 120, 110));

    assertThat(r1).isTrue(); // first envelope: no prior state → intact
    assertThat(r2).isTrue(); // pu=110 == last_u=110 → intact
    assertThat(breaks).isEmpty();
  }

  // ── test 2: primary then backup with no gap ───────────────────────────────

  @Test
  void primaryThenBackupWithNoGap() {
    // Primary: U=100, u=110 → state: last_u=110
    // Backup:  U=111, u=120, pu=110 → pu matches last_u → no break
    validator.handle(makeDepth("btcusdt", 100, 110, 99));
    boolean r2 = validator.handle(makeDepth("btcusdt", 111, 120, 110));

    assertThat(r2).isTrue();
    assertThat(breaks).isEmpty();
  }

  // ── test 3: primary then backup with a gap ────────────────────────────────

  @Test
  void primaryThenBackupWithGap() {
    // Primary: u=200 → state: last_u=200
    // Backup:  U=210, u=219, pu=209 — pu=209 != last_u=200 → break
    validator.handle(makeDepth("btcusdt", 190, 200, 189));
    boolean r2 = validator.handle(makeDepth("btcusdt", 210, 219, 209));

    assertThat(r2).isFalse();
    assertThat(breaks).hasSize(1);
    assertThat(breaks.get(0).symbol()).isEqualTo("btcusdt");
    assertThat(breaks.get(0).detail()).contains("pu=209");
    assertThat(breaks.get(0).detail()).contains("expected=200");
  }

  // ── test 4: multiple symbols are isolated ────────────────────────────────

  @Test
  void multipleSymbolsAreIsolated() {
    // btcusdt chain: u=100
    // ethusdt chain: u=200
    // Then btcusdt gets pu=100 → intact (not affected by ethusdt chain)
    validator.handle(makeDepth("btcusdt", 90, 100, 89));
    validator.handle(makeDepth("ethusdt", 190, 200, 189));

    boolean r3 = validator.handle(makeDepth("btcusdt", 101, 110, 100)); // pu=100 == last_u=100

    assertThat(r3).isTrue();
    assertThat(breaks).isEmpty();

    // Cross-contamination check: ethusdt break should not affect btcusdt
    validator.handle(makeDepth("ethusdt", 210, 220, 209)); // break on ethusdt
    assertThat(breaks).hasSize(1);
    assertThat(breaks.get(0).symbol()).isEqualTo("ethusdt");
  }

  // ── test 5: non-depth envelopes are ignored ──────────────────────────────

  @Test
  void nonDepthEnvelopesIgnored() {
    // Trades envelope should not affect depth chain state
    validator.handle(makeDepth("btcusdt", 90, 100, 89));
    validator.handle(makeTrade("btcusdt")); // should be ignored

    boolean r3 = validator.handle(makeDepth("btcusdt", 101, 110, 100)); // pu=100 == last_u=100
    assertThat(r3).isTrue();
    assertThat(breaks).isEmpty();
  }

  // ── test 6: dedup — first envelope doesn't emit break (no prior state) ───

  @Test
  void firstEnvelopeWithNoPriorStateDoesNotBreak() {
    // Even if pu != 0, with no prior state established, no break emitted
    boolean r = validator.handle(makeDepth("btcusdt", 1000, 1010, 999));
    assertThat(r).isTrue();
    assertThat(breaks).isEmpty();
    // last_u is now 1010
    assertThat(validator.lastUPerSymbol()).containsEntry("binance|btcusdt", 1010L);
  }

  // ── test 7: reset clears state ───────────────────────────────────────────

  @Test
  void resetClearsSymbolState() {
    validator.handle(makeDepth("btcusdt", 90, 100, 89));
    validator.reset("binance", "btcusdt");

    // After reset: next envelope with any pu doesn't break (fresh start)
    boolean r = validator.handle(makeDepth("btcusdt", 500, 510, 490));
    assertThat(r).isTrue();
    assertThat(breaks).isEmpty();
  }
}
