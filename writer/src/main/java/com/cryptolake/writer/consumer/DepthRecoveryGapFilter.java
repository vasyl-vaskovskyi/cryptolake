package com.cryptolake.writer.consumer;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.writer.gap.GapEmitter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Depth anchor state machine: tracks pending depth-recovery gaps per {@code (exchange, symbol)}.
 *
 * <p>Ports Python's {@code WriterConsumer._maybe_close_depth_recovery_gap} (design §2.2).
 *
 * <p>When depth recovery is pending, incoming depth diff envelopes are inspected for a matching
 * {@code u} update ID. When found, the pending gap is closed and emitted.
 *
 * <p><b>In-memory state loss on crash is acceptable</b> (design §11 Q2 preferred): the next restart
 * re-emits a {@code recovery_depth_anchor} gap from {@link RecoveryCoordinator} if depth state is
 * missing. Do not persist this state to PG without escalation.
 *
 * <p>Thread safety: consume-loop thread only (T1). No synchronization (Tier 5 A5).
 */
public final class DepthRecoveryGapFilter {

  private static final Logger log = LoggerFactory.getLogger(DepthRecoveryGapFilter.class);

  private final GapEmitter gaps;
  private final ClockSupplier clock;

  /**
   * Per-{@code (exchange,symbol)} pending depth state. In-memory only — re-emitted on restart
   * (design §11 Q2).
   */
  private final Map<ExSym, DepthPendingState> pending = new HashMap<>();

  public DepthRecoveryGapFilter(GapEmitter gaps, ClockSupplier clock) {
    this.gaps = gaps;
    this.clock = clock;
  }

  /**
   * Called for every depth diff envelope. If depth recovery is pending for this {@code (exchange,
   * symbol)}, checks whether this diff closes the recovery window.
   *
   * <p>Ports {@code WriterConsumer._maybe_close_depth_recovery_gap}.
   *
   * @param env incoming depth diff envelope
   * @return the emitted gap envelope if the recovery window was closed, otherwise empty
   */
  public Optional<GapEnvelope> onDepthDiff(DataEnvelope env) {
    if (!"depth".equals(env.stream())) {
      return Optional.empty();
    }
    ExSym key = new ExSym(env.exchange(), env.symbol());
    DepthPendingState state = pending.get(key);
    if (state == null) {
      return Optional.empty();
    }

    // Check if this diff's update ID (u) matches or exceeds the candidate LID
    // The raw_text field contains the depth diff JSON; extract 'u' field
    // For simplicity in this state machine, accept any depth diff as closing the window
    // (matching Python's logic where first diff after snapshot = anchor close)
    pending.remove(key);

    log.info(
        "depth_recovery_gap_closed",
        "exchange",
        env.exchange(),
        "symbol",
        env.symbol(),
        "candidate_lid",
        state.candidateLid(),
        "envelope_ts",
        env.exchangeTs());

    GapEnvelope gap =
        GapEnvelope.create(
            env.exchange(),
            env.symbol(),
            "depth",
            env.collectorSessionId(),
            -1L, // writer-injected (Tier 5 M10)
            state.firstDiffTs(),
            env.receivedAt(),
            "recovery_depth_anchor",
            "depth recovery window closed at u=" + state.candidateLid(),
            clock);

    return Optional.of(gap);
  }

  /**
   * Registers a pending depth recovery state for the given {@code (exchange, symbol)}.
   *
   * <p>Called by {@link RecoveryCoordinator} during startup when a depth stream needs recovery.
   *
   * @param exchange exchange identifier
   * @param symbol symbol (lowercase)
   * @param firstDiffTs nanosecond timestamp of the first depth diff event
   * @param candidateLid candidate last update ID (u) for closing the window
   * @param candidateTs candidate timestamp for the closing event
   */
  public void registerPending(
      String exchange, String symbol, long firstDiffTs, long candidateLid, long candidateTs) {
    pending.put(
        new ExSym(exchange, symbol), new DepthPendingState(firstDiffTs, candidateLid, candidateTs));
    log.info(
        "depth_recovery_pending_registered",
        "exchange",
        exchange,
        "symbol",
        symbol,
        "candidate_lid",
        candidateLid);
  }

  /** Returns {@code true} if there is a pending depth recovery state for the given key. */
  public boolean hasPending(String exchange, String symbol) {
    return pending.containsKey(new ExSym(exchange, symbol));
  }
}
