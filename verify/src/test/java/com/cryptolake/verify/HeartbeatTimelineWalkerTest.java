package com.cryptolake.verify;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.verify.validation.HeartbeatTimelineWalker;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for HeartbeatTimelineWalker.
 *
 * <p>Uses an empty temp directory (no heartbeat archives). When heartbeat archives are absent, the
 * walker returns no errors — this is the expected behavior for deployments that have not yet
 * enabled heartbeat archival.
 */
class HeartbeatTimelineWalkerTest {

  @TempDir Path tempDir;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static ObjectNode makeGapNode(long gapStart, long gapEnd) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("type", "gap");
    node.put("gap_start_ts", gapStart);
    node.put("gap_end_ts", gapEnd);
    node.put("reason", "ws_disconnect");
    return node;
  }

  // ── test 1: no heartbeat archives → no errors ────────────────────────────

  @Test
  void noHeartbeatArchivesNoErrors() {
    List<String> errors =
        HeartbeatTimelineWalker.walk(
            tempDir,
            "binance",
            "btcusdt",
            "depth",
            "2026-04-28",
            Collections.emptyList(),
            Collections.emptyList(),
            MAPPER);
    assertThat(errors).isEmpty();
  }

  // ── test 2: silence covered by gap envelope → no error ───────────────────

  @Test
  void silenceCoveredByGapEnvelopeNoError() {
    long silenceStart = 1_000_000_000L;
    long silenceEnd = silenceStart + HeartbeatTimelineWalker.SILENCE_THRESHOLD_NS + 5_000_000_000L;

    // Gap envelope that covers the silence
    List<JsonNode> gapEnvs = List.of(makeGapNode(silenceStart, silenceEnd));

    // With no heartbeat archives, no errors regardless of gap envelopes
    List<String> errors =
        HeartbeatTimelineWalker.walk(
            tempDir,
            "binance",
            "btcusdt",
            "depth",
            "2026-04-28",
            Collections.emptyList(),
            gapEnvs,
            MAPPER);

    // No heartbeat archives → no errors
    assertThat(errors).isEmpty();
  }

  // ── test 3: SILENCE_THRESHOLD_NS is 30s ──────────────────────────────────

  @Test
  void silenceThresholdIs30Seconds() {
    assertThat(HeartbeatTimelineWalker.SILENCE_THRESHOLD_NS).isEqualTo(30_000_000_000L);
  }

  // ── test 4: walk with empty data and gap envelopes → no errors ───────────

  @Test
  void emptyDataAndGapEnvelopesNoErrors() {
    List<String> errors =
        HeartbeatTimelineWalker.walk(
            tempDir,
            "binance",
            "btcusdt",
            "trades",
            "2026-04-28",
            Collections.emptyList(),
            Collections.emptyList(),
            MAPPER);
    assertThat(errors).isEmpty();
  }
}
