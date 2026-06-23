package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.CaptureEnvelope;
import com.cryptopanner.common.DurableSegment;
import com.cryptopanner.common.FsHeavyLock;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WsConnectionManagerTest {

  private final ObjectMapper mapper = new ObjectMapper();

  private String trade(long id) {
    return CaptureEnvelope.wsFrame(
            mapper,
            "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":" + id + ",\"T\":" + id + "}}",
            Instant.EPOCH)
        + "\n";
  }

  private TreeSet<Long> idsIn(Path zst) throws Exception {
    TreeSet<Long> ids = new TreeSet<>();
    for (String line : DurableSegment.readLines(zst)) {
      ids.add(CaptureEnvelope.unwrap(mapper, mapper.readTree(line)).get("data").get("t").asLong());
    }
    return ids;
  }

  /** A scripted in-test shadow session, backed by real on-disk overlap files. */
  static class FakeSession implements ShadowSession {
    boolean live;
    boolean promoted;
    boolean closed;
    boolean primaryDropped;
    boolean bothDropped;
    final Deque<Optional<OverlapMinute>> minutes;

    FakeSession(boolean live, List<Optional<OverlapMinute>> minutes) {
      this.live = live;
      this.minutes = new ArrayDeque<>(minutes);
    }

    @Override
    public boolean probeLiveness() {
      return live;
    }

    @Override
    public Optional<OverlapMinute> awaitSealedMinute() {
      return minutes.isEmpty() ? Optional.empty() : minutes.poll();
    }

    @Override
    public boolean primaryDropped() {
      return primaryDropped;
    }

    @Override
    public boolean bothDropped() {
      return bothDropped;
    }

    @Override
    public void promote() {
      promoted = true;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private WsConnectionManager manager(
      ShadowSession session,
      Path lock,
      Path rotations,
      Duration connectionAge,
      Instant now,
      WsConnectionManager.Config config) {
    return new WsConnectionManager(
        () -> session,
        mapper,
        lock,
        rotations,
        () -> Optional.of(connectionAge),
        () -> "rot-1",
        () -> now,
        config);
  }

  /**
   * Builds an overlap minute with one trade pair; ids drive PASS (edge straddle) vs FAIL (hole).
   */
  private OverlapMinute minuteWith(Path dir, String mm, List<Long> primaryIds, List<Long> shadowIds)
      throws Exception {
    Path primary = dir.resolve("seg/btcusdt/trade/2026-06-14/minute-14-" + mm + ".jsonl.zst");
    Path shadow = dir.resolve("seg/btcusdt/trade/2026-06-14/minute-14-" + mm + ".shadow.jsonl.zst");
    DurableSegment.writeLines(primary, primaryIds.stream().map(this::trade).toList());
    DurableSegment.writeLines(shadow, shadowIds.stream().map(this::trade).toList());
    return new OverlapMinute(
        Integer.parseInt(mm),
        Instant.parse("2026-06-14T14:" + mm + ":00Z"),
        List.of(new SegmentPair("btcusdt", "trade", primary, shadow)));
  }

  @Test
  void liveShadowPassingEquivalenceCutsOverPromotesAndLogs(@TempDir Path dir) throws Exception {
    Path primary = dir.resolve("seg/btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst");
    Path shadow = dir.resolve("seg/btcusdt/trade/2026-06-14/minute-14-23.shadow.jsonl.zst");
    DurableSegment.writeLines(primary, List.of(trade(1), trade(2)));
    DurableSegment.writeLines(shadow, List.of(trade(2), trade(3))); // edge straddle → PASS
    Path lock = dir.resolve(".fs-heavy.lock");
    Path rotations = dir.resolve("deploy/rotations.jsonl");

    OverlapMinute minute =
        new OverlapMinute(
            23,
            Instant.parse("2026-06-14T14:23:00Z"),
            List.of(new SegmentPair("btcusdt", "trade", primary, shadow)));
    FakeSession session = new FakeSession(true, List.of(Optional.of(minute)));

    WsConnectionManager mgr =
        manager(
            session,
            lock,
            rotations,
            Duration.ofHours(23),
            Instant.parse("2026-06-14T14:24:00Z"),
            WsConnectionManager.Config.defaults("collector-a"));

    WsConnectionManager.RotateOutcome out = mgr.rotate("SCHEDULED");

    assertEquals(WsConnectionManager.Status.COMPLETED, out.status());
    assertEquals("PASS", out.verifyResult());
    assertTrue(session.promoted, "shadow promoted to primary");
    assertEquals(new TreeSet<>(List.of(1L, 2L, 3L)), idsIn(primary), "primary holds the union");
    assertFalse(Files.exists(shadow), "shadow consumed by the merge");

    JsonNode ev = mapper.readTree(Files.readAllLines(rotations).get(0));
    assertEquals("rot-1", ev.get("rotation_id").asText());
    assertEquals("SCHEDULED", ev.get("reason").asText());
    assertEquals("PASS", ev.get("verify_result").asText());
    assertEquals(
        Optional.empty(), FsHeavyLock.heldBy(lock), "fs-heavy lock released after cutover");
  }

  @Test
  void emitsRotationStartedAndPromotedStructuredEvents(@TempDir Path dir) throws Exception {
    OverlapMinute minute = minuteWith(dir, "23", List.of(1L, 2L), List.of(2L, 3L));
    FakeSession session = new FakeSession(true, List.of(Optional.of(minute)));
    Path logFile = dir.resolve("logs/cryptopanner-collector@a.jsonl");
    WsConnectionManager mgr =
        manager(
                session,
                dir.resolve(".fs-heavy.lock"),
                dir.resolve("rotations.jsonl"),
                Duration.ofHours(23),
                Instant.parse("2026-06-14T14:24:00Z"),
                WsConnectionManager.Config.defaults("collector-a"))
            .withLog(
                new com.cryptopanner.common.StructuredLog(logFile, "cryptopanner-collector", "a"));

    mgr.rotate("SCHEDULED");

    var lines = Files.readAllLines(logFile);
    assertTrue(
        lines.stream().anyMatch(l -> l.contains("\"event\":\"rotation_started\"")),
        "rotation_started (§11.e)");
    assertTrue(
        lines.stream().anyMatch(l -> l.contains("\"event\":\"rotation_promoted\"")),
        "rotation_promoted (§11.e)");
  }

  @Test
  void halfOpenShadowRetriesWithoutCuttingOver(@TempDir Path dir) throws Exception {
    OverlapMinute minute = minuteWith(dir, "23", List.of(1L, 2L), List.of(2L, 3L));
    FakeSession session = new FakeSession(/* live= */ false, List.of(Optional.of(minute)));
    WsConnectionManager mgr =
        manager(
            session,
            dir.resolve(".fs-heavy.lock"),
            dir.resolve("rotations.jsonl"),
            Duration.ofHours(23),
            Instant.parse("2026-06-14T14:24:00Z"),
            WsConnectionManager.Config.defaults("collector-a"));

    WsConnectionManager.RotateOutcome out = mgr.rotate("SCHEDULED");

    assertEquals(WsConnectionManager.Status.RETRY_SHADOW, out.status());
    assertFalse(session.promoted, "a half-open shadow never promotes");
    assertTrue(session.closed, "half-open shadow is closed");
    assertFalse(
        Files.exists(dir.resolve("rotations.jsonl")), "no rotation event on a half-open shadow");
  }

  @Test
  void threeConsecutiveEquivalenceFailsForceCutover(@TempDir Path dir) throws Exception {
    // Every overlap minute has an interior hole at 2 → FAIL. Budget maxFails=3.
    FakeSession session =
        new FakeSession(
            true,
            List.of(
                Optional.of(minuteWith(dir, "23", List.of(1L, 2L, 3L), List.of(1L, 3L))),
                Optional.of(minuteWith(dir, "24", List.of(1L, 2L, 3L), List.of(1L, 3L))),
                Optional.of(minuteWith(dir, "25", List.of(1L, 2L, 3L), List.of(1L, 3L)))));
    Path rotations = dir.resolve("rotations.jsonl");
    WsConnectionManager mgr =
        manager(
            session,
            dir.resolve(".fs-heavy.lock"),
            rotations,
            Duration.ofHours(23),
            Instant.parse("2026-06-14T14:26:00Z"),
            WsConnectionManager.Config.defaults("collector-a"));

    WsConnectionManager.RotateOutcome out = mgr.rotate("SCHEDULED");

    assertEquals(WsConnectionManager.Status.COMPLETED, out.status());
    assertEquals("FORCED", out.verifyResult(), "3rd consecutive fail forces the cutover");
    assertTrue(session.promoted);
    JsonNode ev = mapper.readTree(Files.readAllLines(rotations).get(0));
    assertEquals("FORCED", ev.get("verify_result").asText());
  }

  @Test
  void failThenPassCutsOverOnTheLaterMinute(@TempDir Path dir) throws Exception {
    FakeSession session =
        new FakeSession(
            true,
            List.of(
                Optional.of(minuteWith(dir, "23", List.of(1L, 2L, 3L), List.of(1L, 3L))), // FAIL
                Optional.of(minuteWith(dir, "24", List.of(1L, 2L), List.of(2L, 3L))))); // PASS
    WsConnectionManager mgr =
        manager(
            session,
            dir.resolve(".fs-heavy.lock"),
            dir.resolve("rotations.jsonl"),
            Duration.ofHours(23),
            Instant.parse("2026-06-14T14:25:00Z"),
            WsConnectionManager.Config.defaults("collector-a"));

    WsConnectionManager.RotateOutcome out = mgr.rotate("SCHEDULED");

    assertEquals(WsConnectionManager.Status.COMPLETED, out.status());
    assertEquals("PASS", out.verifyResult());
    assertTrue(session.promoted);
  }

  @Test
  void primaryDropMidOverlapPromotesEarly(@TempDir Path dir) throws Exception {
    OverlapMinute minute = minuteWith(dir, "23", List.of(1L, 2L), List.of(2L, 3L));
    FakeSession session = new FakeSession(true, List.of(Optional.of(minute)));
    session.primaryDropped = true;
    WsConnectionManager mgr =
        manager(
            session,
            dir.resolve(".fs-heavy.lock"),
            dir.resolve("rotations.jsonl"),
            Duration.ofHours(23),
            Instant.parse("2026-06-14T14:24:00Z"),
            WsConnectionManager.Config.defaults("collector-a"));

    WsConnectionManager.RotateOutcome out = mgr.rotate("SCHEDULED");

    assertEquals(WsConnectionManager.Status.COMPLETED, out.status());
    assertEquals("EARLY", out.verifyResult(), "old connection dropped → early cutover");
    assertTrue(session.promoted);
  }

  @Test
  void bothConnectionsDropAbortsTheRotation(@TempDir Path dir) throws Exception {
    OverlapMinute minute = minuteWith(dir, "23", List.of(1L, 2L), List.of(2L, 3L));
    FakeSession session = new FakeSession(true, List.of(Optional.of(minute)));
    session.bothDropped = true;
    WsConnectionManager mgr =
        manager(
            session,
            dir.resolve(".fs-heavy.lock"),
            dir.resolve("rotations.jsonl"),
            Duration.ofHours(23),
            Instant.parse("2026-06-14T14:24:00Z"),
            WsConnectionManager.Config.defaults("collector-a"));

    WsConnectionManager.RotateOutcome out = mgr.rotate("SCHEDULED");

    assertEquals(WsConnectionManager.Status.ABORTED, out.status());
    assertFalse(session.promoted);
    assertTrue(session.closed);
  }

  @Test
  void shadowEndingWithNoSealedMinuteAborts(@TempDir Path dir) throws Exception {
    FakeSession session = new FakeSession(true, List.of()); // probe live, but no minute ever seals
    WsConnectionManager mgr =
        manager(
            session,
            dir.resolve(".fs-heavy.lock"),
            dir.resolve("rotations.jsonl"),
            Duration.ofHours(23),
            Instant.parse("2026-06-14T14:24:00Z"),
            WsConnectionManager.Config.defaults("collector-a"));

    assertEquals(WsConnectionManager.Status.ABORTED, mgr.rotate("SCHEDULED").status());
    assertTrue(session.closed);
  }

  @Test
  void lockHeldByAnotherHolderDefersTheCutover(@TempDir Path dir) throws Exception {
    OverlapMinute minute = minuteWith(dir, "23", List.of(1L, 2L), List.of(2L, 3L));
    FakeSession session = new FakeSession(true, List.of(Optional.of(minute)));
    Path lock = dir.resolve(".fs-heavy.lock");
    Path primary = minute.pairs().get(0).primaryFile();
    var config =
        new WsConnectionManager.Config(
            "collector-a", Duration.ofMillis(150), 3, Duration.ofMinutes(5));
    WsConnectionManager mgr =
        manager(
            session,
            lock,
            dir.resolve("rotations.jsonl"),
            Duration.ofHours(23),
            Instant.parse("2026-06-14T14:24:00Z"),
            config);

    try (FsHeavyLock held = FsHeavyLock.acquire(lock, "sealer", Duration.ofSeconds(1))) {
      WsConnectionManager.RotateOutcome out = mgr.rotate("SCHEDULED");
      assertEquals(WsConnectionManager.Status.DEFERRED, out.status());
      assertFalse(session.promoted, "no promote while another holder owns the fs-heavy lock");
      assertEquals(
          new TreeSet<>(List.of(1L, 2L)),
          idsIn(primary),
          "primary untouched on a deferred cutover");
    }
  }

  @Test
  void operatorTriggerOnYoungConnectionIsRefused(@TempDir Path dir) throws Exception {
    OverlapMinute minute = minuteWith(dir, "23", List.of(1L, 2L), List.of(2L, 3L));
    FakeSession session = new FakeSession(true, List.of(Optional.of(minute)));
    WsConnectionManager mgr =
        manager(
            session,
            dir.resolve(".fs-heavy.lock"),
            dir.resolve("rotations.jsonl"),
            Duration.ofMinutes(2), // younger than the 5-min operator floor
            Instant.parse("2026-06-14T14:24:00Z"),
            WsConnectionManager.Config.defaults("collector-a"));

    WsConnectionManager.RotateOutcome out = mgr.rotate("OPERATOR_TRIGGERED");

    assertEquals(WsConnectionManager.Status.REFUSED, out.status());
    assertFalse(session.promoted, "a trivially-young operator trigger never opens a shadow");
    assertFalse(session.closed, "the shadow is never even opened");
  }

  @Test
  void concurrentRotationIsRejectedAsInProgress(@TempDir Path dir) throws Exception {
    // A reentrant rotate() (while the outer one holds the in-progress flag) is rejected.
    Path lock = dir.resolve(".fs-heavy.lock");
    Path rotations = dir.resolve("rotations.jsonl");
    OverlapMinute minute = minuteWith(dir, "23", List.of(1L, 2L), List.of(2L, 3L));
    WsConnectionManager.Status[] nested = new WsConnectionManager.Status[1];
    WsConnectionManager[] holder = new WsConnectionManager[1];

    // The shadow's liveness probe re-enters rotate() to prove the guard rejects it.
    ShadowSession reentrant =
        new FakeSession(true, List.of(Optional.of(minute))) {
          @Override
          public boolean probeLiveness() {
            try {
              nested[0] = holder[0].rotate("SCHEDULED").status();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            return true;
          }
        };
    holder[0] =
        new WsConnectionManager(
            () -> reentrant,
            mapper,
            lock,
            rotations,
            () -> Optional.of(Duration.ofHours(23)),
            () -> "rot-1",
            () -> Instant.parse("2026-06-14T14:24:00Z"),
            WsConnectionManager.Config.defaults("collector-a"));

    WsConnectionManager.RotateOutcome out = holder[0].rotate("SCHEDULED");

    assertEquals(WsConnectionManager.Status.COMPLETED, out.status(), "outer rotation completes");
    assertEquals(
        WsConnectionManager.Status.IN_PROGRESS, nested[0], "the reentrant rotation is rejected");
  }
}
