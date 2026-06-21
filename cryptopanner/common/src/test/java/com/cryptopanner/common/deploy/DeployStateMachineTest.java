package com.cryptopanner.common.deploy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeployStateMachineTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void historyLogAppendsAndReadsLastMarkerForDeploy(@TempDir Path dir) throws Exception {
    Path history = dir.resolve("deploy/history.jsonl");
    DeployHistoryLog.append(
        history,
        mapper,
        "d-1",
        DeployState.PROMOTING_STARTED,
        Instant.parse("2026-06-09T14:25:00Z"));
    DeployHistoryLog.append(
        history, mapper, "d-1", DeployState.OLD_STOPPED, Instant.parse("2026-06-09T14:25:05Z"));
    DeployHistoryLog.append(
        history,
        mapper,
        "d-1",
        DeployState.ACTIVE_SLOT_FLIPPED,
        Instant.parse("2026-06-09T14:25:06Z"));

    assertEquals(
        Optional.of(DeployState.ACTIVE_SLOT_FLIPPED),
        DeployHistoryLog.lastMarker(history, mapper, "d-1"));
    assertEquals(Optional.empty(), DeployHistoryLog.lastMarker(history, mapper, "other-deploy"));
  }

  @Test
  void lastMarkerEmptyWhenNoHistory(@TempDir Path dir) throws Exception {
    assertEquals(
        Optional.empty(), DeployHistoryLog.lastMarker(dir.resolve("history.jsonl"), mapper, "d-1"));
  }

  @Test
  void freshWhenNoDeployInProgress() {
    assertEquals(DeployResume.Action.FRESH, DeployResume.decide(null));
  }

  @Test
  void completeWhenAlreadyPromoted() {
    assertEquals(DeployResume.Action.COMPLETE, DeployResume.decide(DeployState.PROMOTED));
  }

  @Test
  void abortableBeforeActiveSlotFlip() {
    assertEquals(DeployResume.Action.ABORTABLE, DeployResume.decide(DeployState.PROMOTING_STARTED));
    assertEquals(DeployResume.Action.ABORTABLE, DeployResume.decide(DeployState.OLD_STOPPED));
  }

  @Test
  void forwardOnlyAtAndAfterActiveSlotFlip() {
    assertEquals(
        DeployResume.Action.FORWARD_ONLY, DeployResume.decide(DeployState.ACTIVE_SLOT_FLIPPED));
    assertEquals(
        DeployResume.Action.FORWARD_ONLY, DeployResume.decide(DeployState.NEW_ROLE_FLIPPED));
    assertEquals(DeployResume.Action.FORWARD_ONLY, DeployResume.decide(DeployState.OVERLAP_MERGED));
    assertEquals(
        DeployResume.Action.FORWARD_ONLY, DeployResume.decide(DeployState.STAGING_DRAINED));
  }

  @Test
  void pointOfNoReturnIsTheActiveSlotFlip() {
    assertTrue(DeployState.ACTIVE_SLOT_FLIPPED.isPastPointOfNoReturn());
    assertTrue(DeployState.OVERLAP_MERGED.isPastPointOfNoReturn());
    assertEquals(false, DeployState.OLD_STOPPED.isPastPointOfNoReturn());
  }
}
