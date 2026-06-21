package com.cryptopanner.deploy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.deploy.DeployState;
import java.util.List;
import org.junit.jupiter.api.Test;

class PromotePlanTest {

  @Test
  void freshPromoteRunsAllStepsInOrder() {
    List<DeployState> steps = PromotePlan.remainingSteps(null);
    assertEquals(
        List.of(
            DeployState.PROMOTING_STARTED,
            DeployState.OLD_STOPPED,
            DeployState.ACTIVE_SLOT_FLIPPED,
            DeployState.NEW_ROLE_FLIPPED,
            DeployState.OVERLAP_MERGED,
            DeployState.STAGING_DRAINED,
            DeployState.PROMOTED),
        steps);
  }

  @Test
  void resumeRunsOnlyStepsAfterTheLastMarker() {
    assertEquals(
        List.of(
            DeployState.NEW_ROLE_FLIPPED,
            DeployState.OVERLAP_MERGED,
            DeployState.STAGING_DRAINED,
            DeployState.PROMOTED),
        PromotePlan.remainingSteps(DeployState.ACTIVE_SLOT_FLIPPED));
  }

  @Test
  void completedDeployHasNoRemainingSteps() {
    assertTrue(PromotePlan.remainingSteps(DeployState.PROMOTED).isEmpty());
  }
}
