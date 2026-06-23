package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RotationTriggerTest {

  @Test
  void requestThenConsumeReturnsReasonAndClearsTheTrigger(@TempDir Path dir) throws Exception {
    Path trigger = dir.resolve("deploy/rotation-trigger");

    RotationTrigger.request(trigger, "OPERATOR_TRIGGERED");
    assertTrue(Files.exists(trigger), "request drops a trigger file");

    Optional<String> consumed = RotationTrigger.consume(trigger);

    assertEquals(Optional.of("OPERATOR_TRIGGERED"), consumed);
    assertFalse(Files.exists(trigger), "consume removes the trigger (one-shot)");
  }

  @Test
  void consumeWithNoTriggerReturnsEmpty(@TempDir Path dir) throws Exception {
    assertEquals(Optional.empty(), RotationTrigger.consume(dir.resolve("rotation-trigger")));
  }

  @Test
  void consumeIsOneShot(@TempDir Path dir) throws Exception {
    Path trigger = dir.resolve("rotation-trigger");
    RotationTrigger.request(trigger, "OPERATOR_TRIGGERED");

    assertTrue(RotationTrigger.consume(trigger).isPresent());
    assertEquals(Optional.empty(), RotationTrigger.consume(trigger), "second consume sees nothing");
  }

  @Test
  void blankTriggerFileFallsBackToOperatorReason(@TempDir Path dir) throws Exception {
    Path trigger = dir.resolve("rotation-trigger");
    Files.createDirectories(trigger.getParent());
    Files.writeString(trigger, "   \n");

    assertEquals(Optional.of("OPERATOR_TRIGGERED"), RotationTrigger.consume(trigger));
  }
}
