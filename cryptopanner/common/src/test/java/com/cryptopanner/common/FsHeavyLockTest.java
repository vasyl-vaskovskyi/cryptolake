package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsHeavyLockTest {

  @Test
  void acquireExposesHolderThenFreesOnClose(@TempDir Path dir) throws Exception {
    Path lock = dir.resolve(".fs-heavy.lock");
    try (FsHeavyLock held = FsHeavyLock.acquire(lock, "sealer", Duration.ofSeconds(1))) {
      assertEquals("sealer", FsHeavyLock.heldBy(lock).orElse(null));
    }
    assertEquals(null, FsHeavyLock.heldBy(lock).orElse(null), "freed after close");
  }

  @Test
  void secondAcquireTimesOutWhileHeld(@TempDir Path dir) throws Exception {
    Path lock = dir.resolve(".fs-heavy.lock");
    try (FsHeavyLock held = FsHeavyLock.acquire(lock, "sealer", Duration.ofSeconds(1))) {
      assertThrows(
          TimeoutException.class,
          () -> FsHeavyLock.acquire(lock, "deploy", Duration.ofMillis(300)));
    }
  }

  @Test
  void reacquirableAfterRelease(@TempDir Path dir) throws Exception {
    Path lock = dir.resolve(".fs-heavy.lock");
    FsHeavyLock.acquire(lock, "sealer", Duration.ofSeconds(1)).close();
    try (FsHeavyLock again = FsHeavyLock.acquire(lock, "rotation", Duration.ofSeconds(1))) {
      assertEquals("rotation", FsHeavyLock.heldBy(lock).orElse(null));
    }
  }
}
