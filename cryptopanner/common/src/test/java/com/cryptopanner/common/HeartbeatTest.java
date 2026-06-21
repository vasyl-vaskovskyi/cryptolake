package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HeartbeatTest {

  @Test
  void ageIsEmptyWhenFileAbsent(@TempDir Path dir) throws Exception {
    assertEquals(Optional.empty(), Heartbeat.age(dir.resolve("nope.heartbeat"), Instant.now()));
  }

  @Test
  void touchCreatesFileWithFreshAge(@TempDir Path dir) throws Exception {
    Path hb = dir.resolve("c.heartbeat");
    Heartbeat.touch(hb);
    assertTrue(Files.exists(hb));
    Optional<Duration> age = Heartbeat.age(hb, Instant.now());
    assertTrue(age.isPresent());
    assertTrue(age.get().compareTo(Duration.ofSeconds(5)) < 0, "just-touched heartbeat is fresh");
  }

  @Test
  void ageReflectsMtime(@TempDir Path dir) throws Exception {
    Path hb = dir.resolve("c.heartbeat");
    Heartbeat.touch(hb);
    Instant now = Instant.parse("2026-06-21T12:00:30Z");
    Files.setLastModifiedTime(hb, FileTime.from(Instant.parse("2026-06-21T12:00:00Z")));
    assertEquals(Duration.ofSeconds(30), Heartbeat.age(hb, now).orElseThrow());
  }
}
