package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RotationStatusTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void writesAndReadsIdleStatus(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("cryptopanner-collector@a.rotation.json");
    RotationStatus.write(file, mapper, new RotationStatus("IDLE", 76800, null, null));

    RotationStatus read = RotationStatus.read(file, mapper).orElseThrow();
    assertEquals("IDLE", read.state());
    assertEquals(76800, read.currentConnectionAgeS());
    assertNull(read.rotationId(), "no rotation in flight");
    assertNull(read.oldConnectionAgeS());
  }

  @Test
  void writesAndReadsInFlightRotationStatus(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("rotation.json");
    RotationStatus.write(
        file, mapper, new RotationStatus("OVERLAP_VERIFYING", 82800, "rot-7", 82800L));

    RotationStatus read = RotationStatus.read(file, mapper).orElseThrow();
    assertEquals("OVERLAP_VERIFYING", read.state());
    assertEquals("rot-7", read.rotationId());
    assertEquals(82800L, read.oldConnectionAgeS());
  }

  @Test
  void readReturnsEmptyWhenAbsent(@TempDir Path dir) throws Exception {
    assertTrue(RotationStatus.read(dir.resolve("missing.json"), mapper).isEmpty());
  }
}
