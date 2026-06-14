package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Sha256SidecarTest {

  @Test
  void computeAndWrite_producesShaSumFormat(@TempDir Path tmp) throws IOException {
    Path data = tmp.resolve("hour-14.jsonl.zst");
    Files.writeString(data, "hello\n");
    Path sidecar = tmp.resolve("hour-14.jsonl.zst.sha256");

    Sha256Sidecar.computeAndWrite(data, sidecar);

    String contents = Files.readString(sidecar);
    // sha256("hello\n") = 5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03
    assertEquals(
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03  hour-14.jsonl.zst\n",
        contents);
  }

  @Test
  void readHash_extractsHexFromSidecar(@TempDir Path tmp) throws IOException {
    Path sidecar = tmp.resolve("foo.sha256");
    Files.writeString(
        sidecar, "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03  foo\n");
    assertEquals(
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        Sha256Sidecar.readHash(sidecar));
  }
}
