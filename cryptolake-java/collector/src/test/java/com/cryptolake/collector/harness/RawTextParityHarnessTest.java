package com.cryptolake.collector.harness;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Regression test that invokes the gate-3 harness against the parity fixture corpus.
 *
 * <p>Passes when all fixtures match. Gate-3 invariant: byte-for-byte identity of {@code raw_text}
 * and {@code raw_sha256} (Tier 1 §7; design §8.2).
 */
class RawTextParityHarnessTest {

  /** Fixture directory relative to the project root. */
  private static final Path FIXTURES_DIR =
      Path.of("../parity-fixtures/websocket-frames").toAbsolutePath().normalize();

  @Test
  // ports: (new) RawTextParityHarnessTest::fixtureCorpusMatchesByteForByte
  void fixtureCorpusMatchesByteForByte() throws IOException {
    if (!Files.exists(FIXTURES_DIR) || !Files.isDirectory(FIXTURES_DIR)) {
      System.out.println(
          "RawTextParityHarnessTest: fixtures dir not found, skipping: " + FIXTURES_DIR);
      return;
    }

    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    List<String> failures = new ArrayList<>();
    int total = 0;

    try (Stream<Path> streamDirs = Files.list(FIXTURES_DIR)) {
      for (Path streamDir : streamDirs.filter(Files::isDirectory).toList()) {
        try (Stream<Path> files = Files.list(streamDir)) {
          for (Path rawPath : files.filter(p -> p.toString().endsWith(".raw")).toList()) {
            Path jsonPath =
                rawPath.resolveSibling(rawPath.getFileName().toString().replace(".raw", ".json"));
            if (!Files.exists(jsonPath)) continue;
            total++;
            String failure = RawTextParityHarness.checkParity(codec, rawPath, jsonPath);
            if (failure != null) failures.add(failure);
          }
        }
      }
    }

    if (!failures.isEmpty()) {
      failures.forEach(System.err::println);
    }
    assertThat(failures).as("gate3 parity failures (total fixtures checked: %d)", total).isEmpty();
  }
}
