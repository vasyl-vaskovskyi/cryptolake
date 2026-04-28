package com.cryptolake.consolidation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;

/**
 * Chaos integration test harness.
 *
 * <p>Discovers every {@code tests/chaos/NN_*.sh} scenario script in the repository root and runs
 * each via {@code bash} in a subprocess. Asserts exit code 0 (PASS).
 *
 * <p>Run all scenarios:
 *
 * <pre>
 *   ./gradlew :consolidation:test --tests "*ChaosVerifyIT*"
 * </pre>
 *
 * <p>Run a single scenario (e.g. 01):
 *
 * <pre>
 *   ./gradlew :consolidation:test --tests "*ChaosVerifyIT*" -Dchaos.filter=01
 * </pre>
 *
 * <p>Each scenario is tagged {@code chaos} and {@code integration} so it can be excluded from fast
 * unit-test runs via {@code -Djunit.jupiter.execution.parallel.enabled=false} or a custom tag
 * filter.
 *
 * <p>Scenarios time out after 15 minutes each (the most aggressive scenario — disk fill + recovery
 * — takes ~5 min; allowing 3× headroom). The overall suite can take ~70 minutes.
 *
 * <p>NOTE: This harness DISCOVERS all 23 scenarios. Some scenarios (17–23) test NEW Track-A
 * components and may FAIL if Track A code is not yet fully merged. Failures are annotated in the
 * status report at {@code docs/superpowers/specs/2026-04-28-gap-detection-track-b-status.md}.
 */
@Tag("chaos")
@Tag("integration")
class ChaosVerifyIT {

  /** Timeout per scenario in minutes. */
  private static final int SCENARIO_TIMEOUT_MINUTES = 15;

  /**
   * The chaos scenario directory, relative to the Gradle root project directory.
   *
   * <p>Resolves to {@code <repo_root>/tests/chaos}.
   */
  private static final Path CHAOS_DIR = resolveRepoRoot().resolve("tests/chaos");

  @TestFactory
  List<DynamicTest> allChaosScenarios() throws IOException {
    String filter = System.getProperty("chaos.filter", "");

    List<Path> scenarios = discoverScenarios(filter);

    assertThat(scenarios).as("Expected to discover chaos scenarios in %s", CHAOS_DIR).isNotEmpty();

    return scenarios.stream()
        .map(
            script -> {
              String name = script.getFileName().toString().replace(".sh", "");
              return DynamicTest.dynamicTest(
                  name,
                  () -> {
                    int exitCode = runScenario(script);
                    assertThat(exitCode)
                        .as(
                            "Chaos scenario '%s' should exit 0 (PASS). "
                                + "Check logs/stderr for details. "
                                + "Script: %s",
                            name, script)
                        .isEqualTo(0);
                  });
            })
        .collect(Collectors.toList());
  }

  /**
   * Discovers scenario scripts matching {@code NN_*.sh} in the chaos directory, sorted
   * lexicographically. If a filter is specified (e.g. {@code "01"} or {@code "17"}), only scripts
   * whose base name starts with that filter string are returned.
   */
  private static List<Path> discoverScenarios(String filter) throws IOException {
    if (!Files.isDirectory(CHAOS_DIR)) {
      throw new IllegalStateException("Chaos directory not found: " + CHAOS_DIR);
    }

    try (Stream<Path> stream = Files.list(CHAOS_DIR)) {
      return stream
          .filter(
              p -> {
                String name = p.getFileName().toString();
                return name.matches("\\d{2}_.*\\.sh");
              })
          .filter(
              p -> {
                if (filter == null || filter.isBlank()) return true;
                String name = p.getFileName().toString();
                // Match by NN prefix (zero-padded or not)
                String paddedFilter = String.format("%02d", parseIntOrZero(filter));
                return name.startsWith(paddedFilter + "_") || name.startsWith(filter + "_");
              })
          .sorted()
          .collect(Collectors.toList());
    }
  }

  /**
   * Runs a single scenario script via {@code bash <script>} and returns the exit code.
   *
   * <p>stdout + stderr from the script are inherited by the JVM process so they appear in the
   * Gradle test output.
   */
  private static int runScenario(Path script) throws IOException, InterruptedException {
    ProcessBuilder pb =
        new ProcessBuilder("bash", script.toAbsolutePath().toString())
            .redirectErrorStream(false) // keep stderr separate for readability
            .inheritIO(); // show output in Gradle test logs

    Process process = pb.start();
    boolean finished = process.waitFor(SCENARIO_TIMEOUT_MINUTES, TimeUnit.MINUTES);

    if (!finished) {
      process.destroyForcibly();
      throw new AssertionError(
          String.format(
              "Chaos scenario '%s' timed out after %d minutes.",
              script.getFileName(), SCENARIO_TIMEOUT_MINUTES));
    }

    return process.exitValue();
  }

  /**
   * Resolves the repository root by walking up from the working directory until we find a {@code
   * settings.gradle.kts} file. Falls back to the system property {@code chaos.repoRoot} if set.
   */
  private static Path resolveRepoRoot() {
    String override = System.getProperty("chaos.repoRoot");
    if (override != null && !override.isBlank()) {
      return Paths.get(override);
    }

    // When running via Gradle, the working directory is the subproject directory
    // (consolidation/). Walk up until we find the root marker.
    Path candidate = Paths.get("").toAbsolutePath();
    while (candidate != null) {
      if (Files.exists(candidate.resolve("settings.gradle.kts"))
          || Files.exists(candidate.resolve("settings.gradle"))) {
        return candidate;
      }
      candidate = candidate.getParent();
    }

    // Last resort: assume cwd is the repo root
    return Paths.get("").toAbsolutePath();
  }

  private static int parseIntOrZero(String s) {
    try {
      return Integer.parseInt(s.trim());
    } catch (NumberFormatException e) {
      return 0;
    }
  }
}
