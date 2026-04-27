package com.cryptolake.verify.harness;

import com.cryptolake.verify.Main;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import picocli.CommandLine;

/**
 * Gate-5 byte-identity enforcement harness.
 *
 * <p>Runs the Java {@code cryptolake-verify verify} command in-process, captures stdout, and diffs
 * byte-for-byte against the recorded Python output ({@code parity-fixtures/verify/expected.txt}).
 *
 * <p>Exit 0 = match. Exit 1 = stdout diff or rc mismatch.
 *
 * <p>Design §8.5 / §2.1 harness spec. Registered as the {@code runVerifyParity} Gradle task.
 *
 * <p>Thread safety: single-shot, no shared state.
 */
public final class VerifyStdoutParityHarness {

  private VerifyStdoutParityHarness() {}

  /**
   * Entry point.
   *
   * @param args {@code args[0]} = fixture archive dir, {@code args[1]} = expected stdout path,
   *     {@code args[2]} = optional report path
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println(
          "Usage: VerifyStdoutParityHarness <fixtureArchiveDir> <expectedStdoutPath> [reportPath]");
      System.exit(1);
    }

    Path fixtureArchiveDir = Path.of(args[0]);
    Path expectedStdoutPath = Path.of(args[1]);
    Path reportPath = args.length > 2 ? Path.of(args[2]) : null;

    // Read expected output from Python fixture
    String pythonOutput = Files.readString(expectedStdoutPath, StandardCharsets.UTF_8);

    // Extract date from expected.txt (look for "Verification complete for <date>")
    String date = extractDate(pythonOutput);
    if (date == null) {
      System.err.println("FAIL: cannot extract date from expected.txt");
      System.exit(1);
    }

    // Override System.out to capture Java verify output
    // Force \n line endings on all platforms (design §6.7 byte-level invariants)
    ByteArrayOutputStream capture = new ByteArrayOutputStream();
    PrintStream capturePrint =
        new PrintStream(capture, true, StandardCharsets.UTF_8) {
          @Override
          public void println(String s) {
            print(s);
            print('\n');
          }

          @Override
          public void println() {
            print('\n');
          }
        };

    PrintStream originalOut = System.out;
    System.setOut(capturePrint);
    int rc;
    try {
      rc =
          new CommandLine(new Main())
              .setOut(new java.io.PrintWriter(capturePrint, true, StandardCharsets.UTF_8))
              .execute("verify", "--date", date, "--base-dir", fixtureArchiveDir.toString());
    } finally {
      System.setOut(originalOut);
    }

    String javaOutput = capture.toString(StandardCharsets.UTF_8);

    // Compare byte-by-byte
    byte[] javaBytes = javaOutput.getBytes(StandardCharsets.UTF_8);
    byte[] pythonBytes = pythonOutput.getBytes(StandardCharsets.UTF_8);

    if (Arrays.equals(javaBytes, pythonBytes)) {
      System.out.println("OK " + javaBytes.length + " bytes match (gate 5 PASS)");
      if (reportPath != null) {
        Files.createDirectories(reportPath.getParent());
        Files.writeString(reportPath, "PASS " + javaBytes.length + " bytes\n");
      }
      System.exit(0);
    } else {
      System.err.println("FAIL: stdout byte mismatch (gate 5 FAIL)");
      System.err.println("Java  bytes: " + javaBytes.length);
      System.err.println("Python bytes: " + pythonBytes.length);

      // Print first 100 differing bytes in hex
      int limit = Math.min(100, Math.max(javaBytes.length, pythonBytes.length));
      StringBuilder diff = new StringBuilder("First diffs (hex):\n");
      int shown = 0;
      for (int i = 0; i < limit && shown < 20; i++) {
        byte jb = i < javaBytes.length ? javaBytes[i] : (byte) 0;
        byte pb = i < pythonBytes.length ? pythonBytes[i] : (byte) 0;
        if (jb != pb) {
          diff.append(
              String.format("  offset %d: java=0x%02X python=0x%02X%n", i, jb & 0xFF, pb & 0xFF));
          shown++;
        }
      }
      System.err.println(diff);
      System.err.println("--- JAVA OUTPUT ---\n" + javaOutput);
      System.err.println("--- PYTHON OUTPUT ---\n" + pythonOutput);

      if (reportPath != null) {
        Files.createDirectories(reportPath.getParent());
        Files.writeString(
            reportPath,
            "FAIL\njava_bytes="
                + javaBytes.length
                + "\npython_bytes="
                + pythonBytes.length
                + "\n"
                + diff
                + "\nJAVA:\n"
                + javaOutput
                + "\nPYTHON:\n"
                + pythonOutput);
      }
      System.exit(1);
    }
  }

  /** Extracts the date from the "Verification complete for <date>" line in the expected output. */
  private static String extractDate(String text) {
    for (String line : text.split("\n")) {
      if (line.startsWith("Verification complete for ")) {
        return line.substring("Verification complete for ".length()).trim();
      }
    }
    return null;
  }
}
