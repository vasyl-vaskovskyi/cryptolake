package com.cryptolake.verify.cli;

import com.cryptolake.verify.archive.ArchiveFile;
import com.cryptolake.verify.archive.ArchiveScanner;
import com.cryptolake.verify.archive.DecompressAndParse;
import com.cryptolake.verify.verify.ChecksumVerifier;
import com.cryptolake.verify.verify.DepthReplayVerifier;
import com.cryptolake.verify.verify.DuplicateOffsetChecker;
import com.cryptolake.verify.verify.EnvelopeVerifier;
import com.cryptolake.verify.verify.GapReporter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Gate-5 authoritative stdout source: verifies archive integrity for a date.
 *
 * <p>Ports {@code @cli.command() def verify(...)} from {@code verify.py:290-376}. The Java stdout
 * MUST be byte-identical to the Python output for the same archive set (Tier 3 §20; design §6.7).
 *
 * <p>Tier 5 K2 — all output via {@link System#out} ({@code println}). Tier 5 K3 — returns exit code
 * via {@link Integer} return value (never {@code System.exit} from inside the command).
 */
@Command(name = "verify", description = "Verify archive integrity for a date.")
public final class VerifyCommand implements java.util.concurrent.Callable<Integer> {

  @Option(names = "--date", required = true, description = "Date to verify (YYYY-MM-DD)")
  private String date;

  @Option(
      names = "--base-dir",
      defaultValue = "/data/archive",
      description = "Archive base directory")
  private String baseDir;

  @Option(names = "--exchange", description = "Filter by exchange")
  private String exchange;

  @Option(names = "--symbol", description = "Filter by symbol")
  private String symbol;

  @Option(names = "--stream", description = "Filter by stream")
  private String stream;

  @Option(names = "--full", description = "Full verification including cross-file dedup")
  private boolean full;

  @Option(names = "--repair-checksums", description = "Generate missing .sha256 sidecars")
  private boolean repairChecksums;

  private final ObjectMapper mapper;

  /** Default constructor for picocli (mapper injected from Main's shared instance). */
  public VerifyCommand() {
    this(com.cryptolake.common.envelope.EnvelopeCodec.newMapper());
  }

  /** Constructor for testing with a custom mapper. */
  public VerifyCommand(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public Integer call() throws IOException {
    Path base = Path.of(baseDir);
    List<String> allErrors = new ArrayList<>();
    List<JsonNode> allGaps = new ArrayList<>();
    List<JsonNode> allEnvelopes = new ArrayList<>();

    // Scan for archive files matching the date/exchange/symbol/stream filters
    List<ArchiveFile> files = ArchiveScanner.scan(base, date, exchange, symbol, stream);

    if (files.isEmpty()) {
      System.out.println("No archive files found for date " + date + " in " + baseDir);
      return 0;
    }

    for (ArchiveFile archiveFile : files) {
      Path dataPath = archiveFile.path();
      Path relPath = base.relativize(dataPath);

      // Line 2: per-file header
      System.out.println("Verifying: " + relPath);

      // Sidecar path: dataPath + ".sha256"
      Path sidecarPath = Path.of(dataPath + ".sha256");

      // --repair-checksums: write missing sidecar
      if (repairChecksums && !java.nio.file.Files.exists(sidecarPath)) {
        writeSidecar(dataPath, sidecarPath);
        System.out.println("  Repaired: " + sidecarPath.getFileName());
      }

      // Checksum verification
      allErrors.addAll(ChecksumVerifier.verify(dataPath, sidecarPath));

      // Decompress and parse
      List<JsonNode> envelopes;
      try {
        envelopes = DecompressAndParse.parse(dataPath, mapper);
      } catch (Exception e) {
        allErrors.add("Decompression failed: " + dataPath + " - " + e.getMessage());
        continue;
      }

      allErrors.addAll(EnvelopeVerifier.verify(envelopes));
      allErrors.addAll(DuplicateOffsetChecker.check(envelopes));
      allGaps.addAll(GapReporter.collect(envelopes));

      if (full) {
        allEnvelopes.addAll(envelopes);
      }
    }

    // Cross-file checks when --full is specified
    if (full && !allEnvelopes.isEmpty()) {
      allErrors.addAll(DuplicateOffsetChecker.check(allEnvelopes));
    }

    if (full && !allEnvelopes.isEmpty()) {
      List<JsonNode> depthEnvs = new ArrayList<>();
      List<JsonNode> snapEnvs = new ArrayList<>();
      List<JsonNode> gapEnvs = new ArrayList<>();

      for (JsonNode e : allEnvelopes) {
        String streamType = e.path("stream").asText("");
        String type = e.path("type").asText("");
        if ("depth".equals(streamType) && "data".equals(type)) {
          depthEnvs.add(e);
        } else if ("depth_snapshot".equals(streamType) && "data".equals(type)) {
          snapEnvs.add(e);
        } else if ("gap".equals(type)) {
          gapEnvs.add(e);
        }
      }
      allErrors.addAll(DepthReplayVerifier.verify(depthEnvs, snapEnvs, gapEnvs, mapper));
    }

    // Final report (byte-identical to Python — §6.7 line templates 13-21)
    System.out.println();
    System.out.println("=".repeat(60));
    System.out.println("Verification complete for " + date);
    System.out.println("Files checked: " + files.size());

    if (!allErrors.isEmpty()) {
      System.out.println();
      System.out.println("ERRORS (" + allErrors.size() + "):");
      for (String err : allErrors) {
        System.out.println("  - " + err);
      }
    } else {
      System.out.println("Errors: 0");
    }

    if (!allGaps.isEmpty()) {
      System.out.println();
      System.out.println("GAPS (" + allGaps.size() + "):");
      for (JsonNode gap : allGaps) {
        String sym = gap.path("symbol").asText("");
        String str = gap.path("stream").asText("");
        String reason = gap.path("reason").asText("");
        String detail = gap.path("detail").asText("");
        System.out.println("  - " + sym + "/" + str + ": " + reason + " (" + detail + ")");
      }
    } else {
      System.out.println("Gaps: 0");
    }

    return allErrors.isEmpty() ? 0 : 1;
  }

  /**
   * Writes a SHA-256 sidecar file for the given data path.
   *
   * <p>Format: {@code "{hex} {filename}\n"} (two spaces, matching sha256sum(1) convention). Mirrors
   * writer's Sha256Sidecar.write without introducing a cross-module dependency (Tier 5 I6).
   */
  private static void writeSidecar(Path dataPath, Path sidecarPath) throws IOException {
    String hex = com.cryptolake.common.util.Sha256.hexFile(dataPath);
    String content = hex + "  " + dataPath.getFileName() + "\n";
    java.nio.file.Files.writeString(sidecarPath, content);
  }
}
