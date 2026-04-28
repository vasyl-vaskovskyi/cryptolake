package com.cryptolake.verify;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.util.Sha256;
import com.cryptolake.verify.verify.ChecksumVerifier;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** ports: tests/unit/cli/test_verify.py::TestChecksumVerification */
class ChecksumVerifierTest {

  @TempDir Path tmpDir;

  @Test
  void validChecksumProducesNoErrors() throws IOException {
    // ports: tests/unit/cli/test_verify.py::TestChecksumVerification::test_valid_checksum
    Path dataFile = tmpDir.resolve("hour-0.jsonl.zst");
    Files.writeString(dataFile, "test content", StandardCharsets.UTF_8);

    String hex = Sha256.hexFile(dataFile);
    Path sidecar = tmpDir.resolve("hour-0.jsonl.zst.sha256");
    Files.writeString(sidecar, hex + "  hour-0.jsonl.zst\n");

    List<String> errors = ChecksumVerifier.verify(dataFile, sidecar);
    assertThat(errors).isEmpty();
  }

  @Test
  void corruptedFileDetected() throws IOException {
    // ports: tests/unit/cli/test_verify.py::TestChecksumVerification::test_corrupted_file
    Path dataFile = tmpDir.resolve("hour-0.jsonl.zst");
    Files.writeString(dataFile, "test content", StandardCharsets.UTF_8);

    Path sidecar = tmpDir.resolve("hour-0.jsonl.zst.sha256");
    Files.writeString(sidecar, "deadbeefdeadbeef  hour-0.jsonl.zst\n");

    List<String> errors = ChecksumVerifier.verify(dataFile, sidecar);
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0)).contains("Checksum mismatch");
  }

  @Test
  void missingSidecarReported() throws IOException {
    // ports: tests/unit/cli/test_verify.py::TestChecksumVerification::test_missing_sidecar
    Path dataFile = tmpDir.resolve("hour-0.jsonl.zst");
    Files.writeString(dataFile, "test content", StandardCharsets.UTF_8);
    Path sidecar = tmpDir.resolve("hour-0.jsonl.zst.sha256"); // does not exist

    List<String> errors = ChecksumVerifier.verify(dataFile, sidecar);
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0)).contains("Sidecar not found");
  }
}
