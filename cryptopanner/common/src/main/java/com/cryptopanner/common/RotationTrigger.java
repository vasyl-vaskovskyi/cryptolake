package com.cryptopanner.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * File-based trigger channel for an operator-forced WS rotation (design doc §5.4). The Node Agent
 * runs in a separate process from the Collector, so {@code POST /rotation/trigger} cannot call the
 * in-process {@link java.util.function.Supplier} directly: the agent {@link #request}s by dropping
 * a small trigger file under {@code deploy/}, and the Collector's per-minute scheduler {@link
 * #consume}s it (one-shot) and runs {@code rotate(OPERATOR_TRIGGERED)}. Matches the project's
 * file-based IPC convention (active-slot, rotations.jsonl, lifecycle ledger).
 */
public final class RotationTrigger {

  private static final String DEFAULT_REASON = "OPERATOR_TRIGGERED";

  private RotationTrigger() {}

  /** Agent side: drop a trigger carrying {@code reason}. Overwrites any pending trigger. */
  public static void request(Path triggerFile, String reason) throws IOException {
    Path parent = triggerFile.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    Files.writeString(triggerFile, reason == null ? DEFAULT_REASON : reason);
  }

  /**
   * Collector side: if a trigger is present, return its reason and delete it (one-shot); otherwise
   * empty. A present-but-blank trigger falls back to {@code OPERATOR_TRIGGERED}.
   */
  public static Optional<String> consume(Path triggerFile) throws IOException {
    if (!Files.exists(triggerFile)) {
      return Optional.empty();
    }
    String reason = Files.readString(triggerFile).trim();
    Files.deleteIfExists(triggerFile);
    return Optional.of(reason.isEmpty() ? DEFAULT_REASON : reason);
  }
}
