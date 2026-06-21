package com.cryptopanner.common.deploy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Optional;

/**
 * Append-only deploy marker log at {@code deploy/history.jsonl} (design doc §4.4). Each promote
 * step appends a fsynced {@code {deploy_id, state, ts}} line before proceeding, so {@code resume}
 * can read the last recorded {@link DeployState} for a deploy and continue from there (§4.5).
 */
public final class DeployHistoryLog {

  private DeployHistoryLog() {}

  public static void append(
      Path historyFile, ObjectMapper mapper, String deployId, DeployState state, Instant ts)
      throws IOException {
    Path parent = historyFile.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    ObjectNode node = mapper.createObjectNode();
    node.put("deploy_id", deployId);
    node.put("state", state.name());
    node.put("ts", ts.toString());
    byte[] line;
    try {
      line = (mapper.writeValueAsString(node) + "\n").getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new IOException("deploy marker serialization failed", e);
    }
    try (FileChannel ch =
        FileChannel.open(
            historyFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND)) {
      ch.write(ByteBuffer.wrap(line));
      ch.force(true);
    }
  }

  /** The last marker recorded for {@code deployId}, or empty if the deploy has no markers. */
  public static Optional<DeployState> lastMarker(
      Path historyFile, ObjectMapper mapper, String deployId) throws IOException {
    if (!Files.exists(historyFile)) {
      return Optional.empty();
    }
    DeployState last = null;
    for (String line : Files.readAllLines(historyFile)) {
      if (line.isBlank()) {
        continue;
      }
      JsonNode node;
      try {
        node = mapper.readTree(line);
      } catch (Exception e) {
        continue;
      }
      if (node.path("deploy_id").asText("").equals(deployId)) {
        try {
          last = DeployState.valueOf(node.path("state").asText(""));
        } catch (IllegalArgumentException ignored) {
          // unknown marker name — skip
        }
      }
    }
    return Optional.ofNullable(last);
  }
}
