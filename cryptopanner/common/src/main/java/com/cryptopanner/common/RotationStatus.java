package com.cryptopanner.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * The Collector's published rotation state (master spec §11.c {@code /status.rotation}). The
 * Collector writes this file each tick; the Node Agent reads the active slot's file to populate the
 * {@code rotation} object in its aggregated {@code /status} (the two run in separate processes, so
 * the hand-off is file-based like the heartbeat and rotation-trigger channels).
 *
 * <p>{@code state} ∈ {@code IDLE | OVERLAP_VERIFYING | CUTOVER_PENDING}. {@code rotationId} and
 * {@code oldConnectionAgeS} are present only while a rotation is in flight.
 */
public record RotationStatus(
    String state, long currentConnectionAgeS, String rotationId, Long oldConnectionAgeS) {

  public static void write(Path file, ObjectMapper mapper, RotationStatus status)
      throws IOException {
    ObjectNode node = mapper.createObjectNode();
    node.put("state", status.state());
    node.put("current_connection_age_s", status.currentConnectionAgeS());
    if (status.rotationId() != null) {
      node.put("rotation_id", status.rotationId());
    }
    if (status.oldConnectionAgeS() != null) {
      node.put("old_connection_age_s", status.oldConnectionAgeS());
    }
    Path parent = file.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
    Files.write(tmp, (mapper.writeValueAsString(node)).getBytes(StandardCharsets.UTF_8));
    Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
  }

  public static Optional<RotationStatus> read(Path file, ObjectMapper mapper) throws IOException {
    if (!Files.exists(file)) {
      return Optional.empty();
    }
    JsonNode n = mapper.readTree(Files.readString(file));
    return Optional.of(
        new RotationStatus(
            n.path("state").asText("IDLE"),
            n.path("current_connection_age_s").asLong(0),
            n.hasNonNull("rotation_id") ? n.get("rotation_id").asText() : null,
            n.hasNonNull("old_connection_age_s") ? n.get("old_connection_age_s").asLong() : null));
  }
}
