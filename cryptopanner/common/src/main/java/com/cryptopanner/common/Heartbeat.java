package com.cryptopanner.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Heartbeat liveness file (master spec §11.b). Each running component touches its file every 5s on
 * its main loop; the Node Agent reads the mtime to derive {@link com.cryptopanner.agent
 * ComponentState}. Kept dependency-free so every component can call {@link #touch}.
 */
public final class Heartbeat {

  private Heartbeat() {}

  /** Creates the heartbeat file if needed and sets its mtime to now. */
  public static void touch(Path file) throws IOException {
    if (!Files.exists(file)) {
      Path parent = file.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      Files.createFile(file);
    }
    Files.setLastModifiedTime(file, FileTime.from(Instant.now()));
  }

  /** Time since the file was last touched, or empty if the file does not exist. */
  public static Optional<Duration> age(Path file, Instant now) throws IOException {
    if (!Files.exists(file)) {
      return Optional.empty();
    }
    Instant mtime = Files.getLastModifiedTime(file).toInstant();
    return Optional.of(Duration.between(mtime, now));
  }
}
