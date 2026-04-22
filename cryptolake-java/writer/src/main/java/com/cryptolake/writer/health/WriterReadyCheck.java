package com.cryptolake.writer.health;

import com.cryptolake.common.health.ReadyCheck;
import com.cryptolake.writer.consumer.KafkaConsumerLoop;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer readiness check: verifies consumer assignment and storage writability.
 *
 * <p>Ports Python's {@code Writer._ready_checks} (design §2.11). Returns a {@link Map} with:
 *
 * <ul>
 *   <li>{@code "consumer_connected"} — {@code true} if at least one partition is assigned
 *   <li>{@code "storage_writable"} — {@code true} if the base archive directory is writable
 * </ul>
 *
 * <p>Thread safety: {@link KafkaConsumerLoop#isConnected()} reads a volatile field (safe from any
 * thread). Storage probe is a lightweight temp-file creation (design §11 Q4 preferred — no PG ping;
 * pool size 2 is sufficient for T1 + any future monitoring extension).
 */
public final class WriterReadyCheck implements ReadyCheck {

  private static final Logger log = LoggerFactory.getLogger(WriterReadyCheck.class);

  private final KafkaConsumerLoop loop;
  private final Path baseDir;

  public WriterReadyCheck(KafkaConsumerLoop loop, Path baseDir) {
    this.loop = loop;
    this.baseDir = baseDir;
  }

  /**
   * Returns readiness status.
   *
   * @return map with {@code "consumer_connected"} and {@code "storage_writable"} keys
   */
  @Override
  public Map<String, Boolean> get() {
    boolean connected = loop.isConnected();
    boolean writable = probeStorage();
    return Map.of("consumer_connected", connected, "storage_writable", writable);
  }

  private boolean probeStorage() {
    try {
      Path probe = baseDir.resolve(".writable_probe");
      Files.createDirectories(baseDir);
      Files.writeString(
          probe,
          "ok",
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING);
      Files.deleteIfExists(probe);
      return true;
    } catch (IOException e) {
      log.warn("storage_probe_failed", "base_dir", baseDir.toString(), "error", e.getMessage());
      return false;
    }
  }
}
