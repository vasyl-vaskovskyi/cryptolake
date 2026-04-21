package com.cryptolake.writer.recovery;

import com.cryptolake.writer.state.FileStateRecord;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Startup scan of {@code baseDir} for {@code .jsonl.zst} files; pairs each with its {@code
 * .sha256} sidecar; identifies "sealed" vs "uncommitted (missing sidecar)".
 *
 * <p>Ports Python's {@code WriterConsumer._discover_sealed_files} and truncation logic (design
 * §2.10; Tier 5 I7).
 *
 * <p>Uses {@link Files#walk(Path, java.nio.file.FileVisitOption...)} with try-with-resources (Tier
 * 5 I7 — lazy stream, holds directory handle). Not thread-safe; called once at startup.
 */
public final class SealedFileIndex {

  private static final Logger log = LoggerFactory.getLogger(SealedFileIndex.class);

  private final Path baseDir;
  private Set<Path> sealed = new HashSet<>();

  /**
   * Constructs a {@link SealedFileIndex} rooted at {@code baseDir}.
   *
   * @param baseDir base archive directory
   */
  public SealedFileIndex(Path baseDir) {
    this.baseDir = baseDir;
  }

  /**
   * Scans the base directory and reconciles against PG state.
   *
   * <p>For each {@code .jsonl.zst} file found:
   * <ul>
   *   <li>If a corresponding {@code .sha256} sidecar exists → "sealed" (committed to archive).
   *   <li>If no sidecar → "uncommitted"; delete the file (not yet safe to commit from).
   * </ul>
   *
   * <p>Also truncates files whose on-disk size exceeds the PG-recorded {@code file_byte_size}
   * (crash-recovery write incomplete — Tier 5 I3 watch-out).
   *
   * @param pgState PG file states keyed by {@link TopicPartition}
   * @return a {@link ScanResult} containing sealed paths and housekeeping info
   */
  public ScanResult scanAndReconcile(Map<TopicPartition, List<FileStateRecord>> pgState) {
    if (!Files.exists(baseDir)) {
      log.info("sealed_file_index_base_dir_missing", "base_dir", baseDir.toString());
      this.sealed = new HashSet<>();
      return new ScanResult(Set.of(), List.of(), List.of());
    }

    // Build PG size lookup: filePath → recordedByteSize
    Map<String, Long> pgSizes = new HashMap<>();
    for (List<FileStateRecord> records : pgState.values()) {
      for (FileStateRecord r : records) {
        pgSizes.put(r.filePath(), r.fileByteSize());
      }
    }

    Set<Path> sealedPaths = new HashSet<>();
    List<Path> deletedUncommitted = new ArrayList<>();
    List<Path> truncated = new ArrayList<>();

    // Walk baseDir recursively (Tier 5 I7)
    try (Stream<Path> walk = Files.walk(baseDir)) {
      walk.filter(p -> p.toString().endsWith(".jsonl.zst"))
          .forEach(
              zstPath -> {
                Path sidecar = Path.of(zstPath + ".sha256");
                if (Files.exists(sidecar)) {
                  sealedPaths.add(zstPath);
                  // Check for truncation need (Tier 5 I3 watch-out)
                  Long pgSize = pgSizes.get(zstPath.toString());
                  if (pgSize != null) {
                    try {
                      long fsSize = Files.size(zstPath);
                      if (fsSize > pgSize) {
                        log.warn(
                            "truncating_oversized_file",
                            "path",
                            zstPath.toString(),
                            "fs_size",
                            fsSize,
                            "pg_size",
                            pgSize);
                        try (var fc =
                            java.nio.channels.FileChannel.open(
                                zstPath, java.nio.file.StandardOpenOption.WRITE)) {
                          fc.truncate(pgSize);
                          fc.force(true);
                        }
                        truncated.add(zstPath);
                      }
                    } catch (IOException e) {
                      log.warn(
                          "truncate_check_failed", "path", zstPath.toString(), "error", e.getMessage());
                    }
                  }
                } else {
                  // No sidecar → uncommitted; delete (Tier 1 §6 — do not reconstruct)
                  try {
                    Files.deleteIfExists(zstPath);
                    deletedUncommitted.add(zstPath);
                    log.info("deleted_uncommitted_file", "path", zstPath.toString());
                  } catch (IOException e) {
                    log.warn(
                        "delete_uncommitted_failed",
                        "path",
                        zstPath.toString(),
                        "error",
                        e.getMessage());
                  }
                }
              });
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to scan base directory: " + baseDir, e);
    }

    this.sealed = Collections.unmodifiableSet(sealedPaths);
    return new ScanResult(sealed, deletedUncommitted, truncated);
  }

  /** Returns the set of sealed files (populated after {@link #scanAndReconcile}). */
  public Set<Path> sealedFiles() {
    return sealed;
  }

  /**
   * Result of a {@link SealedFileIndex#scanAndReconcile(Map)} call.
   *
   * @param sealed paths with sidecars (committed)
   * @param deletedUncommitted paths deleted because they lacked sidecars
   * @param truncated paths truncated to PG-recorded size
   */
  public record ScanResult(Set<Path> sealed, List<Path> deletedUncommitted, List<Path> truncated) {}
}
