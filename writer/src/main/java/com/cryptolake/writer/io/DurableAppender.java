package com.cryptolake.writer.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Appends compressed bytes to a file, fsyncing data + metadata before returning (Tier 5 I3).
 *
 * <p>On partial-write failure, truncates back to the pre-write position and fsyncs again (Tier 5
 * I4). This ensures the archive is always a valid sequence of complete zstd frames — the crash
 * recovery path reads {@code file.length()} and truncates; metadata must be durable.
 *
 * <p>Ports Python's {@code consumer.py:1218-1230} write+fsync+truncate pattern.
 *
 * <p>Thread safety: one file at a time per call; no shared state. Called only from T1 consume
 * thread.
 */
public final class DurableAppender {

  /**
   * Appends {@code payload} to {@code path}, creating the file if it does not exist, then fsyncs
   * data + metadata via {@link FileChannel#force(boolean) force(true)}.
   *
   * <p>On {@link IOException} during write: truncates to pre-write position and fsyncs again, then
   * rethrows (Tier 5 I4). The caller (OffsetCommitCoordinator) catches this exception.
   *
   * @param path target archive file path
   * @param payload compressed frame bytes to append
   * @throws IOException if write or fsync fails (after truncation attempt)
   */
  public void appendAndFsync(Path path, byte[] payload) throws IOException {
    // Ensure parent directories exist
    Files.createDirectories(path.getParent());
    try (FileChannel fc =
        FileChannel.open(path, StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
      long posBefore = fc.size(); // use size() since APPEND mode positions at end
      try {
        fc.write(ByteBuffer.wrap(payload));
        fc.force(true); // fsync data + metadata (Tier 5 I3 — force(true) not force(false))
      } catch (IOException e) {
        // Partial-write truncate (Tier 5 I4)
        try {
          fc.truncate(posBefore);
          fc.force(true);
        } catch (IOException ignored) {
          // best-effort truncation; original exception propagates
        }
        throw e;
      }
    }
  }

  /**
   * Returns the expected file size after appending {@code payloadLen} bytes to {@code path}.
   *
   * <p>Used by {@link com.cryptolake.writer.consumer.OffsetCommitCoordinator} when building {@link
   * com.cryptolake.writer.state.FileStateRecord} for PG.
   *
   * @param path archive file path (may not exist yet)
   * @param payloadLen number of bytes that will be appended
   * @return current file size + payloadLen
   * @throws IOException if file size cannot be read
   */
  public long sizeAfter(Path path, int payloadLen) throws IOException {
    long existing = Files.exists(path) ? Files.size(path) : 0L;
    return existing + payloadLen;
  }
}
