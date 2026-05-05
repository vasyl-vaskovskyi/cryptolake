package com.cryptolake.writer.recovery;

import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.writer.rotate.FilePaths;
import com.cryptolake.writer.rotate.Sha256Sidecar;
import com.github.luben.zstd.ZstdDecompressCtx;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Startup utility that heals torn zstd frame tails in {@code *.jsonl.zst} archive files.
 *
 * <p>Used to recover from the on-disk damage caused by SIGKILL/OOM/segfault crashes that interrupt
 * a write mid-frame. The truncate path inside {@link
 * com.cryptolake.writer.io.DurableAppender#appendAndFsync} only runs when the JVM lives long enough
 * to handle the IOException — kernel-level kills bypass it. This scrubber picks up the pieces on
 * the next startup, before the consume loop reads or writes anything.
 *
 * <p>Algorithm: walk {@code baseDir} recursively for {@code *.jsonl.zst} files. For each file,
 * stream-decompress frame-by-frame using {@link ZstdDecompressCtx#decompressDirectByteBufferStream}
 * to find the last valid frame boundary. After each fully-consumed frame the source buffer's
 * position equals the offset of the next byte after the frame; that becomes the running {@code
 * lastGoodEnd}. If decompression of any byte after that fails (torn partial frame, garbage), we
 * truncate to {@code lastGoodEnd}, fsync, and recompute the sibling {@code .sha256} sidecar.
 *
 * <p>Files with a clean frame-aligned EOF are unchanged. Empty files are unchanged. Non-{@code
 * .jsonl.zst} files are ignored.
 *
 * <p>Spec: docs/superpowers/specs/2026-05-05-writer-hold-pause-tail-scrub-memcap-design.md.
 */
public final class ZstdTailScrubber {

  private static final StructuredLogger log = StructuredLogger.of(ZstdTailScrubber.class);
  private static final String ARCHIVE_SUFFIX = ".jsonl.zst";

  /**
   * Output buffer capacity used while walking frames. Frames are individually small (a single
   * compressed JSONL line, typically &lt; 1 KiB). 1 MiB is plenty of headroom and avoids any
   * accidental "destination too small" errors from the streaming decoder.
   */
  private static final int DECOMPRESS_DST_CAPACITY = 1 << 20;

  private ZstdTailScrubber() {}

  /**
   * Walks {@code baseDir} for archive files and heals any with torn tails. See class javadoc for
   * algorithm. Returns the number of files truncated.
   *
   * @param baseDir root archive directory; must exist
   * @return count of files whose tail was truncated
   * @throws IOException if {@code baseDir} cannot be walked
   */
  public static int scrub(Path baseDir) throws IOException {
    if (!Files.isDirectory(baseDir)) {
      return 0;
    }
    AtomicInteger healed = new AtomicInteger(0);
    try (Stream<Path> walk = Files.walk(baseDir)) {
      walk.filter(p -> p.getFileName().toString().endsWith(ARCHIVE_SUFFIX))
          .filter(Files::isRegularFile)
          .forEach(
              file -> {
                try {
                  if (scrubOne(file)) {
                    healed.incrementAndGet();
                  }
                } catch (NoSuchFileException nsfe) {
                  log.debug("scrub_file_disappeared", "path", file.toString());
                } catch (IOException e) {
                  log.warn("scrub_file_failed", "path", file.toString(), "error", e.getMessage());
                }
              });
    }
    return healed.get();
  }

  /** Heals one file. Returns true iff the file was truncated. */
  private static boolean scrubOne(Path file) throws IOException {
    long size = Files.size(file);
    if (size == 0) {
      return false;
    }
    byte[] buf = Files.readAllBytes(file);
    long lastGoodEndOffset = walkFrames(buf);
    if (lastGoodEndOffset == size) {
      return false; // healthy
    }
    // Truncate to last good frame boundary and fsync.
    try (FileChannel fc = FileChannel.open(file, StandardOpenOption.WRITE)) {
      fc.truncate(lastGoodEndOffset);
      fc.force(true);
    }
    // Recompute the sidecar so the .sha256 matches the truncated bytes.
    Path sidecar = FilePaths.sidecarPath(file);
    Sha256Sidecar.write(file, sidecar);
    log.info(
        "LIFECYCLE WRITER_STARTUP_TAIL_TRUNCATED: A previous unexpected exit left a torn zstd"
            + " frame at the tail of an archive file. The scrubber truncated to the last valid"
            + " frame boundary and recomputed the .sha256 sidecar.",
        "path",
        file.toString(),
        "bytes_dropped",
        size - lastGoodEndOffset,
        "last_good_offset",
        lastGoodEndOffset);
    return true;
  }

  /**
   * Walks zstd frames from offset 0 and returns the offset of the byte just past the end of the
   * last valid frame. If the file is entirely garbage from byte 0, returns 0.
   *
   * <p>Uses {@link ZstdDecompressCtx#decompressDirectByteBufferStream} which advances the source
   * buffer's position by the compressed frame size and returns {@code true} when a frame is fully
   * consumed. Any error or short read at the tail is treated as a torn frame: walking stops and
   * {@code lastGoodEnd} is the truncate point.
   */
  private static long walkFrames(byte[] buf) {
    if (buf.length == 0) {
      return 0;
    }
    ByteBuffer src = ByteBuffer.allocateDirect(buf.length);
    src.put(buf);
    src.flip();
    ByteBuffer dst = ByteBuffer.allocateDirect(DECOMPRESS_DST_CAPACITY);
    long lastGoodEnd = 0;
    try (ZstdDecompressCtx ctx = new ZstdDecompressCtx()) {
      while (src.hasRemaining()) {
        dst.clear();
        int posBefore = src.position();
        boolean frameComplete;
        try {
          frameComplete = ctx.decompressDirectByteBufferStream(dst, src);
        } catch (Throwable t) {
          // Malformed frame header / corrupt body — stop walking; lastGoodEnd is the truncate
          // point.
          break;
        }
        if (!frameComplete) {
          // Frame cannot be fully read from the remaining bytes — torn final frame.
          break;
        }
        if (src.position() == posBefore) {
          // Defensive: no progress despite "frame complete". Shouldn't happen for non-empty
          // frames; bail to avoid infinite loop.
          break;
        }
        lastGoodEnd = src.position();
        // Reset context for the next independent frame.
        ctx.reset();
      }
    }
    return lastGoodEnd;
  }
}
