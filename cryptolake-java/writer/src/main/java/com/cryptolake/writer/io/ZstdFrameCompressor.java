package com.cryptolake.writer.io;

import com.github.luben.zstd.Zstd;
import java.util.List;

/**
 * Compresses a list of line bytes into a single independent zstd frame.
 *
 * <p>Ports Python's {@code compressor.py:ZstdFrameCompressor} (design §4.4; Tier 5 I1).
 *
 * <p>Produces ONE independent zstd frame per call. Concatenation of independent frames is a valid
 * zstd stream (zstd spec) — the crash-safety property (truncate at any frame boundary on recovery)
 * depends on this. Never uses {@link com.github.luben.zstd.ZstdOutputStream} for per-batch
 * compression (that produces a single frame for the whole file).
 *
 * <p>Thread safety: {@link Zstd#compress(byte[], int)} is pure and stateless; this class holds no
 * mutable state. Only one caller anyway (consume-loop T1).
 */
public final class ZstdFrameCompressor {

  private final int level;

  /**
   * Constructs a compressor with the given compression level.
   *
   * @param level zstd compression level (default 3 per {@link
   *     com.cryptolake.common.config.WriterConfig})
   */
  public ZstdFrameCompressor(int level) {
    this.level = level;
  }

  /**
   * Joins all lines into a single byte array and compresses as one independent frame (Tier 5 I1).
   *
   * <p>Ports {@code ZstdFrameCompressor.compress_frame(lines)}.
   *
   * @param lines list of line bytes (each already includes the trailing {@code 0x0A})
   * @return compressed bytes forming one independent zstd frame
   */
  public byte[] compressFrame(List<byte[]> lines) {
    int total = 0;
    for (byte[] line : lines) {
      total += line.length;
    }
    byte[] joined = new byte[total];
    int off = 0;
    for (byte[] line : lines) {
      System.arraycopy(line, 0, joined, off, line.length);
      off += line.length;
    }
    return Zstd.compress(joined, level);
  }
}
