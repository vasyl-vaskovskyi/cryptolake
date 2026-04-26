package com.cryptolake.collector.connection;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Accumulates multi-fragment WebSocket text/binary frames into a single complete string.
 *
 * <p>Ports the fragment assembly logic implied by Tier 5 D1: Binance sends single-fragment text
 * frames in practice, but the spec requires correct fragment handling.
 *
 * <p>Thread safety: single-threaded — one instance per {@code WebSocketListenerImpl}; the JDK
 * WebSocket implementation serializes listener callbacks.
 */
public final class FrameAccumulator {

  private final StringBuilder textBuf = new StringBuilder();
  private final ByteArrayOutputStream binaryBuf = new ByteArrayOutputStream();
  private boolean binaryMode = false;

  /**
   * Appends a text fragment. Call for each {@code onText} callback until {@code last=true}, then
   * call {@link #complete()}.
   */
  public void append(CharSequence seg) {
    textBuf.append(seg);
    binaryMode = false;
  }

  /**
   * Appends a binary fragment. Call for each {@code onBinary} callback until {@code last=true},
   * then call {@link #complete()}.
   */
  public void append(ByteBuffer buf) {
    binaryMode = true;
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    binaryBuf.write(bytes, 0, bytes.length);
  }

  /**
   * Returns the complete frame as a UTF-8 string and resets internal state. Safe to call multiple
   * times; subsequent calls after reset return an empty string.
   *
   * <p>For binary frames, the accumulated bytes are decoded as UTF-8.
   */
  public String complete() {
    String result;
    if (binaryMode) {
      result = binaryBuf.toString(StandardCharsets.UTF_8);
    } else {
      result = textBuf.toString();
    }
    reset();
    return result;
  }

  /** Resets all accumulated data — call after {@link #complete()} or on error. */
  public void reset() {
    textBuf.setLength(0);
    binaryBuf.reset();
    binaryMode = false;
  }
}
