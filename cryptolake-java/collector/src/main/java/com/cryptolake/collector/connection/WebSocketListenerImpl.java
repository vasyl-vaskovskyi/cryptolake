package com.cryptolake.collector.connection;

import com.cryptolake.common.logging.StructuredLogger;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Implements {@link WebSocket.Listener} for Binance combined-stream connections.
 *
 * <p>Accumulates multi-fragment text/binary frames via {@link FrameAccumulator} and dispatches the
 * complete UTF-8 string to the capture callback (Tier 5 D1, D2).
 *
 * <p>Backpressure: calls {@code ws.request(1)} only AFTER the capture callback has accepted the
 * frame (design §2.3; Tier 5 D2 — one-at-a-time pacing to match Python's cooperative {@code async
 * for}).
 *
 * <p>Non-data frames (subscription acks) are delivered to {@link SubscriptionHandshake.AckListener}
 * so the handshake can be completed.
 *
 * <p>Thread safety: JDK WebSocket serializes listener callbacks; accumulator is used from this
 * single thread.
 */
public final class WebSocketListenerImpl implements WebSocket.Listener {

  private static final StructuredLogger log = StructuredLogger.of(WebSocketListenerImpl.class);

  private final FrameAccumulator accumulator = new FrameAccumulator();
  private final String socketName;

  /** Called with complete frame text (socket name, frame). */
  private final BiConsumer<String, String> onFrame;

  /** Called on close or error. */
  private final Consumer<String> onClose;

  /** Receives subscription ack frames. */
  private final SubscriptionHandshake.AckListener ackListener;

  public WebSocketListenerImpl(
      String socketName,
      BiConsumer<String, String> onFrame,
      Consumer<String> onClose,
      SubscriptionHandshake.AckListener ackListener) {
    this.socketName = socketName;
    this.onFrame = onFrame;
    this.onClose = onClose;
    this.ackListener = ackListener;
  }

  @Override
  public void onOpen(WebSocket webSocket) {
    log.info("ws_opened", "socket", socketName);
    webSocket.request(1); // Start receiving one frame at a time (Tier 5 D2)
  }

  @Override
  public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
    accumulator.append(data);
    if (last) {
      String frame = accumulator.complete();
      dispatchFrame(webSocket, frame);
    }
    // request(1) called inside dispatchFrame after capture accepts the frame (Tier 5 D2)
    return null;
  }

  @Override
  public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
    accumulator.append(data);
    if (last) {
      String frame = accumulator.complete();
      dispatchFrame(webSocket, frame);
    }
    return null;
  }

  @Override
  public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
    webSocket.request(1);
    return null;
  }

  @Override
  public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
    log.info("ws_closed", "socket", socketName, "code", statusCode, "reason", reason);
    onClose.accept(socketName);
    return null;
  }

  @Override
  public void onError(WebSocket webSocket, Throwable error) {
    log.warn("ws_error", "socket", socketName, "error", error.getMessage());
    onClose.accept(socketName);
  }

  private void dispatchFrame(WebSocket webSocket, String frame) {
    // Check if this is a data frame (has "data": key) or a control frame (ack, etc.)
    if (frame.contains("\"data\":")) {
      onFrame.accept(socketName, frame);
    } else {
      // Subscription ack or other control frame
      if (ackListener != null) {
        ackListener.offer(frame);
      }
    }
    // Request next frame AFTER processing (Tier 5 D2 — one-at-a-time backpressure)
    webSocket.request(1);
  }
}
