package com.cryptolake.collector.connection;

import com.cryptolake.common.logging.StructuredLogger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements the post-connect SUBSCRIBE + ack protocol that defeats Binance's silent-subscription-
 * drop bug (design §2.3; Python reference: {@code connection.py:_subscribe_and_ack}).
 *
 * <p>Sends a {@code {"method":"SUBSCRIBE","params":[...],"id":N}} message and blocks until Binance
 * acks {@code {"result":null,"id":N}}. Without this handshake, ~50% of cold boots see Binance
 * silently fail to wire some subscriptions to the per-stream publisher.
 *
 * <p>Thread safety: invoked once per {@code WebSocketClient.connect()}, on the virtual thread that
 * owns the connection.
 */
public final class SubscriptionHandshake {

  private static final StructuredLogger log = StructuredLogger.of(SubscriptionHandshake.class);

  private static final Duration ACK_TIMEOUT = Duration.ofSeconds(5);
  private static final AtomicInteger ID_COUNTER = new AtomicInteger(1);

  private final ObjectMapper mapper;

  public SubscriptionHandshake(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  /**
   * Sends a SUBSCRIBE message and waits for the ack. Blocks on the calling virtual thread.
   *
   * @param ws the WebSocket to send on
   * @param subscriptions the subscription strings (e.g. {@code ["btcusdt@aggTrade", ...]})
   * @throws ConnectionException if the ack does not arrive within 5s or is negative
   */
  public void subscribe(WebSocket ws, List<String> subscriptions, AckListener ackListener)
      throws ConnectionException, InterruptedException {

    int id = ID_COUNTER.getAndIncrement();
    String json = buildSubscribeMessage(subscriptions, id);

    log.info("sending_subscribe", "id", id, "count", subscriptions.size());
    ws.sendText(json, true).join();

    // Wait for ack — the listener will call our callback when it sees the matching id
    if (!ackListener.waitForAck(id, ACK_TIMEOUT)) {
      throw new ConnectionException("SUBSCRIBE ack timed out for id=" + id);
    }
    log.info("subscribe_ack_received", "id", id);
  }

  private String buildSubscribeMessage(List<String> subscriptions, int id) {
    try {
      return mapper
          .createObjectNode()
          .put("method", "SUBSCRIBE")
          .putPOJO("params", subscriptions)
          .put("id", id)
          .toString();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to build subscribe message", e);
    }
  }

  /** Interface for the WebSocket listener to deliver ack frames back to a waiting handshake. */
  public interface AckListener {
    /**
     * Offers an ack frame text to the listener. Called by {@code WebSocketListenerImpl} when a
     * non-data frame arrives.
     */
    void offer(String frameText);

    /**
     * Blocks until an ack for {@code id} arrives, or the timeout expires.
     *
     * @return {@code true} if ack arrived; {@code false} on timeout
     */
    boolean waitForAck(int id, Duration timeout) throws InterruptedException;
  }

  /** Default {@link AckListener} implementation using a blocking queue. */
  public static final class QueueAckListener implements AckListener {

    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(64);
    private final ObjectMapper mapper;

    public QueueAckListener(ObjectMapper mapper) {
      this.mapper = mapper;
    }

    @Override
    public void offer(String frameText) {
      queue.offer(frameText); // non-blocking; drop on queue-full (listener thread must not block)
    }

    @Override
    public boolean waitForAck(int id, Duration timeout) throws InterruptedException {
      long deadlineNs = System.nanoTime() + timeout.toNanos();
      while (true) {
        long remainingNs = deadlineNs - System.nanoTime();
        if (remainingNs <= 0) return false;
        String frame = queue.poll(remainingNs, TimeUnit.NANOSECONDS);
        if (frame == null) return false; // timeout
        try {
          JsonNode node = mapper.readTree(frame);
          JsonNode idNode = node.get("id");
          if (idNode != null && idNode.asInt() == id) {
            JsonNode result = node.get("result");
            if (result == null || result.isNull()) {
              return true; // success ack
            }
            throw new RuntimeException("Negative ack from Binance: " + frame);
          }
          // Different id — put it back? No — we only expect one ack at a time
        } catch (IOException e) {
          // Non-JSON frame — skip
        }
      }
    }
  }

  /** Exception thrown when the SUBSCRIBE ack is not received within the timeout. */
  public static final class ConnectionException extends Exception {
    public ConnectionException(String message) {
      super(message);
    }
  }
}
