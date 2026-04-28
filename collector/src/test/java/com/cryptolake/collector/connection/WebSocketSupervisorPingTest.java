package com.cryptolake.collector.connection;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.collector.metrics.CollectorMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link WebSocketSupervisor#pingLoop}.
 *
 * <p>Verifies Bug A fix: when {@code sendPing} throws, the supervisor calls {@code ws.abort()} and
 * releases the {@code disconnectLatch} so the main connection loop can exit and reconnect.
 */
class WebSocketSupervisorPingTest {

  /**
   * Minimal {@link WebSocket} stub. Every method returns sensible defaults; only {@link
   * #sendPing(ByteBuffer)} and {@link #abort()} are instrumented.
   */
  private static final class FailingPingWebSocket implements WebSocket {

    final AtomicBoolean abortCalled = new AtomicBoolean(false);
    private final RuntimeException pingException;

    FailingPingWebSocket(RuntimeException pingException) {
      this.pingException = pingException;
    }

    @Override
    public CompletableFuture<WebSocket> sendPing(ByteBuffer message) {
      CompletableFuture<WebSocket> future = new CompletableFuture<>();
      future.completeExceptionally(pingException);
      return future;
    }

    @Override
    public void abort() {
      abortCalled.set(true);
    }

    // ── Unused stubs ──────────────────────────────────────────────────────────

    @Override
    public CompletableFuture<WebSocket> sendText(CharSequence data, boolean last) {
      return CompletableFuture.completedFuture(this);
    }

    @Override
    public CompletableFuture<WebSocket> sendBinary(ByteBuffer data, boolean last) {
      return CompletableFuture.completedFuture(this);
    }

    @Override
    public CompletableFuture<WebSocket> sendPong(ByteBuffer message) {
      return CompletableFuture.completedFuture(this);
    }

    @Override
    public CompletableFuture<WebSocket> sendClose(int statusCode, String reason) {
      return CompletableFuture.completedFuture(this);
    }

    @Override
    public void request(long n) {}

    @Override
    public String getSubprotocol() {
      return "";
    }

    @Override
    public boolean isOutputClosed() {
      return false;
    }

    @Override
    public boolean isInputClosed() {
      return false;
    }
  }

  /**
   * Creates a minimal {@link WebSocketSupervisor} instance sufficient to call pingLoop.
   *
   * <p>Uses a 1-second ping interval (instead of the production 30s) so the test completes in
   * milliseconds rather than a full minute.
   */
  private WebSocketSupervisor buildSupervisor() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    CollectorMetrics metrics = new CollectorMetrics(registry);
    CountDownLatch globalStop = new CountDownLatch(1);
    return new WebSocketSupervisor(
        HttpClient.newHttpClient(),
        null, // adapter — not used by pingLoop
        null, // capture — not used by pingLoop
        null, // depthResync — not used by pingLoop
        List.of(),
        List.of(),
        metrics,
        null, // mapper — not used by pingLoop
        globalStop,
        null, // virtualExec — not used by pingLoop
        "binance",
        1L); // 1-second ping interval for fast tests
  }

  /**
   * Bug A fix: when {@code sendPing} throws, the disconnectLatch must be released so the main
   * connection loop can exit its {@code await()} and trigger a reconnect.
   */
  @Test
  void pingFailure_releasesDisconnectLatch() throws InterruptedException {
    WebSocketSupervisor supervisor = buildSupervisor();
    FailingPingWebSocket ws =
        new FailingPingWebSocket(new RuntimeException("Output closed (simulated)"));

    CountDownLatch disconnectLatch = new CountDownLatch(1);

    // Run pingLoop on a virtual thread; it should return quickly after the first ping fails.
    Thread.ofVirtual()
        .start(
            () -> {
              // Seed the latch so the first await(30s) returns immediately (latch not at 0 yet,
              // but we immediately send a ping because we start the while(true) loop).
              // The ping will fail → branch to abort + countDown.
              supervisor.pingLoop(ws, disconnectLatch);
            });

    // The latch should be released within 4 seconds (1s ping interval + processing).
    boolean released = disconnectLatch.await(4, TimeUnit.SECONDS);
    assertThat(released).as("disconnectLatch must be released on ping failure").isTrue();
  }

  /**
   * Bug A fix: when {@code sendPing} throws, {@code ws.abort()} must be called to force-close the
   * half-open connection.
   */
  @Test
  void pingFailure_callsAbort() throws InterruptedException {
    WebSocketSupervisor supervisor = buildSupervisor();
    FailingPingWebSocket ws =
        new FailingPingWebSocket(new RuntimeException("Output closed (simulated)"));

    CountDownLatch disconnectLatch = new CountDownLatch(1);
    CountDownLatch loopDone = new CountDownLatch(1);

    Thread.ofVirtual()
        .start(
            () -> {
              supervisor.pingLoop(ws, disconnectLatch);
              loopDone.countDown();
            });

    // Wait for pingLoop to finish (1s ping interval + processing buffer).
    boolean done = loopDone.await(4, TimeUnit.SECONDS);
    assertThat(done).as("pingLoop must exit after ping failure").isTrue();
    assertThat(ws.abortCalled.get()).as("ws.abort() must be called on ping failure").isTrue();
  }
}
