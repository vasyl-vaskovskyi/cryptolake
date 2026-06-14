package com.cryptopanner.collector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * WebSocket client for Binance combined-streams. Skeleton-only: no reconnect, no rotation, no
 * back-pressure. Suitable for the smoke test against the Python mock.
 */
public final class BinanceWsClient {

  private final URI endpoint;
  private final List<String> streams;
  private final Consumer<String> onFrame;
  private final AtomicInteger nextId = new AtomicInteger(1);
  private volatile WebSocket ws;

  public BinanceWsClient(URI endpoint, List<String> streams, Consumer<String> onFrame) {
    this.endpoint = endpoint;
    this.streams = streams;
    this.onFrame = onFrame;
  }

  public void start() throws Exception {
    HttpClient http = HttpClient.newHttpClient();
    int subscribeId = nextId.getAndIncrement();
    CompletableFuture<Void> ackSeen = new CompletableFuture<>();

    WebSocket.Listener listener =
        new WebSocket.Listener() {
          private final StringBuilder buf = new StringBuilder();

          @Override
          public void onOpen(WebSocket webSocket) {
            ws = webSocket;
            String sub =
                "{\"method\":\"SUBSCRIBE\",\"params\":[\""
                    + String.join("\",\"", streams)
                    + "\"],\"id\":"
                    + subscribeId
                    + "}";
            ws.sendText(sub, true);
            ws.request(1);
          }

          @Override
          public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buf.append(data);
            if (last) {
              String full = buf.toString();
              buf.setLength(0);
              if (!ackSeen.isDone() && full.contains("\"id\":" + subscribeId)) {
                ackSeen.complete(null);
              } else {
                onFrame.accept(full);
              }
            }
            webSocket.request(1);
            return null;
          }
        };

    ws =
        http.newWebSocketBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .buildAsync(endpoint, listener)
            .get();
    ackSeen.orTimeout(10, TimeUnit.SECONDS).join();
  }

  public void stop() {
    WebSocket w = ws;
    if (w != null) {
      w.sendClose(WebSocket.NORMAL_CLOSURE, "bye").orTimeout(2, TimeUnit.SECONDS);
    }
  }
}
