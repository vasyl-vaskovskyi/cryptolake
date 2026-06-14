package com.cryptopanner.collector.testutil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Multi-session bare-bones WebSocket server for reconnect tests. Accepts clients in a loop until
 * {@link #close()} is called. Each session: RFC 6455 handshake → consume one masked SUBSCRIBE frame
 * (discarded) → send all script frames → close the socket from the server side.
 *
 * <p>Use {@link #sessionCount()} to assert how many reconnects occurred.
 */
public final class ReconnectingTinyWsServer implements AutoCloseable {

  private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

  private final ServerSocket serverSocket;
  private final List<String> script;
  private final ExecutorService exec;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicInteger sessions = new AtomicInteger(0);

  private ReconnectingTinyWsServer(ServerSocket serverSocket, List<String> script) {
    this.serverSocket = serverSocket;
    this.script = script;
    this.exec = Executors.newSingleThreadExecutor(r -> new Thread(r, "reconnecting-ws-accept"));
    exec.submit(this::acceptLoop);
  }

  public static ReconnectingTinyWsServer start(InetSocketAddress addr, List<String> script)
      throws IOException {
    ServerSocket s = new ServerSocket();
    s.setReuseAddress(true);
    s.bind(addr);
    return new ReconnectingTinyWsServer(s, script);
  }

  public int port() {
    return serverSocket.getLocalPort();
  }

  /** Number of fully-completed sessions (handshake + all script frames sent). */
  public int sessionCount() {
    return sessions.get();
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      exec.shutdownNow();
      serverSocket.close();
    }
  }

  // ── accept loop ──────────────────────────────────────────────────────────

  private void acceptLoop() {
    while (!closed.get()) {
      try {
        Socket sock = serverSocket.accept();
        handleSession(sock);
      } catch (IOException e) {
        if (!closed.get()) {
          // Unexpected error — log to stderr so test failures are diagnosable.
          System.err.println("[ReconnectingTinyWsServer] accept error: " + e.getMessage());
        }
        // Either closed or transient; loop will exit on next iteration if closed.
      }
    }
  }

  private void handleSession(Socket sock) {
    try (sock;
        InputStream in = sock.getInputStream();
        OutputStream out = sock.getOutputStream()) {

      // 1. Read the HTTP upgrade request and parse Sec-WebSocket-Key.
      byte[] buf = new byte[4096];
      int n = in.read(buf);
      if (n <= 0) return;
      String req = new String(buf, 0, n, StandardCharsets.US_ASCII);
      String key =
          req.lines()
              .filter(l -> l.toLowerCase().startsWith("sec-websocket-key:"))
              .map(l -> l.substring(l.indexOf(':') + 1).trim())
              .findFirst()
              .orElse(null);
      if (key == null) return;

      // 2. Send 101 Switching Protocols.
      MessageDigest sha = MessageDigest.getInstance("SHA-1");
      String accept =
          Base64.getEncoder()
              .encodeToString(sha.digest((key + GUID).getBytes(StandardCharsets.US_ASCII)));
      String resp =
          "HTTP/1.1 101 Switching Protocols\r\n"
              + "Upgrade: websocket\r\n"
              + "Connection: Upgrade\r\n"
              + "Sec-WebSocket-Accept: "
              + accept
              + "\r\n\r\n";
      out.write(resp.getBytes(StandardCharsets.US_ASCII));
      out.flush();

      // 3. Consume the client's SUBSCRIBE frame (one masked frame — discard payload).
      consumeMaskedFrame(in);

      // 4. Push all script frames to the client.
      for (String msg : script) {
        sendText(out, msg);
      }

      // 5. Count this as a completed session, then let the try-with-resources close the socket,
      //    which signals EOF to the client and triggers its onClose callback.
      sessions.incrementAndGet();

    } catch (Exception e) {
      if (!closed.get()) {
        System.err.println("[ReconnectingTinyWsServer] session error: " + e.getMessage());
      }
    }
  }

  // ── WebSocket frame helpers ───────────────────────────────────────────────

  /**
   * Reads and discards one complete masked WebSocket frame from the stream. Client frames are
   * always masked (RFC 6455 §5.1).
   */
  private static void consumeMaskedFrame(InputStream in) throws IOException {
    int b0 = in.read(); // FIN + opcode
    if (b0 < 0) return;
    int b1 = in.read(); // MASK bit + length
    if (b1 < 0) return;

    boolean masked = (b1 & 0x80) != 0;
    long payloadLen = b1 & 0x7F;

    if (payloadLen == 126) {
      payloadLen = ((in.read() & 0xFF) << 8) | (in.read() & 0xFF);
    } else if (payloadLen == 127) {
      payloadLen = 0;
      for (int i = 0; i < 8; i++) {
        payloadLen = (payloadLen << 8) | (in.read() & 0xFF);
      }
    }

    byte[] maskKey = new byte[4];
    if (masked) {
      int read = 0;
      while (read < 4) {
        int r = in.read(maskKey, read, 4 - read);
        if (r < 0) return;
        read += r;
      }
    }

    // Drain payload bytes.
    long remaining = payloadLen;
    byte[] drain = new byte[1024];
    while (remaining > 0) {
      int r = in.read(drain, 0, (int) Math.min(drain.length, remaining));
      if (r < 0) return;
      remaining -= r;
    }
  }

  /** Sends an unmasked text frame (server → client). */
  private static void sendText(OutputStream out, String s) throws IOException {
    byte[] payload = s.getBytes(StandardCharsets.UTF_8);
    out.write(0x81); // FIN + opcode=text
    if (payload.length < 126) {
      out.write(payload.length);
    } else if (payload.length < 65536) {
      out.write(126);
      out.write((payload.length >>> 8) & 0xFF);
      out.write(payload.length & 0xFF);
    } else {
      out.write(127);
      for (int i = 7; i >= 0; i--) {
        out.write((int) ((payload.length >>> (i * 8)) & 0xFF));
      }
    }
    out.write(payload);
    out.flush();
  }
}
