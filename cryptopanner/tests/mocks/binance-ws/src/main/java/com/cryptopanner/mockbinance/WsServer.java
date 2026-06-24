package com.cryptopanner.mockbinance;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Multi-connection WebSocket server backed by raw sockets (no Jetty / Undertow). RFC 6455 handshake
 * → consume the client's first text frame as a SUBSCRIBE → reply with {@code
 * {"result":null,"id":<id>}} → register the client with a shared {@link FanoutBroadcaster}. A
 * single replay loop loops the fixture at {@code replayRateHz}, restamps event time once per frame,
 * and fans the byte-identical frame to every connected client — so a rotation primary and shadow
 * see the same overlap minute and the equivalence check passes (§14.c).
 */
public final class WsServer implements AutoCloseable {

  private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

  private final int port;
  private final List<String> fixtureLines;
  private final double replayRateHz;
  private final boolean rewriteEventTime;
  private final ServerSocket server;
  private final ExecutorService exec = Executors.newCachedThreadPool();
  private final FanoutBroadcaster broadcaster = new FanoutBroadcaster();
  private volatile boolean closed;

  public WsServer(int port, List<String> fixtureLines, double replayRateHz) throws IOException {
    this(port, fixtureLines, replayRateHz, false);
  }

  /**
   * @param rewriteEventTime when true, each replayed frame's {@code E}/{@code T} timestamps are
   *     restamped to the send instant (see {@link EventTimeRewriter}) so frames fill consecutive
   *     real-time minutes; when false, frames replay verbatim with their fixture timestamps.
   */
  public WsServer(
      int port, List<String> fixtureLines, double replayRateHz, boolean rewriteEventTime)
      throws IOException {
    this.port = port;
    this.fixtureLines = fixtureLines;
    this.replayRateHz = replayRateHz;
    this.rewriteEventTime = rewriteEventTime;
    this.server = new ServerSocket(port);
    exec.submit(this::acceptLoop);
    exec.submit(this::replayLoop);
  }

  public int port() {
    return server.getLocalPort();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    server.close();
    exec.shutdownNow();
  }

  private void acceptLoop() {
    while (!closed) {
      try {
        Socket sock = server.accept();
        exec.submit(() -> handle(sock));
      } catch (IOException e) {
        if (!closed) {
          System.err.println("[mock-ws] accept failed: " + e.getMessage());
        }
        return;
      }
    }
  }

  /**
   * Per-connection handler: handshake → consume the SUBSCRIBE → ack → register the client's output
   * with the shared {@link FanoutBroadcaster}. The single {@link #replayLoop()} thread does all
   * sending, so every connected client (e.g. a rotation primary + shadow) receives byte-identical
   * frames. This thread then just drains the client's inbound bytes to detect disconnect.
   */
  private void handle(Socket sock) {
    System.out.println("[mock-ws] client connected: " + sock.getRemoteSocketAddress());
    try (sock;
        InputStream in = sock.getInputStream()) {
      OutputStream out = sock.getOutputStream();
      handshake(in, out);
      String subscribe = readTextFrame(in);
      if (subscribe == null) {
        System.err.println("[mock-ws] no SUBSCRIBE received; closing");
        return;
      }
      Integer subId = extractIntField(subscribe, "id");
      String ack = "{\"result\":null,\"id\":" + (subId == null ? "null" : subId) + "}";
      sendText(out, ack);
      broadcaster.register(out);
      System.out.println("[mock-ws] client subscribed; clients=" + broadcaster.size());
      try {
        // Drain inbound bytes (pings/etc.) until the client disconnects (read returns -1).
        byte[] discard = new byte[1024];
        while (!closed && in.read(discard) != -1) {
          // ignore — the replay loop is the sender
        }
      } finally {
        broadcaster.unregister(out);
      }
    } catch (Exception e) {
      System.out.println(
          "[mock-ws] client disconnected: "
              + e.getClass().getSimpleName()
              + " "
              + (e.getMessage() == null ? "" : e.getMessage()));
    }
  }

  /**
   * The single sender: builds each (optionally restamped) frame once and fans it to all clients.
   */
  private void replayLoop() {
    long delayMicros = (long) (1_000_000.0 / replayRateHz);
    System.out.println(
        "[mock-ws] replaying "
            + fixtureLines.size()
            + " lines at "
            + replayRateHz
            + " Hz to all clients");
    try {
      while (!closed) {
        for (String line : fixtureLines) {
          if (closed) {
            return;
          }
          String frame =
              rewriteEventTime ? EventTimeRewriter.rewrite(line, System.currentTimeMillis()) : line;
          broadcaster.broadcast(frameBytes(frame));
          TimeUnit.MICROSECONDS.sleep(delayMicros);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // ---- RFC 6455 handshake ----

  private static void handshake(InputStream in, OutputStream out) throws IOException {
    String request = readHttpRequest(in);
    String key =
        request
            .lines()
            .filter(l -> l.toLowerCase().startsWith("sec-websocket-key:"))
            .map(l -> l.substring(l.indexOf(':') + 1).trim())
            .findFirst()
            .orElseThrow(() -> new IOException("missing Sec-WebSocket-Key"));
    MessageDigest sha;
    try {
      sha = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("SHA-1 unavailable", e);
    }
    String accept =
        Base64.getEncoder()
            .encodeToString(sha.digest((key + GUID).getBytes(StandardCharsets.US_ASCII)));
    String response =
        "HTTP/1.1 101 Switching Protocols\r\n"
            + "Upgrade: websocket\r\n"
            + "Connection: Upgrade\r\n"
            + "Sec-WebSocket-Accept: "
            + accept
            + "\r\n\r\n";
    out.write(response.getBytes(StandardCharsets.US_ASCII));
    out.flush();
  }

  private static String readHttpRequest(InputStream in) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    int last = -1, secondLast = -1, thirdLast = -1;
    int b;
    while ((b = in.read()) != -1) {
      buf.write(b);
      // End of headers marker: \r\n\r\n
      if (thirdLast == '\r' && secondLast == '\n' && last == '\r' && b == '\n') {
        break;
      }
      thirdLast = secondLast;
      secondLast = last;
      last = b;
    }
    return buf.toString(StandardCharsets.US_ASCII);
  }

  // ---- WS framing (text only, client→server is masked, server→client unmasked) ----

  private static void sendText(OutputStream out, String s) throws IOException {
    out.write(frameBytes(s));
    out.flush();
  }

  /** Builds a complete unmasked text WS frame (header + payload) as bytes for identical fan-out. */
  static byte[] frameBytes(String s) {
    byte[] payload = s.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream b = new ByteArrayOutputStream(payload.length + 10);
    b.write(0x81); // FIN + opcode text
    if (payload.length < 126) {
      b.write(payload.length);
    } else if (payload.length < 65536) {
      b.write(126);
      b.write((payload.length >>> 8) & 0xff);
      b.write(payload.length & 0xff);
    } else {
      b.write(127);
      for (int i = 7; i >= 0; i--) {
        b.write((int) ((payload.length >>> (i * 8L)) & 0xff));
      }
    }
    b.writeBytes(payload);
    return b.toByteArray();
  }

  /** Reads a single text frame from the client (handles masking). Returns null on close/EOF. */
  private static String readTextFrame(InputStream in) throws IOException {
    int b0 = in.read();
    if (b0 < 0) return null;
    int opcode = b0 & 0x0f;
    if (opcode == 0x8) return null; // close
    if (opcode != 0x1) {
      throw new IOException("unexpected non-text opcode: " + opcode);
    }
    int b1 = in.read();
    if (b1 < 0) return null;
    boolean masked = (b1 & 0x80) != 0;
    long len = b1 & 0x7f;
    if (len == 126) {
      len = (in.read() << 8) | in.read();
    } else if (len == 127) {
      len = 0;
      for (int i = 0; i < 8; i++) len = (len << 8) | in.read();
    }
    byte[] mask = new byte[4];
    if (masked) readFully(in, mask, 4);
    byte[] payload = new byte[(int) len];
    readFully(in, payload, (int) len);
    if (masked) {
      for (int i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
    }
    return new String(payload, StandardCharsets.UTF_8);
  }

  private static void readFully(InputStream in, byte[] buf, int n) throws IOException {
    int off = 0;
    while (off < n) {
      int r = in.read(buf, off, n - off);
      if (r < 0) throw new IOException("EOF mid-frame");
      off += r;
    }
  }

  /**
   * Extracts an integer field by name from a flat JSON object string — robust to whitespace, no
   * actual JSON library needed for this trivial use case.
   */
  static Integer extractIntField(String json, String field) {
    String needle = "\"" + field + "\"";
    int idx = json.indexOf(needle);
    if (idx < 0) return null;
    int colon = json.indexOf(':', idx + needle.length());
    if (colon < 0) return null;
    int p = colon + 1;
    while (p < json.length() && Character.isWhitespace(json.charAt(p))) p++;
    int start = p;
    while (p < json.length() && (Character.isDigit(json.charAt(p)) || json.charAt(p) == '-')) p++;
    if (start == p) return null;
    try {
      return Integer.parseInt(json.substring(start, p));
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
