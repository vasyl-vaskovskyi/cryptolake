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

/**
 * Bare-bones single-client WebSocket server for tests. Performs the RFC 6455 handshake, then sends
 * each line of {@code script} as a text frame, then waits for client close. Not for production.
 */
public final class TinyWsServer implements AutoCloseable {

  private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

  private final ServerSocket server;
  private final ExecutorService exec;
  private final List<String> script;

  private TinyWsServer(ServerSocket server, List<String> script) {
    this.server = server;
    this.script = script;
    this.exec = Executors.newSingleThreadExecutor();
    exec.submit(this::accept);
  }

  public static TinyWsServer start(InetSocketAddress addr, List<String> script) throws IOException {
    ServerSocket s = new ServerSocket();
    s.bind(addr);
    return new TinyWsServer(s, script);
  }

  public int port() {
    return server.getLocalPort();
  }

  @Override
  public void close() throws IOException {
    exec.shutdownNow();
    server.close();
  }

  private void accept() {
    try (Socket sock = server.accept();
        InputStream in = sock.getInputStream();
        OutputStream out = sock.getOutputStream()) {
      // Read HTTP request, capture Sec-WebSocket-Key
      byte[] buf = new byte[4096];
      int n = in.read(buf);
      String req = new String(buf, 0, n, StandardCharsets.US_ASCII);
      String key =
          req.lines()
              .filter(l -> l.toLowerCase().startsWith("sec-websocket-key:"))
              .map(l -> l.substring(l.indexOf(':') + 1).trim())
              .findFirst()
              .orElseThrow();
      // Handshake response
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
      // Skip the client's SUBSCRIBE (the test doesn't validate it).
      // Push the script.
      for (String msg : script) {
        sendText(out, msg);
      }
      // Block until close.
      while (in.read() != -1) {}
    } catch (Exception ignored) {
      // server closing
    }
  }

  private static void sendText(OutputStream out, String s) throws IOException {
    byte[] payload = s.getBytes(StandardCharsets.UTF_8);
    out.write(0x81); // FIN + opcode text
    if (payload.length < 126) {
      out.write(payload.length);
    } else if (payload.length < 65536) {
      out.write(126);
      out.write((payload.length >>> 8) & 0xff);
      out.write(payload.length & 0xff);
    } else {
      out.write(127);
      for (int i = 7; i >= 0; i--) {
        out.write((int) ((payload.length >>> (i * 8)) & 0xff));
      }
    }
    out.write(payload);
    out.flush();
  }
}
