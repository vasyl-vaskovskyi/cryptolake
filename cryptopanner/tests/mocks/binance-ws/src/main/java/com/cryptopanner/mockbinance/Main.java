package com.cryptopanner.mockbinance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class Main {

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(System.getenv().getOrDefault("MOCK_WS_PORT", "9001"));
    double rate = Double.parseDouble(System.getenv().getOrDefault("REPLAY_RATE_HZ", "10"));

    List<String> lines = loadFixture("/fixture.jsonl");
    System.out.println("[mock-ws] loaded " + lines.size() + " lines from classpath:/fixture.jsonl");
    System.out.println("[mock-ws] listening on 0.0.0.0:" + port);

    try (WsServer server = new WsServer(port, lines, rate)) {
      // Park forever; SIGTERM will close the server in the shutdown hook below.
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      server.close();
                    } catch (IOException ignored) {
                    }
                  }));
      Thread.currentThread().join();
    }
  }

  private static List<String> loadFixture(String resourcePath) throws IOException {
    List<String> out = new ArrayList<>();
    try (InputStream in = Main.class.getResourceAsStream(resourcePath)) {
      if (in == null) throw new IOException("fixture not found on classpath: " + resourcePath);
      try (BufferedReader r =
          new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        String line;
        while ((line = r.readLine()) != null) {
          if (!line.isEmpty()) out.add(line);
        }
      }
    }
    return out;
  }
}
