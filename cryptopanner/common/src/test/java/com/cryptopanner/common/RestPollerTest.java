package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RestPollerTest {

  private HttpServer server;
  private URI baseUrl;
  private final ObjectMapper mapper = new ObjectMapper();
  private final List<JsonNode> sink = new ArrayList<>();
  private volatile RequestHandler nextHandler;

  @BeforeEach
  void setUp() throws IOException {
    sink.clear();
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", ex -> nextHandler.handle(ex));
    server.start();
    baseUrl = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  @Test
  void successEnvelopeOn200() throws IOException {
    nextHandler = ex -> respond(ex, 200, "{\"openInterest\":\"12345\"}");
    Map<String, String> params = new LinkedHashMap<>();
    params.put("symbol", "BTCUSDT");

    new RestPoller(
            RestPoller.newHttpClient(),
            mapper,
            baseUrl,
            "/fapi/v1/openInterest",
            params,
            captureSink())
        .pollOnce();

    assertEquals(1, sink.size());
    JsonNode env = sink.get(0);
    assertEquals("rest_response", env.get("envelope").asText());
    assertEquals("/fapi/v1/openInterest", env.get("endpoint").asText());
    assertEquals("BTCUSDT", env.get("params").get("symbol").asText());
    assertEquals(200, env.get("http_status").asInt());
    // Body stored verbatim in `raw` (never parsed-and-re-serialized) + provable hash — same
    // raw-fidelity guarantee as ws_frame (master spec invariant 3.a). No parsed `response`.
    assertEquals("{\"openInterest\":\"12345\"}", env.get("raw").asText());
    assertNotNull(env.get("poll_issued_at"));
    assertNotNull(env.get("received_at"));
    assertFalse(env.has("response"));
    assertFalse(env.has("error"));
  }

  @Test
  void successResponseStoredByteFaithfulWithMatchingHash() throws IOException {
    // Odd whitespace + non-alphabetical key order: a parse-then-re-serialize would normalize this,
    // so surviving verbatim proves true byte-fidelity. The hash must be over those exact bytes.
    String body = "{ \"b\" : 2,\n  \"a\":1 }";
    nextHandler = ex -> respond(ex, 200, body);

    new RestPoller(
            RestPoller.newHttpClient(),
            mapper,
            baseUrl,
            "/fapi/v1/exchangeInfo",
            Map.of(),
            captureSink())
        .pollOnce();

    JsonNode env = sink.get(0);
    assertEquals(body, env.get("raw").asText());
    assertEquals(CaptureEnvelope.sha256Hex(body), env.get("raw_sha256").asText());
  }

  @Test
  void errorEnvelopeOnHttp500KeepsBodyVerbatim() throws IOException {
    nextHandler = ex -> respond(ex, 500, "{\"code\":-1000,\"msg\":\"internal\"}");

    new RestPoller(
            RestPoller.newHttpClient(), mapper, baseUrl, "/fapi/v1/depth", Map.of(), captureSink())
        .pollOnce();

    JsonNode env = sink.get(0);
    assertEquals("rest_response", env.get("envelope").asText());
    assertEquals(500, env.get("http_status").asInt());
    // A non-200 still carries bytes from Binance: keep them verbatim + hashed (3.a) ...
    assertEquals("{\"code\":-1000,\"msg\":\"internal\"}", env.get("raw").asText());
    assertEquals(
        CaptureEnvelope.sha256Hex("{\"code\":-1000,\"msg\":\"internal\"}"),
        env.get("raw_sha256").asText());
    // ... plus an error object for classification (no `message`: `raw` already holds the body).
    JsonNode err = env.get("error");
    assertEquals("HTTP_500", err.get("class").asText());
    assertEquals(500, err.get("http_status").asInt());
    assertFalse(env.has("response"));
  }

  @Test
  void errorEnvelopeOnConnectionRefused() throws IOException {
    int deadPort;
    try (ServerSocket s = new ServerSocket(0)) {
      deadPort = s.getLocalPort();
    }
    URI deadUrl = URI.create("http://127.0.0.1:" + deadPort);

    new RestPoller(
            RestPoller.newHttpClient(), mapper, deadUrl, "/anything", Map.of(), captureSink())
        .pollOnce();

    JsonNode env = sink.get(0);
    assertEquals("rest_response", env.get("envelope").asText());
    assertTrue(env.get("http_status").isNull());
    // Nothing was received from Binance, so there is no `raw`/`raw_sha256` to attach: absence of
    // `raw` is exactly how a consumer tells "no response captured" from "Binance returned an
    // error".
    assertFalse(env.has("raw"));
    assertFalse(env.has("raw_sha256"));
    JsonNode err = env.get("error");
    assertEquals("CONNECTION", err.get("class").asText());
    assertNotNull(err.get("message"));
    assertTrue(err.get("http_status").isNull());
  }

  @Test
  void multipleParamsEncodedInQueryString() throws IOException {
    AtomicReference<String> seenQuery = new AtomicReference<>();
    nextHandler =
        ex -> {
          seenQuery.set(ex.getRequestURI().getRawQuery());
          respond(ex, 200, "{}");
        };
    Map<String, String> params = new LinkedHashMap<>();
    params.put("symbol", "BTCUSDT");
    params.put("limit", "100");

    new RestPoller(
            RestPoller.newHttpClient(), mapper, baseUrl, "/fapi/v1/depth", params, captureSink())
        .pollOnce();

    assertEquals("symbol=BTCUSDT&limit=100", seenQuery.get());
    JsonNode envParams = sink.get(0).get("params");
    assertEquals("BTCUSDT", envParams.get("symbol").asText());
    assertEquals("100", envParams.get("limit").asText());
  }

  @Test
  void emptyParamsProducesNoQueryString() throws IOException {
    AtomicReference<String> seenQuery = new AtomicReference<>();
    nextHandler =
        ex -> {
          seenQuery.set(ex.getRequestURI().getRawQuery());
          respond(ex, 200, "{}");
        };

    new RestPoller(
            RestPoller.newHttpClient(),
            mapper,
            baseUrl,
            "/fapi/v1/exchangeInfo",
            Map.of(),
            captureSink())
        .pollOnce();

    assertNull(seenQuery.get());
  }

  @Test
  void pollOnceReturnsTrueOnSuccessAndFalseOnError() throws IOException {
    nextHandler = ex -> respond(ex, 200, "{}");
    RestPoller ok =
        new RestPoller(RestPoller.newHttpClient(), mapper, baseUrl, "/ok", Map.of(), b -> {});
    org.junit.jupiter.api.Assertions.assertTrue(ok.pollOnce(), "200 should report success");

    nextHandler = ex -> respond(ex, 500, "{}");
    RestPoller bad =
        new RestPoller(RestPoller.newHttpClient(), mapper, baseUrl, "/bad", Map.of(), b -> {});
    assertFalse(bad.pollOnce(), "500 should report failure");
  }

  private Consumer<byte[]> captureSink() {
    return bytes -> {
      try {
        String line = new String(bytes, StandardCharsets.UTF_8);
        assertTrue(line.endsWith("\n"), "envelope must be newline-terminated");
        sink.add(mapper.readTree(line));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  private static void respond(HttpExchange ex, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(status, bytes.length);
    ex.getResponseBody().write(bytes);
    ex.close();
  }

  @FunctionalInterface
  private interface RequestHandler {
    void handle(HttpExchange ex) throws IOException;
  }
}
