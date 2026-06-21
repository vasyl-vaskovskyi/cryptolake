package com.cryptopanner.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Polls one Binance REST endpoint and writes the spec §8.d wrapper envelope to a sink. One poller
 * per (endpoint, params) tuple — the caller schedules cadence.
 *
 * <p>Failure envelopes are written for HTTP errors (4xx/5xx), timeouts, and connection errors so
 * downstream consumers can distinguish "no response captured" from "Binance returned an error"
 * (master spec §8.d invariant on raw fidelity).
 */
public final class RestPoller {

  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

  private final HttpClient client;
  private final ObjectMapper mapper;
  private final String endpoint;
  private final Map<String, String> params;
  private final URI uri;
  private final Consumer<byte[]> sink;

  public RestPoller(
      HttpClient client,
      ObjectMapper mapper,
      URI baseUrl,
      String endpoint,
      Map<String, String> params,
      Consumer<byte[]> sink) {
    this.client = client;
    this.mapper = mapper;
    this.endpoint = endpoint;
    this.params = new LinkedHashMap<>(params);
    this.sink = sink;
    this.uri = URI.create(baseUrl.toString() + endpoint + queryString(this.params));
  }

  /** Builds an HttpClient with the §8.d-mandated timeouts and keep-alive. */
  public static HttpClient newHttpClient() {
    return HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(CONNECT_TIMEOUT)
        .build();
  }

  /**
   * Issues one HTTP GET, then writes either a success or failure envelope to the sink. Returns
   * {@code true} on HTTP 200 (success), {@code false} on any error (non-200, timeout, connection)
   * so the caller can schedule a retry (§8.d).
   */
  public boolean pollOnce() {
    Instant issuedAt = Instant.now();
    HttpRequest req = HttpRequest.newBuilder(uri).GET().timeout(REQUEST_TIMEOUT).build();
    try {
      HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
      Instant receivedAt = Instant.now();
      if (resp.statusCode() == 200) {
        writeSuccess(issuedAt, receivedAt, resp.statusCode(), resp.body());
        return true;
      }
      writeHttpError(issuedAt, receivedAt, resp.statusCode(), resp.body());
      return false;
    } catch (HttpTimeoutException e) {
      writeTransportError(issuedAt, Instant.now(), "TIMEOUT", String.valueOf(e.getMessage()));
      return false;
    } catch (Exception e) {
      writeTransportError(
          issuedAt,
          Instant.now(),
          "CONNECTION",
          e.getClass().getSimpleName() + ": " + e.getMessage());
      return false;
    }
  }

  /**
   * HTTP 200: store the response body verbatim in {@code raw} (never parse-then-re-serialize) with
   * a {@code raw_sha256} over those exact bytes — the same raw-fidelity guarantee as {@code
   * ws_frame} (master spec invariant 3.a).
   */
  private void writeSuccess(Instant issuedAt, Instant receivedAt, int status, String body) {
    ObjectNode env = envelope(issuedAt, receivedAt);
    env.put("http_status", status);
    putRaw(env, body);
    emit(env);
  }

  /**
   * Non-200 HTTP response: bytes were still received from Binance, so keep them verbatim + hashed
   * ({@code raw}/{@code raw_sha256}), plus an {@code error} object for classification. No {@code
   * message} — {@code raw} already holds the body byte-for-byte.
   */
  private void writeHttpError(Instant issuedAt, Instant receivedAt, int status, String body) {
    ObjectNode env = envelope(issuedAt, receivedAt);
    env.put("http_status", status);
    putRaw(env, body);
    ObjectNode err = env.putObject("error");
    err.put("class", "HTTP_" + status);
    err.put("http_status", status);
    emit(env);
  }

  /**
   * Timeout / connection failure: nothing was received from Binance, so there is no {@code raw} to
   * attach. The absence of {@code raw} is how a consumer distinguishes "no response captured" from
   * "Binance returned an error" (master spec §8.d).
   */
  private void writeTransportError(
      Instant issuedAt, Instant receivedAt, String klass, String message) {
    ObjectNode env = envelope(issuedAt, receivedAt);
    env.putNull("http_status");
    ObjectNode err = env.putObject("error");
    err.put("class", klass);
    err.put("message", message);
    err.putNull("http_status");
    emit(env);
  }

  private static void putRaw(ObjectNode env, String body) {
    env.put("raw_sha256", CaptureEnvelope.sha256Hex(body));
    env.put("raw", body);
  }

  private ObjectNode envelope(Instant issuedAt, Instant receivedAt) {
    ObjectNode env = mapper.createObjectNode();
    env.put("envelope", "rest_response");
    env.put("endpoint", endpoint);
    ObjectNode p = env.putObject("params");
    for (Map.Entry<String, String> e : params.entrySet()) p.put(e.getKey(), e.getValue());
    env.put("poll_issued_at", issuedAt.toString());
    env.put("received_at", receivedAt.toString());
    return env;
  }

  private void emit(ObjectNode env) {
    try {
      byte[] line = (mapper.writeValueAsString(env) + "\n").getBytes(StandardCharsets.UTF_8);
      sink.accept(line);
    } catch (Exception e) {
      System.err.println("[rest-poller] envelope serialize failed: " + e.getMessage());
    }
  }

  private static String queryString(Map<String, String> params) {
    if (params.isEmpty()) return "";
    StringBuilder sb = new StringBuilder("?");
    boolean first = true;
    for (Map.Entry<String, String> e : params.entrySet()) {
      if (!first) sb.append("&");
      sb.append(e.getKey()).append("=").append(e.getValue());
      first = false;
    }
    return sb.toString();
  }
}
