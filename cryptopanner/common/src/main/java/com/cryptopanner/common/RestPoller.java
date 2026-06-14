package com.cryptopanner.common;

import com.fasterxml.jackson.databind.JsonNode;
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

  /** Issues one HTTP GET, then writes either a success or failure envelope to the sink. */
  public void pollOnce() {
    Instant issuedAt = Instant.now();
    HttpRequest req = HttpRequest.newBuilder(uri).GET().timeout(REQUEST_TIMEOUT).build();
    try {
      HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
      Instant receivedAt = Instant.now();
      if (resp.statusCode() == 200) {
        JsonNode body = mapper.readTree(resp.body());
        writeSuccess(issuedAt, receivedAt, resp.statusCode(), body);
      } else {
        writeError(
            issuedAt, receivedAt, "HTTP_" + resp.statusCode(), resp.body(), resp.statusCode());
      }
    } catch (HttpTimeoutException e) {
      writeError(issuedAt, Instant.now(), "TIMEOUT", String.valueOf(e.getMessage()), null);
    } catch (Exception e) {
      writeError(
          issuedAt,
          Instant.now(),
          "CONNECTION",
          e.getClass().getSimpleName() + ": " + e.getMessage(),
          null);
    }
  }

  private void writeSuccess(Instant issuedAt, Instant receivedAt, int status, JsonNode body) {
    ObjectNode env = envelope(issuedAt, receivedAt);
    env.put("http_status", status);
    env.set("response", body);
    emit(env);
  }

  private void writeError(
      Instant issuedAt, Instant receivedAt, String klass, String message, Integer status) {
    ObjectNode env = envelope(issuedAt, receivedAt);
    if (status != null) env.put("http_status", status);
    else env.putNull("http_status");
    ObjectNode err = env.putObject("error");
    err.put("class", klass);
    err.put("message", message);
    if (status != null) err.put("http_status", status);
    else err.putNull("http_status");
    emit(env);
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
