package com.cryptolake.verify.gaps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * One-shot HTTP client for Binance REST API.
 *
 * <p>Ports {@code _fetch_historical_page(session, url, max_retries)} from {@code gaps.py}. Max 5
 * retries; honours {@code Retry-After} header; exponential backoff (Tier 5 D4, D7).
 *
 * <p>Tier 5 D3 — single {@link HttpClient} per CLI process; D4 — check status explicitly; D5 —
 * {@code BodyHandlers.ofByteArray()} + explicit UTF-8 decode; D7 — inline exp-backoff, no library.
 *
 * <p>Thread safety: {@link HttpClient} is thread-safe; stateless class.
 */
public final class BinanceRestClient {

  private static final int MAX_RETRIES = 5;
  private static final int MAX_BODY_EXCERPT = 200;

  private final HttpClient httpClient;
  private final ObjectMapper mapper;

  public BinanceRestClient(HttpClient httpClient, ObjectMapper mapper) {
    this.httpClient = httpClient;
    this.mapper = mapper;
  }

  /**
   * Fetches the JSON response from {@code url} with retry logic.
   *
   * @param url fully-qualified URL to fetch
   * @return parsed {@link JsonNode} response
   * @throws IOException on non-retryable HTTP errors
   * @throws EndpointUnavailableException on HTTP 400/403/5xx
   * @throws InterruptedException if the thread is interrupted during sleep
   */
  public JsonNode fetchPage(String url) throws IOException, InterruptedException {
    for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
      HttpRequest req =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(Duration.ofSeconds(30))
              .GET()
              .build();

      HttpResponse<byte[]> resp;
      try {
        resp = httpClient.send(req, BodyHandlers.ofByteArray());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // Tier 5 A4
        throw e;
      }

      int sc = resp.statusCode();

      if (sc == 429 || sc == 418) {
        // Rate-limited — honour Retry-After header (Tier 5 D4)
        long retryAfter = resp.headers().firstValueAsLong("Retry-After").orElse(1L << attempt);
        Thread.sleep(Duration.ofSeconds(retryAfter));
        continue;
      }

      if (sc == 400 || sc == 403 || sc >= 500) {
        String body = new String(resp.body(), StandardCharsets.UTF_8);
        throw new EndpointUnavailableException(
            sc + ": " + body.substring(0, Math.min(MAX_BODY_EXCERPT, body.length())));
      }

      if (sc >= 400) {
        throw new IOException(
            "HTTP "
                + sc
                + ": "
                + new String(resp.body(), StandardCharsets.UTF_8)
                    .substring(0, Math.min(MAX_BODY_EXCERPT, resp.body().length)));
      }

      // Success
      return mapper.readTree(resp.body());
    }

    throw new IOException("Max retries (" + MAX_RETRIES + ") exceeded for URL: " + url);
  }
}
