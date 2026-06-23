package com.cryptopanner.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * An {@link AlertChannel} that POSTs the message as JSON {@code {"text": ...}} to a configured
 * webhook URL — the shape used for both the Telegram and WhatsApp channels (§13.a). The operator
 * bakes any channel-specific routing (e.g. a Telegram {@code chat_id}) into the URL's query string,
 * so this class stays channel-agnostic. A blank URL disables the channel (WhatsApp is optional).
 *
 * <p>Delivery failures are swallowed into a {@code false} return — alert delivery must never throw
 * into the scrape loop.
 */
public final class WebhookAlertChannel implements AlertChannel {

  private final String name;
  private final String url;
  private final HttpClient http;
  private final ObjectMapper mapper;

  public WebhookAlertChannel(String name, String url, HttpClient http, ObjectMapper mapper) {
    this.name = name;
    this.url = url;
    this.http = http;
    this.mapper = mapper;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean enabled() {
    return url != null && !url.isBlank();
  }

  @Override
  public boolean send(DispatchedMessage message) {
    if (!enabled()) {
      return false;
    }
    try {
      ObjectNode payload = mapper.createObjectNode();
      payload.put("text", message.title() + "\n" + message.body());
      String body = mapper.writeValueAsString(payload);
      HttpRequest req =
          HttpRequest.newBuilder(URI.create(url))
              .timeout(Duration.ofSeconds(10))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();
      HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
      return resp.statusCode() / 100 == 2;
    } catch (Exception e) {
      return false;
    }
  }
}
