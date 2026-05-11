package com.cryptolake.verify.audit;

import com.cryptolake.common.logging.StructuredLogger;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Posts a single synthetic Alertmanager v2 alert to a configured webhook URL.
 *
 * <p>Alertmanager v2 API: {@code POST /api/v2/alerts} expects a JSON array of postable-alert
 * objects. Each alert has {@code labels} and optional {@code annotations}; {@code startsAt} /
 * {@code endsAt} are optional and omitted here — Alertmanager fills them in when absent.
 *
 * <p>The URL is read from the environment variable {@code CRYPTOLAKE_ALERTMANAGER_WEBHOOK}; when
 * unset or blank the default {@code http://127.0.0.1:9093/api/v2/alerts} is used.
 *
 * <p>{@link #post} never throws. All failures (non-2xx, I/O errors, interrupted) are logged at WARN
 * level and return {@code false}.
 */
public final class AlertmanagerNotifier {

  static final String DEFAULT_WEBHOOK_URL = "http://127.0.0.1:9093/api/v2/alerts";
  static final String ENV_VAR = "CRYPTOLAKE_ALERTMANAGER_WEBHOOK";

  private static final StructuredLogger LOG = StructuredLogger.of(AlertmanagerNotifier.class);

  private final String webhookUrl;
  private final HttpClient httpClient;
  private final ObjectMapper mapper;

  /**
   * Creates a notifier with an explicit webhook URL and an injected {@link HttpClient}.
   *
   * @param webhookUrl target URL (Alertmanager {@code /api/v2/alerts})
   * @param httpClient HTTP client to use for posting
   * @param mapper Jackson mapper for JSON serialisation
   */
  public AlertmanagerNotifier(String webhookUrl, HttpClient httpClient, ObjectMapper mapper) {
    this.webhookUrl = webhookUrl;
    this.httpClient = httpClient;
    this.mapper = mapper;
  }

  /**
   * Constructs a notifier using the URL from the environment variable {@code
   * CRYPTOLAKE_ALERTMANAGER_WEBHOOK}, falling back to {@link #DEFAULT_WEBHOOK_URL} when unset or
   * blank.
   *
   * @param mapper Jackson mapper for JSON serialisation
   * @return a ready-to-use {@link AlertmanagerNotifier}
   */
  public static AlertmanagerNotifier fromEnv(ObjectMapper mapper) {
    String envUrl = System.getenv(ENV_VAR);
    String url = (envUrl != null && !envUrl.isBlank()) ? envUrl : DEFAULT_WEBHOOK_URL;
    HttpClient client =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    return new AlertmanagerNotifier(url, client, mapper);
  }

  /**
   * POSTs one synthetic alert to Alertmanager. Never throws.
   *
   * @param alertname label {@code "alertname"}
   * @param severity label {@code "severity"} (e.g. {@code "critical"})
   * @param summary annotation {@code "summary"}
   * @param description annotation {@code "description"}
   * @param extraLabels additional labels to merge (may be {@code null}; treated as empty)
   * @return {@code true} if the server responded with a 2xx status, {@code false} on any failure
   */
  public boolean post(
      String alertname,
      String severity,
      String summary,
      String description,
      Map<String, String> extraLabels) {
    try {
      String jsonBody = buildBody(alertname, severity, summary, description, extraLabels);

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(webhookUrl))
              .timeout(Duration.ofSeconds(10))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
              .build();

      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      int status = response.statusCode();

      if (status >= 200 && status < 300) {
        return true;
      }

      LOG.warn(
          "alertmanager_post_failed", "url", webhookUrl, "status", status, "alertname", alertname);
      return false;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn(
          "alertmanager_post_interrupted",
          "url",
          webhookUrl,
          "alertname",
          alertname,
          "error",
          e.getMessage());
      return false;
    } catch (Exception e) {
      LOG.warn(
          "alertmanager_post_error",
          "url",
          webhookUrl,
          "alertname",
          alertname,
          "error",
          e.getMessage());
      return false;
    }
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private String buildBody(
      String alertname,
      String severity,
      String summary,
      String description,
      Map<String, String> extraLabels)
      throws Exception {

    // Build labels: start with extraLabels (lower priority), then set core labels so they always
    // win even if extraLabels tried to override them.
    Map<String, String> labels = new LinkedHashMap<>();
    if (extraLabels != null) {
      labels.putAll(extraLabels);
    }
    labels.put("alertname", alertname);
    labels.put("severity", severity);

    Map<String, Object> alert = new LinkedHashMap<>();
    alert.put("labels", labels);
    alert.put("annotations", Map.of("summary", summary, "description", description));

    return mapper.writeValueAsString(List.of(alert));
  }
}
