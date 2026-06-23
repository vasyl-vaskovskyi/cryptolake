package com.cryptopanner.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cryptopanner.common.EnvelopeCodec;
import com.cryptopanner.monitor.Alert.AlertType;
import com.cryptopanner.monitor.testutil.StubHttpServer;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** §13.a — the real HTTP-POST webhook alert channel (Telegram / WhatsApp). */
class WebhookAlertChannelTest {

  private StubHttpServer hook;

  @BeforeEach
  void setUp() throws Exception {
    hook = new StubHttpServer().route("/hook", 200, "{\"ok\":true}");
  }

  @AfterEach
  void tearDown() {
    hook.close();
  }

  private DispatchedMessage message() {
    return DispatchedMessage.single(
        Alert.of(
            "vps-fra-1",
            "cryptopanner-sealer",
            AlertType.COMPONENT_DOWN,
            "is down",
            Instant.EPOCH));
  }

  private WebhookAlertChannel channel(String url) {
    return new WebhookAlertChannel(
        "telegram", url, NodeScraper.newHttpClient(), EnvelopeCodec.newMapper());
  }

  @Test
  void postsMessageTextToWebhook() {
    WebhookAlertChannel ch = channel(hook.baseUrl() + "/hook");
    assertTrue(ch.enabled());
    assertTrue(ch.send(message()));

    assertEquals(1, hook.received().size());
    StubHttpServer.Received r = hook.received().get(0);
    assertEquals("POST", r.method());
    assertEquals("/hook", r.path());
    assertTrue(r.body().contains("COMPONENT_DOWN"), r.body());
    assertTrue(r.body().contains("cryptopanner-sealer"), r.body());
  }

  @Test
  void blankUrlIsDisabledAndSendsNothing() {
    WebhookAlertChannel ch = channel("");
    assertFalse(ch.enabled());
    assertFalse(ch.send(message()));
    assertTrue(hook.received().isEmpty());
  }

  @Test
  void nonOkResponseReturnsFalse() {
    hook.route("/hook", 500, "boom");
    assertFalse(channel(hook.baseUrl() + "/hook").send(message()));
  }

  @Test
  void unreachableWebhookReturnsFalseAndDoesNotThrow() {
    assertFalse(channel("http://127.0.0.1:1/hook").send(message()));
  }
}
