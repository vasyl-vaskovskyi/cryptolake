package com.cryptopanner.monitor;

/**
 * A destination for alert messages (§13.a). Telegram is primary; WhatsApp is the optional secondary
 * — both receive every alert. Implementations never throw on a delivery failure: alerting must not
 * crash the scrape loop, so {@link #send} returns {@code false} instead.
 */
public interface AlertChannel {

  String name();

  /** False when not configured (e.g. blank webhook URL); the dispatcher skips disabled channels. */
  boolean enabled();

  /** Deliver one message. Returns true on a 2xx; false on disabled, non-2xx, or transport error. */
  boolean send(DispatchedMessage message);
}
