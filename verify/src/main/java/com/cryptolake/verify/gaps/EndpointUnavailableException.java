package com.cryptolake.verify.gaps;

/**
 * Ports {@code EndpointUnavailableError} from {@code gaps.py}.
 *
 * <p>Thrown when a Binance REST endpoint returns HTTP 400, 403, or 5xx — indicating an
 * unrecoverable error for that endpoint/stream. Tier 2 §13 — {@link RuntimeException}, not checked.
 */
public final class EndpointUnavailableException extends RuntimeException {

  public EndpointUnavailableException(String message) {
    super(message);
  }
}
