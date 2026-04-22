package com.cryptolake.writer.state;

/**
 * Wraps {@code SQLException} and other PG-layer errors so they never cross public method boundaries
 * as checked exceptions (Tier 2 §13; design §7.1).
 *
 * <p>Extends {@code RuntimeException}; no checked-exception leaks (Tier 5 G2 equivalent for PG
 * layer).
 */
public final class CryptoLakeStateException extends RuntimeException {

  public CryptoLakeStateException(String message, Throwable cause) {
    super(message, cause);
  }

  public CryptoLakeStateException(String message) {
    super(message);
  }
}
