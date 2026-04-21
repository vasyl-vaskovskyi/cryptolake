package com.cryptolake.common.config;

/**
 * Single unchecked config failure type for the CryptoLake module.
 *
 * <p>Ports Python's {@code ConfigValidationError}. Declared as {@code RuntimeException} so it never
 * leaks as a checked exception across module boundaries (Tier 2 §13; Tier 5 G2).
 *
 * <p>Thrown by {@link YamlConfigLoader} for missing files, YAML parse errors, and Hibernate
 * Validator constraint violations.
 */
public final class CryptoLakeConfigException extends RuntimeException {

  public CryptoLakeConfigException(String msg) {
    super(msg);
  }

  public CryptoLakeConfigException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
