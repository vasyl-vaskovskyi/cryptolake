package com.cryptolake.common.logging;

import net.logstash.logback.argument.StructuredArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * SLF4J facade that preserves the structlog convention: "event name is the log message; KV pairs
 * become top-level JSON fields" (Tier 5 H1, H2).
 *
 * <p>KV pairs are passed as alternating {@code key, value} varargs, converted to {@link
 * StructuredArguments#keyValue} entries for the Logstash encoder. This emits each pair as a
 * top-level JSON field, matching Python structlog's {@code logger.info("event", key=val)}.
 *
 * <p>Thread safety: the backing SLF4J logger is thread-safe; MDC is per-thread (ThreadLocal in
 * Logback — works correctly with virtual threads when used in try-with-resources).
 */
public final class StructuredLogger {

  private final Logger delegate;

  private StructuredLogger(Logger delegate) {
    this.delegate = delegate;
  }

  /** Creates a {@code StructuredLogger} backed by a per-class SLF4J logger (Tier 5 H1). */
  public static StructuredLogger of(Class<?> cls) {
    return new StructuredLogger(LoggerFactory.getLogger(cls));
  }

  /** Logs at INFO level. {@code kvs} are alternating key/value pairs. */
  public void info(String event, Object... kvs) {
    if (delegate.isInfoEnabled()) {
      delegate.info(event, toStructuredArgs(kvs));
    }
  }

  /** Logs at WARN level. {@code kvs} are alternating key/value pairs. */
  public void warn(String event, Object... kvs) {
    if (delegate.isWarnEnabled()) {
      delegate.warn(event, toStructuredArgs(kvs));
    }
  }

  /** Logs at ERROR level with an exception. {@code kvs} are alternating key/value pairs. */
  public void error(String event, Throwable t, Object... kvs) {
    if (delegate.isErrorEnabled()) {
      Object[] args = toStructuredArgs(kvs);
      Object[] withThrowable = new Object[args.length + 1];
      System.arraycopy(args, 0, withThrowable, 0, args.length);
      withThrowable[args.length] = t;
      delegate.error(event, withThrowable);
    }
  }

  /** Logs at DEBUG level. {@code kvs} are alternating key/value pairs. */
  public void debug(String event, Object... kvs) {
    if (delegate.isDebugEnabled()) {
      delegate.debug(event, toStructuredArgs(kvs));
    }
  }

  /**
   * Opens an MDC context with the given key/value pairs. Use in try-with-resources (Tier 5 H3).
   *
   * <p>Example:
   *
   * <pre>{@code
   * try (var ctx = StructuredLogger.mdc("symbol", symbol, "stream", stream)) {
   *     handler.handle(...);
   * }
   * }</pre>
   *
   * @param keyValuePairs alternating key, value strings
   */
  public static AutoCloseable mdc(String... keyValuePairs) {
    if (keyValuePairs.length % 2 != 0) {
      throw new IllegalArgumentException("mdc() requires alternating key/value pairs");
    }
    String[] keys = new String[keyValuePairs.length / 2];
    for (int i = 0; i < keyValuePairs.length; i += 2) {
      MDC.put(keyValuePairs[i], keyValuePairs[i + 1]);
      keys[i / 2] = keyValuePairs[i];
    }
    return () -> {
      for (String k : keys) {
        MDC.remove(k);
      }
    };
  }

  private static Object[] toStructuredArgs(Object[] kvs) {
    if (kvs.length == 0) return new Object[0];
    if (kvs.length % 2 != 0) {
      throw new IllegalArgumentException(
          "kvs must be alternating key/value pairs, got " + kvs.length + " args");
    }
    Object[] args = new Object[kvs.length / 2];
    for (int i = 0; i < kvs.length; i += 2) {
      args[i / 2] = StructuredArguments.keyValue((String) kvs[i], kvs[i + 1]);
    }
    return args;
  }
}
