package com.cryptolake.common.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;

/**
 * Optional programmatic Logback initialization.
 *
 * <p>Primary config is declarative ({@code logback.xml} classpath resource). This class provides
 * the entry point for the {@code level} argument historically passed to Python's {@code
 * setup_logging(level)}.
 *
 * <p>Calling {@link #setLevel(String)} without prior class loading is safe — the static initializer
 * has no side effects.
 */
public final class LogInit {

  private LogInit() {}

  /**
   * Sets the root logger level programmatically (e.g., {@code "DEBUG"}, {@code "WARN"}).
   *
   * <p>Valid values: {@code TRACE}, {@code DEBUG}, {@code INFO}, {@code WARN}, {@code ERROR}.
   * Unrecognised values default to {@code INFO}.
   */
  public static void setLevel(String level) {
    LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
    ch.qos.logback.classic.Logger root =
        ctx.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    // Normalise Python-style "WARNING" → "WARN" so Level.toLevel recognises it.
    String normalised = "WARNING".equalsIgnoreCase(level) ? "WARN" : level;
    Level lvl = Level.toLevel(normalised, Level.INFO);
    root.setLevel(lvl);
  }
}
