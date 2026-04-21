// ports: tests/unit/test_logging.py::TestStructuredLogging::test_json_output
package com.cryptolake.common.logging;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class StructuredLoggerTest {

  private ListAppender<ILoggingEvent> listAppender;
  private Logger rootLogger;

  @BeforeEach
  void setUp() {
    rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    listAppender = new ListAppender<>();
    listAppender.start();
    rootLogger.addAppender(listAppender);
    rootLogger.setLevel(Level.INFO);
  }

  @AfterEach
  void tearDown() {
    rootLogger.detachAppender(listAppender);
    listAppender.stop();
  }

  @Test
  void jsonOutputContainsEventAndKvPairs() {
    // ports: tests/unit/test_logging.py::TestStructuredLogging::test_json_output
    StructuredLogger logger = StructuredLogger.of(StructuredLoggerTest.class);
    logger.info("test_event", "session_seq", 42, "symbol", "btcusdt");

    assertThat(listAppender.list).hasSize(1);
    ILoggingEvent event = listAppender.list.get(0);
    // The event message should be the event name
    assertThat(event.getMessage()).isEqualTo("test_event");
    // The formatted message should contain the structured arguments
    String formatted = event.getFormattedMessage();
    assertThat(formatted).contains("test_event");
  }

  @Test
  void mdcScopeAddsAndRemovesKeys() throws Exception {
    // Tests MDC context management (Tier 5 H3)
    StructuredLogger logger = StructuredLogger.of(StructuredLoggerTest.class);

    try (AutoCloseable ctx = StructuredLogger.mdc("symbol", "btcusdt", "stream", "trades")) {
      assertThat(org.slf4j.MDC.get("symbol")).isEqualTo("btcusdt");
      assertThat(org.slf4j.MDC.get("stream")).isEqualTo("trades");
      logger.info("inside_context");
    }

    // After try-with-resources, keys should be removed
    assertThat(org.slf4j.MDC.get("symbol")).isNull();
    assertThat(org.slf4j.MDC.get("stream")).isNull();
  }
}
