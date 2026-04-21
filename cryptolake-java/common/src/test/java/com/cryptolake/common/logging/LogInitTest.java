// ports: tests/unit/test_logging.py::TestStructuredLogging::test_warning_level_suppresses_info
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

class LogInitTest {

  private ListAppender<ILoggingEvent> listAppender;
  private Logger rootLogger;
  private Level originalLevel;

  @BeforeEach
  void setUp() {
    rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    originalLevel = rootLogger.getLevel();
    listAppender = new ListAppender<>();
    listAppender.start();
    rootLogger.addAppender(listAppender);
  }

  @AfterEach
  void tearDown() {
    rootLogger.detachAppender(listAppender);
    listAppender.stop();
    // Restore original level
    rootLogger.setLevel(originalLevel);
  }

  @Test
  void warningLevelSuppressesInfo() {
    // ports: tests/unit/test_logging.py::TestStructuredLogging::test_warning_level_suppresses_info
    LogInit.setLevel("WARNING");
    StructuredLogger logger = StructuredLogger.of(LogInitTest.class);

    logger.info("should_be_suppressed");
    logger.warn("should_appear");

    long infoCount =
        listAppender.list.stream()
            .filter(e -> "should_be_suppressed".equals(e.getMessage()))
            .count();
    long warnCount =
        listAppender.list.stream()
            .filter(e -> "should_appear".equals(e.getMessage()))
            .count();

    assertThat(infoCount).isEqualTo(0);
    assertThat(warnCount).isEqualTo(1);
  }
}
