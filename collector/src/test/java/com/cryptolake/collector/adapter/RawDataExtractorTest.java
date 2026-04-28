package com.cryptolake.collector.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RawDataExtractor}.
 *
 * <p>New tests (design §8.1 table). Validates byte-identity of the balanced-brace extraction.
 */
class RawDataExtractorTest {

  @Test
  // ports: (new) RawDataExtractorTest::simpleObject
  void simpleObject() {
    String frame = "{\"stream\":\"btcusdt@aggTrade\",\"data\":{\"e\":\"aggTrade\"}}";
    int dataIdx = frame.indexOf("\"data\":");
    int dataStart = dataIdx + "\"data\":".length();
    String result = RawDataExtractor.extractDataValue(frame, dataStart);
    assertThat(result).isEqualTo("{\"e\":\"aggTrade\"}");
  }

  @Test
  // ports: (new) RawDataExtractorTest::nestedObjectsPreserveBytes
  void nestedObjectsPreserveBytes() {
    String frame = "{\"stream\":\"s\",\"data\":{\"outer\":{\"inner\":42},\"x\":1}}";
    int dataIdx = frame.indexOf("\"data\":");
    int dataStart = dataIdx + "\"data\":".length();
    String result = RawDataExtractor.extractDataValue(frame, dataStart);
    assertThat(result).isEqualTo("{\"outer\":{\"inner\":42},\"x\":1}");
  }

  @Test
  // ports: (new) RawDataExtractorTest::stringWithEscapedBraces
  void stringWithEscapedBraces() {
    // A value that contains escaped quotes but no actual braces
    String frame = "{\"stream\":\"s\",\"data\":{\"p\":\"a\\\"b\"}}";
    int dataIdx = frame.indexOf("\"data\":");
    int dataStart = dataIdx + "\"data\":".length();
    String result = RawDataExtractor.extractDataValue(frame, dataStart);
    assertThat(result).isEqualTo("{\"p\":\"a\\\"b\"}");
  }

  @Test
  // ports: (new) RawDataExtractorTest::unbalancedBracesThrows
  void unbalancedBracesThrows() {
    String frame = "{\"stream\":\"s\",\"data\":{\"e\":42}";
    // Truncate at start: data value is incomplete
    String trunc = "{\"stream\":\"s\",\"data\":{\"e\":42";
    int dataIdx = trunc.indexOf("\"data\":");
    int dataStart = dataIdx + "\"data\":".length();
    assertThatThrownBy(() -> RawDataExtractor.extractDataValue(trunc, dataStart))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  // ports: (new) RawDataExtractorTest::unicodeAndEmojiPreserved
  void unicodeAndEmojiPreserved() {
    // The byte identity rule applies to arbitrary UTF-8 content
    String unicode = "{\"e\":\"data\",\"s\":\"BTCUSDT\",\"c\":\"\\u00e9\"}";
    String frame = "{\"stream\":\"s\",\"data\":" + unicode + "}";
    int dataIdx = frame.indexOf("\"data\":");
    int dataStart = dataIdx + "\"data\":".length();
    String result = RawDataExtractor.extractDataValue(frame, dataStart);
    assertThat(result).isEqualTo(unicode);
  }
}
