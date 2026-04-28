package com.cryptolake.collector.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FrameAccumulator}.
 *
 * <p>Ports tests from design §8.1 table; Tier 5 D1.
 */
class FrameAccumulatorTest {

  private FrameAccumulator accumulator;

  @BeforeEach
  void setUp() {
    accumulator = new FrameAccumulator();
  }

  @Test
  // ports: (new) FrameAccumulatorTest::singleFragmentCompleteReturnsString
  void singleFragmentCompleteReturnsString() {
    accumulator.append("{\"e\":\"aggTrade\"}");
    assertThat(accumulator.complete()).isEqualTo("{\"e\":\"aggTrade\"}");
  }

  @Test
  // ports: (new) FrameAccumulatorTest::multiFragmentAccumulatesUntilLast
  void multiFragmentAccumulatesUntilLast() {
    accumulator.append("{\"e\":");
    accumulator.append("\"aggTrade\"");
    accumulator.append("}");
    assertThat(accumulator.complete()).isEqualTo("{\"e\":\"aggTrade\"}");
  }

  @Test
  // ports: (new) FrameAccumulatorTest::resetBetweenFrames
  void resetBetweenFrames() {
    accumulator.append("frame1");
    accumulator.complete(); // consumes
    accumulator.append("frame2");
    assertThat(accumulator.complete()).isEqualTo("frame2");
  }

  @Test
  // ports: (new) FrameAccumulatorTest::multiByteUtf8Preserved
  void multiByteUtf8Preserved() {
    // Multi-byte UTF-8 characters must survive round-trip through binary path
    String text = "{\"s\":\"BTC中文\"}"; // Chinese characters
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    accumulator.append(ByteBuffer.wrap(bytes));
    assertThat(accumulator.complete()).isEqualTo(text);
  }
}
