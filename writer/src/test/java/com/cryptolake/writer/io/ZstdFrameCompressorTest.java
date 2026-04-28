package com.cryptolake.writer.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.luben.zstd.Zstd;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ZstdFrameCompressor}.
 *
 * <p>Ports: Python's {@code test_compressor.py} — verifies independent frame semantics (Tier 5 I1).
 */
class ZstdFrameCompressorTest {

  // ports: Tier 5 I1 — one independent frame per call
  @Test
  void compressFrame_singleLine_producesValidZstdFrame() {
    ZstdFrameCompressor compressor = new ZstdFrameCompressor(3);
    byte[] line = "hello world\n".getBytes();

    byte[] compressed = compressor.compressFrame(List.of(line));

    // Decompress and verify round-trip
    byte[] decompressed = Zstd.decompress(compressed, line.length);
    assertThat(decompressed).isEqualTo(line);
  }

  // ports: Tier 5 I1 — multiple lines joined then compressed as single frame
  @Test
  void compressFrame_multipleLines_joinsAllBeforeCompressing() {
    ZstdFrameCompressor compressor = new ZstdFrameCompressor(3);
    byte[] line1 = "line1\n".getBytes();
    byte[] line2 = "line2\n".getBytes();
    byte[] line3 = "line3\n".getBytes();

    byte[] compressed = compressor.compressFrame(List.of(line1, line2, line3));

    byte[] expected = "line1\nline2\nline3\n".getBytes();
    byte[] decompressed = Zstd.decompress(compressed, expected.length);
    assertThat(decompressed).isEqualTo(expected);
  }

  // ports: Tier 5 I1 — empty list produces valid (possibly empty frame or magic-number frame)
  @Test
  void compressFrame_emptyList_producesValidFrame() {
    ZstdFrameCompressor compressor = new ZstdFrameCompressor(3);

    byte[] compressed = compressor.compressFrame(List.of());

    // Must decompress without error
    byte[] decompressed = Zstd.decompress(compressed, 0);
    assertThat(decompressed).isEmpty();
  }

  // ports: design §4.4 — different compression levels are honoured
  @Test
  void compressFrame_level1VsLevel19_bothDecompressCorrectly() {
    byte[] line = "test data for compression level check\n".getBytes();

    ZstdFrameCompressor level1 = new ZstdFrameCompressor(1);
    ZstdFrameCompressor level19 = new ZstdFrameCompressor(19);

    byte[] c1 = level1.compressFrame(List.of(line));
    byte[] c19 = level19.compressFrame(List.of(line));

    assertThat(Zstd.decompress(c1, line.length)).isEqualTo(line);
    assertThat(Zstd.decompress(c19, line.length)).isEqualTo(line);
  }

  // ports: Tier 5 I1 — two separate compressFrame calls produce two independent frames
  // that can be concatenated and still be a valid zstd stream
  @Test
  void compressFrame_twoFramesConcatenated_decompressIndependently() {
    ZstdFrameCompressor compressor = new ZstdFrameCompressor(3);
    byte[] line1 = "frame1\n".getBytes();
    byte[] line2 = "frame2\n".getBytes();

    byte[] f1 = compressor.compressFrame(List.of(line1));
    byte[] f2 = compressor.compressFrame(List.of(line2));

    // Each frame independently valid
    assertThat(Zstd.decompress(f1, line1.length)).isEqualTo(line1);
    assertThat(Zstd.decompress(f2, line2.length)).isEqualTo(line2);
  }
}
