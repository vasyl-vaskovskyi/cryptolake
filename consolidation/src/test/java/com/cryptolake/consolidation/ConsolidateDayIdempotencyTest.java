package com.cryptolake.consolidation;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.consolidation.core.ConsolidateDay;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConsolidateDayIdempotencyTest {

  private final ObjectMapper mapper = EnvelopeCodec.newMapper();

  @TempDir Path baseDir;

  @Test
  void skipsWhenDailyFileAlreadyExists() throws IOException {
    Path dateDir = baseDir.resolve("binance/btcusdt/trades/2026-04-01");
    Files.createDirectories(dateDir);
    Path dailyFile = dateDir.resolve("2026-04-01.jsonl.zst");
    byte[] marker = "preserve-me".getBytes(StandardCharsets.UTF_8);
    Files.write(dailyFile, marker);

    ConsolidateDay.ConsolidateResult result =
        ConsolidateDay.run(baseDir, "binance", "btcusdt", "trades", "2026-04-01", mapper);

    assertThat(result.success()).isTrue();
    assertThat(Files.readAllBytes(dailyFile))
        .as("existing daily file must not be overwritten")
        .isEqualTo(marker);
  }
}
