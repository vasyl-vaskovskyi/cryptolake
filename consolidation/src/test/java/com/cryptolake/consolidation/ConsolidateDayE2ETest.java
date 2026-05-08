package com.cryptolake.consolidation;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.consolidation.core.ConsolidateDay;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConsolidateDayE2ETest {

  private final ObjectMapper mapper = EnvelopeCodec.newMapper();

  @TempDir Path baseDir;

  @Test
  void streamsTwoSparseHoursAndSynthesizes22Gaps() throws IOException {
    Path dateDir = baseDir.resolve("binance/btcusdt/trades/2026-04-01");
    Files.createDirectories(dateDir);

    writeHourFile(dateDir.resolve("hour-05.jsonl.zst"), List.of(trade(100L), trade(101L)));
    writeHourFile(dateDir.resolve("hour-12.jsonl.zst"), List.of(trade(200L)));

    ConsolidateDay.ConsolidateResult result =
        ConsolidateDay.run(baseDir, "binance", "btcusdt", "trades", "2026-04-01", mapper);

    assertThat(result.success()).isTrue();
    assertThat(result.dataRecords()).isEqualTo(3L);
    assertThat(result.gapRecords()).isEqualTo(22L); // 22 missing hours
    assertThat(result.missingHours()).isEqualTo(22);
    assertThat(result.sourceFilesCount()).isEqualTo(2);

    Path dailyFile = dateDir.resolve("2026-04-01.jsonl.zst");
    Path sidecar = dateDir.resolve("2026-04-01.jsonl.zst.sha256");
    Path manifest = dateDir.resolve("2026-04-01.manifest.json");
    assertThat(dailyFile).exists();
    assertThat(sidecar).exists();
    assertThat(manifest).exists();

    // Hourly source files were cleaned up after verify
    assertThat(dateDir.resolve("hour-05.jsonl.zst")).doesNotExist();
    assertThat(dateDir.resolve("hour-12.jsonl.zst")).doesNotExist();

    // Daily file roundtrips: 25 lines, hour ordering preserved (gaps before/around data)
    List<JsonNode> records = readJsonl(dailyFile);
    assertThat(records).hasSize(25);
    long gaps = records.stream().filter(r -> "gap".equals(r.path("type").asText())).count();
    assertThat(gaps).isEqualTo(22L);
  }

  private void writeHourFile(Path target, List<JsonNode> envelopes) throws IOException {
    try (var fc = Files.newOutputStream(target);
        var zstd = new ZstdOutputStream(fc, 3)) {
      for (JsonNode env : envelopes) {
        zstd.write(mapper.writeValueAsBytes(env));
        zstd.write(0x0A);
      }
    }
  }

  private List<JsonNode> readJsonl(Path zstdFile) throws IOException {
    List<JsonNode> out = new ArrayList<>();
    try (var in = Files.newInputStream(zstdFile);
        var zin = new ZstdInputStream(in);
        var br = new BufferedReader(new InputStreamReader(zin, StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (!line.isEmpty()) out.add(mapper.readTree(line));
      }
    }
    return out;
  }

  private JsonNode trade(long aggId) throws IOException {
    var raw = new ByteArrayOutputStream();
    raw.write(("{\"a\":" + aggId + "}").getBytes(StandardCharsets.UTF_8));
    String envJson =
        "{\"v\":1,"
            + "\"type\":\"data\","
            + "\"exchange\":\"binance\","
            + "\"symbol\":\"btcusdt\","
            + "\"stream\":\"trades\","
            + "\"received_at\":1700000000000000000,"
            + "\"exchange_ts\":1700000000000000000,"
            + "\"collector_session_id\":\"test\","
            + "\"session_seq\":1,"
            + "\"raw_text\":\"{\\\"a\\\":"
            + aggId
            + "}\"}";
    return mapper.readTree(envJson);
  }
}
