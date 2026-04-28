package com.cryptolake.writer.durability;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for LifecycleJournalReader.
 *
 * <p>Writes journal files in the same format that LifecycleJournal produces, then verifies the
 * reader parses them correctly.
 */
class LifecycleJournalReaderTest {

  @TempDir Path tempDir;

  private static final String PRIMARY_ID = "binance-collector-01";
  private static final String BACKUP_ID = "binance-collector-backup";

  /** Writes a JSONL lifecycle journal entry under {@code dataDir/cryptolake/<collectorId>/}. */
  private void writeJournal(Path dataDir, String collectorId, String... jsonLines)
      throws Exception {
    Path dir = dataDir.resolve("cryptolake").resolve(collectorId);
    Files.createDirectories(dir);
    Path journal = dir.resolve("lifecycle.jsonl");
    StringBuilder sb = new StringBuilder();
    for (String line : jsonLines) {
      sb.append(line).append('\n');
    }
    Files.writeString(journal, sb.toString());
  }

  @Test
  void readAllReturnsEntriesFromBothCollectors() throws Exception {
    writeJournal(
        tempDir,
        PRIMARY_ID,
        "{\"ts_ns\":1000,\"event\":\"start\",\"host_boot_id\":\"boot-1\",\"collector_session_id\":\"s1\"}");
    writeJournal(
        tempDir,
        BACKUP_ID,
        "{\"ts_ns\":2000,\"event\":\"start\",\"host_boot_id\":\"boot-1\",\"collector_session_id\":\"s2\"}");

    Map<String, LifecycleJournalReader.JournalEntry> entries =
        LifecycleJournalReader.readAll(tempDir, PRIMARY_ID, BACKUP_ID);

    assertThat(entries).containsKey("s1");
    assertThat(entries).containsKey("s2");
    assertThat(entries.get("s1").event()).isEqualTo("start");
    assertThat(entries.get("s1").hostBootId()).isEqualTo("boot-1");
    assertThat(entries.get("s2").event()).isEqualTo("start");
  }

  @Test
  void wasCleanShutdownReturnsTrueForCleanSession() throws Exception {
    writeJournal(
        tempDir,
        PRIMARY_ID,
        "{\"ts_ns\":1000,\"event\":\"start\",\"host_boot_id\":\"b\",\"collector_session_id\":\"s1\"}",
        "{\"ts_ns\":2000,\"event\":\"clean_shutdown\",\"host_boot_id\":\"b\","
            + "\"collector_session_id\":\"s1\",\"planned\":false}");

    Map<String, LifecycleJournalReader.JournalEntry> entries =
        LifecycleJournalReader.readAll(tempDir, PRIMARY_ID, BACKUP_ID);

    // Most-recent entry for s1 is clean_shutdown
    assertThat(LifecycleJournalReader.wasCleanShutdown(entries, "s1")).isTrue();
  }

  @Test
  void wasCleanShutdownReturnsFalseForStartOnlySession() throws Exception {
    writeJournal(
        tempDir,
        PRIMARY_ID,
        "{\"ts_ns\":1000,\"event\":\"start\",\"host_boot_id\":\"b\",\"collector_session_id\":\"s1\"}");

    Map<String, LifecycleJournalReader.JournalEntry> entries =
        LifecycleJournalReader.readAll(tempDir, PRIMARY_ID, BACKUP_ID);

    assertThat(LifecycleJournalReader.wasCleanShutdown(entries, "s1")).isFalse();
  }

  @Test
  void missingJournalFilesResultInEmptyMap() {
    // No files written
    Map<String, LifecycleJournalReader.JournalEntry> entries =
        LifecycleJournalReader.readAll(tempDir, PRIMARY_ID, BACKUP_ID);
    assertThat(entries).isEmpty();
  }

  @Test
  void readSinceFiltersOldEntries() throws Exception {
    writeJournal(
        tempDir,
        PRIMARY_ID,
        "{\"ts_ns\":100,\"event\":\"start\",\"host_boot_id\":\"b\",\"collector_session_id\":\"old\"}",
        "{\"ts_ns\":5000,\"event\":\"start\",\"host_boot_id\":\"b\",\"collector_session_id\":\"new\"}");

    Map<String, LifecycleJournalReader.JournalEntry> entries =
        LifecycleJournalReader.readSince(tempDir, PRIMARY_ID, BACKUP_ID, 1000L);

    assertThat(entries).doesNotContainKey("old");
    assertThat(entries).containsKey("new");
  }

  @Test
  void corruptLinesAreSkipped() throws Exception {
    writeJournal(
        tempDir,
        PRIMARY_ID,
        "{\"ts_ns\":1000,\"event\":\"start\",\"host_boot_id\":\"b\",\"collector_session_id\":\"s1\"}",
        "NOT_VALID_JSON",
        "{\"ts_ns\":2000,\"event\":\"start\",\"host_boot_id\":\"b\",\"collector_session_id\":\"s2\"}");

    Map<String, LifecycleJournalReader.JournalEntry> entries =
        LifecycleJournalReader.readAll(tempDir, PRIMARY_ID, BACKUP_ID);

    assertThat(entries).containsKey("s1");
    assertThat(entries).containsKey("s2");
    assertThat(entries).hasSize(2);
  }
}
