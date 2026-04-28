package com.cryptolake.collector.durability;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaOutageJournalTest {

  @TempDir Path tempDir;

  @Test
  void writeThenReadReturnsOutageStart() {
    KafkaOutageJournal journal = new KafkaOutageJournal(tempDir, "collector-01");

    journal.recordOutageStart(123_456_789_000L);

    OptionalLong result = journal.readOutageStart();
    assertThat(result).hasValue(123_456_789_000L);
  }

  @Test
  void readOnMissingFileReturnsEmpty() {
    KafkaOutageJournal journal = new KafkaOutageJournal(tempDir, "collector-01");
    // No write — file doesn't exist
    assertThat(journal.readOutageStart()).isEmpty();
  }

  @Test
  void truncateClearsRecord() {
    KafkaOutageJournal journal = new KafkaOutageJournal(tempDir, "collector-01");
    journal.recordOutageStart(999L);
    assertThat(journal.readOutageStart()).hasValue(999L);

    journal.truncate();
    assertThat(journal.readOutageStart()).isEmpty();
  }

  @Test
  void secondRecordOutageStartReplacesFirst() {
    KafkaOutageJournal journal = new KafkaOutageJournal(tempDir, "collector-01");

    journal.recordOutageStart(100L);
    journal.recordOutageStart(200L); // second write without truncate — last-writer-wins

    assertThat(journal.readOutageStart()).hasValue(200L);
  }

  @Test
  void truncateIsIdempotentWhenFileAbsent() {
    KafkaOutageJournal journal = new KafkaOutageJournal(tempDir, "collector-01");
    // Should not throw when file does not exist
    journal.truncate();
    assertThat(journal.readOutageStart()).isEmpty();
  }
}
