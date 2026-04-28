package com.cryptolake.collector.backup;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BackupChainReader} static helpers.
 *
 * <p>Ports {@code test_backup_chain_reader.py} tests. Integration tests (Testcontainers Kafka) are
 * skeleton-only here; the static helper tests are unit-level.
 */
class BackupChainReaderTest {

  @Test
  // ports: tests/unit/collector/test_backup_chain_reader.py::test_other_depth_topic_primary
  void otherDepthTopicPrimaryPrefixReturnsBackup() {
    // Primary (empty prefix) → backup
    String result = BackupChainReader.otherDepthTopic("", "binance");
    assertThat(result).isEqualTo("backup.binance.depth");
  }

  @Test
  // ports: tests/unit/collector/test_backup_chain_reader.py::test_other_depth_topic_backup
  void otherDepthTopicBackupPrefixReturnsPrimary() {
    // Backup → primary (empty prefix)
    String result = BackupChainReader.otherDepthTopic("backup.", "binance");
    assertThat(result).isEqualTo("binance.depth");
  }
}
