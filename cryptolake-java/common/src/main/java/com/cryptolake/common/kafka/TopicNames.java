package com.cryptolake.common.kafka;

import java.nio.charset.StandardCharsets;

/**
 * Shared topic-name and key helpers ensuring collector and writer agree byte-for-byte.
 *
 * <p>Ports the {@code f"{topic_prefix}{exchange}.{stream}"} pattern (Tier 5 M12, M13). Topic key
 * is {@code symbol.getBytes(UTF_8)} — preserves Kafka partition assignment when both Python and
 * Java collectors run concurrently during failover (Tier 5 M13).
 *
 * <p>Stateless. Thread-safe.
 */
public final class TopicNames {

  private TopicNames() {}

  /**
   * Returns the topic name for a given stream.
   *
   * @param topicPrefix prefix (empty string for primary, {@code "backup."} for backup — Tier 5
   *     M12). Must NOT be null (use {@code ""} for no prefix).
   * @param exchange exchange identifier (e.g. {@code "binance"})
   * @param stream stream type (e.g. {@code "trades"})
   */
  public static String forStream(String topicPrefix, String exchange, String stream) {
    return topicPrefix + exchange + "." + stream;
  }

  /**
   * Returns the Kafka partition key for a symbol: {@code symbol.getBytes(UTF_8)} (Tier 5 M13).
   *
   * <p>Identical partition assignment as Python's {@code key = symbol.encode()}.
   */
  public static byte[] symbolKey(String symbol) {
    return symbol.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Strips the backup prefix from a topic name (Tier 5 C7).
   *
   * <p>Uses {@link String#substring} to strip the prefix at most once — NOT {@code replace(prefix,
   * "")} which would strip all occurrences.
   *
   * @return {@code topic} with {@code backupPrefix} removed from the start, or {@code topic}
   *     unchanged if it doesn't start with {@code backupPrefix}
   */
  public static String stripBackupPrefix(String topic, String backupPrefix) {
    return topic.startsWith(backupPrefix) ? topic.substring(backupPrefix.length()) : topic;
  }
}
