package com.cryptolake.writer.consumer;

/**
 * Composite key for recovery high-water tracking: {@code (topic, partition, filePath)}.
 *
 * <p>Design §6.11. Immutable record (Tier 2 §12). Not serialized.
 */
record RecoveryKey(String topic, int partition, String filePath) {}
