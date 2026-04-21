package com.cryptolake.writer.consumer;

import com.cryptolake.writer.StreamKey;
import com.cryptolake.writer.state.StreamCheckpoint;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;

/**
 * Result of the startup recovery scan: pending seeks and loaded stream checkpoints.
 *
 * <p>Design §6.11. Immutable record (Tier 2 §12). Not serialized.
 */
public record RecoveryResult(
    Map<TopicPartition, Long> seeks, Map<StreamKey, StreamCheckpoint> checkpoints) {}
