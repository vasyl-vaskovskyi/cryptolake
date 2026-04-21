package com.cryptolake.writer.consumer;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies pending Kafka seeks on partition assignment and flushes+commits before revocation.
 *
 * <p>Ports Python's {@code _on_assign} / {@code _on_revoke} callbacks (design §2.2; Tier 5 C4).
 *
 * <p>On assignment: applies pending seeks from {@link RecoveryCoordinator#pendingSeeks()} via
 * {@link KafkaConsumer#seek(TopicPartition, long)} INSIDE the listener (not by mutating the
 * collection arg — Tier 5 C4). Also updates the volatile {@code assignedPartitions} set for
 * {@link KafkaConsumerLoop#isConnected()}.
 *
 * <p>On revoke: calls {@link OffsetCommitCoordinator#commitBeforeRevoke(Collection)} SYNCHRONOUSLY
 * (blocking the poll thread). This is safer than submitting to an executor (design §3.2 footnote;
 * Tier 5 C4 watch-out).
 *
 * <p>Thread safety: called only from the Kafka poll thread (same virtual thread as consume loop
 * T1). Listener methods run on T1.
 */
public final class PartitionAssignmentListener implements ConsumerRebalanceListener {

  private static final Logger log = LoggerFactory.getLogger(PartitionAssignmentListener.class);

  private final KafkaConsumer<byte[], byte[]> consumer;
  private final RecoveryCoordinator recovery;
  private final OffsetCommitCoordinator committer;
  private final Set<TopicPartition> assignedSink; // volatile-swapped; read by isConnected()

  public PartitionAssignmentListener(
      KafkaConsumer<byte[], byte[]> consumer,
      RecoveryCoordinator recovery,
      OffsetCommitCoordinator committer,
      Set<TopicPartition> assignedSink) {
    this.consumer = consumer;
    this.recovery = recovery;
    this.committer = committer;
    this.assignedSink = assignedSink;
  }

  /**
   * Called when partitions are assigned. Applies pending seeks and updates the assigned set.
   *
   * <p>Ports Python's {@code _on_assign}: seeks via {@link KafkaConsumer#seek(TopicPartition,
   * long)} inside the listener (Tier 5 C4 — not by mutating the collection arg).
   */
  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    Map<TopicPartition, Long> seeks = recovery.pendingSeeks();
    for (TopicPartition tp : partitions) {
      Long seekTo = seeks.get(tp);
      if (seekTo != null) {
        consumer.seek(tp, seekTo); // Tier 5 C4 — seek inside listener
        recovery.clearPendingSeek(tp);
        log.info("recovery_seek_applied",
            "topic", tp.topic(),
            "partition", tp.partition(),
            "offset", seekTo);
      }
    }
    assignedSink.addAll(partitions);
    log.info("partitions_assigned", "count", partitions.size());
  }

  /**
   * Called when partitions are revoked. Flushes+commits synchronously before losing ownership.
   *
   * <p>Ports Python's {@code _on_revoke} flush + commit (Tier 5 C4; design §3.2 — synchronous,
   * not submitted to executor). Throwing from this listener aborts poll — use a blocking call
   * instead.
   */
  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    log.info("partitions_revoked", "count", partitions.size());
    // Synchronous commit before losing ownership (Tier 5 C4 watch-out)
    committer.commitBeforeRevoke(partitions);
    assignedSink.removeAll(partitions);
  }
}
