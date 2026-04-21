package com.cryptolake.writer.state;

/**
 * PG-persisted file state record: tracks where a file is on disk and the last committed Kafka
 * offset for that file.
 *
 * <p>Ports Python's {@code FileState} (design §6.5). PG table:
 * {@code writer_file_state(topic, partition, file_path, high_water_offset, file_byte_size,
 * updated_at)} with composite PK {@code (topic, partition, file_path)}.
 *
 * <p>{@code partition} is {@code int}; {@code highWaterOffset} is {@code long} (Tier 5 M8).
 * Immutable record (Tier 2 §12).
 */
public record FileStateRecord(
    String topic, int partition, long highWaterOffset, String filePath, long fileByteSize) {}
