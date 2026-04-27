package com.cryptolake.verify.archive;

import java.nio.file.Path;

/**
 * Immutable record representing a discovered archive file with its decomposed metadata.
 *
 * <p>Ports the path decomposition from {@code verify.py:307-310}.
 *
 * <p>Tier 2 §12 — record (immutable, no setters).
 */
public record ArchiveFile(Path path, String exchange, String symbol, String stream, String date) {}
