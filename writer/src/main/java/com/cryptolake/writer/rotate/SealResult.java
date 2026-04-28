package com.cryptolake.writer.rotate;

import java.nio.file.Path;

/**
 * Result of sealing a file (completing a rotation): paths to the data file and sidecar, plus the
 * final byte size.
 *
 * <p>Design §6.11. Immutable record (Tier 2 §12). Not serialized.
 */
public record SealResult(Path dataPath, Path sidecarPath, long byteSize) {}
