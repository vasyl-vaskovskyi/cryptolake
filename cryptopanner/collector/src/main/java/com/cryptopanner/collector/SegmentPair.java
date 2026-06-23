package com.cryptopanner.collector;

import java.nio.file.Path;

/**
 * One overlap-minute file pair for a single {@code (symbol, stream)} during a WS rotation: the
 * primary connection's sealed segment and the shadow connection's {@code .shadow} segment (design
 * doc §5.3). The cutover merges {@code shadowFile} into {@code primaryFile}.
 */
public record SegmentPair(String symbol, String stream, Path primaryFile, Path shadowFile) {}
