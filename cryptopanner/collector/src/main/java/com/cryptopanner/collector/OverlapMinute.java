package com.cryptopanner.collector;

import java.time.Instant;
import java.util.List;

/**
 * A single sealed overlap minute during a WS rotation (design doc §5.2 step 3): the minute-of-hour
 * it covers, its UTC start instant, and the per-{@code (symbol, stream)} primary/shadow file pairs
 * to verify and merge.
 */
public record OverlapMinute(int minuteOfHour, Instant minuteStart, List<SegmentPair> pairs) {}
