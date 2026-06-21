package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DepthChainTrackerTest {

  @Test
  void firstFrameIsAlwaysABreakNeedingInitialSnapshot() {
    DepthChainTracker t = new DepthChainTracker();
    assertTrue(t.isBreak(5, 0), "first depth frame has no anchor → needs a snapshot");
  }

  @Test
  void contiguousChainIsNotABreak() {
    DepthChainTracker t = new DepthChainTracker();
    t.isBreak(5, 0); // first
    // next frame's pu must equal the previous frame's u (5).
    assertFalse(t.isBreak(8, 5));
    assertFalse(t.isBreak(12, 8));
  }

  @Test
  void puMismatchIsABreak() {
    DepthChainTracker t = new DepthChainTracker();
    t.isBreak(5, 0); // first, lastU=5
    assertTrue(t.isBreak(10, 7), "pu(7) != lastU(5) → chain break");
  }

  @Test
  void recoversChainAfterABreak() {
    DepthChainTracker t = new DepthChainTracker();
    t.isBreak(5, 0); // first, lastU=5
    t.isBreak(10, 7); // break, lastU updated to 10
    assertFalse(t.isBreak(13, 10), "chain resumes from the post-break frame's u");
  }
}
