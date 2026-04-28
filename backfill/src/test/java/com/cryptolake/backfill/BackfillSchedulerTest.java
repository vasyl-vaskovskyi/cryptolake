package com.cryptolake.backfill;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** new — basic backfill scheduler lifecycle tests */
class BackfillSchedulerTest {

  @TempDir Path tmpDir;

  @Test
  void stopSignalTerminatesScheduler() throws Exception {
    // new — basic stop lifecycle
    BackfillScheduler scheduler =
        new BackfillScheduler(tmpDir, 60L, EnvelopeCodec.newMapper()); // 60s interval

    // Start on a virtual thread
    Thread t = Thread.ofVirtual().start(scheduler::run);

    // Signal stop immediately
    scheduler.stop();
    t.join(2000L);

    assertThat(t.isAlive()).isFalse();
  }
}
