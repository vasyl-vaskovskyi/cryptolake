package com.cryptolake.collector.durability;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.util.Clocks;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LifecycleJournalTest {

  @TempDir Path tempDir;

  // ── round-trip tests (3) ─────────────────────────────────────────────────

  @Test
  void roundTrip_start_readSince() {
    LifecycleJournal journal = new LifecycleJournal(tempDir, "collector-01", Clocks.fixed(1_000L));

    journal.recordStart("boot-abc", "session-xyz");

    List<LifecycleEvent> events = journal.readSince(0);
    assertThat(events).hasSize(1);
    LifecycleEvent ev = events.get(0);
    assertThat(ev.event()).isEqualTo("start");
    assertThat(ev.hostBootId()).isEqualTo("boot-abc");
    assertThat(ev.collectorSessionId()).isEqualTo("session-xyz");
    assertThat(ev.tsNs()).isEqualTo(1_000L);
    assertThat(ev.planned()).isNull();
    assertThat(ev.maintenanceId()).isNull();
  }

  @Test
  void roundTrip_cleanShutdown_withMaintenanceId() {
    LifecycleJournal journal = new LifecycleJournal(tempDir, "collector-01", Clocks.fixed(5_000L));

    journal.recordCleanShutdown("boot-abc", "session-xyz", true, "maint-42");

    List<LifecycleEvent> events = journal.readSince(0);
    assertThat(events).hasSize(1);
    LifecycleEvent ev = events.get(0);
    assertThat(ev.event()).isEqualTo("clean_shutdown");
    assertThat(ev.planned()).isTrue();
    assertThat(ev.maintenanceId()).isEqualTo("maint-42");
  }

  @Test
  void roundTrip_multipleEventsOrdered() {
    AtomicLong tick = new AtomicLong(0);
    LifecycleJournal journal = new LifecycleJournal(tempDir, "col", tick::getAndIncrement);

    journal.recordStart("boot-1", "s1");
    journal.recordCleanShutdown("boot-1", "s1", false, null);
    journal.recordStart("boot-2", "s2");

    List<LifecycleEvent> all = journal.readSince(0);
    assertThat(all).hasSize(3);
    assertThat(all.get(0).event()).isEqualTo("start");
    assertThat(all.get(1).event()).isEqualTo("clean_shutdown");
    assertThat(all.get(2).event()).isEqualTo("start");
    assertThat(all.get(2).hostBootId()).isEqualTo("boot-2");
  }

  // ── corrupt-line test ────────────────────────────────────────────────────

  @Test
  void corruptLastLineIsDroppedOnRead() throws Exception {
    LifecycleJournal journal = new LifecycleJournal(tempDir, "collector-01", Clocks.fixed(1_000L));
    journal.recordStart("boot-abc", "session-xyz");

    // Append a corrupt (non-JSON) line directly to the file
    Path path = journal.journalPath();
    Files.writeString(path, "NOT_JSON_AT_ALL\n", java.nio.file.StandardOpenOption.APPEND);

    List<LifecycleEvent> events = journal.readSince(0);
    // Only the valid event should be returned; corrupt line silently dropped
    assertThat(events).hasSize(1);
    assertThat(events.get(0).event()).isEqualTo("start");
  }

  // ── prune test ───────────────────────────────────────────────────────────

  @Test
  void pruneDropsOldEntries() {
    long baseNs = 1_000_000_000_000L; // 1000s in ns
    AtomicLong tick = new AtomicLong(baseNs);
    LifecycleJournal journal = new LifecycleJournal(tempDir, "col", tick::get);

    // Record event at baseNs (old)
    journal.recordStart("boot-1", "s1");

    // Advance clock by 8 days in ns
    tick.set(baseNs + Duration.ofDays(8).toNanos());
    journal.recordStart("boot-2", "s2"); // this one is new (within 7-day window)

    // Prune with 7-day retention
    journal.pruneOlderThan(Duration.ofDays(7));

    List<LifecycleEvent> remaining = journal.readSince(0);
    assertThat(remaining).hasSize(1);
    assertThat(remaining.get(0).hostBootId()).isEqualTo("boot-2");
  }

  // ── readSince filter test ────────────────────────────────────────────────

  @Test
  void readSinceFiltersOlderEvents() {
    AtomicLong tick = new AtomicLong(0);
    LifecycleJournal journal = new LifecycleJournal(tempDir, "col", tick::getAndIncrement);

    journal.recordStart("boot-1", "s1"); // ts_ns=0
    journal.recordStart("boot-1", "s2"); // ts_ns=1
    journal.recordStart("boot-1", "s3"); // ts_ns=2

    List<LifecycleEvent> since1 = journal.readSince(1);
    assertThat(since1).hasSize(2);
    assertThat(since1.get(0).collectorSessionId()).isEqualTo("s2");
  }
}
