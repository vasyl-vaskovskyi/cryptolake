package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SlotManagerTest {

  @Test
  void defaultsToSlotAWhenFileAbsent(@TempDir Path dir) throws Exception {
    SlotManager sm = new SlotManager(dir.resolve("active-slot"));
    assertEquals(SlotManager.Slot.A, sm.active(), "bootstrap default is slot a (§3.3)");
    assertEquals(SlotManager.Slot.B, sm.candidate(), "candidate is the other slot");
  }

  @Test
  void readsExistingSlot(@TempDir Path dir) throws Exception {
    Path f = dir.resolve("active-slot");
    Files.writeString(f, "b\n");
    SlotManager sm = new SlotManager(f);
    assertEquals(SlotManager.Slot.B, sm.active());
    assertEquals(SlotManager.Slot.A, sm.candidate());
  }

  @Test
  void flipCompareAndSwapWritesNewSlot(@TempDir Path dir) throws Exception {
    Path f = dir.resolve("active-slot");
    SlotManager sm = new SlotManager(f);
    sm.flip(SlotManager.Slot.A, SlotManager.Slot.B); // current defaults to A
    assertEquals(SlotManager.Slot.B, sm.active());
    assertEquals("b", Files.readString(f).trim(), "durably written");
  }

  @Test
  void flipRejectsWhenCurrentDiffersFromExpected(@TempDir Path dir) throws Exception {
    Path f = dir.resolve("active-slot");
    Files.writeString(f, "a\n");
    SlotManager sm = new SlotManager(f);
    // CAS guard: expected B but current is A → abort (catches operator interference, §4.4 step 4).
    assertThrows(
        IllegalStateException.class, () -> sm.flip(SlotManager.Slot.B, SlotManager.Slot.A));
    assertEquals(SlotManager.Slot.A, sm.active(), "unchanged after a rejected flip");
  }

  @Test
  void slotOtherAlternates() {
    assertEquals(SlotManager.Slot.B, SlotManager.Slot.A.other());
    assertEquals(SlotManager.Slot.A, SlotManager.Slot.B.other());
  }
}
