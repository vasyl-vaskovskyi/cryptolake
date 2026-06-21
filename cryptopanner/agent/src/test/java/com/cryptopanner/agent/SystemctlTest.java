package com.cryptopanner.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SystemctlTest {

  @Test
  void mapsPlainComponentToUnit() {
    assertEquals("cryptopanner-sealer.service", Systemctl.unit("sealer"));
    assertEquals("cryptopanner-uploader.service", Systemctl.unit("uploader"));
  }

  @Test
  void mapsCollectorSlotToTemplatedUnit() {
    assertEquals("cryptopanner-collector@a.service", Systemctl.unit("collector/a"));
    assertEquals("cryptopanner-collector@b.service", Systemctl.unit("collector/b"));
  }
}
