package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class PeriodSelectorTest {

  @Test
  void parsesDay() {
    var p = PeriodSelector.parse("--day", "2026-05-11", null, null);
    assertThat(p.startMs()).isEqualTo(1778457600000L); // 2026-05-11T00:00:00Z
    assertThat(p.endMs()).isEqualTo(1778543999999L); // 2026-05-11T23:59:59.999Z
  }

  @Test
  void parsesWeekIso() {
    var p = PeriodSelector.parse("--week", "2026-W19", null, null);
    assertThat(p.startMs()).isEqualTo(1777852800000L); // Mon 2026-05-04T00:00:00Z
    assertThat(p.endMs()).isEqualTo(1778457599999L); // Sun 2026-05-10T23:59:59.999Z
  }

  @Test
  void parsesHour() {
    var p = PeriodSelector.parse("--hour", "2026-05-11T09", null, null);
    assertThat(p.startMs()).isEqualTo(1778490000000L);
    assertThat(p.endMs()).isEqualTo(1778493599999L);
  }

  @Test
  void parsesSinceUntil() {
    var p = PeriodSelector.parse(null, null, "2026-05-11T08:00:00Z", "2026-05-11T10:00:00Z");
    assertThat(p.startMs()).isEqualTo(1778486400000L); // 2026-05-11T08:00:00Z
    assertThat(p.endMs()).isEqualTo(1778493600000L); // 2026-05-11T10:00:00Z
  }

  @Test
  void rejectsNoMode() {
    assertThatThrownBy(() -> PeriodSelector.parse(null, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--day")
        .hasMessageContaining("--since");
  }
}
