package com.cryptolake.collector.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ReconnectPolicy}.
 *
 * <p>Ports reconnect backoff tests (design §8.1 table; Tier 5 D7).
 */
class ReconnectPolicyTest {

  private ReconnectPolicy policy;

  @BeforeEach
  void setUp() {
    policy = new ReconnectPolicy();
  }

  @Test
  // ports: (new) ReconnectPolicyTest::exponentialBackoffCappedAt60s
  void exponentialBackoffCappedAt60s() {
    long b1 = policy.nextBackoffMillis(); // 1s
    long b2 = policy.nextBackoffMillis(); // 2s
    long b3 = policy.nextBackoffMillis(); // 4s
    long b4 = policy.nextBackoffMillis(); // 8s
    long b5 = policy.nextBackoffMillis(); // 16s
    long b6 = policy.nextBackoffMillis(); // 32s
    long b7 = policy.nextBackoffMillis(); // 60s (capped)
    long b8 = policy.nextBackoffMillis(); // 60s (capped)

    assertThat(b1).isEqualTo(1_000L);
    assertThat(b2).isEqualTo(2_000L);
    assertThat(b3).isEqualTo(4_000L);
    assertThat(b7).isEqualTo(60_000L);
    assertThat(b8).isEqualTo(60_000L);
  }

  @Test
  // ports: (new) ReconnectPolicyTest::resetOnConnectRestoresOneSecond
  void resetOnConnectRestoresOneSecond() {
    policy.nextBackoffMillis();
    policy.nextBackoffMillis();
    policy.reset();
    assertThat(policy.nextBackoffMillis()).isEqualTo(1_000L);
  }

  @Test
  // ports: (new) ReconnectPolicyTest::shouldProactivelyReconnectAt23h50m
  void shouldProactivelyReconnectAt23h50m() {
    Instant connectTime = Instant.now().minus(24, ChronoUnit.HOURS);
    Instant now = Instant.now();
    assertThat(policy.shouldProactivelyReconnect(connectTime, now)).isTrue();
  }

  @Test
  void shouldNotProactivelyReconnectUnder23h50m() {
    Instant connectTime = Instant.now().minus(1, ChronoUnit.HOURS);
    Instant now = Instant.now();
    assertThat(policy.shouldProactivelyReconnect(connectTime, now)).isFalse();
  }
}
