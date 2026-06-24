package com.cryptopanner.mockbinance;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.junit.jupiter.api.Test;

/**
 * §14.c "serve identical fan-out to two parallel connections": the broadcaster writes the exact
 * same bytes to every registered client, so a rotation shadow connection sees a byte-identical
 * overlap minute to the primary and the equivalence check passes.
 */
class FanoutBroadcasterTest {

  @Test
  void broadcastsIdenticalBytesToAllSinks() {
    FanoutBroadcaster b = new FanoutBroadcaster();
    ByteArrayOutputStream a = new ByteArrayOutputStream();
    ByteArrayOutputStream c = new ByteArrayOutputStream();
    b.register(a);
    b.register(c);

    byte[] frame = "{\"E\":123}".getBytes();
    b.broadcast(frame);

    assertArrayEquals(frame, a.toByteArray());
    assertArrayEquals(frame, c.toByteArray());
    assertArrayEquals(a.toByteArray(), c.toByteArray());
  }

  @Test
  void dropsSinksThatThrowAndKeepsDeliveringToOthers() {
    FanoutBroadcaster b = new FanoutBroadcaster();
    ByteArrayOutputStream good = new ByteArrayOutputStream();
    OutputStream broken =
        new OutputStream() {
          @Override
          public void write(int x) throws IOException {
            throw new IOException("client gone");
          }

          @Override
          public void write(byte[] x) throws IOException {
            throw new IOException("client gone");
          }
        };
    b.register(good);
    b.register(broken);
    assertEquals(2, b.size());

    b.broadcast("x".getBytes());

    assertEquals(1, b.size(), "a throwing sink is dropped");
    assertArrayEquals("x".getBytes(), good.toByteArray());
  }

  @Test
  void unregisteredSinkStopsReceiving() {
    FanoutBroadcaster b = new FanoutBroadcaster();
    ByteArrayOutputStream a = new ByteArrayOutputStream();
    b.register(a);
    b.broadcast("1".getBytes());
    b.unregister(a);
    b.broadcast("2".getBytes());
    assertArrayEquals("1".getBytes(), a.toByteArray());
  }

  @Test
  void broadcastWithNoSinksIsANoOp() {
    new FanoutBroadcaster().broadcast("nothing".getBytes()); // must not throw
  }
}
