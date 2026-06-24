package com.cryptopanner.mockbinance;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fans the exact same bytes out to every registered client sink (§14.c "serve identical fan-out to
 * two parallel connections"). One replay loop builds each WS frame once and broadcasts it, so a
 * rotation shadow connection receives a byte-identical overlap minute to the primary and the
 * equivalence check passes. A sink that throws on write (client gone) is dropped; broadcasting with
 * no sinks is a no-op (frames are simply lost, as on a live exchange with no subscriber).
 */
public final class FanoutBroadcaster {

  private final Set<OutputStream> sinks = ConcurrentHashMap.newKeySet();

  public void register(OutputStream sink) {
    sinks.add(sink);
  }

  public void unregister(OutputStream sink) {
    sinks.remove(sink);
  }

  public int size() {
    return sinks.size();
  }

  /** Write {@code frame} to every registered sink; drop any that throw. */
  public void broadcast(byte[] frame) {
    for (OutputStream sink : sinks) {
      try {
        synchronized (sink) {
          sink.write(frame);
          sink.flush();
        }
      } catch (IOException e) {
        sinks.remove(sink);
      }
    }
  }
}
