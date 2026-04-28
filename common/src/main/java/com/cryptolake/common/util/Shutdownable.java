package com.cryptolake.common.util;

/**
 * Uniform clean-shutdown contract for service components.
 *
 * <p>The writer and collector modules implement this interface on their main service components so
 * that the service's {@code Main} can chain shutdown calls uniformly without importing
 * service-specific types into the shutdown path.
 */
public interface Shutdownable {
  /**
   * Signals the component to shut down cleanly.
   *
   * @throws InterruptedException if the calling thread is interrupted while waiting for shutdown to
   *     complete.
   */
  void shutdown() throws InterruptedException;
}
