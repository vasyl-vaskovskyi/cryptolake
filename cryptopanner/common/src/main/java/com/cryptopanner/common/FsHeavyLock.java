package com.cryptopanner.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * Node-level mutex over {@code /data/cryptopanner/.fs-heavy.lock} (master spec §6.a.7), shared by
 * the Sealer's hourly merge, deploy promote (§4.4), and WS rotation cutover (§5.2) so they never
 * contend for disk I/O. Backed by an OS advisory file lock ({@code flock}-equivalent via {@link
 * FileChannel#tryLock}); the holder name is written into the file so {@code /status} can report
 * {@code fs_heavy_lock.held_by} (§11.c). {@link #acquire} blocks up to a timeout, then throws.
 */
public final class FsHeavyLock implements AutoCloseable {

  private final FileChannel channel;
  private final FileLock lock;
  private final Path lockFile;

  private FsHeavyLock(FileChannel channel, FileLock lock, Path lockFile) {
    this.channel = channel;
    this.lock = lock;
    this.lockFile = lockFile;
  }

  /** Acquires the lock for {@code holder}, blocking up to {@code timeout}. */
  public static FsHeavyLock acquire(Path lockFile, String holder, Duration timeout)
      throws IOException, TimeoutException {
    Path parent = lockFile.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    FileChannel channel =
        FileChannel.open(
            lockFile, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    long deadline = System.nanoTime() + timeout.toNanos();
    while (true) {
      try {
        FileLock l = channel.tryLock();
        if (l != null) {
          writeHolder(channel, holder);
          return new FsHeavyLock(channel, l, lockFile);
        }
      } catch (OverlappingFileLockException contendedInProcess) {
        // another holder in this JVM — treat as contended and keep waiting
      }
      if (System.nanoTime() >= deadline) {
        channel.close();
        throw new TimeoutException("could not acquire " + lockFile + " within " + timeout);
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        channel.close();
        throw new IOException("interrupted while acquiring " + lockFile, e);
      }
    }
  }

  /** The current holder name written in the lock file, or empty if free/unmarked. */
  public static Optional<String> heldBy(Path lockFile) throws IOException {
    if (!Files.exists(lockFile)) {
      return Optional.empty();
    }
    String s = Files.readString(lockFile).trim();
    return s.isEmpty() ? Optional.empty() : Optional.of(s);
  }

  private static void writeHolder(FileChannel channel, String holder) throws IOException {
    channel.truncate(0);
    channel.position(0);
    channel.write(ByteBuffer.wrap(holder.getBytes(StandardCharsets.UTF_8)));
    channel.force(true);
  }

  @Override
  public void close() throws IOException {
    try {
      channel.truncate(0); // clear the holder marker → heldBy reports free
      channel.force(true);
      lock.release();
    } finally {
      channel.close();
    }
  }
}
