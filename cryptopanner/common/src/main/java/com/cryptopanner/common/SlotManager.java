package com.cryptopanner.common;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * Reads and atomically flips the production slot marker at {@code deploy/active-slot} (design doc
 * §3.3, §4.4). The two collector systemd units {@code @a}/{@code @b} are slots, not lifecycle
 * states; exactly one is active production at a time, recorded as the literal {@code a} or {@code
 * b}.
 *
 * <p>{@link #flip} is a durable compare-and-swap: it refuses to write unless the file currently
 * holds the expected old slot (catching operator interference), then writes the new value via
 * tmp→fsync→rename→fsync-dir so a crash leaves either the old or the new value, never a torn file.
 */
public final class SlotManager {

  /** A deploy slot. Slots alternate freely across deploys; the label carries no other meaning. */
  public enum Slot {
    A("a"),
    B("b");

    private final String token;

    Slot(String token) {
      this.token = token;
    }

    public String token() {
      return token;
    }

    public Slot other() {
      return this == A ? B : A;
    }

    static Slot parse(String s) {
      return switch (s.trim()) {
        case "a" -> A;
        case "b" -> B;
        default -> throw new IllegalArgumentException("invalid active-slot value: '" + s + "'");
      };
    }
  }

  private final Path activeSlotFile;

  public SlotManager(Path activeSlotFile) {
    this.activeSlotFile = activeSlotFile;
  }

  /** The active production slot, defaulting to {@link Slot#A} on bootstrap (file absent). */
  public Slot active() throws IOException {
    if (!Files.exists(activeSlotFile)) {
      return Slot.A;
    }
    return Slot.parse(Files.readString(activeSlotFile));
  }

  /** The candidate slot for the next deploy: the one that is not active. */
  public Slot candidate() throws IOException {
    return active().other();
  }

  /**
   * Compare-and-swap the active slot from {@code expectedOld} to {@code newSlot}, durably. Throws
   * {@link IllegalStateException} if the current value is not {@code expectedOld}.
   */
  public synchronized void flip(Slot expectedOld, Slot newSlot) throws IOException {
    Slot current = active();
    if (current != expectedOld) {
      throw new IllegalStateException(
          "active-slot CAS failed: expected " + expectedOld + " but found " + current);
    }
    Path parent = activeSlotFile.toAbsolutePath().getParent();
    Path tmp = activeSlotFile.resolveSibling(activeSlotFile.getFileName() + ".tmp");
    try (FileChannel ch =
        FileChannel.open(
            tmp,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      ch.write(java.nio.ByteBuffer.wrap((newSlot.token() + "\n").getBytes(StandardCharsets.UTF_8)));
      ch.force(true);
    }
    Files.move(tmp, activeSlotFile, StandardCopyOption.ATOMIC_MOVE);
    if (parent != null) {
      try (FileChannel dir = FileChannel.open(parent, StandardOpenOption.READ)) {
        dir.force(true);
      } catch (IOException ignored) {
        // directory fsync is best-effort on some platforms
      }
    }
  }
}
