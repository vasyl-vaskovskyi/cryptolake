# Writer Hold-Pause + Tail Scrub + Memory Cap Design

**Date:** 2026-05-05
**Branch:** `writer-hold-pause-tail-scrub-memcap` (off `main`)
**Related:** chaos scenario 02 end-to-end run on `main` revealed a silent SIGKILL of the writer during a long disk-full hold, leaving torn zstd frames on disk that fail verify decompression.

## Problem

The chaos-02 run dated 2026-05-05 19:35 UTC executed cleanly through hold entry (`WRITER_DISK_FULL_HOLD_ENTERED` at 19:38:46) and hold exit (`WRITER_DISK_FULL_HOLD_EXITED` at 19:40:49). Then the writer JVM died silently between 19:40:49 and 19:41:20 — no exception in the log, no graceful shutdown trace, just 31 seconds of silence followed by a fresh JVM bootstrap line. The writer was killed externally (SIGKILL or kernel OOM-killer), and `restart: unless-stopped` brought it back up.

The chaos test then ran `dc stop writer` + tar at 19:42:49+, captured the post-restart state, and `cryptolake-verify` failed with two "Decompression failed: Data corruption detected" errors on `btcusdt/depth/.../hour-19.jsonl.zst` and `btcusdt/open_interest/.../hour-19.jsonl.zst`. Both files have torn zstd frames — partial bytes from a write that was interrupted by SIGKILL.

Diagnosis. Three problems interact:

1. During the disk-full hold, the writer's consume loop kept calling `primary.poll(...)` every iteration. Each poll fetched up to 500 records from Kafka and routed them into `BufferManager.add`. The hold gate in `OffsetCommitCoordinator.flushAndCommit` early-returns 0 without flushing buffers, so records accumulate unbounded. With 7 streams × kilobytes/record × 500 records/poll × ~30 polls during 120 s of hold, the buffer grew large enough to exhaust the JVM heap.
2. The writer container has no memory limit set in `docker-compose.yml`. The JVM default heap on Docker Desktop is sized to roughly half the container's perceived memory (effectively 1–2 GB). When the JVM hit its limit, depending on the page-allocation moment, the kernel OOM-killer fires SIGKILL on the JVM process. SIGKILL is uncatchable; no Java code runs (no `appendAndFsync` truncate-on-error, no shutdown hook, no log line).
3. `DurableAppender.appendAndFsync` does have a truncate-on-`IOException` recovery path (lines 44–53), but SIGKILL bypasses it entirely. After SIGKILL, the on-disk file has whatever the kernel had committed before the kill — typically a complete prefix of valid frames followed by a torn partial frame. On JVM restart, the writer opens the file in APPEND mode and writes new frames after the torn one, without checking the tail. The result is a file zstd cannot fully decompress.

Operationally, this surfaces as a chaos-02 verify failure today, and as a real production data corruption risk under any future SIGKILL/OOM/segfault.

## Goals

1. Prevent writer OOM during disk-full hold by pausing Kafka consumption when any hold is active, so records remain in Kafka rather than accumulating in the writer's in-memory buffer.
2. Heal torn zstd-frame tails on writer startup so an archive damaged by any unexpected exit (SIGKILL, OOM, segfault, host crash) is automatically truncated to its last valid frame and the sidecar is recomputed before the consume loop reads or writes anything.
3. Bound JVM memory usage so a future buffer-growth regression manifests as a logged `OutOfMemoryError` and a clean exit, not a kernel SIGKILL with no diagnosis.
4. Free the chaosfs (`itsthenetwork/nfs-server-alpine`) image after every chaos teardown so repeated runs do not retain a stale upstream image.

## Non-goals

- Adding a `BufferManager` max-bytes cap with eviction. The intent is "Kafka 48 h retention is the durability layer" — losing records from buffer memory while Kafka still has them would violate the spec's commit-after-fsync invariant on subsequent successful flushes that rely on those records still being in the buffer to flush.
- Modifying `DurableAppender.appendAndFsync`'s truncate-on-`IOException` path. It already works correctly for `IOException` and is unchanged.
- Modifying `DiskFullHoldController` or `PgOutageHoldController`. Their state machines are correct; we only consume them.
- Adding hold-aware semantics to `commitSealedHour` or `commitBeforeRevoke`. Spec for the prior wiring branch already documented this as a known limitation.
- Re-running the chaos test in CI as part of this branch's gate. Chaos suite is end-to-end and runs separately. This branch's CI gate is `./gradlew :writer:build` plus the new unit tests.

## Architecture

```
┌────────────────────────────── B (Kafka pause during hold) ──────────────────────────────┐
│                                                                                          │
│  KafkaConsumerLoop.run() — at the top of every while-loop iteration:                    │
│                                                                                          │
│    boolean nowHeld = committer.isAnyHoldActive();                                       │
│    if (nowHeld != lastKnownHeld) {                                                      │
│      if (nowHeld)  primary.pause(primary.assignment());                                 │
│      else          primary.resume(primary.assignment());                                │
│      log LIFECYCLE WRITER_KAFKA_CONSUMPTION_(PAUSED|RESUMED)                            │
│      lastKnownHeld = nowHeld;                                                           │
│    } else if (nowHeld) {                                                                │
│      primary.pause(primary.assignment());  // re-pause for any new partitions on rebal. │
│    }                                                                                     │
│                                                                                          │
│  → primary.poll() returns 0 records while paused; backupTail.poll() runs unaffected.    │
│  → BufferManager.add NOT called for primary records during hold; memory stays bounded.  │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────── C (startup zstd tail scrub) ──────────────────────────────┐
│                                                                                          │
│  Main.java startup sequence — AFTER rotator.writeMissingSidecars(), BEFORE               │
│  RecoveryCoordinator.runOnStartup():                                                     │
│                                                                                          │
│    int healed = ZstdTailScrubber.scrub(Path.of(baseDir));                               │
│                                                                                          │
│  ZstdTailScrubber.scrub: walk tree for *.jsonl.zst files. For each:                     │
│    - Read the entire file into a byte[] (typical hour file ≤ 100 MB).                   │
│    - Use zstd-jni's `Zstd.findFrameCompressedSize(byte[], int, int)` to parse frames:   │
│        offset = 0; lastGoodEndOffset = 0;                                                │
│        while (offset < file.length()) {                                                  │
│          frameSize = Zstd.findFrameCompressedSize(buf, offset, len - offset);           │
│          if (frameSize <= 0 || throws) break;                                            │
│          offset += frameSize; lastGoodEndOffset = offset;                                │
│        }                                                                                  │
│    - If lastGoodEndOffset < file.size():                                                 │
│        - Open RW, truncate to lastGoodEndOffset, fsync.                                  │
│        - Recompute the .sha256 sidecar via Sha256Sidecar.write.                          │
│        - Log LIFECYCLE WRITER_STARTUP_TAIL_TRUNCATED                                     │
│             path=...  bytes_dropped=N  last_good_offset=M                                │
│    - If lastGoodEndOffset == 0 (entirely corrupt), truncate to 0 (file is now empty;     │
│      verify treats empty files as no-op).                                                │
│    - Errors during the walk (e.g., NoSuchFileException from concurrent removal) are     │
│      swallowed at DEBUG level; the file is left alone.                                   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────── E (memory cap + JVM heap) ────────────────────────────────┐
│                                                                                          │
│  docker-compose.yml writer service:                                                      │
│    deploy:                                                                               │
│      resources:                                                                          │
│        limits:                                                                           │
│          memory: 768M                                                                    │
│    environment:                                                                          │
│      JAVA_TOOL_OPTIONS: "-Xmx512m -Xms256m"                                             │
│      (existing env vars preserved)                                                       │
│                                                                                          │
│  Effect: future buffer-overflow regression yields java.lang.OutOfMemoryError logged     │
│  via slf4j BEFORE the JVM dies, plus the JVM's hs_err_pid file. Kernel OOM-killer is    │
│  not the failure mode of first resort.                                                   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────── chaosfs image cleanup on teardown ────────────────────────────┐
│                                                                                          │
│  tests/chaos/common.sh teardown_stack(), AFTER `dc down -v --remove-orphans --rmi local`│
│  and AFTER `rm -rf $HOST_DATA_DIR`:                                                      │
│                                                                                          │
│    docker rmi itsthenetwork/nfs-server-alpine@sha256:7fa99ae...0ba20 2>/dev/null \      │
│      || true                                                                             │
│                                                                                          │
│  Removes the cached upstream image so each scenario start is hermetic.                   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

## Components and file-level changes

| File | Change |
|---|---|
| `writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java` | Add a public `boolean isAnyHoldActive()` method that returns the OR of both controllers' `isHoldActive()`, with the same `!= null` null-tolerance the existing code uses. ~6 lines. |
| `writer/src/main/java/com/cryptolake/writer/consumer/KafkaConsumerLoop.java` | Add a private `boolean lastKnownHeld = false` field. Add a private `applyHoldPauseState()` method called at the top of every `while (!stopRequested)` iteration BEFORE `primary.poll`. The method: queries `committer.isAnyHoldActive()`, calls `primary.pause/resume(primary.assignment())` on edge-changes, logs `LIFECYCLE WRITER_KAFKA_CONSUMPTION_(PAUSED\|RESUMED)` on edge-changes only, and re-pauses `primary.assignment()` every iteration while held to handle rebalance-during-hold. ~30 lines including imports + javadoc. |
| `writer/src/main/java/com/cryptolake/writer/recovery/ZstdTailScrubber.java` | New utility class with one public static method `int scrub(Path baseDir)`. Walks `baseDir` recursively for `*.jsonl.zst` files; for each, parses zstd frames from the start to find the last valid frame boundary; truncates anything beyond it; recomputes the corresponding `.sha256` sidecar via `Sha256Sidecar.write`. Returns the number of files healed. Logs structured events. ~100 lines. |
| `writer/src/main/java/com/cryptolake/writer/Main.java` | Insert one line: `int healedFiles = ZstdTailScrubber.scrub(Path.of(baseDir));` AFTER the existing `rotator.writeMissingSidecars()` call and BEFORE `RecoveryCoordinator` is constructed. Log `startup_tail_scrub_complete` with the healed count. |
| `writer/src/test/java/com/cryptolake/writer/recovery/ZstdTailScrubberTest.java` | Five unit tests using `@TempDir`: clean-file-unchanged, torn-tail-truncated, entirely-corrupt-truncates-to-zero, ignores-non-zstd, returns-correct-count. Uses real `ZstdFrameCompressor` to write valid frames, then appends raw bytes for the torn case. ~150 lines. |
| `writer/src/test/java/com/cryptolake/writer/consumer/KafkaConsumerLoopHoldPauseTest.java` | Four Mockito tests: never-pauses-when-inactive, pauses-on-active-edge, no-re-pause-log-while-active, resumes-on-inactive-edge. Drives a stub `OffsetCommitCoordinator` and a mocked `KafkaConsumer`. ~110 lines. |
| `docker-compose.yml` | Add to the `writer` service: `deploy: { resources: { limits: { memory: 768M } } }` and `JAVA_TOOL_OPTIONS: "-Xmx512m -Xms256m"` in `environment:`. ~5 lines. |
| `tests/chaos/common.sh` | In `teardown_stack`, after the existing `rm -rf "$HOST_DATA_DIR"` line, add: `docker rmi itsthenetwork/nfs-server-alpine@sha256:7fa99ae65c23c5af87dd4300e543a86b119ed15ba61422444207efc7abd0ba20 2>/dev/null \|\| true` plus a `msg "Removed chaosfs image"` line. ~3 lines. |

`DurableAppender.java`, `DiskFullHoldController.java`, `PgOutageHoldController.java`, `BufferManager.java`, `RecordHandler.java`, `FileRotator.java`, `RecoveryCoordinator.java` are NOT modified.

## Data flow — the four interesting paths

**Path 1 — Disk-full entry pauses primary.** `OffsetCommitCoordinator.flushAndCommit` catch routes ENOSPC to `diskHold.onWriteError(e)` which flips `holdActive=true` (existing behavior). Catch returns 0. Control returns to the consume loop; current iteration finishes (one final `primary.poll` may have buffered up to 500 records — bounded). Next iteration's `applyHoldPauseState` reads `isAnyHoldActive=true`, sees `lastKnownHeld=false`, calls `primary.pause(primary.assignment())`, logs `LIFECYCLE WRITER_KAFKA_CONSUMPTION_PAUSED`. Subsequent iterations: `nowHeld == lastKnownHeld == true`, no log emit, but `primary.pause` is called again on the current assignment to cover rebalance-during-hold.

**Path 2 — Disk-full exit resumes primary.** The retry-loop in `DiskFullHoldController` probes free space; on success calls `onRecovery` which flips `holdActive=false`. Next consume-loop iteration: `applyHoldPauseState` reads `isAnyHoldActive=false`, sees `lastKnownHeld=true`, calls `primary.resume(primary.assignment())`, logs `LIFECYCLE WRITER_KAFKA_CONSUMPTION_RESUMED`. The next `primary.poll` returns the records that accumulated in Kafka during the hold. Records flow into `BufferManager`, next flush succeeds, archive catches up.

**Path 3 — Startup tail scrub.** `Main.main` runs the existing `rotator.writeMissingSidecars()` (writes sidecars for files that lack them — orthogonal). Then `ZstdTailScrubber.scrub(Path.of(baseDir))` walks `baseDir` recursively. For each `*.jsonl.zst`, it reads the file bytes and walks frame-by-frame via `com.github.luben.zstd.Zstd.findFrameCompressedSize(byte[], int, int)`. The loop tracks `lastGoodEndOffset = offset just past the last frame whose `findFrameCompressedSize` returned a valid size. When `findFrameCompressedSize` throws (malformed frame) or returns ≤ 0, the loop stops. If `lastGoodEndOffset < file.size()`, the scrubber reopens RW, truncates to `lastGoodEndOffset`, fsyncs, recomputes the sidecar via `Sha256Sidecar.write`, and logs `LIFECYCLE WRITER_STARTUP_TAIL_TRUNCATED`. Healthy files (clean EOF on a frame boundary) are unchanged.

**Path 4 — JVM heap pressure with bounded heap.** Hypothetically, if a future regression makes path 1 ineffective (e.g., the pause is called but Kafka still buffers internally beyond `MAX_POLL_RECORDS`), buffer growth resumes. With `-Xmx512m`, the JVM hits its limit and throws `java.lang.OutOfMemoryError`. The consume loop's outer catch is `catch (Exception e)` which does NOT catch `Error` types — design §7.2 explicitly propagates `Error` types. The error escapes the consume loop, the `executor.submit` Runnable's `finally` block calls `shutdownLatch.countDown()`, `Main` unblocks and runs the `catch (RuntimeException | Error startupErr)` cleanup. The `OutOfMemoryError` is logged via slf4j before the JVM exits with a non-zero status. Container exits, `restart: unless-stopped` brings it back. On startup, path 3 runs and heals any torn frames left by the OOM-mid-write.

**Path 5 — Chaos teardown.** `tests/chaos/common.sh` teardown_stack runs (after the per-scenario asserts). `dc down -v --remove-orphans --rmi local` removes per-project local-build images. `rm -rf "$HOST_DATA_DIR"` removes host scratch dir. New step: `docker rmi itsthenetwork/nfs-server-alpine@sha256:<digest> 2>/dev/null || true` removes the cached upstream chaosfs image. Best-effort; failure is silent.

## Error handling and edge cases

- **E1. Pause races with rebalance.** Re-pausing `primary.assignment()` every iteration while held, not just on the entry-edge, ensures new partitions assigned during a rebalance also get paused. Kafka client treats `pause` on already-paused partitions as a no-op.
- **E2. Pause edge mid-flush.** Hold can flip active inside `flushAndCommit` (catch block). The current consume-loop iteration finishes its work; the next iteration applies the pause. Worst-case extra-buffered records: one `primary.poll` worth = 500 records (`MAX_POLL_RECORDS_CONFIG`). Bounded.
- **E3. Scrubber on healthy files.** Frame-walking using `ZstdInputStream` is fast — header-only parsing, not full decompression. ~50 MB files × 7 streams scanned in well under 1 second on startup.
- **E4. Scrubber on entirely corrupt file.** Truncate to 0; the existing FileRotator/verify treat empty files as no-op.
- **E5. Sidecar recomputation.** Always recompute (option (a) from design discussion). Keeps the invariant "every non-empty `.jsonl.zst` has a fresh `.sha256`" intact immediately after scrubber returns.
- **E6. Concurrent file removal during scrub.** `Files.walk` then `Files.size`/`FileChannel.open` may throw `NoSuchFileException`. Catch and skip. Log at DEBUG.
- **E7. Pause/resume on closed consumer.** Only happens during shutdown after `stopRequested=true`, in which case the consume loop has already broken out. Defensively wrap pause/resume in try/catch (Exception), log and continue.
- **E8. Memory limit too small for steady state.** 512 MB heap leaves ~256 MB headroom in 768 MB container for off-heap (DirectByteBuffer for Kafka, native zstd state, threads). If steady-state is observed to push close to 512 MB on a clean (non-chaos) startup, bump to 1 G; do not pre-tune.
- **E9. `docker rmi` while image in use.** rmi runs after `dc down -v` removes the chaosfs container. No users at rmi time. Race window guarded by `2>/dev/null || true`.
- **E10. Image used by future scenario.** Currently only scenario 02. Removing after every scenario forces re-pull on each. Acceptable cost (~150 MB pull, fast on local network). If multiple scenarios use it, refactor to suite-level cleanup.

## Testing strategy

**Existing coverage to preserve.** All 5 `OffsetCommitCoordinatorHoldIntegrationTest`, 3 `OffsetCommitCoordinatorTest` legacy, `DiskFullHoldControllerTest`, `PgOutageHoldControllerTest`, and the writer's other unit tests pass unchanged. Adding `isAnyHoldActive()` is a non-breaking accessor.

**New `KafkaConsumerLoopHoldPauseTest`.** Mockito-driven. Stubs `OffsetCommitCoordinator.isAnyHoldActive()` per test. Mocks `KafkaConsumer<byte[], byte[]>` to verify `pause`/`resume` interactions. Drives the consume loop via direct `run()` invocation in a small executor and shuts it down via `requestShutdown` after a few iterations. Four test methods:

1. `consumeLoop_doesNotPause_whenHoldNeverActive` — `isAnyHoldActive` returns false. Run loop a few iterations. Assert `consumer.pause` and `consumer.resume` never called.
2. `consumeLoop_pausesPrimary_whenHoldFlipsActive` — `isAnyHoldActive` returns false then true. Assert exactly one `WRITER_KAFKA_CONSUMPTION_PAUSED` log line; `consumer.pause` called at least once.
3. `consumeLoop_doesNotEmitDuplicatePauseLog_whileHoldStaysActive` — `isAnyHoldActive` returns true throughout. Assert exactly one `WRITER_KAFKA_CONSUMPTION_PAUSED` log line over multiple iterations.
4. `consumeLoop_resumesPrimary_whenHoldFlipsInactive` — `isAnyHoldActive` returns true then false. Assert exactly one `WRITER_KAFKA_CONSUMPTION_RESUMED` log line; `consumer.resume` called at least once.

**New `ZstdTailScrubberTest`.** Uses JUnit5 `@TempDir`. Five test methods:

1. `scrub_leavesHealthyFileUnchanged` — write 3 valid zstd frames + sidecar via real `ZstdFrameCompressor` and `Sha256Sidecar`. Run scrubber. Assert file size unchanged, sidecar unchanged.
2. `scrub_truncatesTornTail_andRecomputesSidecar` — write 2 valid frames; append 100 bytes of zstd-magic-prefix + truncated content. Run scrubber. Assert file size = end of frame 2; sidecar matches new content.
3. `scrub_truncatesEntirelyCorruptFile_toZero` — write garbage bytes (no zstd magic). Run scrubber. Assert file size = 0; sidecar matches empty content.
4. `scrub_ignoresNonZstdFiles` — write `*.txt`, `*.tmp`, `*.sha256` files. Run scrubber. Assert all unchanged.
5. `scrub_returnsHealedCount` — set up a tree with 3 healthy + 2 torn files. Assert returns 2.

**Build gates.**
```
./gradlew :writer:test
./gradlew :writer:build       # includes spotlessCheck + installDist
docker compose --file docker-compose.yml config > /tmp/dc-merged.yml
bash -n tests/chaos/common.sh
```

CI runs these via `./gradlew build`. The chaos suite is end-to-end and runs separately.

## Acceptance criteria

- `./gradlew :writer:build` BUILD SUCCESSFUL.
- `KafkaConsumerLoopHoldPauseTest` and `ZstdTailScrubberTest` both pass with all sub-tests green.
- Pre-existing tests (`OffsetCommitCoordinatorHoldIntegrationTest`, `OffsetCommitCoordinatorTest`, controller tests, etc.) still pass.
- `docker compose --file docker-compose.yml config` parses without error and the merged output shows `deploy.resources.limits.memory: 768M` and `JAVA_TOOL_OPTIONS: "-Xmx512m -Xms256m"` on the writer service.
- `tests/chaos/common.sh` syntax-checks via `bash -n`.
- After this branch merges, a chaos-02 run is expected to: emit `LIFECYCLE WRITER_KAFKA_CONSUMPTION_PAUSED` after hold entry; emit `LIFECYCLE WRITER_KAFKA_CONSUMPTION_RESUMED` after hold exit; the writer JVM does NOT die; the writer's shutdown via `dc stop writer` runs cleanly; verify reports `ERRORS=0` (legitimate `disk_full_hold` gap envelopes are still expected and need a separate chaos-script assertion tweak, out of scope here). End-to-end validation is the operator's call, not part of this branch's CI gate.

## Out of scope (deliberate)

- Modifying `BufferManager` to enforce a max-bytes cap. The Kafka pause makes this unnecessary.
- Modifying `DurableAppender.appendAndFsync`'s truncate-on-IOException path. Already correct.
- Wiring controllers into `commitSealedHour` or `commitBeforeRevoke` (known limitation E4 from the prior wiring spec).
- Pausing the `BackupTailConsumer`. It uses `auto.offset.reset=latest` and is liveness-only; never accumulates.
- Tuning the 30 s probe interval, the 50 MiB recovery threshold, the 3-failure PG threshold, or the `MAX_POLL_RECORDS_CONFIG=500`.
- Adjusting chaos-02's `expect_no_gaps_check` assertion to allow `disk_full_hold` envelopes — that is a chaos-test script change that belongs on a separate (chaos-02-specific) follow-up branch.
- Re-running the chaos suite as part of this branch's CI gate.
