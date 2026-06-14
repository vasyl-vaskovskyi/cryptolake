# Walking Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an end-to-end CryptoPanner pipeline that captures `trade` frames for `btcusdt` from a mock Binance WS server, seals them into an hourly file, uploads to MinIO, and validates with a basic `cryptopanner-verify verify` command — exiting with `ERRORS=0`.

**Architecture:** Single-symbol, single-stream, single-node. No hot-swap, no rotation, no Monitor, no Node Agent, no REST backfill, no sequence-ID validation. Wall-clock minute bucketing (server-event-time bucketing is post-skeleton work). The skeleton runs against a Python mock that replays a captured fixture; MinIO stands in for IONOS S3. Success is one sealed hourly file + sidecar + manifest in S3, verified by the audit CLI.

**Tech Stack:**
- Java 21, Maven (parent + 7 modules already in place)
- `java.net.http.HttpClient` for WebSocket client (no external WS library)
- `zstd-jni` for compression
- `jackson-databind` + `jackson-dataformat-yaml` for JSON / YAML
- AWS SDK v2 (`software.amazon.awssdk:s3`) for S3
- `picocli` for the verify CLI
- Python 3 with `websockets` library for the mock-binance-ws (dev-only)
- JUnit 5 for unit tests; shell smoke test for end-to-end

**Scope explicitly excluded from this plan:**
- Hot-swap deploy mechanics (design doc Variant A)
- WS rotation (design doc Variant B)
- Node Agent (`/status`, `/metrics`, `/restart`, `/rotation/trigger`)
- Monitor + dashboard + alerting
- REST backfill at hourly merge
- Sequence-ID validation
- `.fs-heavy.lock` (single-process pipeline, no contention)
- `active-slot` file (single Collector, no slot logic)
- `frame_buffer_window` / `seal_grace_window` (use simple wall-clock boundary)
- Server-event-time bucketing (use local wall-clock receive time for skeleton)
- Per-slot heartbeats and structured log events from §11.e
- `manifest_schema_version` evolution machinery (just write version=1)
- Most §10.d manifest fields (only `minutes_present`, `minutes_missing`, `file_sha256`, `file_size_bytes`, `record_count`)

These are deliberately deferred. The skeleton validates that the **core pipeline** — capture → write → seal → upload → verify — works end-to-end before we add complexity.

## File Structure

The skeleton creates files in 5 of the 7 Maven modules. `agent` and `monitor` remain empty stubs for now.

```
cryptopanner/
├── common/src/main/java/com/cryptopanner/common/
│   ├── Paths.java              # UTC date/hour/minute → file path
│   ├── Sha256Sidecar.java      # sha256sum-style sidecar reader/writer
│   └── config/
│       └── SkeletonConfig.java # minimal YAML config record
│
├── collector/src/main/java/com/cryptopanner/collector/
│   ├── Main.java               # entry point; parses --max-runtime-s
│   ├── BinanceWsClient.java    # connects, subscribes, emits frames
│   └── MinuteSegmentWriter.java # writes raw frames into per-minute zstd files
│
├── sealer/src/main/java/com/cryptopanner/sealer/
│   ├── Main.java               # entry point; parses --symbol --stream --date --hour
│   ├── HourMerger.java         # concatenates minutes for a given hour
│   └── ManifestWriter.java     # writes skeleton manifest (5 fields)
│
├── uploader/src/main/java/com/cryptopanner/uploader/
│   ├── Main.java               # entry point; parses --symbol --stream --date --hour
│   └── S3Uploader.java         # manifest-last upload + integrity check
│
├── verify/src/main/java/com/cryptopanner/verify/
│   ├── Main.java               # picocli root
│   └── VerifyCommand.java      # `verify` subcommand
│
├── tests/mocks/binance-ws/
│   ├── mock_ws.py              # Python websockets server replays fixture
│   ├── fixture.jsonl           # ~600 lines of captured btcusdt@trade frames
│   ├── requirements.txt        # websockets library
│   └── Dockerfile              # python:3.12-slim + the mock
│
├── tests/skeleton/
│   ├── run-skeleton.sh         # end-to-end smoke test
│   └── README.md               # how to run it locally
│
└── config/dev/
    └── skeleton.yaml           # dev config (mock endpoints, MinIO creds)
```

Each Java file < 200 LOC for the skeleton (focused responsibility per file, matches the §6 invariant of holding code in one's head at once). The Maven `appassembler-maven-plugin` already configured produces `target/install/bin/<component>` launcher scripts at `mvn package` time.

---

## Task 0: Maven wrapper bootstrap

**Files:**
- Create: `cryptopanner/mvnw`, `cryptopanner/mvnw.cmd`
- Create: `cryptopanner/.mvn/wrapper/maven-wrapper.properties`

This generates the Maven wrapper so the skeleton is buildable on a developer's machine with no Maven install.

- [ ] **Step 1: Run `mvn wrapper:wrapper` to generate the wrapper**

```bash
cd /Users/vasyl/data/cryptolake/cryptopanner
mvn -N wrapper:wrapper -Dmaven=3.9.6
```

Expected: creates `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties`.

- [ ] **Step 2: Verify the wrapper works**

```bash
./mvnw --version
```

Expected: prints `Apache Maven 3.9.6` and the JDK info.

- [ ] **Step 3: Commit**

```bash
git add mvnw mvnw.cmd .mvn/
git commit -m "chore(cryptopanner): bootstrap maven wrapper (mvnw)"
```

---

## Task 1: common — Paths.java (UTC path derivation)

**Files:**
- Create: `cryptopanner/common/src/main/java/com/cryptopanner/common/Paths.java`
- Test: `cryptopanner/common/src/test/java/com/cryptopanner/common/PathsTest.java`

Produces the UTC-canonicalized paths the other modules use (master spec §6.a, §10.c). One responsibility: turn `(symbol, stream, instant)` into the right `Path`.

- [ ] **Step 1: Write the failing test**

```java
// common/src/test/java/com/cryptopanner/common/PathsTest.java
package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PathsTest {

  @Test
  void minuteSegment_buildsCanonicalUtcPath() {
    Path base = Path.of("/data/cryptopanner/segments");
    Instant t = Instant.parse("2026-06-14T14:23:47.512Z");
    Path actual = Paths.minuteSegment(base, "btcusdt", "trade", t);
    assertEquals(
        Path.of(
            "/data/cryptopanner/segments/btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst"),
        actual);
  }

  @Test
  void hourSealed_buildsCanonicalUtcPath() {
    Path base = Path.of("/data/cryptopanner/sealed");
    Instant t = Instant.parse("2026-06-14T14:23:47.512Z");
    Path actual = Paths.hourSealed(base, "btcusdt", "trade", t);
    assertEquals(
        Path.of(
            "/data/cryptopanner/sealed/btcusdt/trade/2026-06-14/hour-14.jsonl.zst"),
        actual);
  }

  @Test
  void s3Key_buildsCanonicalKey() {
    Instant t = Instant.parse("2026-06-14T14:23:47.512Z");
    String actual = Paths.s3Key("vps-fra-1", "btcusdt", "trade", t);
    assertEquals(
        "vps-fra-1/btcusdt/trade/2026-06-14/hour-14.jsonl.zst", actual);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
./mvnw -pl common test -Dtest=PathsTest
```

Expected: BUILD FAILURE — `Paths` class not found.

- [ ] **Step 3: Write the minimal implementation**

```java
// common/src/main/java/com/cryptopanner/common/Paths.java
package com.cryptopanner.common;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * UTC-canonicalized path derivation per master spec §6.a and §10.c.
 *
 * <p>All paths are computed in UTC; the caller's local timezone is irrelevant.
 */
public final class Paths {

  private static final DateTimeFormatter DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter HOUR =
      DateTimeFormatter.ofPattern("HH").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter HOUR_MINUTE =
      DateTimeFormatter.ofPattern("HH-mm").withZone(ZoneOffset.UTC);

  private Paths() {}

  /** segments/<symbol>/<stream>/<date>/minute-<HH-MM>.jsonl.zst */
  public static Path minuteSegment(Path base, String symbol, String stream, Instant t) {
    return base.resolve(symbol)
        .resolve(stream)
        .resolve(DATE.format(t))
        .resolve("minute-" + HOUR_MINUTE.format(t) + ".jsonl.zst");
  }

  /** sealed/<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst */
  public static Path hourSealed(Path base, String symbol, String stream, Instant t) {
    return base.resolve(symbol)
        .resolve(stream)
        .resolve(DATE.format(t))
        .resolve("hour-" + HOUR.format(t) + ".jsonl.zst");
  }

  /** <node-id>/<symbol>/<stream>/<date>/hour-<HH>.jsonl.zst */
  public static String s3Key(String nodeId, String symbol, String stream, Instant t) {
    return nodeId + "/" + symbol + "/" + stream + "/" + DATE.format(t) + "/hour-" + HOUR.format(t)
        + ".jsonl.zst";
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
./mvnw -pl common test -Dtest=PathsTest
```

Expected: BUILD SUCCESS, 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add common/src/main/java/com/cryptopanner/common/Paths.java \
        common/src/test/java/com/cryptopanner/common/PathsTest.java
git commit -m "feat(common): UTC-canonicalized paths for segments, sealed, S3 keys"
```

---

## Task 2: common — Sha256Sidecar.java

**Files:**
- Create: `cryptopanner/common/src/main/java/com/cryptopanner/common/Sha256Sidecar.java`
- Test: `cryptopanner/common/src/test/java/com/cryptopanner/common/Sha256SidecarTest.java`

`sha256sum`-style format per master spec §10.a: `<lowercase 64 hex>  <bare filename>\n`. Lets `sha256sum -c` work directly with no custom tooling.

- [ ] **Step 1: Write the failing test**

```java
// common/src/test/java/com/cryptopanner/common/Sha256SidecarTest.java
package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Sha256SidecarTest {

  @Test
  void computeAndWrite_producesShaSumFormat(@TempDir Path tmp) throws IOException {
    Path data = tmp.resolve("hour-14.jsonl.zst");
    Files.writeString(data, "hello\n");
    Path sidecar = tmp.resolve("hour-14.jsonl.zst.sha256");

    Sha256Sidecar.computeAndWrite(data, sidecar);

    String contents = Files.readString(sidecar);
    // sha256("hello\n") = "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"
    assertEquals(
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03  hour-14.jsonl.zst\n",
        contents);
  }

  @Test
  void readHash_extractsHexFromSidecar(@TempDir Path tmp) throws IOException {
    Path sidecar = tmp.resolve("foo.sha256");
    Files.writeString(
        sidecar, "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03  foo\n");
    assertEquals(
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        Sha256Sidecar.readHash(sidecar));
  }
}
```

- [ ] **Step 2: Run the test (should fail)**

```bash
./mvnw -pl common test -Dtest=Sha256SidecarTest
```

Expected: BUILD FAILURE — `Sha256Sidecar` not found.

- [ ] **Step 3: Write the minimal implementation**

```java
// common/src/main/java/com/cryptopanner/common/Sha256Sidecar.java
package com.cryptopanner.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * sha256sum-style sidecar format per master spec §10.a.
 *
 * <p>Content: {@code <lowercase 64 hex> + "  " + <bare filename> + "\n"}. Compatible with
 * {@code sha256sum -c} from any shell.
 */
public final class Sha256Sidecar {

  private Sha256Sidecar() {}

  /** Compute SHA-256 of {@code data} and write the sidecar atomically (write+rename). */
  public static void computeAndWrite(Path data, Path sidecar) throws IOException {
    String hex = sha256Hex(data);
    String line = hex + "  " + data.getFileName().toString() + "\n";
    Path tmp = sidecar.resolveSibling(sidecar.getFileName() + ".tmp");
    Files.writeString(tmp, line);
    // Atomic rename; on POSIX this is one syscall.
    Files.move(tmp, sidecar, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
  }

  /** Extract the hex hash from a sidecar file. */
  public static String readHash(Path sidecar) throws IOException {
    String content = Files.readString(sidecar);
    int sp = content.indexOf(' ');
    if (sp != 64) {
      throw new IOException("malformed sidecar: " + sidecar);
    }
    return content.substring(0, sp);
  }

  private static String sha256Hex(Path data) throws IOException {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
    try (InputStream in = Files.newInputStream(data)) {
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) > 0) {
        md.update(buf, 0, n);
      }
    }
    byte[] digest = md.digest();
    StringBuilder hex = new StringBuilder(64);
    for (byte b : digest) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
./mvnw -pl common test -Dtest=Sha256SidecarTest
```

Expected: BUILD SUCCESS, 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add common/src/main/java/com/cryptopanner/common/Sha256Sidecar.java \
        common/src/test/java/com/cryptopanner/common/Sha256SidecarTest.java
git commit -m "feat(common): sha256sum-style sidecar reader/writer"
```

---

## Task 3: common — SkeletonConfig.java (minimal YAML config)

**Files:**
- Create: `cryptopanner/common/src/main/java/com/cryptopanner/common/config/SkeletonConfig.java`
- Create: `cryptopanner/config/dev/skeleton.yaml`
- Test: `cryptopanner/common/src/test/java/com/cryptopanner/common/config/SkeletonConfigTest.java`

Skeleton-only config (full §15 schema is post-skeleton work). Just the fields the 4 components need.

- [ ] **Step 1: Create the dev config file**

```yaml
# config/dev/skeleton.yaml
node_id: dev-node
symbol:  btcusdt
stream:  trade

# Capture target — mock-binance-ws in the dev stack
ws_endpoint_url: ws://localhost:9001/ws

# Local filesystem layout
paths:
  segments: /tmp/cryptopanner/segments
  sealed:   /tmp/cryptopanner/sealed

# Skeleton-only knob: how long the Collector runs before exiting
collector_max_runtime_s: 180

# S3 (MinIO in dev)
storage:
  endpoint:           http://localhost:9000
  bucket:             cryptopanner-dev
  access_key:         cryptopanner
  secret_key:         changeme-dev
  region:             us-east-1
  path_style_access:  true
```

- [ ] **Step 2: Write the failing test**

```java
// common/src/test/java/com/cryptopanner/common/config/SkeletonConfigTest.java
package com.cryptopanner.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SkeletonConfigTest {

  @Test
  void parsesAllFields(@TempDir Path tmp) throws IOException {
    Path yaml = tmp.resolve("skeleton.yaml");
    Files.writeString(
        yaml,
        """
        node_id: vps-fra-1
        symbol:  btcusdt
        stream:  trade
        ws_endpoint_url: ws://mock-binance-ws:9001/ws
        paths:
          segments: /data/cryptopanner/segments
          sealed:   /data/cryptopanner/sealed
        collector_max_runtime_s: 120
        storage:
          endpoint:          http://minio:9000
          bucket:            cryptopanner-prod
          access_key:        AK
          secret_key:        SK
          region:            us-east-1
          path_style_access: true
        """);

    SkeletonConfig cfg = SkeletonConfig.load(yaml);

    assertEquals("vps-fra-1", cfg.nodeId());
    assertEquals("btcusdt", cfg.symbol());
    assertEquals("trade", cfg.stream());
    assertEquals("ws://mock-binance-ws:9001/ws", cfg.wsEndpointUrl());
    assertEquals(Path.of("/data/cryptopanner/segments"), cfg.paths().segments());
    assertEquals(Path.of("/data/cryptopanner/sealed"), cfg.paths().sealed());
    assertEquals(120, cfg.collectorMaxRuntimeS());
    assertEquals("http://minio:9000", cfg.storage().endpoint());
    assertEquals("cryptopanner-prod", cfg.storage().bucket());
    assertEquals(true, cfg.storage().pathStyleAccess());
  }
}
```

- [ ] **Step 3: Run the test (should fail)**

```bash
./mvnw -pl common test -Dtest=SkeletonConfigTest
```

Expected: BUILD FAILURE — `SkeletonConfig` not found.

- [ ] **Step 4: Write the minimal implementation**

```java
// common/src/main/java/com/cryptopanner/common/config/SkeletonConfig.java
package com.cryptopanner.common.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Skeleton-only YAML config. Replaced by the full §15 schema once we add hot-swap, rotation,
 * Monitor, Agent, etc.
 */
public record SkeletonConfig(
    String nodeId,
    String symbol,
    String stream,
    String wsEndpointUrl,
    Paths paths,
    int collectorMaxRuntimeS,
    Storage storage) {

  public record Paths(Path segments, Path sealed) {}

  public record Storage(
      String endpoint,
      String bucket,
      String accessKey,
      String secretKey,
      String region,
      boolean pathStyleAccess) {}

  public static SkeletonConfig load(Path yaml) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    return mapper.readValue(yaml.toFile(), SkeletonConfig.class);
  }
}
```

- [ ] **Step 5: Run the test (should pass)**

```bash
./mvnw -pl common test -Dtest=SkeletonConfigTest
```

Expected: BUILD SUCCESS, 1 test passes.

- [ ] **Step 6: Commit**

```bash
git add common/src/main/java/com/cryptopanner/common/config/SkeletonConfig.java \
        common/src/test/java/com/cryptopanner/common/config/SkeletonConfigTest.java \
        config/dev/skeleton.yaml
git commit -m "feat(common): minimal YAML config for the skeleton"
```

---

## Task 4: tests/mocks/binance-ws — fixture + Python server

**Files:**
- Create: `cryptopanner/tests/mocks/binance-ws/fixture.jsonl`
- Create: `cryptopanner/tests/mocks/binance-ws/mock_ws.py`
- Create: `cryptopanner/tests/mocks/binance-ws/requirements.txt`
- Create: `cryptopanner/tests/mocks/binance-ws/Dockerfile`

A WS server that replays a small fixture of `btcusdt@trade` frames. Each connection gets the full fixture replayed at a configurable rate.

- [ ] **Step 1: Create the fixture**

```bash
mkdir -p tests/mocks/binance-ws
cat > tests/mocks/binance-ws/fixture.jsonl <<'EOF'
{"stream":"btcusdt@trade","data":{"e":"trade","E":1718380800000,"s":"BTCUSDT","t":3500000001,"p":"60001.00","q":"0.001","T":1718380799990,"m":false}}
{"stream":"btcusdt@trade","data":{"e":"trade","E":1718380800100,"s":"BTCUSDT","t":3500000002,"p":"60001.10","q":"0.002","T":1718380800050,"m":true}}
{"stream":"btcusdt@trade","data":{"e":"trade","E":1718380800200,"s":"BTCUSDT","t":3500000003,"p":"60001.20","q":"0.005","T":1718380800150,"m":false}}
EOF
# Synthetically extend to 600 lines with monotonically increasing trade IDs and timestamps,
# spaced ~100ms apart. The exact contents don't matter — the skeleton only checks that what
# the Collector receives is what the Sealer + Uploader pass through.
awk 'BEGIN{ for(i=4; i<=600; i++) {
  E = 1718380800000 + i*100; T = E - 10;
  printf "{\"stream\":\"btcusdt@trade\",\"data\":{\"e\":\"trade\",\"E\":%d,\"s\":\"BTCUSDT\",\"t\":%d,\"p\":\"60001.00\",\"q\":\"0.001\",\"T\":%d,\"m\":false}}\n", E, 3500000000+i, T;
}}' >> tests/mocks/binance-ws/fixture.jsonl
wc -l tests/mocks/binance-ws/fixture.jsonl
```

Expected: `600 tests/mocks/binance-ws/fixture.jsonl`.

- [ ] **Step 2: Create the requirements**

```bash
cat > tests/mocks/binance-ws/requirements.txt <<'EOF'
websockets==12.0
EOF
```

- [ ] **Step 3: Create the mock WS server**

```python
# tests/mocks/binance-ws/mock_ws.py
"""
Minimal Binance USD-M Futures WS mock.

Behavior:
- Accepts a single combined-streams WS connection on /ws.
- Reads a SUBSCRIBE message of the form {"method":"SUBSCRIBE","params":[...],"id":<int>}.
- Responds with {"result":null,"id":<id>}.
- Replays fixture.jsonl line-by-line at ~10 lines/second.
- Loops the fixture until the client disconnects.
"""
import asyncio
import json
import os
import sys
from pathlib import Path

import websockets


FIXTURE = Path(__file__).parent / "fixture.jsonl"
REPLAY_RATE_HZ = float(os.environ.get("REPLAY_RATE_HZ", "10"))


async def serve(ws):
    print(f"[mock-ws] client connected: {ws.remote_address}", flush=True)
    try:
        sub = await asyncio.wait_for(ws.recv(), timeout=10.0)
    except asyncio.TimeoutError:
        print("[mock-ws] no SUBSCRIBE within 10s; closing", flush=True)
        return
    try:
        sub_obj = json.loads(sub)
    except json.JSONDecodeError:
        print(f"[mock-ws] non-JSON SUBSCRIBE: {sub!r}", flush=True)
        return
    if sub_obj.get("method") != "SUBSCRIBE":
        print(f"[mock-ws] first message not SUBSCRIBE: {sub_obj}", flush=True)
        return
    await ws.send(json.dumps({"result": None, "id": sub_obj.get("id")}))
    print(f"[mock-ws] subscribed to {len(sub_obj.get('params', []))} streams", flush=True)

    lines = FIXTURE.read_text().splitlines()
    delay = 1.0 / REPLAY_RATE_HZ
    while True:
        for line in lines:
            await ws.send(line)
            await asyncio.sleep(delay)


async def main():
    host = os.environ.get("MOCK_WS_HOST", "0.0.0.0")
    port = int(os.environ.get("MOCK_WS_PORT", "9001"))
    print(f"[mock-ws] listening on {host}:{port}", flush=True)
    async with websockets.serve(serve, host, port):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[mock-ws] shutting down", flush=True)
        sys.exit(0)
```

- [ ] **Step 4: Create the Dockerfile**

```dockerfile
# tests/mocks/binance-ws/Dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY mock_ws.py fixture.jsonl ./
EXPOSE 9001
CMD ["python", "mock_ws.py"]
```

- [ ] **Step 5: Smoke-test the mock locally**

```bash
cd tests/mocks/binance-ws
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python mock_ws.py &
MOCK_PID=$!
sleep 1
# Send a SUBSCRIBE and read a few frames
python3 - <<'PY'
import asyncio, json, websockets

async def go():
    async with websockets.connect("ws://localhost:9001/ws") as ws:
        await ws.send(json.dumps({"method": "SUBSCRIBE", "params": ["btcusdt@trade"], "id": 1}))
        ack = await ws.recv()
        print("ACK:", ack)
        for _ in range(3):
            print("FRAME:", await ws.recv())

asyncio.run(go())
PY
kill $MOCK_PID
deactivate
```

Expected:
```
ACK: {"result": null, "id": 1}
FRAME: {"stream":"btcusdt@trade","data":...}
FRAME: ...
FRAME: ...
```

- [ ] **Step 6: Commit**

```bash
git add tests/mocks/binance-ws/
git commit -m "feat(mock): Python websockets server replaying btcusdt@trade fixture"
```

---

## Task 5: collector — BinanceWsClient.java

**Files:**
- Create: `cryptopanner/collector/src/main/java/com/cryptopanner/collector/BinanceWsClient.java`
- Test: `cryptopanner/collector/src/test/java/com/cryptopanner/collector/BinanceWsClientTest.java`

WS client built on `java.net.http.HttpClient`'s `WebSocket` (Java 11+, no external dep). Connects, sends SUBSCRIBE, waits for ACK, emits each subsequent text frame as a raw string to a consumer.

- [ ] **Step 1: Write the failing test**

```java
// collector/src/test/java/com/cryptopanner/collector/BinanceWsClientTest.java
package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
// Light WS server using Java 21's built-in HttpServer + a tiny WS upgrade.
// In practice, the test launches the Python mock or an in-process Java equivalent.
// For brevity here, the test uses `TinyWsServer` (defined in test/util) that echoes a fixed script.
import com.cryptopanner.collector.testutil.TinyWsServer;

class BinanceWsClientTest {

  private TinyWsServer server;

  @BeforeEach
  void setUp() throws Exception {
    server =
        TinyWsServer.start(
            new InetSocketAddress("127.0.0.1", 0),
            List.of(
                "{\"result\":null,\"id\":1}",
                "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":1}}",
                "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":2}}",
                "{\"stream\":\"btcusdt@trade\",\"data\":{\"t\":3}}"));
  }

  @AfterEach
  void tearDown() throws Exception {
    server.close();
  }

  @Test
  void connectsSubscribesAndEmitsFrames() throws Exception {
    URI uri = URI.create("ws://127.0.0.1:" + server.port() + "/ws");
    CopyOnWriteArrayList<String> seen = new CopyOnWriteArrayList<>();
    BinanceWsClient client =
        new BinanceWsClient(uri, List.of("btcusdt@trade"), seen::add);

    client.start();
    // The server scripts 4 messages; wait until we see the 3 trade frames.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (seen.size() < 3 && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    client.stop();

    assertEquals(3, seen.size());
    assertTrue(seen.get(0).contains("\"t\":1"));
    assertTrue(seen.get(2).contains("\"t\":3"));
  }
}
```

- [ ] **Step 2: Create the `TinyWsServer` test utility**

```java
// collector/src/test/java/com/cryptopanner/collector/testutil/TinyWsServer.java
package com.cryptopanner.collector.testutil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

/**
 * Bare-bones single-client WebSocket server for tests. Performs the RFC 6455 handshake, then sends
 * each line of {@code script} as a text frame, then waits for client close. Not for production.
 */
public final class TinyWsServer implements AutoCloseable {

  private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

  private final ServerSocket server;
  private final ExecutorService exec;
  private final List<String> script;

  private TinyWsServer(ServerSocket server, List<String> script) {
    this.server = server;
    this.script = script;
    this.exec = Executors.newSingleThreadExecutor();
    exec.submit(this::accept);
  }

  public static TinyWsServer start(InetSocketAddress addr, List<String> script) throws IOException {
    ServerSocket s = new ServerSocket();
    s.bind(addr);
    return new TinyWsServer(s, script);
  }

  public int port() {
    return server.getLocalPort();
  }

  @Override
  public void close() throws IOException {
    exec.shutdownNow();
    server.close();
  }

  private void accept() {
    try (Socket sock = server.accept();
        InputStream in = sock.getInputStream();
        OutputStream out = sock.getOutputStream()) {
      // Read HTTP request, capture Sec-WebSocket-Key
      byte[] buf = new byte[4096];
      int n = in.read(buf);
      String req = new String(buf, 0, n, StandardCharsets.US_ASCII);
      String key = req.lines()
          .filter(l -> l.toLowerCase().startsWith("sec-websocket-key:"))
          .map(l -> l.substring(l.indexOf(':') + 1).trim())
          .findFirst()
          .orElseThrow();
      // Handshake response
      MessageDigest sha = MessageDigest.getInstance("SHA-1");
      String accept = Base64.getEncoder().encodeToString(sha.digest((key + GUID).getBytes(StandardCharsets.US_ASCII)));
      String resp =
          "HTTP/1.1 101 Switching Protocols\r\n"
              + "Upgrade: websocket\r\n"
              + "Connection: Upgrade\r\n"
              + "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";
      out.write(resp.getBytes(StandardCharsets.US_ASCII));
      out.flush();
      // Skip the client's SUBSCRIBE (optional for the skeleton test).
      // Then push the script.
      for (String msg : script) {
        sendText(out, msg);
      }
      // Block until close.
      while (in.read() != -1) {}
    } catch (Exception ignored) {
      // server closing
    }
  }

  private static void sendText(OutputStream out, String s) throws IOException {
    byte[] payload = s.getBytes(StandardCharsets.UTF_8);
    out.write(0x81); // FIN + opcode text
    if (payload.length < 126) {
      out.write(payload.length);
    } else if (payload.length < 65536) {
      out.write(126);
      out.write((payload.length >>> 8) & 0xff);
      out.write(payload.length & 0xff);
    } else {
      out.write(127);
      for (int i = 7; i >= 0; i--) {
        out.write((int) ((payload.length >>> (i * 8)) & 0xff));
      }
    }
    out.write(payload);
    out.flush();
  }
}
```

- [ ] **Step 3: Run the test (should fail)**

```bash
./mvnw -pl collector -am test -Dtest=BinanceWsClientTest
```

Expected: BUILD FAILURE — `BinanceWsClient` not found.

- [ ] **Step 4: Write the implementation**

```java
// collector/src/main/java/com/cryptopanner/collector/BinanceWsClient.java
package com.cryptopanner.collector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * WebSocket client for Binance combined-streams. Skeleton-only: no reconnect, no rotation, no
 * back-pressure. Suitable for the smoke test against the Python mock.
 */
public final class BinanceWsClient {

  private final URI endpoint;
  private final List<String> streams;
  private final Consumer<String> onFrame;
  private final AtomicInteger nextId = new AtomicInteger(1);
  private volatile WebSocket ws;

  public BinanceWsClient(URI endpoint, List<String> streams, Consumer<String> onFrame) {
    this.endpoint = endpoint;
    this.streams = streams;
    this.onFrame = onFrame;
  }

  public void start() throws Exception {
    HttpClient http = HttpClient.newHttpClient();
    int subscribeId = nextId.getAndIncrement();
    CompletableFuture<Void> ackSeen = new CompletableFuture<>();

    WebSocket.Listener listener =
        new WebSocket.Listener() {
          private final StringBuilder buf = new StringBuilder();

          @Override
          public void onOpen(WebSocket webSocket) {
            ws = webSocket;
            String sub =
                "{\"method\":\"SUBSCRIBE\",\"params\":[\""
                    + String.join("\",\"", streams)
                    + "\"],\"id\":"
                    + subscribeId
                    + "}";
            ws.sendText(sub, true);
            ws.request(1);
          }

          @Override
          public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buf.append(data);
            if (last) {
              String full = buf.toString();
              buf.setLength(0);
              if (!ackSeen.isDone() && full.contains("\"id\":" + subscribeId)) {
                ackSeen.complete(null);
              } else {
                onFrame.accept(full);
              }
            }
            webSocket.request(1);
            return null;
          }
        };

    ws =
        http.newWebSocketBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .buildAsync(endpoint, listener)
            .get();
    ackSeen.orTimeout(10, java.util.concurrent.TimeUnit.SECONDS).join();
  }

  public void stop() {
    WebSocket w = ws;
    if (w != null) {
      w.sendClose(WebSocket.NORMAL_CLOSURE, "bye").orTimeout(2, java.util.concurrent.TimeUnit.SECONDS);
    }
  }
}
```

- [ ] **Step 5: Run the test (should pass)**

```bash
./mvnw -pl collector -am test -Dtest=BinanceWsClientTest
```

Expected: BUILD SUCCESS, 1 test passes.

- [ ] **Step 6: Commit**

```bash
git add collector/src/main/java/com/cryptopanner/collector/BinanceWsClient.java \
        collector/src/test/java/com/cryptopanner/collector/BinanceWsClientTest.java \
        collector/src/test/java/com/cryptopanner/collector/testutil/TinyWsServer.java
git commit -m "feat(collector): minimal WebSocket client with SUBSCRIBE/ACK gate"
```

---

## Task 6: collector — MinuteSegmentWriter.java

**Files:**
- Create: `cryptopanner/collector/src/main/java/com/cryptopanner/collector/MinuteSegmentWriter.java`
- Test: `cryptopanner/collector/src/test/java/com/cryptopanner/collector/MinuteSegmentWriterTest.java`

Writes raw frames into `minute-HH-MM.jsonl.zst` files, rotating at local-wall-clock minute boundaries. At rotation, fsync + sidecar. Skeleton-only: uses **local receive time** for bucketing (not server-event-time), no frame-buffer / seal-grace windows, no late-frame handling.

- [ ] **Step 1: Write the failing test**

```java
// collector/src/test/java/com/cryptopanner/collector/MinuteSegmentWriterTest.java
package com.cryptopanner.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MinuteSegmentWriterTest {

  /** A clock that returns whatever the caller sets via {@link #set(Instant)}. */
  static final class StubClock extends Clock {
    private volatile Instant now = Instant.parse("2026-06-14T14:23:00Z");
    void set(Instant i) { this.now = i; }
    @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
    @Override public Clock withZone(java.time.ZoneId z) { return this; }
    @Override public Instant instant() { return now; }
  }

  @Test
  void writesAndRotatesAtMinuteBoundary(@TempDir Path base) throws IOException {
    StubClock clock = new StubClock();
    try (MinuteSegmentWriter w = new MinuteSegmentWriter(base, "btcusdt", "trade", clock)) {
      clock.set(Instant.parse("2026-06-14T14:23:10Z"));
      w.accept("frame-a-1\n".getBytes());
      w.accept("frame-a-2\n".getBytes());

      // Cross the minute boundary.
      clock.set(Instant.parse("2026-06-14T14:24:01Z"));
      w.accept("frame-b-1\n".getBytes());
    }

    Path minute23 =
        base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst");
    Path minute24 =
        base.resolve("btcusdt/trade/2026-06-14/minute-14-24.jsonl.zst");
    assertTrue(Files.exists(minute23), "minute-14-23 file missing");
    assertTrue(Files.exists(minute24), "minute-14-24 file missing");
    assertTrue(
        Files.exists(base.resolve("btcusdt/trade/2026-06-14/minute-14-23.jsonl.zst.sha256")));

    // Decompress and verify content.
    byte[] decompressed23 = decompress(Files.readAllBytes(minute23));
    assertEquals("frame-a-1\nframe-a-2\n", new String(decompressed23));
    byte[] decompressed24 = decompress(Files.readAllBytes(minute24));
    assertEquals("frame-b-1\n", new String(decompressed24));
  }

  private static byte[] decompress(byte[] zstd) {
    long size = Zstd.decompressedSize(zstd);
    byte[] out = new byte[(int) size];
    Zstd.decompress(out, zstd);
    return out;
  }
}
```

- [ ] **Step 2: Run the test (should fail)**

```bash
./mvnw -pl collector -am test -Dtest=MinuteSegmentWriterTest
```

Expected: BUILD FAILURE — `MinuteSegmentWriter` not found.

- [ ] **Step 3: Write the implementation**

```java
// collector/src/main/java/com/cryptopanner/collector/MinuteSegmentWriter.java
package com.cryptopanner.collector;

import com.cryptopanner.common.Paths;
import com.cryptopanner.common.Sha256Sidecar;
import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Writes raw frames into per-minute zstd files. Skeleton-only:
 *
 * <ul>
 *   <li>Bucket by <em>local receive time</em>, not server-event-time (post-skeleton work).
 *   <li>No frame-buffer / seal-grace windows.
 *   <li>One zstd frame per minute file (re-compresses at rotation).
 * </ul>
 */
public final class MinuteSegmentWriter implements AutoCloseable {

  private static final DateTimeFormatter MINUTE_KEY =
      DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(ZoneOffset.UTC);

  private final Path baseSegments;
  private final String symbol;
  private final String stream;
  private final Clock clock;

  private String currentMinuteKey;
  private ByteArrayOutputStream buffer;

  public MinuteSegmentWriter(Path baseSegments, String symbol, String stream, Clock clock) {
    this.baseSegments = baseSegments;
    this.symbol = symbol;
    this.stream = stream;
    this.clock = clock;
  }

  /** Accept one raw frame (caller must include trailing LF). */
  public synchronized void accept(byte[] frame) throws IOException {
    Instant now = clock.instant();
    String minuteKey = MINUTE_KEY.format(now);
    if (currentMinuteKey != null && !minuteKey.equals(currentMinuteKey)) {
      sealCurrent();
    }
    if (currentMinuteKey == null) {
      currentMinuteKey = minuteKey;
      buffer = new ByteArrayOutputStream();
    }
    buffer.write(frame);
  }

  @Override
  public synchronized void close() throws IOException {
    sealCurrent();
  }

  private void sealCurrent() throws IOException {
    if (currentMinuteKey == null || buffer.size() == 0) {
      currentMinuteKey = null;
      buffer = null;
      return;
    }
    Instant minuteInstant = minuteInstantFromKey(currentMinuteKey);
    Path dataPath = Paths.minuteSegment(baseSegments, symbol, stream, minuteInstant);
    Files.createDirectories(dataPath.getParent());

    byte[] raw = buffer.toByteArray();
    byte[] compressed = Zstd.compress(raw, 3);
    Path tmp = dataPath.resolveSibling(dataPath.getFileName() + ".tmp");
    try (FileChannel ch =
        FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      ch.write(java.nio.ByteBuffer.wrap(compressed));
      ch.force(true);
    }
    Files.move(tmp, dataPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE);

    Path sidecar = dataPath.resolveSibling(dataPath.getFileName() + ".sha256");
    Sha256Sidecar.computeAndWrite(dataPath, sidecar);

    currentMinuteKey = null;
    buffer = null;
  }

  private static Instant minuteInstantFromKey(String key) {
    int y = Integer.parseInt(key.substring(0, 4));
    int mo = Integer.parseInt(key.substring(4, 6));
    int d = Integer.parseInt(key.substring(6, 8));
    int h = Integer.parseInt(key.substring(8, 10));
    int mi = Integer.parseInt(key.substring(10, 12));
    return java.time.LocalDateTime.of(y, mo, d, h, mi).toInstant(ZoneOffset.UTC);
  }
}
```

- [ ] **Step 4: Run the test (should pass)**

```bash
./mvnw -pl collector -am test -Dtest=MinuteSegmentWriterTest
```

Expected: BUILD SUCCESS, 1 test passes.

- [ ] **Step 5: Commit**

```bash
git add collector/src/main/java/com/cryptopanner/collector/MinuteSegmentWriter.java \
        collector/src/test/java/com/cryptopanner/collector/MinuteSegmentWriterTest.java
git commit -m "feat(collector): minute-segment writer with zstd + sha256 sidecar"
```

---

## Task 7: collector — Main.java (wire WS client + writer)

**Files:**
- Create: `cryptopanner/collector/src/main/java/com/cryptopanner/collector/Main.java`

Wires `BinanceWsClient` → `MinuteSegmentWriter`, runs for `collector_max_runtime_s` seconds, then cleanly shuts down.

- [ ] **Step 1: Write the entry point**

```java
// collector/src/main/java/com/cryptopanner/collector/Main.java
package com.cryptopanner.collector;

import com.cryptopanner.common.config.SkeletonConfig;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class Main {

  public static void main(String[] args) throws Exception {
    Path configPath =
        Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    SkeletonConfig cfg = SkeletonConfig.load(configPath);

    AtomicLong framesSeen = new AtomicLong();
    MinuteSegmentWriter writer =
        new MinuteSegmentWriter(cfg.paths().segments(), cfg.symbol(), cfg.stream(), Clock.systemUTC());

    BinanceWsClient client =
        new BinanceWsClient(
            URI.create(cfg.wsEndpointUrl()),
            List.of(cfg.symbol() + "@" + cfg.stream()),
            frame -> {
              try {
                writer.accept((frame + "\n").getBytes(StandardCharsets.UTF_8));
                long n = framesSeen.incrementAndGet();
                if (n % 100 == 0) {
                  System.out.println("[collector] frames seen: " + n);
                }
              } catch (Exception e) {
                System.err.println("[collector] write error: " + e.getMessage());
              }
            });

    client.start();
    System.out.println("[collector] started; running for " + cfg.collectorMaxRuntimeS() + "s");

    Thread.sleep(cfg.collectorMaxRuntimeS() * 1000L);

    System.out.println("[collector] stopping after " + framesSeen.get() + " frames");
    client.stop();
    writer.close();
    System.out.println("[collector] done");
  }
}
```

- [ ] **Step 2: Build and verify it compiles**

```bash
./mvnw -pl collector -am package -DskipTests
```

Expected: BUILD SUCCESS. `collector/target/install/bin/collector` exists.

- [ ] **Step 3: Commit**

```bash
git add collector/src/main/java/com/cryptopanner/collector/Main.java
git commit -m "feat(collector): main entry point wiring WS client + segment writer"
```

---

## Task 8: sealer — HourMerger.java

**Files:**
- Create: `cryptopanner/sealer/src/main/java/com/cryptopanner/sealer/HourMerger.java`
- Test: `cryptopanner/sealer/src/test/java/com/cryptopanner/sealer/HourMergerTest.java`

Reads all `minute-HH-MM.jsonl.zst` files for a given hour, concatenates their decompressed content in minute-filename order, recompresses as `hour-HH.jsonl.zst`, writes sidecar.

- [ ] **Step 1: Write the failing test**

```java
// sealer/src/test/java/com/cryptopanner/sealer/HourMergerTest.java
package com.cryptopanner.sealer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HourMergerTest {

  @Test
  void mergesMinutesInOrder(@TempDir Path tmp) throws IOException {
    Path segments = tmp.resolve("segments");
    Path sealed = tmp.resolve("sealed");
    Path minDir = segments.resolve("btcusdt/trade/2026-06-14");
    Files.createDirectories(minDir);
    writeMinute(minDir, "minute-14-00.jsonl.zst", "a1\na2\n");
    writeMinute(minDir, "minute-14-01.jsonl.zst", "b1\n");
    writeMinute(minDir, "minute-14-02.jsonl.zst", "c1\nc2\nc3\n");

    HourMerger merger = new HourMerger(segments, sealed);
    HourMerger.Result result =
        merger.mergeHour("btcusdt", "trade", Instant.parse("2026-06-14T14:00:00Z"));

    Path expectedFile =
        sealed.resolve("btcusdt/trade/2026-06-14/hour-14.jsonl.zst");
    assertTrue(Files.exists(expectedFile));
    assertTrue(Files.exists(expectedFile.resolveSibling("hour-14.jsonl.zst.sha256")));

    byte[] decompressed = decompress(Files.readAllBytes(expectedFile));
    assertEquals("a1\na2\nb1\nc1\nc2\nc3\n", new String(decompressed));
    assertEquals(6, result.recordCount());
    assertEquals(List.of(0, 1, 2), result.minutesPresent());
  }

  private static void writeMinute(Path dir, String name, String content) throws IOException {
    byte[] z = Zstd.compress(content.getBytes(), 3);
    Files.write(dir.resolve(name), z);
  }

  private static byte[] decompress(byte[] zstd) {
    long size = Zstd.decompressedSize(zstd);
    byte[] out = new byte[(int) size];
    Zstd.decompress(out, zstd);
    return out;
  }
}
```

- [ ] **Step 2: Run the test (should fail)**

```bash
./mvnw -pl sealer -am test -Dtest=HourMergerTest
```

Expected: BUILD FAILURE.

- [ ] **Step 3: Write the implementation**

```java
// sealer/src/main/java/com/cryptopanner/sealer/HourMerger.java
package com.cryptopanner.sealer;

import com.cryptopanner.common.Paths;
import com.cryptopanner.common.Sha256Sidecar;
import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads minute-segment files for one hour and produces one sealed hour file. Skeleton-only: no
 * backfill, no sequence-ID validation, no event arrays.
 */
public final class HourMerger {

  private static final Pattern MINUTE_NAME = Pattern.compile("minute-(\\d{2})-(\\d{2})\\.jsonl\\.zst");
  private static final DateTimeFormatter DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter HOUR =
      DateTimeFormatter.ofPattern("HH").withZone(ZoneOffset.UTC);

  private final Path baseSegments;
  private final Path baseSealed;

  public HourMerger(Path baseSegments, Path baseSealed) {
    this.baseSegments = baseSegments;
    this.baseSealed = baseSealed;
  }

  /** Result of one merge, used by the manifest writer. */
  public record Result(
      Path file, String sha256Hex, long fileSizeBytes, long recordCount, List<Integer> minutesPresent) {}

  public Result mergeHour(String symbol, String stream, Instant hourStart) throws IOException {
    String date = DATE.format(hourStart);
    String hour = HOUR.format(hourStart);
    Path segDir = baseSegments.resolve(symbol).resolve(stream).resolve(date);
    if (!Files.isDirectory(segDir)) {
      throw new IOException("no segments directory: " + segDir);
    }
    List<Path> minutesInHour = new ArrayList<>();
    try (DirectoryStream<Path> ds = Files.newDirectoryStream(segDir, "minute-*.jsonl.zst")) {
      for (Path p : ds) {
        Matcher m = MINUTE_NAME.matcher(p.getFileName().toString());
        if (m.matches() && m.group(1).equals(hour)) {
          minutesInHour.add(p);
        }
      }
    }
    minutesInHour.sort((a, b) -> a.getFileName().compareTo(b.getFileName()));

    ByteArrayOutputStream merged = new ByteArrayOutputStream();
    List<Integer> present = new ArrayList<>();
    long records = 0;
    for (Path p : minutesInHour) {
      byte[] compressed = Files.readAllBytes(p);
      long size = Zstd.decompressedSize(compressed);
      byte[] raw = new byte[(int) size];
      Zstd.decompress(raw, compressed);
      merged.write(raw);
      for (byte b : raw) {
        if (b == '\n') records++;
      }
      Matcher m = MINUTE_NAME.matcher(p.getFileName().toString());
      m.matches();
      present.add(Integer.parseInt(m.group(2)));
    }

    byte[] mergedBytes = merged.toByteArray();
    byte[] compressedOut = Zstd.compress(mergedBytes, 3);

    Path out = Paths.hourSealed(baseSealed, symbol, stream, hourStart);
    Files.createDirectories(out.getParent());
    Path tmp = out.resolveSibling(out.getFileName() + ".tmp");
    try (FileChannel ch =
        FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      ch.write(java.nio.ByteBuffer.wrap(compressedOut));
      ch.force(true);
    }
    Files.move(tmp, out, java.nio.file.StandardCopyOption.ATOMIC_MOVE);

    Path sidecar = out.resolveSibling(out.getFileName() + ".sha256");
    Sha256Sidecar.computeAndWrite(out, sidecar);

    return new Result(out, Sha256Sidecar.readHash(sidecar), Files.size(out), records, present);
  }
}
```

- [ ] **Step 4: Run the test (should pass)**

```bash
./mvnw -pl sealer -am test -Dtest=HourMergerTest
```

Expected: BUILD SUCCESS, 1 test passes.

- [ ] **Step 5: Commit**

```bash
git add sealer/src/main/java/com/cryptopanner/sealer/HourMerger.java \
        sealer/src/test/java/com/cryptopanner/sealer/HourMergerTest.java
git commit -m "feat(sealer): hourly merge of minute-segment files"
```

---

## Task 9: sealer — ManifestWriter.java

**Files:**
- Create: `cryptopanner/sealer/src/main/java/com/cryptopanner/sealer/ManifestWriter.java`
- Test: `cryptopanner/sealer/src/test/java/com/cryptopanner/sealer/ManifestWriterTest.java`

Writes the skeleton manifest (5 fields). Full §10.d schema is post-skeleton work.

- [ ] **Step 1: Write the failing test**

```java
// sealer/src/test/java/com/cryptopanner/sealer/ManifestWriterTest.java
package com.cryptopanner.sealer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManifestWriterTest {

  @Test
  void writesPrettyPrintedManifestWithExpectedFields(@TempDir Path tmp) throws IOException {
    Path target = tmp.resolve("hour-14.manifest.json");
    HourMerger.Result merge =
        new HourMerger.Result(
            tmp.resolve("hour-14.jsonl.zst"),
            "deadbeef".repeat(8),
            12345L,
            42L,
            List.of(0, 1, 2));

    ManifestWriter.write(
        target,
        "dev-node",
        "btcusdt",
        "trade",
        Instant.parse("2026-06-14T14:00:00Z"),
        merge,
        Instant.parse("2026-06-14T15:02:08Z"));

    String contents = Files.readString(target);
    assertTrue(contents.contains("\n"), "must be pretty-printed");
    JsonNode root = new ObjectMapper().readTree(contents);
    assertEquals(1, root.get("manifest_schema_version").asInt());
    assertEquals("dev-node", root.get("node").asText());
    assertEquals("btcusdt", root.get("symbol").asText());
    assertEquals("trade", root.get("stream").asText());
    assertEquals("2026-06-14", root.get("date").asText());
    assertEquals(14, root.get("hour").asInt());
    assertEquals(3, root.get("minutes_present").size());
    assertEquals(42L, root.get("record_count").asLong());
  }
}
```

- [ ] **Step 2: Run the test (should fail)**

```bash
./mvnw -pl sealer -am test -Dtest=ManifestWriterTest
```

Expected: BUILD FAILURE.

- [ ] **Step 3: Write the implementation**

```java
// sealer/src/main/java/com/cryptopanner/sealer/ManifestWriter.java
package com.cryptopanner.sealer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** Skeleton manifest writer. Full §10.d fields are post-skeleton work. */
public final class ManifestWriter {

  private static final DateTimeFormatter DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

  private ManifestWriter() {}

  public static void write(
      Path target,
      String node,
      String symbol,
      String stream,
      Instant hourStart,
      HourMerger.Result merge,
      Instant sealedAt)
      throws IOException {
    ObjectMapper mapper =
        new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    ObjectNode root = mapper.createObjectNode();
    root.put("manifest_schema_version", 1);
    root.put("node", node);
    root.put("symbol", symbol);
    root.put("stream", stream);
    root.put("date", DATE.format(hourStart));
    root.put("hour", hourStart.atOffset(ZoneOffset.UTC).getHour());
    root.put("sealed_at", sealedAt.toString());
    ArrayNode minutes = root.putArray("minutes_present");
    for (int m : merge.minutesPresent()) {
      minutes.add(m);
    }
    root.putArray("minutes_missing"); // empty for skeleton (we don't track gaps)
    root.put("file_sha256", merge.sha256Hex());
    root.put("file_size_bytes", merge.fileSizeBytes());
    root.put("record_count", merge.recordCount());

    // Pretty-printed, LF line endings, no trailing newline (master spec §10.d).
    String json = mapper.writeValueAsString(root);
    Files.writeString(target, json);
  }
}
```

- [ ] **Step 4: Run the test (should pass)**

```bash
./mvnw -pl sealer -am test -Dtest=ManifestWriterTest
```

Expected: BUILD SUCCESS, 1 test passes.

- [ ] **Step 5: Commit**

```bash
git add sealer/src/main/java/com/cryptopanner/sealer/ManifestWriter.java \
        sealer/src/test/java/com/cryptopanner/sealer/ManifestWriterTest.java
git commit -m "feat(sealer): skeleton manifest writer (5 mandatory fields)"
```

---

## Task 10: sealer — Main.java

**Files:**
- Create: `cryptopanner/sealer/src/main/java/com/cryptopanner/sealer/Main.java`

CLI: `sealer --date YYYY-MM-DD --hour 0-23`. Reads from `segments/`, writes to `sealed/`, writes the manifest.

- [ ] **Step 1: Write the entry point**

```java
// sealer/src/main/java/com/cryptopanner/sealer/Main.java
package com.cryptopanner.sealer;

import com.cryptopanner.common.config.SkeletonConfig;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

public final class Main {

  public static void main(String[] args) throws Exception {
    Path configPath =
        Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    SkeletonConfig cfg = SkeletonConfig.load(configPath);

    String dateStr = required(args, "--date");
    int hour = Integer.parseInt(required(args, "--hour"));
    Instant hourStart =
        LocalDateTime.of(LocalDate.parse(dateStr), LocalTime.of(hour, 0))
            .toInstant(ZoneOffset.UTC);

    HourMerger merger = new HourMerger(cfg.paths().segments(), cfg.paths().sealed());
    System.out.println("[sealer] merging hour " + hour + " of " + dateStr);
    HourMerger.Result result = merger.mergeHour(cfg.symbol(), cfg.stream(), hourStart);
    System.out.println(
        "[sealer] merged " + result.recordCount() + " records into " + result.file());

    Path manifestPath =
        result.file().resolveSibling("hour-" + String.format("%02d", hour) + ".manifest.json");
    ManifestWriter.write(
        manifestPath,
        cfg.nodeId(),
        cfg.symbol(),
        cfg.stream(),
        hourStart,
        result,
        Instant.now());
    System.out.println("[sealer] manifest written: " + manifestPath);
  }

  private static String required(String[] args, String flag) {
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals(flag)) return args[i + 1];
    }
    throw new IllegalArgumentException("missing required flag: " + flag);
  }
}
```

- [ ] **Step 2: Build and verify**

```bash
./mvnw -pl sealer -am package -DskipTests
```

Expected: BUILD SUCCESS. `sealer/target/install/bin/sealer` exists.

- [ ] **Step 3: Commit**

```bash
git add sealer/src/main/java/com/cryptopanner/sealer/Main.java
git commit -m "feat(sealer): main entry point with --date/--hour flags"
```

---

## Task 11: uploader — S3Uploader.java

**Files:**
- Create: `cryptopanner/uploader/src/main/java/com/cryptopanner/uploader/S3Uploader.java`
- Test: `cryptopanner/uploader/src/test/java/com/cryptopanner/uploader/S3UploaderTest.java`

S3 PUT in the order data → sidecar → manifest. Verifies via HeadObject after each. Uses path-style for MinIO.

- [ ] **Step 1: Write the failing test**

For the skeleton, we'll do an in-memory test using S3Mock-jvm — no real MinIO required for the unit test. Add the dep to the uploader pom first.

Edit `cryptopanner/uploader/pom.xml`, add inside `<dependencies>`:

```xml
<dependency>
  <groupId>com.adobe.testing</groupId>
  <artifactId>s3mock-junit5</artifactId>
  <version>3.8.0</version>
  <scope>test</scope>
</dependency>
```

Then:

```java
// uploader/src/test/java/com/cryptopanner/uploader/S3UploaderTest.java
package com.cryptopanner.uploader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

class S3UploaderTest {

  @RegisterExtension
  static final S3MockExtension S3 =
      S3MockExtension.builder().silent().withSecureConnection(false).build();

  @Test
  void uploadsAllThreeObjectsInOrder(@TempDir Path tmp) throws IOException {
    Path data = tmp.resolve("hour-14.jsonl.zst");
    Path sidecar = tmp.resolve("hour-14.jsonl.zst.sha256");
    Path manifest = tmp.resolve("hour-14.manifest.json");
    Files.writeString(data, "compressed-bytes-stand-in");
    Files.writeString(sidecar, "deadbeef".repeat(8) + "  hour-14.jsonl.zst\n");
    Files.writeString(manifest, "{\"manifest_schema_version\":1}");

    S3Client s3 =
        S3Client.builder()
            .endpointOverride(URI.create(S3.getServiceEndpoint()))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")))
            .region(Region.US_EAST_1)
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .build();
    s3.createBucket(CreateBucketRequest.builder().bucket("bucket").build());

    new S3Uploader(s3, "bucket")
        .upload("vps-fra-1/btcusdt/trade/2026-06-14/hour-14.jsonl.zst", data, sidecar, manifest);

    assertTrue(
        objectExists(s3, "bucket", "vps-fra-1/btcusdt/trade/2026-06-14/hour-14.jsonl.zst"));
    assertTrue(
        objectExists(s3, "bucket", "vps-fra-1/btcusdt/trade/2026-06-14/hour-14.jsonl.zst.sha256"));
    assertTrue(
        objectExists(s3, "bucket", "vps-fra-1/btcusdt/trade/2026-06-14/hour-14.manifest.json"));
  }

  private static boolean objectExists(S3Client s3, String bucket, String key) {
    try {
      s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
```

- [ ] **Step 2: Run the test (should fail)**

```bash
./mvnw -pl uploader -am test -Dtest=S3UploaderTest
```

Expected: BUILD FAILURE — `S3Uploader` not found.

- [ ] **Step 3: Write the implementation**

```java
// uploader/src/main/java/com/cryptopanner/uploader/S3Uploader.java
package com.cryptopanner.uploader;

import java.io.IOException;
import java.nio.file.Path;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** S3 upload in master spec §9.c.1 order: data → sidecar → manifest. */
public final class S3Uploader {

  private final S3Client s3;
  private final String bucket;

  public S3Uploader(S3Client s3, String bucket) {
    this.s3 = s3;
    this.bucket = bucket;
  }

  /** Upload the three objects for one (symbol, stream, hour). Verifies via HeadObject after each. */
  public void upload(String dataKey, Path data, Path sidecar, Path manifest) throws IOException {
    put(dataKey, data);
    headOrThrow(dataKey);
    put(dataKey + ".sha256", sidecar);
    headOrThrow(dataKey + ".sha256");
    String manifestKey = dataKey.replaceFirst("\\.jsonl\\.zst$", ".manifest.json");
    put(manifestKey, manifest);
    headOrThrow(manifestKey);
  }

  private void put(String key, Path file) {
    s3.putObject(
        PutObjectRequest.builder().bucket(bucket).key(key).build(),
        RequestBody.fromFile(file));
  }

  private void headOrThrow(String key) {
    s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
  }
}
```

- [ ] **Step 4: Run the test (should pass)**

```bash
./mvnw -pl uploader -am test -Dtest=S3UploaderTest
```

Expected: BUILD SUCCESS, 1 test passes.

- [ ] **Step 5: Commit**

```bash
git add uploader/pom.xml \
        uploader/src/main/java/com/cryptopanner/uploader/S3Uploader.java \
        uploader/src/test/java/com/cryptopanner/uploader/S3UploaderTest.java
git commit -m "feat(uploader): S3 upload with manifest-last ordering"
```

---

## Task 12: uploader — Main.java

**Files:**
- Create: `cryptopanner/uploader/src/main/java/com/cryptopanner/uploader/Main.java`

CLI: `uploader --date --hour`. Builds the S3 client from config, locates the local sealed files, uploads, deletes locals on success.

- [ ] **Step 1: Write the entry point**

```java
// uploader/src/main/java/com/cryptopanner/uploader/Main.java
package com.cryptopanner.uploader;

import com.cryptopanner.common.Paths;
import com.cryptopanner.common.config.SkeletonConfig;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public final class Main {

  public static void main(String[] args) throws Exception {
    Path configPath =
        Path.of(System.getProperty("config", "/etc/cryptopanner/config.yaml"));
    SkeletonConfig cfg = SkeletonConfig.load(configPath);
    String dateStr = required(args, "--date");
    int hour = Integer.parseInt(required(args, "--hour"));
    Instant hourStart =
        LocalDateTime.of(LocalDate.parse(dateStr), LocalTime.of(hour, 0))
            .toInstant(ZoneOffset.UTC);

    Path data = Paths.hourSealed(cfg.paths().sealed(), cfg.symbol(), cfg.stream(), hourStart);
    Path sidecar = data.resolveSibling(data.getFileName() + ".sha256");
    Path manifest =
        data.resolveSibling("hour-" + String.format("%02d", hour) + ".manifest.json");
    if (!Files.exists(data) || !Files.exists(sidecar) || !Files.exists(manifest)) {
      throw new IllegalStateException("missing sealed files; run sealer first");
    }

    S3Client s3 =
        S3Client.builder()
            .endpointOverride(URI.create(cfg.storage().endpoint()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        cfg.storage().accessKey(), cfg.storage().secretKey())))
            .region(Region.of(cfg.storage().region()))
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(cfg.storage().pathStyleAccess())
                    .build())
            .build();

    // Idempotent bucket-create — skeleton convenience; production would do this once.
    try {
      s3.createBucket(CreateBucketRequest.builder().bucket(cfg.storage().bucket()).build());
    } catch (Exception ignored) {
      // already exists
    }

    String key = Paths.s3Key(cfg.nodeId(), cfg.symbol(), cfg.stream(), hourStart);
    System.out.println("[uploader] uploading to s3://" + cfg.storage().bucket() + "/" + key);
    new S3Uploader(s3, cfg.storage().bucket()).upload(key, data, sidecar, manifest);

    System.out.println("[uploader] success; cleaning local sealed files");
    Files.delete(data);
    Files.delete(sidecar);
    Files.delete(manifest);
  }

  private static String required(String[] args, String flag) {
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals(flag)) return args[i + 1];
    }
    throw new IllegalArgumentException("missing flag: " + flag);
  }
}
```

- [ ] **Step 2: Build and verify**

```bash
./mvnw -pl uploader -am package -DskipTests
```

Expected: BUILD SUCCESS. `uploader/target/install/bin/uploader` exists.

- [ ] **Step 3: Commit**

```bash
git add uploader/src/main/java/com/cryptopanner/uploader/Main.java
git commit -m "feat(uploader): main entry point uploading sealed files to S3"
```

---

## Task 13: verify — VerifyCommand.java

**Files:**
- Create: `cryptopanner/verify/src/main/java/com/cryptopanner/verify/Main.java`
- Create: `cryptopanner/verify/src/main/java/com/cryptopanner/verify/VerifyCommand.java`
- Test: `cryptopanner/verify/src/test/java/com/cryptopanner/verify/VerifyCommandTest.java`

picocli root + a `verify` subcommand that downloads the three S3 objects for a (symbol, stream, date, hour), recomputes SHA-256 of the data, compares against sidecar and against `manifest.file_sha256`. Prints `ERRORS=N` and exits non-zero on any mismatch.

- [ ] **Step 1: Add s3mock to verify pom for tests**

Edit `cryptopanner/verify/pom.xml`, add inside `<dependencies>`:

```xml
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>s3</artifactId>
</dependency>
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
</dependency>
<dependency>
  <groupId>com.adobe.testing</groupId>
  <artifactId>s3mock-junit5</artifactId>
  <version>3.8.0</version>
  <scope>test</scope>
</dependency>
```

- [ ] **Step 2: Write the failing test**

```java
// verify/src/test/java/com/cryptopanner/verify/VerifyCommandTest.java
package com.cryptopanner.verify;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.cryptopanner.common.Sha256Sidecar;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class VerifyCommandTest {

  @RegisterExtension
  static final S3MockExtension S3 =
      S3MockExtension.builder().silent().withSecureConnection(false).build();

  @Test
  void exitsZeroWhenAllChecksPass(@TempDir Path tmp) throws IOException {
    S3Client s3 = client();
    s3.createBucket(CreateBucketRequest.builder().bucket("b").build());
    String key = "vps-fra-1/btcusdt/trade/2026-06-14/hour-14.jsonl.zst";
    Path data = tmp.resolve("hour-14.jsonl.zst");
    Files.writeString(data, "compressed-stand-in");
    Path sidecar = tmp.resolve("hour-14.jsonl.zst.sha256");
    Sha256Sidecar.computeAndWrite(data, sidecar);
    String hex = Sha256Sidecar.readHash(sidecar);
    String manifestJson =
        "{\n  \"manifest_schema_version\": 1,\n  \"file_sha256\": \"" + hex + "\"\n}";

    s3.putObject(PutObjectRequest.builder().bucket("b").key(key).build(), RequestBody.fromFile(data));
    s3.putObject(
        PutObjectRequest.builder().bucket("b").key(key + ".sha256").build(),
        RequestBody.fromFile(sidecar));
    s3.putObject(
        PutObjectRequest.builder()
            .bucket("b")
            .key(key.replace(".jsonl.zst", ".manifest.json"))
            .build(),
        RequestBody.fromString(manifestJson));

    int code =
        new CommandLine(new Main())
            .execute(
                "verify",
                "--endpoint", S3.getServiceEndpoint(),
                "--bucket", "b",
                "--node-id", "vps-fra-1",
                "--symbol", "btcusdt",
                "--stream", "trade",
                "--date", "2026-06-14",
                "--hour", "14");
    assertEquals(0, code);
  }

  private static S3Client client() {
    return S3Client.builder()
        .endpointOverride(URI.create(S3.getServiceEndpoint()))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk")))
        .region(Region.US_EAST_1)
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .build();
  }
}
```

- [ ] **Step 3: Run the test (should fail)**

```bash
./mvnw -pl verify -am test -Dtest=VerifyCommandTest
```

Expected: BUILD FAILURE — `Main` and `VerifyCommand` not found.

- [ ] **Step 4: Write the implementation**

```java
// verify/src/main/java/com/cryptopanner/verify/Main.java
package com.cryptopanner.verify;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "cryptopanner-verify",
    mixinStandardHelpOptions = true,
    subcommands = {VerifyCommand.class},
    description = "CryptoPanner audit / integrity CLI (skeleton).")
public final class Main implements Runnable {

  public static void main(String[] args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }

  @Override
  public void run() {
    new CommandLine(this).usage(System.out);
  }
}
```

```java
// verify/src/main/java/com/cryptopanner/verify/VerifyCommand.java
package com.cryptopanner.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@Command(name = "verify", description = "Download and integrity-check one (symbol, stream, hour).")
public final class VerifyCommand implements Callable<Integer> {

  @Option(names = "--endpoint", required = true) String endpoint;
  @Option(names = "--bucket", required = true) String bucket;
  @Option(names = "--node-id", required = true) String nodeId;
  @Option(names = "--symbol", required = true) String symbol;
  @Option(names = "--stream", required = true) String stream;
  @Option(names = "--date", required = true) String date;
  @Option(names = "--hour", required = true) int hour;
  @Option(names = "--access-key", defaultValue = "cryptopanner") String accessKey;
  @Option(names = "--secret-key", defaultValue = "changeme-dev") String secretKey;
  @Option(names = "--region", defaultValue = "us-east-1") String region;

  @Override
  public Integer call() throws Exception {
    S3Client s3 =
        S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .region(Region.of(region))
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .build();

    String prefix = nodeId + "/" + symbol + "/" + stream + "/" + date;
    String dataKey = prefix + "/hour-" + String.format("%02d", hour) + ".jsonl.zst";
    String sidecarKey = dataKey + ".sha256";
    String manifestKey = prefix + "/hour-" + String.format("%02d", hour) + ".manifest.json";

    byte[] data = fetch(s3, dataKey);
    byte[] sidecar = fetch(s3, sidecarKey);
    byte[] manifest = fetch(s3, manifestKey);

    int errors = 0;
    String computedSha = sha256Hex(data);
    String sidecarHex = new String(sidecar).split("\\s+")[0];
    String manifestSha =
        new ObjectMapper().readTree(manifest).get("file_sha256").asText();

    if (!computedSha.equals(sidecarHex)) {
      System.err.println("MISMATCH: computed=" + computedSha + " sidecar=" + sidecarHex);
      errors++;
    }
    if (!computedSha.equals(manifestSha)) {
      System.err.println("MISMATCH: computed=" + computedSha + " manifest=" + manifestSha);
      errors++;
    }

    System.out.println("ERRORS=" + errors);
    return errors == 0 ? 0 : 1;
  }

  private byte[] fetch(S3Client s3, String key) throws Exception {
    try (ResponseInputStream<GetObjectResponse> in =
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())) {
      return in.readAllBytes();
    }
  }

  private static String sha256Hex(byte[] bytes) throws Exception {
    byte[] d = MessageDigest.getInstance("SHA-256").digest(bytes);
    StringBuilder sb = new StringBuilder(64);
    for (byte b : d) sb.append(String.format("%02x", b));
    return sb.toString();
  }
}
```

- [ ] **Step 5: Run the test (should pass)**

```bash
./mvnw -pl verify -am test -Dtest=VerifyCommandTest
```

Expected: BUILD SUCCESS, 1 test passes.

- [ ] **Step 6: Commit**

```bash
git add verify/pom.xml \
        verify/src/main/java/com/cryptopanner/verify/Main.java \
        verify/src/main/java/com/cryptopanner/verify/VerifyCommand.java \
        verify/src/test/java/com/cryptopanner/verify/VerifyCommandTest.java
git commit -m "feat(verify): verify subcommand with SHA-256 round-trip check"
```

---

## Task 14: End-to-end smoke test

**Files:**
- Create: `cryptopanner/tests/skeleton/run-skeleton.sh`
- Create: `cryptopanner/tests/skeleton/README.md`

Runs the whole chain locally without docker-compose (single host, all processes started by the script). Faster feedback than full compose.

- [ ] **Step 1: Write the smoke-test script**

```bash
#!/usr/bin/env bash
# tests/skeleton/run-skeleton.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

# 1) Build everything
./mvnw -q -DskipTests package

# 2) Start MinIO in background (docker required)
MINIO_NAME="cryptopanner-skeleton-minio"
docker rm -f "$MINIO_NAME" >/dev/null 2>&1 || true
docker run -d --name "$MINIO_NAME" -p 9000:9000 \
  -e MINIO_ROOT_USER=cryptopanner \
  -e MINIO_ROOT_PASSWORD=changeme-dev \
  minio/minio:RELEASE.2024-05-10T01-41-38Z server /data >/dev/null

# 3) Start the mock WS server
cd tests/mocks/binance-ws
[ -d .venv ] || python3 -m venv .venv
. .venv/bin/activate
pip install -q -r requirements.txt
python mock_ws.py &
MOCK_PID=$!
deactivate
cd "$REPO_ROOT"

# Ensure cleanup on exit
cleanup() {
  kill "$MOCK_PID" >/dev/null 2>&1 || true
  docker rm -f "$MINIO_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# 4) Wait for both services
for _ in {1..10}; do
  curl -sf http://localhost:9000/minio/health/ready >/dev/null && break
  sleep 1
done

# 5) Clean local data
rm -rf /tmp/cryptopanner
mkdir -p /tmp/cryptopanner/segments /tmp/cryptopanner/sealed

# 6) Run Collector (it self-exits after collector_max_runtime_s)
./collector/target/install/bin/collector \
  -Dconfig="$REPO_ROOT/config/dev/skeleton.yaml"

# 7) Find the date+hour the Collector wrote.
DATE=$(ls /tmp/cryptopanner/segments/btcusdt/trade/ | sort | tail -1)
HOUR=$(ls /tmp/cryptopanner/segments/btcusdt/trade/"$DATE"/ \
  | grep -oP 'minute-\K\d{2}' | sort -u | tail -1)
echo "[skeleton] sealing hour $HOUR of $DATE"

# 8) Run Sealer
./sealer/target/install/bin/sealer \
  -Dconfig="$REPO_ROOT/config/dev/skeleton.yaml" \
  --date "$DATE" --hour "$HOUR"

# 9) Run Uploader
./uploader/target/install/bin/uploader \
  -Dconfig="$REPO_ROOT/config/dev/skeleton.yaml" \
  --date "$DATE" --hour "$HOUR"

# 10) Run Verify
./verify/target/install/bin/verify verify \
  --endpoint http://localhost:9000 \
  --bucket cryptopanner-dev \
  --node-id dev-node \
  --symbol btcusdt --stream trade \
  --date "$DATE" --hour "$HOUR" \
  --access-key cryptopanner --secret-key changeme-dev

echo "[skeleton] OK"
```

- [ ] **Step 2: Make it executable**

```bash
chmod +x tests/skeleton/run-skeleton.sh
```

- [ ] **Step 3: Write the README**

```bash
cat > tests/skeleton/README.md <<'EOF'
# Walking-skeleton smoke test

Runs the full pipeline end-to-end on a single host:

1. Builds all modules with Maven.
2. Starts MinIO in Docker.
3. Starts the Python mock-binance-ws.
4. Runs Collector against the mock for `collector_max_runtime_s` seconds.
5. Runs Sealer to merge the captured minutes into one hour file.
6. Runs Uploader to push the sealed file to MinIO with manifest-last ordering.
7. Runs `verify verify` to recompute the SHA and assert ERRORS=0.

## Run

```bash
bash tests/skeleton/run-skeleton.sh
```

Tunables (edit `config/dev/skeleton.yaml`):
- `collector_max_runtime_s` — default 180s. Set to 60 for fast iteration.

## Requirements

- Docker (for MinIO)
- Python 3.10+ (for the mock)
- JDK 21 (for everything else)
EOF
```

- [ ] **Step 4: Run the smoke test**

```bash
bash tests/skeleton/run-skeleton.sh
```

Expected:
```
[collector] started; running for 180s
[collector] frames seen: 100
...
[collector] stopping after 1800 frames
[collector] done
[skeleton] sealing hour 14 of 2026-06-14
[sealer] merging hour 14 of 2026-06-14
[sealer] merged 1800 records into ...
[sealer] manifest written: ...
[uploader] uploading to s3://cryptopanner-dev/dev-node/btcusdt/trade/2026-06-14/hour-14.jsonl.zst
[uploader] success; cleaning local sealed files
ERRORS=0
[skeleton] OK
```

- [ ] **Step 5: Commit**

```bash
git add tests/skeleton/
git commit -m "feat(skeleton): end-to-end smoke test wiring all four components"
```

---

## Self-review checklist

After all tasks complete, the working tree should:

1. Build cleanly: `./mvnw verify`
2. Pass all unit tests across modules.
3. Successfully run `bash tests/skeleton/run-skeleton.sh` end-to-end with `ERRORS=0`.
4. Have a `verify/target/install/bin/verify` launcher that prints a usage screen when run with no args.
5. Produce a sealed hour file + sidecar + manifest in MinIO under the canonical UTC-keyed S3 path.

Anything that doesn't satisfy points 1–5 is a plan defect to fix before moving on.

## What this skeleton deliberately does NOT do (deferred work)

- Hot-swap deploys (Variant A, design doc §4) — slot-templated systemd, `active-slot`, OverlapMerger.
- Daily WS rotation (Variant B, design doc §5) — shadow connection, EquivalenceChecker.
- Server-event-time bucketing — currently using local receive time.
- Frame-buffer / seal-grace windows.
- REST backfill at hourly merge (gap-fillable streams).
- Sequence-ID validation.
- Node Agent (`/status`, `/metrics`, `/restart`, `/rotation/trigger`).
- Monitor + dashboard + alerting.
- Heartbeat files.
- Multi-stream / multi-symbol capture (currently hardcoded to one each).
- The §10.d manifest beyond 5 minimal fields.
- `.fs-heavy.lock` (single-process pipeline, no contention).
- `manifest_schema_version` evolution policy.
- Linux fsync edge cases vs macOS F_FULLFSYNC.
- Chaos scenarios from §14.e.

These are the deliberate non-goals. Building any of them while the skeleton works is a separate plan.
