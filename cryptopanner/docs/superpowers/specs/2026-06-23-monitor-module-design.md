# 2026-06-23 — Monitor module design

The cross-node **Monitor** (master spec §11.c–d, §13) is the only fully-unimplemented
CryptoPanner service. It runs on a separate VPS (different provider than the nodes), scrapes
every Node Agent's `/status` (+`/metrics`) on a fixed interval, evaluates the §13.a alert
conditions, dispatches deduplicated/correlated alerts to Telegram/WhatsApp, orchestrates
component restarts through the Node Agent, serves a dashboard, and runs a dead-man's switch
against Healthchecks.io.

`monitor/pom.xml` and `common`'s `MonitorConfig` (§15.c) already exist. This design covers the
net-new Java source under `monitor/src` plus the small `MonitorConfig` extensions needed for
thresholds the current record omits.

## Invariants honored

- **The Monitor is never in the data path** (§12.g/h). Its outage loses visibility, never data.
- **No false-positive restarts** (§13.d): two-tier check — systemd `failed`/`inactive` AND
  heartbeat-derived `stuck`/`down`. `degraded` is observation-only (except the cliff exception).
- **Active-slot targeting** (§13.b): collector restarts hit the active slot only.

## Components (small, independently-testable units, mirroring `agent`)

Package `com.cryptopanner.monitor`.

| Unit | Responsibility | §ref |
|---|---|---|
| `StatusSnapshot` (+ Jackson record) | Typed model of the `/status` JSON: components→state/heartbeat_age/pid/uptime, active_slot, fs_heavy_lock.held_by, deploy.state, rotation.state/current_connection_age_s, vps.disk | §11.c |
| `MetricsSnapshot` | Parsed subset of the OpenMetrics `/metrics` text: `sealed_files_pending_upload`, `current_connection_age_seconds`, counters used for rate alerts | §11.c |
| `NodeScraper` | Poll one node's `/status` + `/metrics` over HTTP → `ScrapeResult` (snapshot pair \| failure). Uses `RestPoller.newHttpClient()` config | §11.c |
| `NodeStateTracker` | Per-node stateful history feeding the evaluator: consecutive scrape failures, time-in-degraded per component, time-in-deploy-state, last-seen connection age | §13.a/d |
| `AlertEvaluator` | **Pure** function `(snapshot, metrics, tracker, now, thresholds) → List<Alert>`. The heart; exhaustively unit-tested | §13.a |
| `Alert` | record: `severity (WARNING/CRITICAL)`, `node`, `component` (nullable), `type` (enum), `message`, `firedAt` | §13.a |
| `AlertDispatcher` | Dedup key `(node,component,type)` 1h TTL; correlation (≥N nodes same type within window → one combined message); one-shot `recovered` on clear | §13.e |
| `AlertChannel` → `TelegramSender`, `WhatsappSender` | HTTP-POST webhook senders; both receive every alert | §13.a |
| `RestartOrchestrator` | Two-tier gate → `POST /restart/<component>` (bearer + `X-Timestamp`); backoff `[5s,15s,60s,300s]`; circuit breaker (3 fails / 5 min → trip + Critical); active-slot targeting; cliff-approach lock-release exception | §13.b/d |
| `DeadMansSwitch` | Push Healthchecks.io every 60s; daily 02:00 UTC self-test alert | §13.c |
| `MonitorServer` | `/dashboard` (HTML, 5s auto-refresh), `/api/nodes` (JSON), own `/status`+`/metrics`+heartbeat | §11.d |
| `Main` | Composition root: load `MonitorConfig`, build one `EnvelopeCodec.newMapper()`, scrape scheduler, shutdown hook | — |

## Data flow (one scrape cycle, per `scrape_interval_s`)

```
for each node:
  ScrapeResult = NodeScraper.poll(node)          # /status + /metrics over HTTP
  tracker.record(node, result, now)              # update failure counts / state timers
  alerts += AlertEvaluator.evaluate(node, result, tracker, now, thresholds)
  if AlertEvaluator says restart-eligible:
      RestartOrchestrator.consider(node, component)   # two-tier gate + backoff + breaker
AlertDispatcher.dispatch(alerts, now)            # dedup + correlate + recover → channels
MonitorServer.publish(latest snapshots)          # feeds /api/nodes + /dashboard
DeadMansSwitch.tick(now)                          # 60s push + 02:00 self-test (own cadence)
```

The scrape scheduler is a `ScheduledExecutorService` (single thread is sufficient at operator
scale; per-node scrapes fan out on virtual threads if latency demands). `CLOCK_MONOTONIC` is not
needed here — wall clock drives both scrape cadence and the 02:00 self-test; all timers are
injectable `Supplier<Instant>` for tests.

## Alert conditions — coverage in this pass

**Fully implemented (signal available remotely):**

- Component `degraded` persisting > `degraded_persisting` (Warning); `stuck` (Warning);
  `down` (Critical) — from `/status.components[].state` + `NodeStateTracker` timers.
- Node unreachable: 3 consecutive scrape failures (Critical) — from `ScrapeResult` failures.
- Disk `/data` > 80% (Warning) / > 95% (Critical) — from `/status.vps.disk["/data"].percent`.
- Deploy state machine stuck (no progression) > 1h (Warning) — `/status.deploy.state` + timer.
- Active-slot mismatch (Critical) — `/status.active_slot` vs which collector slot is `running`.
- WS connection age > `connection_max_age + 30m` (Critical) — `/status.rotation.current_connection_age_s`.
- Upload backlog (Warning) — `/metrics` `cryptopanner_sealed_files_pending_upload` > 0 sustained.
- Circuit breaker tripped (Critical) — Monitor-internal from `RestartOrchestrator`.
- Monitor own-health / total-outage — Healthchecks.io dead-man (external).

**Deferred — blocked on a node-side signal not yet exposed remotely** (documented, not silently
dropped): precise upload-backlog *age* in minutes, REST failed-poll *rate*, clock-skew degree,
ws-disconnect *duration*, and rotation-verify-failure *counts*. These live in node logs / would
need new `/status` fields or a log-event feed. `AlertEvaluator` is built so each slots in as a new
rule + threshold without touching the loop. A `// DEFERRED(§13.a):` comment marks each gap.

## Restart policy detail (§13.b/d)

1. **Gate**: restart only if (systemd `failed`/`inactive` — i.e. state `down`) **AND** heartbeat
   state ∈ {`stuck`,`down`}. `running` never restarts; `degraded` never restarts **except** the
   cliff exception below.
2. **Cliff exception (§13.d)**: if `fs_heavy_lock.held_by` is a `degraded` component AND some
   collector slot's connection age has crossed `connection_max_age + 30m`, restart the lock-holder
   to release the lock. Counts toward the breaker.
3. **Backoff** `[5s,15s,60s,300s]` per `(node,component)`; **circuit breaker** trips after 3
   failures in 5 min → no more attempts + Critical alert.
4. **Collector targeting**: restart `cryptopanner-collector@<active_slot>` only.

## Alert pipeline detail (§13.e)

- **Dedup**: key `(node, component, type)`; repeat within `dedup_ttl` (1h) suppressed.
- **Correlation**: ≥ `threshold_nodes` (3) distinct nodes firing the same `type` within
  `window` (1m) → one combined message listing the nodes, instead of N pages.
- **Recovery**: when a previously-firing key clears, emit one `recovered` message on every channel.

## Config extensions (in `common`, TDD'd there)

`MonitorConfig.Warning`/`Critical` currently omit several §15.c keys. Add (Jackson-ignored
unknowns already make the YAML forward-compatible): `Warning.uploadBacklogAge`, `Warning.deployStuck`,
`Warning.restFailedPollRatePct`, `Warning.restFailedPollWindow`; keep `Critical` as-is plus
`Critical.restRateLimitPersistenceHours`. Each duration via `ConfigParse`. Existing tests stay green.

## Testing strategy (§14.h)

JUnit 5, mirroring `AgentServerTest`. No docker.

- **Scrape loop**: stand up a plain `com.sun.net.httpserver.HttpServer` returning canned
  `/status` + `/metrics`; point `NodeScraper` at its ephemeral port. Simulate failures by stopping
  it or returning 500.
- **Evaluator**: pure unit tests over synthetic snapshots + tracker states — one test per §13.a
  condition, plus boundary tests (exactly-at-threshold, just-under, just-over).
- **Restart**: mock Node Agent (in-test `HttpServer`) exposing `/restart/<component>`; assert
  active-slot targeting, two-tier gate, backoff, breaker trip, cliff exception.
- **Dispatcher**: injected clock; assert dedup suppression, correlation grouping, recovery.
- **Channels**: in-test `HttpServer` captures POST bodies — assert payload + that both channels fire.
- **Dead-man**: injected clock + capturing `HttpServer`; assert 60s push cadence + 02:00 self-test.
- **Server**: drive `/dashboard` + `/api/nodes` over real HTTP (AgentServerTest pattern).

## Build order (strict TDD: failing test → impl → green → commit)

1. `MonitorConfig` threshold extensions (common).
2. `StatusSnapshot` + `MetricsSnapshot` parsers.
3. `NodeScraper` (+ in-test HttpServer harness).
4. `NodeStateTracker` + `AlertEvaluator` (the core; largest test surface).
5. `AlertDispatcher`.
6. `AlertChannel` + `TelegramSender`/`WhatsappSender`.
7. `RestartOrchestrator`.
8. `DeadMansSwitch`.
9. `MonitorServer` + dashboard + `/api/nodes`.
10. `Main` wiring + shutdown hook + a live smoke.

## Non-goals (this pass)

- Historical metrics storage / Prometheus replacement (the Monitor only reads current values).
- Tailscale/VPN setup (deployment concern, §6.f).
- The deferred log-event-backed alert *degrees* listed above (engine-ready, awaiting node signal).
