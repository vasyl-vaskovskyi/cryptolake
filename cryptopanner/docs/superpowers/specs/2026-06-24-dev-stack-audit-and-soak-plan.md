# 2026-06-24 — Dev-stack audit + soak harness plan

Audit of why `make dev-up` / the §14.e soak can't run today, and the plan to deliver a runnable
soak. Ground-truth read of `docker-compose.yml`, `Makefile`, `tests/soak/_common.sh`,
`tests/mocks/`, `config/dev/`, and the collector/agent/sealer/uploader entry points.

## Findings (blockers, by severity)

| # | Blocker | Kind | Fix path |
|---|---|---|---|
| 1 | **No `Dockerfile`** — every app service uses `build: .`; `make dev-up` can't build an image. | build artifact (TDD-exempt §14.k) | docker increment |
| 2 | **No `config/dev/config.yaml`** — every service mounts it; only `skeleton.yaml`/`real-binance.yaml`/`monitor.yaml` exist. | YAML (exempt) | create now |
| 3 | **Agent reports everything `down` without systemd.** `HeartbeatState.classify` returns `DOWN` when `!systemdActive`, and agent `Main` wires `systemdActive = Systemctl.isActive(unit)`. No systemd in dev *or* docker containers → the Monitor sees all components down, dashboard never shows `running` (contradicts §14.b bring-up success). No "test mode" exists despite §14.j. | **code gap → TDD** | fix now |
| 4 | **Collector runtime paths are hardcoded `/tmp/cryptopanner-collector@<slot>.{heartbeat,rotation.json}`** (not node-scoped), and the agent reads the same fixed paths. Two nodes on one host collide. This is *why* §14.e relies on container isolation. | code constraint | docker isolates; or node-scope (code) for a process 2-node |
| 5 | **Compose collector command `["--slot=a","--role=primary"]` is dead** — the collector reads its slot from `CRYPTOPANNER_SLOT` env, ignores those args. | compose fix (exempt) | docker increment |
| 6 | **Mock images don't exist.** Compose references `cryptopanner/mock-binance-rest:dev` and `cryptopanner/mock-healthchecks:dev`; no modules/Dockerfiles back them. The `mock-binance-ws` module already serves **both** WS (9001) and REST (9002) in one process, so `mock-binance-rest` is redundant. `mock-healthchecks` has no implementation. | code/build | fold REST into ws; add a tiny healthchecks mock (or make optional) |
| 7 | **Sealer/uploader are one-shot** (`--date --hour`), no daemon. A soak captures, then invokes seal+upload for the captured hour (exactly the `_common.sh` chaos flow). | n/a | orchestration |
| 8 | **§14.e wants two independent nodes** (own S3 prefix, §3.d); compose models one node's two `@a`/`@b` slots (shared volume + node_id). | scope | docker increment |
| 9 | **`tests/soak/run.sh` does not exist** (`tests/soak/` has only the chaos `_common.sh`). | the deliverable | build now |

## Existing infrastructure (reusable)

`tests/soak/_common.sh` is a **process-based** chaos harness (despite its location): it builds the
appassembler binaries, starts **one MinIO container** + the `mock-binance-ws` process, runs the
collector against `config/dev/skeleton.yaml`, discovers the captured (date,hour), then runs
sealer → uploader → verify and asserts on the local manifest. This is the project's *actual,
working* end-to-end pattern; only MinIO needs docker. The `verify`, `sealer`, `uploader`,
`collector`, `mock-binance-ws` binaries all build via `mvn package` (appassembler `installDist`).

## Decision

**Build the soak process-based and single-host, reusing the `_common.sh` pattern, single-node
first — structured so bring-up is a swappable function and the assertions are shared.** Then layer
the docker-compose + two-node variant as a clearly-scoped follow-on.

Rationale:
- The §14.e *value* is in the orchestration + pass-criteria assertions (verify `ERRORS=0`,
  rotation event in manifest, independence, late-frame rate). Those are identical whether services
  run as processes or containers, so they are written once and reused.
- The process pattern is proven (chaos `_common.sh`) and **runnable in this environment now**
  (docker present only for MinIO). The full docker path needs a Dockerfile + two nonexistent mock
  images — a large yak-shave that would block delivering any runnable soak.
- The one thing process-mode can't do on a single host is **two nodes** (finding #4: hardcoded
  `/tmp` paths collide). That is the natural boundary for the docker increment (container `/tmp`
  isolation), or a separate node-scoping refactor.

Prerequisite code fix (finding #3) is done first, TDD: an agent **no-systemd test mode** so a
non-systemd stack (process *or* docker) reports real component states. This also directly enables
exercising the Monitor against a live agent (the prior "test monitor during a real run" question).

## Plan (this pass)

1. **Audit doc** (this file).
2. **TDD agent no-systemd liveness** (finding #3): a pure `AgentLiveness.heartbeatActive(age, stuck)`
   helper, wired into agent `Main` behind `CRYPTOPANNER_AGENT_NO_SYSTEMD`; production path unchanged.
3. **`config/dev/config.yaml`** (finding #2): full §15.b node config pointed at the dev stack
   (MinIO, mock-binance), agent block, no runtime cap. Verified by loading via `NodeConfig`.
4. **`tests/soak/run.sh`** (finding #9): process-based single-node soak — MinIO + mock-binance +
   collector + agent (no-systemd) + monitor; run for a (dev-shortened) window, trigger one WS
   rotation, seal+upload the captured hour, then assert: `verify` exits 0 `ERRORS=0`; the rotation
   event is in the manifest; the Monitor's `/api/nodes` shows the node reachable with components
   `running`. Bring-up isolated in a function for the later compose swap.

## Deferred (next increment — the docker/two-node variant)

- `Dockerfile` (multi-stage: maven build → runtime image; entrypoint dispatches on the service
  arg; sets `CRYPTOPANNER_SLOT` for collectors).
- `mock-healthchecks` mock (TDD) + fold `mock-binance-rest` into the existing ws mock; fix compose
  collector slot env (finding #5).
- Extend compose to **two independent nodes** (distinct `node_id`, data volumes, agent ports) and
  re-point `run.sh` bring-up at `docker compose`; assert the §3.d independence + per-node S3 prefix
  criteria the single-host process soak can't cover.
- The §14.g 2h/24h pre-release variant.
