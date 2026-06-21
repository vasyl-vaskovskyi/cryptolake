# ops — node deployment artifacts

systemd units and the bootstrap script for a production CryptoPanner node
(master spec §6, design doc §3.3). These are ops artifacts (TDD-exempt, §14.k);
they are not exercised by `mvn verify`.

## Layout

- `systemd/cryptopanner-collector@.service` — slot-templated collector (`@a`/`@b`);
  exactly one slot is active production per `deploy/active-slot`. Slots alternate per deploy.
- `systemd/cryptopanner-{sealer,uploader,agent}.service` — the other three node services.
- `install.sh` — creates the `cryptopanner` user + `/data/cryptopanner` layout (§6.a),
  bootstraps `active-slot=a`, generates the agent bearer token (§4.f), installs the units,
  and starts slot `@a` plus sealer/uploader/agent.

## Install

```bash
sudo PREFIX=/opt/cryptopanner ops/install.sh
# then place your §15.b config at /etc/cryptopanner/config.yaml and restart the units
```

## Deploy a new JAR (make-before-break, §4)

```bash
cryptopanner-deploy stage   <version>     # stage candidate into the empty slot, start it
cryptopanner-deploy verify  <deploy-id>   # equivalence-check the overlap minutes
cryptopanner-deploy promote <deploy-id>   # atomic switchover (resumable via `resume`)
```

Daily WS rotation is automatic (collector-internal, design doc §5); an operator can force
one with `POST /rotation/trigger` on the Node Agent (e.g. after a symbol-set change, §15.f).
