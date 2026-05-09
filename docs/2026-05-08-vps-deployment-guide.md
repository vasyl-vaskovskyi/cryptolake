# CryptoLake VPS Deployment Guide

End-to-end instructions for bringing up CryptoLake on a fresh Ubuntu 24.04 LTS VPS, hardening the host, and running the Java stack from pre-built Docker images.

Audience: operator who has just provisioned an empty VPS and has SSH access. The host bring-up is split into two purpose-built scripts — `infra/vps/harden.sh` for security only and `infra/vps/bootstrap.sh` for components and interactive secret entry — plus `scripts/publish-images.sh` (run from your laptop) for shipping images without a registry. For day-2 updates without data loss, see `docs/2026-05-08-rolling-update-plan.md` and `scripts/rolling-update.sh`.

---

## 1. Prerequisites

Before touching the VPS:

- **VPS specs (minimum):** 2 vCPU, 4 GB RAM, 80 GB SSD. Recommended: 4 vCPU, 8 GB RAM, 160 GB SSD. Disk grows with archive depth (~5–15 GB/day per symbol/stream depending on volatility).
- **OS:** Ubuntu 24.04 LTS (Noble). The `setup.sh` script targets this release.
- **SSH keypair on your laptop:** `ssh-keygen -t ed25519 -C "you@host"` if you don't have one.
- **Public IP of your laptop** for SSH allowlisting: `curl -s https://checkip.amazonaws.com`.
- **Secrets ready** to paste into `.env`:
  - `POSTGRES_PASSWORD` — strong random
  - `WEBHOOK_URL` — optional alertmanager webhook
  - `CALLMEBOT_PHONE`, `CALLMEBOT_APIKEY` — only if you use the WhatsApp bridge
- **Local Docker** (for the image-publish flow). `docker --version` should report ≥ 24.

Optional but recommended:

- **Tailscale** account if you want to reach Grafana/Prometheus over a private mesh instead of SSH tunnels.

---

## 2. Provision the VPS

Any provider works (Hetzner, DigitalOcean, OVH, Vultr, AWS Lightsail). When creating the instance:

- OS image: **Ubuntu 24.04 LTS**.
- SSH: paste your public key (from `~/.ssh/id_ed25519.pub`). Do **not** rely on a password.
- Firewall: leave the provider firewall fully closed except port **22/tcp** from your laptop's IP. Everything else is blocked at the host level by UFW later.
- Optional: attach a dedicated data volume mounted at `/data` if your provider supports it. Otherwise the OS disk is fine for now.

Note the public IP. The rest of this guide refers to it as `$VPS_IP` and assumes username `cryptolake` (create whatever name you prefer — the scripts derive it from `$SUDO_USER`).

---

## 3. Initial connect and user setup

First connect from your laptop:

```bash
ssh root@$VPS_IP
```

Inside the VPS, create a non-root user with sudo and copy your authorized key over:

```bash
adduser --disabled-password --gecos "" cryptolake
usermod -aG sudo cryptolake
mkdir -p /home/cryptolake/.ssh
cp /root/.ssh/authorized_keys /home/cryptolake/.ssh/
chown -R cryptolake:cryptolake /home/cryptolake/.ssh
chmod 700 /home/cryptolake/.ssh
chmod 600 /home/cryptolake/.ssh/authorized_keys
echo "cryptolake ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/cryptolake
chmod 440 /etc/sudoers.d/cryptolake
```

From now on log in as that user — never as root again:

```bash
exit
ssh cryptolake@$VPS_IP
```

If your provider lets you, also disable root SSH at the cloud-firewall level. The next step does it at the OS level too.

---

## 4. Clone the repo and harden the host

```bash
ssh cryptolake@$VPS_IP

sudo apt-get update && sudo apt-get install -y git
sudo mkdir -p /opt/cryptolake
sudo chown $USER:$USER /opt/cryptolake
git clone https://github.com/<your-org>/cryptolake.git /opt/cryptolake
cd /opt/cryptolake
sudo bash infra/vps/harden.sh
```

`harden.sh` does **security only** and is idempotent:

| Step | Effect |
|------|--------|
| SSH hardening | `PermitRootLogin no`, `PasswordAuthentication no`, `KbdInteractiveAuthentication no`, `AuthenticationMethods publickey`. After this point the host is **SSH-key only** — no passwords accepted. |
| `ufw` | Default deny incoming + allow 22/tcp. Everything else blocked. |
| `fail2ban` | SSH jail: 3 retries / 10 min → 1 h ban. |
| `unattended-upgrades` | Daily security updates auto-applied. |

The script refuses to run if `~/.ssh/authorized_keys` is empty for your user — disabling password auth without a key in place would lock you out. Verify from a **second terminal** before closing the first:

```bash
ssh cryptolake@$VPS_IP                                    # must succeed via key
ssh -o PreferredAuthentications=password cryptolake@$VPS_IP   # must FAIL
```

---

## 5. Bootstrap the host (interactive)

```bash
sudo bash infra/vps/bootstrap.sh
```

`bootstrap.sh` walks you through the remaining setup. It prompts for:

- `POSTGRES_PASSWORD` — auto-generate (recommended) or paste your own.
- WhatsApp/CallMeBot alerting (optional, skip with `n`).
- `HOST_DATA_DIR` (default `/data`).
- Initial `CRYPTOLAKE_TAG` (default `latest`).

It writes `/opt/cryptolake/.env` with `chmod 600`, then installs:

| Step | Effect |
|------|--------|
| Docker CE + compose plugin | Installs from Docker's apt repo. Adds your user to `docker` group. |
| 2 GB swap file | Avoids OOM on 4 GB hosts under transient pressure. |
| `/data` directory | Owned by your user; matches `HOST_DATA_DIR`. |
| Docker log rotation | 50 MB × 3 files per container at the daemon level. |
| systemd unit `cryptolake.service` | Brings the stack up on boot. |
| Disk-monitor cron | Every 6 h; warns when `/data` ≥ 80 %. |

**Log out and back in** so the `docker` group takes effect:

```bash
exit
ssh cryptolake@$VPS_IP
```

---

## 6. Push images from your laptop

You don't need the JDK on the VPS. Build images locally and ship them over SSH using the publish script.

On **your laptop** (with the repo checked out):

```bash
# Build all four Java images and load them on the VPS
scripts/publish-images.sh cryptolake@$VPS_IP

# Or pin to a git SHA so you can roll back later
CRYPTOLAKE_TAG=$(git rev-parse --short HEAD) \
    scripts/publish-images.sh cryptolake@$VPS_IP
```

The script does, for each of `collector`, `writer`, `backfill`, `consolidation`:

```
docker build -f docker/Dockerfile.<svc> -t cryptolake/<svc>:<tag> .
docker save cryptolake/<svc>:<tag> | ssh <host> 'docker load'
```

No registry, no source tree on prod. `collector-backup` uses the same `cryptolake/collector` image as `collector`.

---

## 7. First-time start

On the **VPS**:

```bash
cd /opt/cryptolake
# Match the tag you published from your laptop (omit if you used :latest)
echo "CRYPTOLAKE_TAG=$(git rev-parse --short HEAD)" >> .env

# Bring the stack up via the systemd unit installed by setup.sh
sudo systemctl start cryptolake
```

Or, equivalently, with raw compose:

```bash
docker compose -f docker-compose.yml -f docker-compose.vps.yml up -d
```

The first run also applies 30-min retention to `backup.binance.*` topics via `scripts/setup-backup-topics.sh`. `infra/vps/deploy.sh` handles that automatically; if you started by hand, run it once after Redpanda becomes healthy:

```bash
bash scripts/setup-backup-topics.sh
```

---

## 8. Verify

All control-plane ports are bound to **127.0.0.1 only**, so verifying from your laptop means SSH-tunneling them:

```bash
# From your laptop
ssh -L 3000:127.0.0.1:3000 \
    -L 9090:127.0.0.1:9090 \
    -L 8000:127.0.0.1:8000 \
    -L 8001:127.0.0.1:8001 \
    -L 8002:127.0.0.1:8002 \
    -L 8003:127.0.0.1:8003 \
    -L 8004:127.0.0.1:8004 \
    cryptolake@$VPS_IP
```

Then in another terminal:

```bash
curl -s http://127.0.0.1:8000/ready    # collector primary
curl -s http://127.0.0.1:8004/ready    # collector backup
curl -s http://127.0.0.1:8001/ready    # writer
curl -s http://127.0.0.1:8002/ready    # backfill scheduler
curl -s http://127.0.0.1:8003/ready    # consolidation scheduler
open http://127.0.0.1:9090/targets     # all prometheus jobs should be UP
open http://127.0.0.1:3000             # Grafana — single-page CryptoLake overview
```

The consolidation `/ready` and `/metrics` endpoints are new — they were added when fixing the `8003` exposure. If `consolidation:8003` shows as DOWN in Prometheus targets after a few minutes, your image is older than the fix.

On the VPS itself:

```bash
docker compose -f docker-compose.yml -f docker-compose.vps.yml ps
docker compose -f docker-compose.yml -f docker-compose.vps.yml logs -f writer
verify/build/install/verify/bin/verify verify --base-dir /data --date $(date -u -d 'yesterday' +%F)
```

The last command requires `:verify:installDist` to have been built on the host — or run a one-off container from the consolidation image, which already bundles `verify`'s dependencies.

---

## 9. Day-2 operations

### Updates

Use the rolling-update flow — see `docs/2026-05-08-rolling-update-plan.md` for the rationale and guarantees.

```bash
# laptop
git pull
CRYPTOLAKE_TAG=$(git rev-parse --short HEAD) \
    scripts/publish-images.sh cryptolake@$VPS_IP

# VPS — restart services one at a time, writer first, with healthcheck gating
cd /opt/cryptolake
sudo ./scripts/rolling-update.sh $(git rev-parse --short HEAD)
```

The script enforces preflight checks (images loaded, current stack healthy, not inside the 02:20–03:30 UTC consolidation window) and rolls back any service that fails to become `/ready` within 90 s. Services already on the new tag are skipped on re-run.

For maintenance restarts that the writer should classify as `planned` (not `host_reboot`), use the wrapper described in `CLAUDE.md`:

```bash
sudo bash scripts/cryptolake-maintenance.sh restart
```

### Backups

Two things need backing up:

1. **`/data`** (compressed archives + sidecars). At ~5–15 GB/day per stream, a daily `restic` or `borg` snapshot to S3/B2 is reasonable. Snapshot from the VPS while writers are live — files past their hour boundary are immutable.
2. **`postgres_data` Docker volume**. Hot dump nightly:

   ```bash
   docker exec cryptolake-postgres-1 \
       pg_dumpall -U cryptolake | gzip > /data/backups/pg-$(date +%F).sql.gz
   ```

   Keep 14 days, ship to off-VPS storage.

### Log volume

Docker daemon limits each container's logs to 50 MB × 3 files via `setup.sh`. Compose adds per-service overrides on top (collector/writer 50 MB × 5, others 10 MB × 3). If `/var/lib/docker/containers` grows unexpectedly, check that those overrides are still in `docker-compose.yml`.

### Firewall

Inbound from anywhere: only 22/tcp. Don't open Prometheus/Grafana/health ports to the public internet — keep using the SSH tunnels above. To open one anyway (don't):

```bash
sudo ufw allow from <your_ip>/32 to any port 9090 proto tcp
```

To open SSH from a new IP: `sudo ufw allow from <ip>/32 to any port 22 proto tcp`.

### Tailscale (optional)

Easier than SSH tunnels for daily dashboard access:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up --ssh
```

Then bind dashboards to the Tailscale interface only (replace `127.0.0.1` with `100.x.y.z` in compose). Or just `tailscale serve` Prometheus on a magic-DNS hostname.

### Disk-full hygiene

`disk-monitor.sh` fires every 6 h via cron and warns when `/data` ≥ 80 %. The chaos suite covers a real `disk_full_hold` scenario (`tests/chaos/02_disk_full_hold.sh`) — if you ever hit it in prod, the writer pauses cleanly until space frees up.

### Updates of the host itself

`unattended-upgrades` applies security patches automatically. Reboot once a month or whenever a kernel update lands:

```bash
sudo bash /opt/cryptolake/scripts/cryptolake-maintenance.sh stop
sudo reboot
# after boot:
sudo systemctl start cryptolake
```

---

## 10. Troubleshooting

| Symptom | First check |
|---------|-------------|
| `docker compose up -d` errors with "image not found" | You skipped `publish-images.sh` or the tag in `.env` doesn't match the loaded image. `docker image ls cryptolake/*`. |
| `consolidation:8003` is DOWN in Prometheus | Container is older than the fix that exposed `/metrics`. Re-publish images. |
| Writer container OOMs | Check `JAVA_TOOL_OPTIONS` in compose — writer is bounded to `-Xmx512m` for a reason. If you're running a much bigger symbol set, raise it together with the compose `deploy.resources.limits.memory`. |
| `cryptolake-verify` reports gaps with `reason=host_reboot` after a planned restart | You restarted with `sudo reboot` directly. Use `scripts/cryptolake-maintenance.sh restart` so the lifecycle ledger gets a `maintenance` intent first. |
| `docker compose ps` shows `consolidation` unhealthy after first start | Healthcheck waits 30 s for `/ready`. If it's still failing, check `docker compose logs consolidation` — the most common cause is `--base-dir` not matching where the writer writes. The default is `/data` to match `HOST_DATA_DIR`. |

---

## 11. What's deliberately not here

- **Public TLS / reverse proxy.** The stack is private-by-default; if you ever want Grafana exposed publicly, put Caddy or Traefik in front of it on a separate compose file and add a UFW rule for 443.
- **Secrets manager.** `.env` is fine for a single-host deployment. If you grow to multiple hosts, switch to age-encrypted env files or a real secrets backend.
- **S3 archival.** Design `2026-03-13` defers it. Today the archive lives on `/data` only; back it up out-of-band as in §9.
- **Multi-host failover.** Redundancy is inside the stack (collector + collector-backup), not across hosts. Losing the VPS loses live capture; restore from your `/data` backup and replay from `binance.vision` for what's beyond Binance's WS retention.
