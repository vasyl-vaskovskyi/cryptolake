#!/usr/bin/env bash
# CryptoPanner node bootstrap (master spec §6.a filesystem, §3.3 slot bootstrap).
# Idempotent: safe to re-run. Run as root on a fresh VPS.
set -euo pipefail

PREFIX=${PREFIX:-/opt/cryptopanner}
DATA=${DATA:-/data/cryptopanner}
ETC=${ETC:-/etc/cryptopanner}
USER=${CP_USER:-cryptopanner}

echo "==> creating system user ${USER}"
if ! id "${USER}" >/dev/null 2>&1; then
  useradd --system --home-dir "${DATA}" --shell /usr/sbin/nologin "${USER}"
fi

echo "==> creating data layout under ${DATA} (§6.a)"
install -d -o "${USER}" -g "${USER}" -m 0750 \
  "${DATA}" "${DATA}/segments" "${DATA}/sealed" "${DATA}/staging" \
  "${DATA}/deploy" "${DATA}/deploy/superseded" "${DATA}/logs"

echo "==> bootstrap active-slot = a (§3.3)"
if [ ! -f "${DATA}/deploy/active-slot" ]; then
  echo a > "${DATA}/deploy/active-slot"
  chown "${USER}:${USER}" "${DATA}/deploy/active-slot"
fi

echo "==> config + secrets under ${ETC}"
install -d -o "${USER}" -g "${USER}" -m 0750 "${ETC}"
if [ ! -f "${ETC}/config.yaml" ]; then
  echo "  (place your §15.b config.yaml at ${ETC}/config.yaml)"
fi
if [ ! -f "${ETC}/agent.token" ]; then
  echo "==> generating agent bearer token (§4.f: >=32 random bytes, base64url, no padding)"
  head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n' > "${ETC}/agent.token"
  chown "${USER}:${USER}" "${ETC}/agent.token"
  chmod 0600 "${ETC}/agent.token"
fi

echo "==> installing systemd units"
install -m 0644 "$(dirname "$0")/systemd/"*.service /etc/systemd/system/
systemctl daemon-reload

echo "==> enabling units; starting only the active slot (@a) + the other services (§3.3)"
systemctl enable cryptopanner-collector@a.service cryptopanner-collector@b.service \
  cryptopanner-sealer.service cryptopanner-uploader.service cryptopanner-agent.service
systemctl start cryptopanner-collector@a.service \
  cryptopanner-sealer.service cryptopanner-uploader.service cryptopanner-agent.service
# Slot b stays enabled but stopped — the first deploy stages into it and flips active-slot.

echo "==> done. Verify with: systemctl status 'cryptopanner-*' and curl the agent /status."
