#!/usr/bin/env bash
# Hardens a fresh Ubuntu 24.04 VPS for hosting CryptoLake.
#
# Scope (security ONLY — does not install Docker or the project):
#   - SSH: disable password + keyboard-interactive auth, require pubkey
#   - SSH: lock down PermitRootLogin, AllowAgentForwarding, X11
#   - UFW firewall: deny incoming, allow 22/tcp from anywhere by default
#   - fail2ban with an SSH jail
#   - Unattended security upgrades
#
# Run as a non-root user with sudo:  sudo bash infra/vps/harden.sh
#
# After this script runs, password-based SSH and root SSH are GONE.
# Only the user(s) who already have entries in ~/.ssh/authorized_keys
# can log in. The script refuses to run if it can't find any.
#
set -euo pipefail

red()    { printf '\033[0;31m[x]\033[0m %s\n' "$*" >&2; }
yellow() { printf '\033[1;33m[!]\033[0m %s\n' "$*"; }
green()  { printf '\033[0;32m[+]\033[0m %s\n' "$*"; }
section(){ echo; printf '\033[1m== %s ==\033[0m\n' "$*"; }

require_root() {
  if [[ $EUID -ne 0 ]]; then
    red "Run with sudo: sudo bash infra/vps/harden.sh"
    exit 1
  fi
  if [[ -z "${SUDO_USER:-}" || "$SUDO_USER" == "root" ]]; then
    red "Run via sudo as a non-root user (not as root directly)."
    red "Disabling root SSH while logged in as root would lock you out."
    exit 1
  fi
}

# Lockout protection: confirm the invoking user has an authorized_keys
# file with at least one key. We're about to disable password auth, so
# without a key in place this would brick the machine.
require_authorized_keys() {
  local home
  home=$(getent passwd "$SUDO_USER" | cut -d: -f6)
  local keys="$home/.ssh/authorized_keys"
  if [[ ! -s "$keys" ]]; then
    red "$SUDO_USER has no $keys (or it is empty)."
    red "If we disable password auth now you will be locked out."
    red "Copy your public key into place first:"
    red "  ssh-copy-id $SUDO_USER@<this-host>   # from your laptop"
    exit 1
  fi
  local n
  n=$(grep -cv '^\s*\(#\|$\)' "$keys")
  green "Found $n SSH key(s) in $keys — safe to disable passwords."
}

apply_ssh_hardening() {
  section "SSH hardening"
  local conf=/etc/ssh/sshd_config.d/99-cryptolake-hardening.conf
  cat > "$conf" <<'EOF'
# Cryptolake — SSH hardening (managed by infra/vps/harden.sh)
PermitRootLogin no
PasswordAuthentication no
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
UsePAM yes
PubkeyAuthentication yes
AuthenticationMethods publickey
PermitEmptyPasswords no
MaxAuthTries 3
LoginGraceTime 30
ClientAliveInterval 300
ClientAliveCountMax 2
X11Forwarding no
AllowAgentForwarding no
AllowTcpForwarding yes
EOF
  chmod 644 "$conf"

  # Validate before reloading — a malformed sshd_config can lock the host.
  if ! sshd -t; then
    red "sshd config validation failed; not reloading. See above."
    rm -f "$conf"
    exit 1
  fi
  systemctl reload ssh || systemctl reload sshd
  green "SSH: passwords + keyboard-interactive disabled, pubkey-only enforced"
}

apply_ufw() {
  section "UFW firewall"
  apt-get install -y -qq ufw >/dev/null
  # Default: deny incoming, allow outgoing
  ufw default deny incoming >/dev/null
  ufw default allow outgoing >/dev/null
  ufw allow 22/tcp comment 'ssh' >/dev/null
  if ! ufw status | grep -q 'Status: active'; then
    yellow "Enabling UFW (current SSH session preserved by 'allow 22/tcp')..."
    ufw --force enable >/dev/null
  fi
  ufw status numbered | sed 's/^/  /'
}

apply_fail2ban() {
  section "fail2ban"
  apt-get install -y -qq fail2ban >/dev/null
  local jail=/etc/fail2ban/jail.d/cryptolake-ssh.conf
  cat > "$jail" <<'EOF'
[sshd]
enabled  = true
port     = ssh
maxretry = 3
findtime = 600
bantime  = 3600
EOF
  systemctl enable --now fail2ban >/dev/null
  systemctl restart fail2ban
  green "fail2ban: 3 retries / 10 min → 1 h ban"
}

apply_unattended_upgrades() {
  section "Unattended security upgrades"
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq unattended-upgrades >/dev/null
  cat > /etc/apt/apt.conf.d/20auto-upgrades <<'EOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
APT::Periodic::AutocleanInterval "7";
EOF
  green "Daily security updates enabled"
}

main() {
  require_root
  require_authorized_keys

  section "Updating package index"
  apt-get update -qq

  apply_ssh_hardening
  apply_ufw
  apply_fail2ban
  apply_unattended_upgrades

  echo
  green "Hardening complete."
  echo
  echo "Verify from a SECOND terminal before closing this one:"
  echo "  ssh $SUDO_USER@<this-host>          # must succeed via key"
  echo "  ssh -o PreferredAuthentications=password $SUDO_USER@<this-host>"
  echo "                                       # must FAIL (passwords off)"
  echo
  echo "Next: run infra/vps/bootstrap.sh to install Docker + the project."
}

main "$@"
