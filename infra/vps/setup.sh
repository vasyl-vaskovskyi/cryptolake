#!/usr/bin/env bash
# CryptoLake VPS Setup Script (Ubuntu 24.04)
# Run from repo root: sudo bash infra/vps/setup.sh
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_DIR="/data"
SWAP_SIZE="2G"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[+]${NC} $1"; }
warn()  { echo -e "${YELLOW}[!]${NC} $1"; }
error() { echo -e "${RED}[x]${NC} $1"; exit 1; }

# --- Pre-checks ---
[[ $EUID -ne 0 ]] && error "Run with sudo: sudo bash infra/vps/setup.sh"

REAL_USER="${SUDO_USER:-}"
[[ -z "$REAL_USER" || "$REAL_USER" == "root" ]] && error "Run as a regular user with sudo, not as root directly"

info "Setting up CryptoLake on $(hostname) for user: $REAL_USER"
info "Repo directory: $REPO_DIR"

# --- 1. System packages ---
info "Updating system packages..."
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get upgrade -y -qq

# --- 2. Install Docker ---
if ! command -v docker &>/dev/null; then
    info "Installing Docker..."
    apt-get install -y -qq ca-certificates curl gnupg
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
        > /etc/apt/sources.list.d/docker.list
    apt-get update -qq
    apt-get install -y -qq docker-ce docker-ce-cli containerd.io \
        docker-buildx-plugin docker-compose-plugin
    systemctl enable docker
else
    info "Docker already installed: $(docker --version)"
fi

if ! groups "$REAL_USER" | grep -q '\bdocker\b'; then
    info "Adding $REAL_USER to docker group..."
    usermod -aG docker "$REAL_USER"
    warn "Log out and back in for docker group to take effect"
fi

# --- 3. Install host dependencies (needed by sampler config) ---
info "Installing dependencies (curl, jq)..."
apt-get install -y -qq curl jq

# --- 4. SSH hardening ---
SSH_HARDENING="/etc/ssh/sshd_config.d/99-cryptolake-hardening.conf"
if [[ ! -f "$SSH_HARDENING" ]]; then
    info "Applying SSH hardening..."
    cat > "$SSH_HARDENING" <<'EOF'
PermitRootLogin no
MaxAuthTries 3
LoginGraceTime 30
ClientAliveInterval 300
ClientAliveCountMax 2
X11Forwarding no
AllowAgentForwarding no
EOF
    systemctl reload sshd
else
    info "SSH hardening already applied"
fi

# --- 5. fail2ban tuning ---
F2B_JAIL="/etc/fail2ban/jail.d/cryptolake-ssh.conf"
if [[ ! -f "$F2B_JAIL" ]]; then
    if command -v fail2ban-client &>/dev/null; then
        info "Configuring fail2ban SSH jail..."
        cat > "$F2B_JAIL" <<'EOF'
[sshd]
enabled = true
port = ssh
maxretry = 3
findtime = 600
bantime = 3600
EOF
        systemctl restart fail2ban
    else
        warn "fail2ban not installed — skipping jail configuration"
    fi
else
    info "fail2ban SSH jail already configured"
fi

# --- 6. Unattended security upgrades ---
if ! dpkg -l 2>/dev/null | grep -q unattended-upgrades; then
    info "Enabling unattended security upgrades..."
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq unattended-upgrades
    cat > /etc/apt/apt.conf.d/20auto-upgrades <<'EOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
APT::Periodic::AutocleanInterval "7";
EOF
else
    info "Unattended upgrades already enabled"
fi

# --- 7. Swap file ---
if ! swapon --show | grep -q /swapfile; then
    info "Creating ${SWAP_SIZE} swap file..."
    fallocate -l "$SWAP_SIZE" /swapfile
    chmod 600 /swapfile
    mkswap /swapfile >/dev/null
    swapon /swapfile
    if ! grep -q '/swapfile' /etc/fstab; then
        echo '/swapfile none swap sw 0 0' >> /etc/fstab
    fi
else
    info "Swap already active: $(swapon --show --noheadings)"
fi

# --- 8. Data directory ---
if [[ ! -d "$DATA_DIR" ]]; then
    info "Creating data directory at $DATA_DIR..."
    mkdir -p "$DATA_DIR"
fi
chown "$REAL_USER:$REAL_USER" "$DATA_DIR"
info "Data directory: $DATA_DIR (owner: $REAL_USER)"

# --- 9. Docker daemon log rotation ---
DOCKER_DAEMON="/etc/docker/daemon.json"
if [[ ! -f "$DOCKER_DAEMON" ]] || ! grep -q 'max-size' "$DOCKER_DAEMON" 2>/dev/null; then
    info "Configuring Docker log rotation..."
    mkdir -p /etc/docker
    cat > "$DOCKER_DAEMON" <<'EOF'
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "50m",
    "max-file": "3"
  }
}
EOF
    systemctl restart docker
else
    info "Docker log rotation already configured"
fi

# --- 10. Environment file ---
ENV_FILE="$REPO_DIR/.env"
if [[ ! -f "$ENV_FILE" ]]; then
    info "Creating .env from .env.example..."
    cp "$REPO_DIR/.env.example" "$ENV_FILE"
    chown "$REAL_USER:$REAL_USER" "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    warn ">>> Edit $ENV_FILE and fill in your secrets before deploying <<<"
else
    info ".env already exists (permissions: $(stat -c '%a' "$ENV_FILE"))"
fi

# --- 11. Systemd service ---
info "Installing systemd service..."
sed "s|WorkingDirectory=.*|WorkingDirectory=$REPO_DIR|g" \
    "$REPO_DIR/infra/vps/cryptolake.service" \
    > /etc/systemd/system/cryptolake.service
systemctl daemon-reload
systemctl enable cryptolake.service
info "Enabled cryptolake.service (auto-start on boot)"

# --- 12. Disk monitoring cron ---
CRON_SCRIPT="/usr/local/bin/cryptolake-disk-monitor"
cp "$REPO_DIR/infra/vps/disk-monitor.sh" "$CRON_SCRIPT"
chmod +x "$CRON_SCRIPT"
# Add cron job (every 6 hours) if not already present
if ! crontab -l 2>/dev/null | grep -q cryptolake-disk-monitor; then
    (crontab -l 2>/dev/null; echo "0 */6 * * * $CRON_SCRIPT") | crontab -
fi
info "Installed disk monitoring cron (every 6 hours)"

# --- 13. UFW verification ---
info "Verifying firewall..."
if ufw status | grep -q "Status: active"; then
    info "UFW is active:"
    ufw status | grep -v "^$"
else
    warn "UFW is not active! Run: sudo ufw allow 22 && sudo ufw enable"
fi

# --- Summary ---
echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  CryptoLake VPS setup complete!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo "  Next steps:"
echo "  1. Edit $ENV_FILE with your secrets"
echo "  2. Log out and back in (for docker group)"
echo "  3. Run: bash infra/vps/deploy.sh"
echo ""
echo "  Commands:"
echo "    Start:   sudo systemctl start cryptolake"
echo "    Stop:    sudo systemctl stop cryptolake"
echo "    Status:  sudo systemctl status cryptolake"
echo "    Logs:    cd $REPO_DIR && docker compose -f docker-compose.yml -f docker-compose.vps.yml logs -f"
echo "    Monitor: sampler -c $REPO_DIR/infra/sampler/sampler.yml"
echo ""
