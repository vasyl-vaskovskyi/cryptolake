# CryptoLake AWS Deployment

Single EC2 instance running the full docker-compose stack with CloudWatch monitoring and automated EBS backups.

## Prerequisites

- AWS CLI configured with credentials (`aws configure`)
- An EC2 key pair created in your target region
- Your public IP address (for SSH access)

## Deploy

```bash
# Find your public IP
MY_IP=$(curl -s https://checkip.amazonaws.com)

# Read secrets without writing them into shell history
read -s POSTGRES_PASSWORD
echo

# Deploy the stack
aws cloudformation deploy \
  --template-file infra/aws/cryptolake-ec2.yaml \
  --stack-name cryptolake \
  --parameter-overrides \
    SSHKeyName=your-key-pair-name \
    SSHAllowCIDR="${MY_IP}/32" \
    InstanceArchitecture=x86_64 \
    GitRepoURL=https://github.com/your-org/cryptolake.git \
    PostgresPassword="$POSTGRES_PASSWORD" \
  --capabilities CAPABILITY_IAM \
  --region us-east-1

unset POSTGRES_PASSWORD

# Get outputs (SSH command, etc.)
aws cloudformation describe-stacks \
  --stack-name cryptolake \
  --query 'Stacks[0].Outputs' \
  --output table
```

The stack takes ~10 minutes. CloudFormation waits for the UserData bootstrap
to complete (Docker install, image builds, stack startup, and service health
checks) before reporting `CREATE_COMPLETE`.

`InstanceArchitecture` must match the selected EC2 instance type. Keep
`x86_64` for `t3`, `m6i`, `c6i`, etc. Use `arm64` only with Graviton families
such as `t4g`, `m7g`, or `c7g`.

## Deploy to an Existing EC2 Instance

If you already have a running EC2 instance (not managed by CloudFormation), SSH
in and set up the stack manually:

```bash
ssh -i ~/.ssh/your-key.pem ec2-user@<your-instance-ip>

# Install Docker
sudo dnf update -y
sudo dnf install -y docker git xfsprogs
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker ec2-user

# Install Docker Compose plugin
sudo mkdir -p /usr/local/lib/docker/cli-plugins
ARCH=$(uname -m)
case "$ARCH" in
  x86_64) COMPOSE_ARCH=x86_64 ;;
  aarch64|arm64) COMPOSE_ARCH=aarch64 ;;
esac
sudo curl -SL "https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-$COMPOSE_ARCH" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# Re-login to pick up the docker group, then:
exit
ssh -i ~/.ssh/your-key.pem ec2-user@<your-instance-ip>

# Mount data volume (skip if already mounted)
# Identify the device (lsblk), then:
sudo mkfs.xfs /dev/<device>          # only if not yet formatted
sudo mkdir -p /data
sudo mount /dev/<device> /data
VOL_UUID=$(sudo blkid -s UUID -o value /dev/<device>)
echo "UUID=$VOL_UUID /data xfs defaults,nofail 0 2" | sudo tee -a /etc/fstab

# Clone and start the stack
sudo mkdir -p /opt/cryptolake
sudo chown ec2-user:ec2-user /opt/cryptolake
git clone -b main <your-repo-url> /opt/cryptolake
cd /opt/cryptolake

# Create .env
cat > .env << 'EOF'
POSTGRES_PASSWORD=<your-postgres-password>
WEBHOOK_URL=
HOST_DATA_DIR=/data
EOF
chmod 600 .env

docker compose up -d --build
docker compose ps   # verify all services are healthy
```

> **Note:** This does not set up CloudWatch agent, AWS Backup, or the Elastic IP.
> Those are managed by the CloudFormation template. If you need them on an
> existing instance, install the CloudWatch agent manually and configure AWS
> Backup through the console.

## Connect

```bash
# SSH into the instance
ssh -i ~/.ssh/your-key.pem ec2-user@<ElasticIP>

# Check the stack
cd /opt/cryptolake
docker compose ps
```

## Monitor

- **CloudWatch Metrics**: `CryptoLake` namespace — memory, disk, disk I/O
- **CloudWatch Logs**: `/cryptolake/docker` log group — all container logs
- **AWS Backup**: Daily EBS snapshots at 05:00 UTC, 7-day retention
- **Sampler**: Real-time terminal dashboard over SSH — run `sampler -c infra/sampler/sampler.yml` (no port forwarding needed)

## Host Lifecycle Agent

The lifecycle agent runs on the host (outside Docker) and records container
start/stop/die events to a JSONL ledger.  The writer reads this ledger on
startup to promote restart gap classification from generic Phase 1
(`collector`/`host`/`system`) to component-specific causes (`redpanda`,
`postgres`, `writer`).

### Install

The lifecycle agent requires a dedicated `cryptolake` user and a Python
virtualenv with the project's dependencies.

```bash
ssh -i ~/.ssh/your-key.pem ec2-user@<ElasticIP>

# 1. Create the cryptolake service user (if it doesn't already exist)
sudo useradd -r -s /sbin/nologin -d /opt/cryptolake cryptolake
sudo usermod -aG docker cryptolake

# 2. Create the Python virtualenv
cd /opt/cryptolake
python3 -m venv .venv
.venv/bin/pip install -e .

# 3. Create the ledger directory
sudo mkdir -p /data/.cryptolake/lifecycle
sudo chown cryptolake:docker /data/.cryptolake/lifecycle

# 4. Install the systemd unit
sudo cp /opt/cryptolake/infra/aws/systemd/cryptolake-lifecycle-agent.service \
    /etc/systemd/system/

# 5. Enable and start
sudo systemctl daemon-reload
sudo systemctl enable cryptolake-lifecycle-agent
sudo systemctl start cryptolake-lifecycle-agent

# 6. Verify
sudo systemctl status cryptolake-lifecycle-agent
journalctl -u cryptolake-lifecycle-agent -f
```

### What it provides (Phase 2 strict classification)

Without the lifecycle agent (Phase 1 only):
- Restart gaps classify as `collector`, `host`, or `system`
- Writer-only or redpanda crashes show as `collector/unclean_exit` or `system/unknown`

With the lifecycle agent (Phase 2):
- Container-specific die events are recorded with exit codes
- Writer can promote to `redpanda/unclean_exit`, `postgres/operator_shutdown`, etc.
- Maintenance intents written to the ledger survive PG outages

## Planned Maintenance

Use the maintenance wrapper to record intent before stopping services:

```bash
# Planned restart (records intent, then restarts)
scripts/cryptolake-maintenance.sh restart --reason "scheduled deploy"

# Planned stop (records intent, then stops)
scripts/cryptolake-maintenance.sh stop --reason "hardware maintenance"
```

The wrapper:
1. Writes maintenance intent to the host lifecycle ledger (always succeeds)
2. Attempts to write intent to PostgreSQL via CLI (best-effort — requires
   host-level access to port 5432; in the default docker-compose topology
   PostgreSQL is only on the internal Docker network, so this step is
   effectively ledger-only unless you add a `127.0.0.1:5432:5432` port
   binding to the `postgres` service)
3. Stops or restarts Docker Compose services

This ensures restart gaps are classified as `planned=true` even if PG is
the maintenance target.

## Update

```bash
ssh -i ~/.ssh/your-key.pem ec2-user@<ElasticIP>
cd /opt/cryptolake
git pull
docker compose up -d --build
```

## Update SSH allow-list

If your IP changes:

```bash
MY_IP=$(curl -s https://checkip.amazonaws.com)
aws cloudformation update-stack \
  --stack-name cryptolake \
  --use-previous-template \
  --parameters \
    ParameterKey=SSHKeyName,UsePreviousValue=true \
    ParameterKey=SSHAllowCIDR,ParameterValue="${MY_IP}/32" \
    ParameterKey=InstanceArchitecture,UsePreviousValue=true \
    ParameterKey=GitRepoURL,UsePreviousValue=true \
    ParameterKey=GitBranch,UsePreviousValue=true \
    ParameterKey=PostgresPassword,UsePreviousValue=true \
  --capabilities CAPABILITY_IAM \
  --region us-east-1
```

## Tear down

```bash
aws cloudformation delete-stack --stack-name cryptolake --region us-east-1
```

The data volume has `DeletionPolicy: Snapshot` — a final snapshot is taken
automatically before deletion.

## Cost estimate (~us-east-1)

| Resource | Monthly |
|----------|---------|
| EC2 t3.xlarge (24/7) | ~$120 |
| EBS gp3 500 GB + 30 GB root | ~$42 |
| Elastic IP | ~$3.65 |
| CloudWatch (logs + metrics) | ~$6 |
| AWS Backup snapshots | ~$5 |
| Data transfer (~50 GB) | ~$4.50 |
| **Total** | **~$181** |
