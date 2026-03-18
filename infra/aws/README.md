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
read -s GRAFANA_PASSWORD
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
    GrafanaPassword="$GRAFANA_PASSWORD" \
  --capabilities CAPABILITY_IAM \
  --region us-east-1

unset POSTGRES_PASSWORD GRAFANA_PASSWORD

# Get outputs (SSH command, Grafana tunnel, etc.)
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

## Connect

```bash
# SSH into the instance
ssh -i ~/.ssh/your-key.pem ec2-user@<ElasticIP>

# Check the stack
cd /opt/cryptolake
docker compose ps

# Grafana (via SSH tunnel)
ssh -L 3000:localhost:3000 -i ~/.ssh/your-key.pem ec2-user@<ElasticIP>
# Then open http://localhost:3000
```

## Monitor

- **CloudWatch Metrics**: `CryptoLake` namespace — memory, disk, disk I/O
- **CloudWatch Logs**: `/cryptolake/docker` log group — all container logs
- **AWS Backup**: Daily EBS snapshots at 05:00 UTC, 7-day retention
- **Grafana**: Application-level dashboards via SSH tunnel on port 3000

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
    ParameterKey=GrafanaPassword,UsePreviousValue=true \
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
