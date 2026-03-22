# CryptoLake AWS EC2 Deployment — Design Spec

## Goal

Deploy the existing docker-compose stack (collector, writer, redpanda, postgres, prometheus, grafana, alertmanager) onto a single EC2 instance with security group (SSH-only), EBS data volume, CloudWatch agent, and automated EBS backups — all defined in a CloudFormation template.

## Architecture

Single EC2 instance running Docker Compose in the default VPC. An attached EBS gp3 volume at `/data` holds compressed market archives. The CloudWatch agent ships system metrics (CPU, memory, disk) and Docker container logs to CloudWatch. SSH tunnel is the only access method — no ports exposed besides 22. AWS Backup handles daily EBS snapshots with 7-day retention.

## Decisions

- **Default VPC**: No custom networking. Keeps the template simple. Can be added later.
- **SSH-only**: Grafana via `ssh -L 3000:localhost:3000`. Zero attack surface beyond SSH.
- **Amazon Linux 2023**: Docker available via `dnf`, CloudWatch agent via `rpm`.
- **Architecture parameter selects x86_64 or arm64 AMIs**: Must match the chosen EC2 instance type.
- **gp3 EBS**: 3000 IOPS baseline, no provisioned cost. Sufficient for Writer fsync workload.
- **Pinned shared AZ selection**: Instance and EBS volume both use `!Select [0, !GetAZs '']` to avoid a CloudFormation create-order deadlock.
- **UserData bootstrap**: Installs Docker + Compose, formats/mounts EBS, installs CloudWatch agent, clones repo, starts stack, and verifies service health. No AMI baking needed.
- **IAM**: `CloudWatchAgentServerPolicy` + `AmazonSSMManagedInstanceCore` (SSM as SSH backup).
- **No S3 sync yet**: Archives stay on EBS. S3 archival is a separate future task.

## What the template creates

| Resource | Type | Details |
|----------|------|---------|
| Security Group | `AWS::EC2::SecurityGroup` | SSH (22) from `SSHAllowCIDR` parameter |
| IAM Role | `AWS::IAM::Role` | EC2 assume role, CloudWatch + SSM policies |
| Instance Profile | `AWS::IAM::InstanceProfile` | Attaches role to instance |
| EC2 Instance | `AWS::EC2::Instance` | AL2023, parameterized type, UserData bootstrap |
| EBS Data Volume | `AWS::EC2::Volume` | gp3, parameterized size, same AZ as instance |
| Volume Attachment | `AWS::EC2::VolumeAttachment` | Mounts EBS to instance at `/dev/xvdf` |
| Elastic IP | `AWS::EC2::EIP` | Stable public IP |
| EIP Association | `AWS::EC2::EIPAssociation` | Binds EIP to instance |
| Backup Vault | `AWS::Backup::BackupVault` | Stores EBS snapshots |
| Backup Plan | `AWS::Backup::BackupPlan` | Daily snapshot, 7-day retention |
| Backup Selection | `AWS::Backup::BackupSelection` | Targets the data volume |

## Parameters

| Name | Default | Description |
|------|---------|-------------|
| `SSHKeyName` | (required) | Existing EC2 key pair name |
| `SSHAllowCIDR` | (required) | IP range allowed to SSH (e.g. `1.2.3.4/32`) |
| `InstanceType` | `t3.xlarge` | EC2 instance type |
| `InstanceArchitecture` | `x86_64` | AMI architecture; must match the EC2 instance type |
| `DataVolumeSize` | `500` | EBS data volume size in GB |
| `GitRepoURL` | (required) | Git repo URL to clone |
| `GitBranch` | `main` | Branch to checkout |
| `PostgresPassword` | (required) | Password for the PostgreSQL database (`NoEcho`) |
| `GrafanaPassword` | (required) | Password for the Grafana admin user (`NoEcho`) |

## CloudWatch Agent Config

Metrics collected (60s interval):
- `mem_used_percent` — memory usage
- `disk_used_percent` — `/` and `/data` partitions
- `diskio_*` — read/write bytes and ops on all block devices

Logs collected:
- Docker container logs from `/var/lib/docker/containers/*/*.log`
  → CloudWatch Logs group `/cryptolake/docker`, 30-day retention
- UserData bootstrap log from `/var/log/user-data.log`
  → CloudWatch Logs group `/cryptolake/user-data`, 7-day retention

## UserData Bootstrap

The instance uses `CreationPolicy` with `cfn-signal` so CloudFormation
waits up to 15 minutes for the bootstrap to succeed before reporting
`CREATE_COMPLETE`. On failure, the signal reports the error and the stack
rolls back.

The EBS volume detection includes an NVMe fallback (`/dev/nvme1n1`) for
Nitro-based instance types that remap `/dev/xvdf`. The fstab entry uses
the volume UUID for reliability across reboots.

## Estimated Monthly Cost (us-east-1)

| Resource | Cost |
|----------|------|
| EC2 t3.xlarge (24/7) | ~$120 |
| EBS gp3 500 GB | ~$40 |
| Elastic IP | ~$3.65 |
| CloudWatch Logs (5 GB) | ~$2.50 |
| CloudWatch Metrics (custom) | ~$3 |
| AWS Backup (500 GB snaps, 7-day) | ~$5 |
| Data transfer (~50 GB) | ~$4.50 |
| **Total** | **~$179/month** |
