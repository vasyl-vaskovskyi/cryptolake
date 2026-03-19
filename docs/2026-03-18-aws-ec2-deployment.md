# CryptoLake AWS EC2 Deployment — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a CloudFormation template that deploys the CryptoLake docker-compose stack on a single EC2 instance with security group, EBS data volume, CloudWatch agent, and automated backups.

**Architecture:** Single CloudFormation stack creates an EC2 instance in the default VPC with an attached gp3 EBS volume for `/data`. The instance and data volume are pinned to the same selected AZ to avoid a create-order deadlock. UserData bootstraps Docker, Docker Compose, CloudWatch agent, clones the repo, starts the stack, and waits for service health. Security group allows SSH only. AWS Backup takes daily EBS snapshots.

**Tech Stack:** AWS CloudFormation (YAML), Amazon Linux 2023, Docker Compose, CloudWatch Agent

**Spec:** `docs/superpowers/specs/2026-03-18-aws-ec2-deployment-design.md`

---

## Chunk 1: CloudFormation Template

### Task 1: Create the CloudFormation template with Parameters and IAM resources

**Files:**
- Create: `infra/aws/cryptolake-ec2.yaml`

- [ ] **Step 1: Create the template header and Parameters section**

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: >-
  CryptoLake EC2 deployment — single instance running docker-compose
  with CloudWatch agent, EBS data volume, and automated backups.

Parameters:
  SSHKeyName:
    Type: AWS::EC2::KeyPair::KeyName
    Description: Name of an existing EC2 key pair for SSH access

  SSHAllowCIDR:
    Type: String
    Description: CIDR block allowed to SSH (e.g. 203.0.113.5/32)
    AllowedPattern: '(\d{1,3}\.){3}\d{1,3}/\d{1,2}'
    ConstraintDescription: Must be a valid CIDR (e.g. 1.2.3.4/32)

  InstanceType:
    Type: String
    Default: t3.xlarge
    Description: EC2 instance type (4 vCPU, 16 GB recommended minimum)

  InstanceArchitecture:
    Type: String
    Default: x86_64
    AllowedValues:
      - x86_64
      - arm64
    Description: Must match the selected EC2 instance type architecture

  DataVolumeSize:
    Type: Number
    Default: 500
    MinValue: 100
    Description: Size of the /data EBS volume in GB

  GitRepoURL:
    Type: String
    Description: Git repository URL to clone (HTTPS or SSH)

  GitBranch:
    Type: String
    Default: main
    Description: Git branch to checkout

Conditions:
  UseArm64: !Equals [!Ref InstanceArchitecture, arm64]
```

- [ ] **Step 2: Add IAM Role, Policies, and Instance Profile**

```yaml
Resources:
  InstanceRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: ec2.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - !Sub 'arn:${AWS::Partition}:iam::aws:policy/CloudWatchAgentServerPolicy'
        - !Sub 'arn:${AWS::Partition}:iam::aws:policy/AmazonSSMManagedInstanceCore'
      Tags:
        - Key: Name
          Value: !Sub '${AWS::StackName}-role'

  InstanceProfile:
    Type: AWS::IAM::InstanceProfile
    Properties:
      Roles:
        - !Ref InstanceRole
```

- [ ] **Step 3: Commit skeleton**

```bash
git add infra/aws/cryptolake-ec2.yaml
git commit -m "feat(infra): add CloudFormation template skeleton with parameters and IAM"
```

---

### Task 2: Add Security Group, Elastic IP, EBS Volume, and EC2 Instance

**Files:**
- Modify: `infra/aws/cryptolake-ec2.yaml`

- [ ] **Step 1: Add Security Group**

```yaml
  SecurityGroup:
    Type: AWS::EC2::SecurityGroup
    Properties:
      GroupDescription: !Sub '${AWS::StackName} — SSH only'
      SecurityGroupIngress:
        - IpProtocol: tcp
          FromPort: 22
          ToPort: 22
          CidrIp: !Ref SSHAllowCIDR
          Description: SSH access from allowed CIDR
      Tags:
        - Key: Name
          Value: !Sub '${AWS::StackName}-sg'
```

- [ ] **Step 2: Add Elastic IP and EIP Association**

```yaml
  ElasticIP:
    Type: AWS::EC2::EIP
    Properties:
      Domain: vpc
      Tags:
        - Key: Name
          Value: !Sub '${AWS::StackName}-eip'

  EIPAssociation:
    Type: AWS::EC2::EIPAssociation
    Properties:
      InstanceId: !Ref EC2Instance
      AllocationId: !GetAtt ElasticIP.AllocationId
```

- [ ] **Step 3: Add EBS Data Volume and Attachment**

```yaml
  DataVolume:
    Type: AWS::EC2::Volume
    DeletionPolicy: Snapshot
    Properties:
      AvailabilityZone: !Select [0, !GetAZs '']
      Size: !Ref DataVolumeSize
      VolumeType: gp3
      Encrypted: true
      Tags:
        - Key: Name
          Value: !Sub '${AWS::StackName}-data'
        - Key: Backup
          Value: 'true'

  DataVolumeAttachment:
    Type: AWS::EC2::VolumeAttachment
    Properties:
      InstanceId: !Ref EC2Instance
      VolumeId: !Ref DataVolume
      Device: /dev/xvdf
```

- [ ] **Step 4: Add EC2 Instance with UserData**

The UserData script:
1. Installs Docker and Docker Compose plugin
2. Formats and mounts the EBS data volume at `/data`
3. Adds fstab entry for persistence across reboots
4. Installs and configures the CloudWatch agent
5. Clones the repo, creates `.env`, starts docker-compose

```yaml
  EC2Instance:
    Type: AWS::EC2::Instance
    Properties:
      AvailabilityZone: !Select [0, !GetAZs '']
      ImageId: !If
        - UseArm64
        - '{{resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64}}'
        - '{{resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64}}'
      InstanceType: !Ref InstanceType
      KeyName: !Ref SSHKeyName
      IamInstanceProfile: !Ref InstanceProfile
      SecurityGroupIds:
        - !GetAtt SecurityGroup.GroupId
      BlockDeviceMappings:
        - DeviceName: /dev/xvda
          Ebs:
            VolumeSize: 30
            VolumeType: gp3
            Encrypted: true
      Tags:
        - Key: Name
          Value: !Sub '${AWS::StackName}'
      UserData:
        Fn::Base64: !Sub |
          #!/bin/bash
          set -euo pipefail
          exec > >(tee /var/log/user-data.log) 2>&1

          echo "=== Installing Docker ==="
          dnf update -y
          dnf install -y docker git xfsprogs aws-cfn-bootstrap
          systemctl enable docker
          systemctl start docker
          usermod -aG docker ec2-user

          # Install Docker Compose plugin
          mkdir -p /usr/local/lib/docker/cli-plugins
          ARCH=$(uname -m)
          case "$ARCH" in
            x86_64) COMPOSE_ARCH=x86_64 ;;
            aarch64|arm64) COMPOSE_ARCH=aarch64 ;;
            *) echo "ERROR: Unsupported architecture: $ARCH"; exit 1 ;;
          esac
          curl -SL "https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-$${COMPOSE_ARCH}" \
            -o /usr/local/lib/docker/cli-plugins/docker-compose
          chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

          echo "=== Formatting and mounting EBS data volume ==="
          # Wait for the volume to attach
          while [ ! -e /dev/xvdf ]; do sleep 1; done
          # Only format if not already formatted
          if ! blkid /dev/xvdf; then
            mkfs.xfs /dev/xvdf
          fi
          mkdir -p /data
          mount /dev/xvdf /data
          # Add fstab entry for reboot persistence
          echo '/dev/xvdf /data xfs defaults,nofail 0 2' >> /etc/fstab

          echo "=== Installing CloudWatch Agent ==="
          dnf install -y amazon-cloudwatch-agent
          cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << 'CWCONFIG'
          {
            "agent": {
              "metrics_collection_interval": 60,
              "logfile": "/opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log"
            },
            "metrics": {
              "namespace": "CryptoLake",
              "append_dimensions": {
                "InstanceId": "${!aws:InstanceId}"
              },
              "metrics_collected": {
                "mem": {
                  "measurement": ["mem_used_percent"]
                },
                "disk": {
                  "measurement": ["disk_used_percent"],
                  "resources": ["/", "/data"],
                  "ignore_file_system_types": ["sysfs", "devtmpfs", "tmpfs"]
                },
                "diskio": {
                  "measurement": ["reads", "writes", "read_bytes", "write_bytes"],
                  "resources": ["xvdf"]
                }
              }
            },
            "logs": {
              "logs_collected": {
                "files": {
                  "collect_list": [
                    {
                      "file_path": "/var/lib/docker/containers/*/*.log",
                      "log_group_name": "/cryptolake/docker",
                      "log_stream_name": "{instance_id}/{file_name}",
                      "retention_in_days": 30
                    },
                    {
                      "file_path": "/var/log/user-data.log",
                      "log_group_name": "/cryptolake/user-data",
                      "log_stream_name": "{instance_id}",
                      "retention_in_days": 7
                    }
                  ]
                }
              }
            }
          }
          CWCONFIG
          /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
            -a fetch-config -m ec2 \
            -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s

          echo "=== Cloning repo and starting stack ==="
          cd /opt
          git clone -b ${GitBranch} ${GitRepoURL} cryptolake
          cd cryptolake

          # Create .env file
          cat > .env << 'ENVFILE'
          POSTGRES_PASSWORD=changeme-in-production
          GF_ADMIN_PASSWORD=changeme-in-production
          WEBHOOK_URL=
          GRAFANA_BIND=127.0.0.1
          HOST_DATA_DIR=/data
          ENVFILE

          # Start the stack and wait for health
          docker compose up -d --build
          REQUIRED_SERVICES="postgres redpanda collector writer prometheus grafana alertmanager"
          unhealthy_services=""
          for i in $(seq 1 60); do
            unhealthy_services=""
            for service in $REQUIRED_SERVICES; do
              container_id=$(docker compose ps -q "$service" || true)
              if [ -z "$container_id" ]; then
                unhealthy_services="$unhealthy_services ${service}:missing"
                continue
              fi
              status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id")
              if [ "$status" != "healthy" ] && [ "$status" != "running" ]; then
                unhealthy_services="$unhealthy_services ${service}:$status"
              fi
            done
            if [ -z "$unhealthy_services" ]; then
              break
            fi
            sleep 5
          done
          if [ -n "$unhealthy_services" ]; then
            echo "ERROR: Services failed to become healthy in time:$unhealthy_services"
            exit 1
          fi

          echo "=== Bootstrap complete ==="
```

- [ ] **Step 5: Commit EC2 and networking resources**

```bash
git add infra/aws/cryptolake-ec2.yaml
git commit -m "feat(infra): add EC2 instance, security group, EBS volume, CloudWatch agent"
```

---

### Task 3: Add AWS Backup and Outputs

**Files:**
- Modify: `infra/aws/cryptolake-ec2.yaml`

- [ ] **Step 1: Add AWS Backup resources for daily EBS snapshots**

```yaml
  BackupVault:
    Type: AWS::Backup::BackupVault
    Properties:
      BackupVaultName: !Sub '${AWS::StackName}-vault'

  BackupPlan:
    Type: AWS::Backup::BackupPlan
    Properties:
      BackupPlan:
        BackupPlanName: !Sub '${AWS::StackName}-daily'
        BackupPlanRule:
          - RuleName: DailyBackup
            TargetBackupVault: !Ref BackupVault
            ScheduleExpression: 'cron(0 5 * * ? *)'
            StartWindowMinutes: 60
            CompletionWindowMinutes: 180
            Lifecycle:
              DeleteAfterDays: 7

  BackupRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: backup.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - !Sub 'arn:${AWS::Partition}:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForBackup'
        - !Sub 'arn:${AWS::Partition}:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForRestores'

  BackupSelection:
    Type: AWS::Backup::BackupSelection
    Properties:
      BackupPlanId: !Ref BackupPlan
      BackupSelection:
        SelectionName: !Sub '${AWS::StackName}-data-volume'
        IamRoleArn: !GetAtt BackupRole.Arn
        ListOfTags:
          - ConditionType: STRINGEQUALS
            ConditionKey: Backup
            ConditionValue: 'true'
```

- [ ] **Step 2: Add Outputs section**

```yaml
Outputs:
  PublicIP:
    Description: Elastic IP address for SSH access
    Value: !Ref ElasticIP

  SSHCommand:
    Description: SSH command to connect
    Value: !Sub 'ssh -i ~/.ssh/${SSHKeyName}.pem ec2-user@${ElasticIP}'

  GrafanaTunnel:
    Description: SSH tunnel command for Grafana access
    Value: !Sub 'ssh -L 3000:localhost:3000 -i ~/.ssh/${SSHKeyName}.pem ec2-user@${ElasticIP}'

  InstanceId:
    Description: EC2 Instance ID
    Value: !Ref EC2Instance

  DataVolumeId:
    Description: EBS Data Volume ID
    Value: !Ref DataVolume
```

- [ ] **Step 3: Commit backup and outputs**

```bash
git add infra/aws/cryptolake-ec2.yaml
git commit -m "feat(infra): add AWS Backup daily snapshots and stack outputs"
```

---

## Chunk 2: Validate and Document

### Task 4: Validate the CloudFormation template

**Files:**
- Read: `infra/aws/cryptolake-ec2.yaml`

- [ ] **Step 1: Validate template syntax with cfn-lint (via awsiac MCP tool)**

Use the `validate_cloudformation_template` MCP tool to check the template for syntax errors, invalid resource properties, and schema violations.

Expected: No errors. Warnings about SSM parameter resolution are acceptable.

- [ ] **Step 2: Check template compliance with cfn-guard (via awsiac MCP tool)**

Use the `check_cloudformation_template_compliance` MCP tool to validate security and compliance rules.

Expected: Pass. EBS encryption is enabled, security group is restrictive, IAM uses managed policies.

- [ ] **Step 3: Fix any issues found by validation**

Address errors or warnings from steps 1-2. Re-run validation until clean.

- [ ] **Step 4: Commit any fixes**

```bash
git add infra/aws/cryptolake-ec2.yaml
git commit -m "fix(infra): address CloudFormation validation findings"
```

---

### Task 5: Add deployment instructions

**Files:**
- Create: `infra/aws/README.md`

- [ ] **Step 1: Write deployment README**

```markdown
# CryptoLake AWS Deployment

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

## Connect

```bash
# SSH into the instance
ssh -i ~/.ssh/your-key.pem ec2-user@<ElasticIP>

# Grafana (via SSH tunnel)
ssh -L 3000:localhost:3000 -i ~/.ssh/your-key.pem ec2-user@<ElasticIP>
# Then open http://localhost:3000
```

## Monitor

- CloudWatch Logs: `/cryptolake/docker` log group
- CloudWatch Metrics: `CryptoLake` namespace (memory, disk, disk I/O)
- AWS Backup: Daily EBS snapshots with 7-day retention

## Update the stack

```bash
# SSH in and pull latest code
ssh -i ~/.ssh/your-key.pem ec2-user@<ElasticIP>
cd /opt/cryptolake
git pull
docker compose up -d --build
```

## Tear down

```bash
aws cloudformation delete-stack --stack-name cryptolake --region us-east-1
```

Note: The data volume has `DeletionPolicy: Snapshot` — a final snapshot
is taken automatically before deletion.
```

- [ ] **Step 2: Commit README**

```bash
git add infra/aws/README.md
git commit -m "docs(infra): add AWS deployment instructions"
```

---

## Deployment Checklist

After running `aws cloudformation deploy`:

- [ ] Stack reaches `CREATE_COMPLETE` status
- [ ] SSH into instance: `ssh -i key.pem ec2-user@<EIP>`
- [ ] Verify Docker running: `docker compose ps` (all services healthy)
- [ ] Verify data volume: `df -h /data` (shows correct size)
- [ ] Verify CloudWatch metrics: check `CryptoLake` namespace in console
- [ ] Verify CloudWatch Logs: check `/cryptolake/docker` log group
- [ ] Verify Grafana tunnel: `ssh -L 3000:localhost:3000 ...`, open `localhost:3000`
- [ ] Verify backups: check AWS Backup console for scheduled backup plan
