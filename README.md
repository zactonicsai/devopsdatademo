# zacamikafka — Custom Kafka-Ready AMI

This project builds a custom AWS AMI named **`zacamikafka`** from the latest Amazon Linux 2023 base image, pre-installed with **Python**, **Java 21 (Corretto, LTS)**, and **all Apache Kafka prerequisites**. It also provisions an IAM instance profile so instances launched from the AMI can be reached via **AWS Systems Manager (SSM) Session Manager** — no SSH keys or open inbound ports required.

Three approaches are documented:
1. **Terraform** (`main.tf`) — declarative, automated build.
2. **Manual AWS CLI** — the same steps run by hand.
3. **Ansible** (`kafka-prereqs.yml`) — a configuration-management alternative for installing prerequisites.

---

## What gets installed

- Python 3 + pip + dev/build tooling (`gcc`, `make`, `git`)
- Java 21 (Amazon Corretto, LTS) and its devel package — Kafka 4.x runs on the JVM (Java 17+; 21 and 25 also supported)
- Apache Kafka 4.3.0 binaries under `/opt/kafka`
- Networking/diagnostic tools: `nc`, `telnet`, `jq`
- Python Kafka client libraries: `kafka-python`, `confluent-kafka`
- SSM agent enabled (preinstalled on AL2023)

---

## Prerequisites

- Terraform >= 1.5.0
- AWS CLI v2 configured with credentials (`aws configure`)
- An AWS account with permission to create EC2 instances, AMIs, IAM roles, and security groups
- Outbound internet access from the builder subnet (for package downloads)

---

## Quick start (Terraform)

```bash
terraform init
terraform plan
terraform apply
```

After apply, the new AMI ID is shown in the `new_ami_id` output. Launch an instance from it, attaching the `zacamikafka-ssm-profile` instance profile, then connect:

```bash
aws ssm start-session --target i-xxxxxxxxxxxxxxxxx
```

---

## Terraform file — line-by-line explanation

### `terraform { ... }` block
Pins the required Terraform version (`>= 1.5.0`) and declares the AWS provider (`hashicorp/aws ~> 5.0`). Pinning prevents surprise breakages when newer, incompatible versions are released.

### `provider "aws"`
Configures the AWS provider to operate in the region set by `var.aws_region`. All resources are created in this region.

### Variables
- `aws_region` — target region (default `us-east-1`).
- `instance_type` — the builder instance size (`t3.medium` gives enough memory/CPU for fast package installs).
- `ami_name` — the resulting image name (`zacamikafka`).
- `subnet_id` — optional explicit subnet; if blank, the config falls back to the default VPC's first subnet so it runs with zero extra setup.

### `data "aws_ami" "base"`
Queries AWS for the **latest** Amazon Linux 2023 x86_64 HVM image owned by Amazon (`most_recent = true`). This guarantees you always build on the newest patched base image rather than a hard-coded, aging AMI ID.

### `data "aws_vpc" "default"` / `data "aws_subnets" "default"`
Looks up the account's default VPC and its subnets so the builder has somewhere to launch when no subnet is supplied.

### `locals.builder_subnet`
Chooses the user-supplied subnet if present, otherwise the first default subnet — a small bit of logic that keeps the module turnkey.

### IAM block (`assume`, `aws_iam_role.ssm`, `ssm_core`, `aws_iam_instance_profile.ssm`)
- The assume-role policy lets the EC2 service assume the role.
- The role attaches the AWS-managed **`AmazonSSMManagedInstanceCore`** policy, which is exactly what the SSM agent needs to register with Systems Manager and serve Session Manager connections.
- The instance profile is the wrapper EC2 actually attaches; SSM access "comes from" this profile being present on the instance, which is why no SSH ingress is needed.

### `aws_security_group.builder`
Creates a security group with **egress only** (all outbound). SSM Session Manager works entirely over outbound HTTPS (443) to AWS endpoints, so no inbound rules — and therefore no exposed SSH — are required. This is the security win of SSM over key-based SSH.

### `aws_instance.builder`
Launches an instance from the base AMI with the SSM instance profile and the egress-only SG. The `user_data` shell script does the heavy lifting: updates the OS, installs Python/Java/utilities, downloads and unpacks Kafka into `/opt/kafka`, installs Python Kafka clients, enables the SSM agent, and drops a completion marker file. Note the `$${...}` escaping — Terraform interpolates `${...}`, so doubling the `$` passes a literal `${...}` through to the shell.

### `aws_ami_from_instance.zacamikafka`
Snapshots the now-provisioned instance into a reusable AMI named `zacamikafka`. `depends_on` ensures the instance exists first.

> **Production note:** `aws_ami_from_instance` snapshots as soon as the instance is *running*, which may race the `user_data` script. For reliable builds, prefer **HashiCorp Packer** (which waits for provisioning to finish) or gate the snapshot behind an SSM RunCommand that polls for `/var/tmp/provisioning-complete`.

### Outputs
- `base_ami_id` — the source image used.
- `new_ami_id` — the AMI you'll launch from.
- `ssm_instance_profile` — the profile to attach to future instances for SSM access.

---

## Manual steps with the AWS CLI

The following reproduces the Terraform build by hand.

### 1. Find the latest Amazon Linux 2023 AMI
```bash
BASE_AMI=$(aws ec2 describe-images \
  --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023.*-x86_64" \
            "Name=virtualization-type,Values=hvm" \
  --query 'sort_by(Images,&CreationDate)[-1].ImageId' \
  --output text)
echo "$BASE_AMI"
```

### 2. Create the IAM role + instance profile for SSM
```bash
cat > trust.json <<'JSON'
{ "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow",
    "Principal": { "Service": "ec2.amazonaws.com" },
    "Action": "sts:AssumeRole" }] }
JSON

aws iam create-role --role-name zacamikafka-ssm-role \
  --assume-role-policy-document file://trust.json

aws iam attach-role-policy --role-name zacamikafka-ssm-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore

aws iam create-instance-profile --instance-profile-name zacamikafka-ssm-profile
aws iam add-role-to-instance-profile \
  --instance-profile-name zacamikafka-ssm-profile \
  --role-name zacamikafka-ssm-role
```

### 3. Create an egress-only security group
```bash
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" \
  --query 'Vpcs[0].VpcId' --output text)

SG_ID=$(aws ec2 create-security-group \
  --group-name zacamikafka-builder-sg \
  --description "egress only" --vpc-id "$VPC_ID" \
  --query 'GroupId' --output text)
# Default SG already allows all egress; no inbound rules added.
```

### 4. Write user-data and launch the builder instance
Save the same provisioning script (see `main.tf`'s `user_data`) to `userdata.sh`, then:
```bash
INSTANCE_ID=$(aws ec2 run-instances \
  --image-id "$BASE_AMI" \
  --instance-type t3.medium \
  --iam-instance-profile Name=zacamikafka-ssm-profile \
  --security-group-ids "$SG_ID" \
  --user-data file://userdata.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=zacamikafka-builder}]' \
  --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-status-ok --instance-ids "$INSTANCE_ID"
```

### 5. (Recommended) Verify provisioning finished via SSM
```bash
aws ssm start-session --target "$INSTANCE_ID"
# inside: ls /var/tmp/provisioning-complete && java -version && /opt/kafka/bin/kafka-topics.sh --version
```

### 6. Create the AMI
```bash
AMI_ID=$(aws ec2 create-image \
  --instance-id "$INSTANCE_ID" \
  --name zacamikafka \
  --description "Amazon Linux 2023 + Python + Java + Kafka prereqs" \
  --query 'ImageId' --output text)

aws ec2 wait image-available --image-ids "$AMI_ID"
echo "New AMI: $AMI_ID"
```

### 7. Clean up the builder
```bash
aws ec2 terminate-instances --instance-ids "$INSTANCE_ID"
```

---

## Connecting via SSM

Any instance launched from `zacamikafka` with the `zacamikafka-ssm-profile` attached can be reached without SSH:
```bash
aws ssm start-session --target i-xxxxxxxxxxxxxxxxx
```
Requirements: the SSM agent running (baked into the AMI), the instance profile attached, and outbound HTTPS reachability to SSM endpoints (NAT gateway, internet gateway, or VPC endpoints for `ssm`, `ssmmessages`, `ec2messages`).

---

## Kafka broker + health dashboard instance

`kafka-instance.tf` launches a single EC2 instance **from the `zacamikafka` AMI** that runs two systemd services:

1. **A single-node Apache Kafka broker in KRaft mode** (no ZooKeeper) — listening on `localhost:9092`, controller quorum on `localhost:9093`.
2. **A Python health/test/metrics API + dashboard** (`kafka_api.py` serving `dashboard.html`) on `127.0.0.1:8080`.

The dashboard is a Carbon-styled console with dark/light mode and live SVG graphs: throughput over time (sent vs received msg/s), round-trip latency (avg and p95), and a latency-distribution histogram. A round-trip test produces N messages of a chosen size to a topic, consumes them back, and measures end-to-end timing, surfacing average/p95 latency, throughput, and error rate.

### Security model — SSM port-forwarding only

The broker security group has **no inbound rules**. The dashboard binds to `127.0.0.1`, so it is never exposed to the network. You reach it by tunneling port 8080 to your laptop over Session Manager:

```bash
# from the Terraform output `dashboard_access_command`
aws ssm start-session \
  --target i-xxxxxxxxxxxxxxxxx \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
```

Then open `http://localhost:8080` in your browser. For an interactive shell on the broker, use the `broker_shell_command` output (`aws ssm start-session --target i-xxxx`).

### API endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/health` | Broker reachability, topic count, uptime |
| GET | `/api/metrics` | Cumulative + rolling throughput/latency metrics and histogram |
| POST | `/api/test` | Run a produce → consume round-trip test (`{count, size, topic}`) |

### Building only the AMI (skip the instance)

The instance is gated behind a variable. To build just the image:

```bash
terraform apply -var deploy_kafka_instance=false
```

### How the bootstrap works

The dashboard and API files are injected into `user_data` via Terraform's `filebase64()` and decoded on the instance, which avoids any shell-escaping issues with the HTML/Python content. The script then writes a KRaft `server.properties`, formats the storage directory once (with a static `controller.quorum.voters`, so **without** `--standalone` — Kafka 4.2+ rejects combining the two), and registers both systemd units so the broker and dashboard survive reboots.

> **Note on instance sizing:** the default is `t3.large` to give the JVM broker and the API enough memory headroom. Override with `-var kafka_instance_type=...`.

---

## Pros and cons

### Terraform approach
**Pros**
- Declarative and version-controlled; the whole image pipeline lives in code.
- Repeatable and reviewable; easy to diff and audit changes.
- Manages IAM, networking, and the AMI together as one lifecycle.

**Cons**
- `aws_ami_from_instance` can snapshot before `user_data` completes — a real correctness risk.
- Terraform isn't a purpose-built image baker; state must be managed, and a leftover builder instance lingers unless you destroy it.
- Limited visibility into provisioning failures inside `user_data`.

### Manual AWS CLI approach
**Pros**
- Maximum transparency — you see and control each step.
- No tooling beyond the AWS CLI; great for learning and one-off builds.
- Easy to insert manual verification before snapshotting.

**Cons**
- Error-prone and not repeatable; hard to keep consistent across people/environments.
- No state tracking; cleanup and drift are manual.
- Scales poorly.

### Ansible approach
**Pros**
- Idempotent, readable tasks with clear failure reporting per step.
- Reusable for both image baking *and* ongoing configuration of running fleets.
- Can run over SSM (`community.aws.aws_ssm`) — no SSH required.

**Cons**
- Adds a control-node/tooling dependency (Ansible, collections).
- Still needs an orchestration layer (or Packer's Ansible provisioner) to actually produce an AMI.
- Slower than a simple shell script for trivial installs.

> **Best practice:** For production, use **Packer** with the **Ansible provisioner** — Packer reliably waits for provisioning and bakes the AMI, while Ansible keeps the install logic idempotent and shareable. Terraform then consumes the resulting AMI ID for deployment.

---

## Ansible usage

The `kafka-prereqs.yml` playbook installs the same prerequisites. Run it against a freshly launched Amazon Linux 2023 instance, then snapshot it into the `zacamikafka` AMI.

Over SSH:
```bash
ansible-playbook -i 'HOST,' -u ec2-user --private-key key.pem kafka-prereqs.yml
```

Over SSM (no SSH; requires the `community.aws` collection):
```bash
ansible-galaxy collection install community.aws amazon.aws
# inventory configured with ansible_connection=community.aws.aws_ssm
ansible-playbook -i inventory_ssm.yml kafka-prereqs.yml
```

After the playbook succeeds, create the image:
```bash
aws ec2 create-image --instance-id i-xxxx --name zacamikafka \
  --description "AL2023 + Python + Java + Kafka prereqs (Ansible)"
```

---

## Files

| File | Purpose |
|------|---------|
| `main.tf` | Terraform build of the `zacamikafka` AMI with SSM access |
| `kafka-instance.tf` | Launches the KRaft broker + dashboard EC2 instance from the AMI (SSM-only) |
| `dashboard.html` | Carbon-styled health/throughput console with live SVG graphs |
| `kafka_api.py` | Python health/test/metrics API that drives the dashboard |
| `kafka-prereqs.yml` | Ansible playbook installing the same prerequisites |
| `zacamikafka-docs.html` | Standalone documentation page (Carbon style) |
| `README.md` | This document |

> `kafka-instance.tf` reads `dashboard.html` and `kafka_api.py` from the same directory via `filebase64()`, so keep all four files together when running Terraform.
