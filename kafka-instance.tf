###############################################################################
# kafka-instance.tf
# Launches a single EC2 instance FROM the zacamikafka AMI that runs:
#   1. a single-node Apache Kafka broker in KRaft mode (systemd unit)
#   2. a Python health/test/metrics API + dashboard (systemd unit)
#
# Access is SSM-only: the security group has NO inbound rules. Reach the
# dashboard by port-forwarding 8080 over Session Manager (see outputs / README).
#
# This file depends on resources defined in main.tf:
#   - aws_ami_from_instance.zacamikafka  (the custom AMI)
#   - aws_iam_instance_profile.ssm       (grants SSM access)
#   - data.aws_vpc.default / local.builder_subnet
###############################################################################

variable "kafka_instance_type" {
  description = "Instance type for the running Kafka broker + dashboard"
  type        = string
  default     = "t3.large" # broker (JVM) + API; 8 GiB RAM headroom
}

variable "deploy_kafka_instance" {
  description = "Set false to build only the AMI and skip launching the broker instance"
  type        = bool
  default     = true
}

###############################################################################
# Security group: egress only. No inbound — dashboard is reached via SSM tunnel.
###############################################################################
resource "aws_security_group" "kafka" {
  count       = var.deploy_kafka_instance ? 1 : 0
  name        = "${var.ami_name}-broker-sg"
  description = "Kafka broker + dashboard - egress only, access via SSM port-forward"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.ami_name}-broker-sg" }
}

###############################################################################
# App delivery via S3.
#
# WHY S3 (not inline user_data): EC2 user_data is hard-capped at 16 KB. The
# dashboard + API are larger than that even base64/gzipped, so we host them in
# a private S3 bucket and have the instance pull them at boot using its IAM
# role. This also DECOUPLES the app from the AMI: edit dashboard.html or
# kafka_api.py and `terraform apply` re-uploads them — no AMI rebuild needed.
###############################################################################

# Random suffix so the bucket name is globally unique.
resource "random_id" "bucket" {
  count       = var.deploy_kafka_instance ? 1 : 0
  byte_length = 4
}

resource "aws_s3_bucket" "app" {
  count         = var.deploy_kafka_instance ? 1 : 0
  bucket        = "${var.ami_name}-app-${random_id.bucket[0].hex}"
  force_destroy = true
  tags          = { Name = "${var.ami_name}-app" }
}

resource "aws_s3_bucket_public_access_block" "app" {
  count                   = var.deploy_kafka_instance ? 1 : 0
  bucket                  = aws_s3_bucket.app[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload the two app files. etag triggers re-upload whenever the file changes.
resource "aws_s3_object" "dashboard" {
  count        = var.deploy_kafka_instance ? 1 : 0
  bucket       = aws_s3_bucket.app[0].id
  key          = "dashboard.html"
  source       = "${path.module}/dashboard.html"
  etag         = filemd5("${path.module}/dashboard.html")
  content_type = "text/html"
}

resource "aws_s3_object" "api" {
  count  = var.deploy_kafka_instance ? 1 : 0
  bucket = aws_s3_bucket.app[0].id
  key    = "kafka_api.py"
  source = "${path.module}/kafka_api.py"
  etag   = filemd5("${path.module}/kafka_api.py")
}

# Allow the instance's SSM role to read this bucket.
resource "aws_iam_role_policy" "app_read" {
  count = var.deploy_kafka_instance ? 1 : 0
  name  = "${var.ami_name}-app-read"
  role  = aws_iam_role.ssm.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.app[0].arn, "${aws_s3_bucket.app[0].arn}/*"]
    }]
  })
}

###############################################################################
# Bootstrap script. Small now — it fetches the app from S3 rather than
# embedding it, so it stays well under the 16 KB user_data limit.
# Flush-left: inner heredocs write config verbatim, so no indentation.
###############################################################################
locals {
  app_bucket_id = one(aws_s3_bucket.app[*].id)

  kafka_user_data = <<EOF
#!/bin/bash
set -euxo pipefail

APP_DIR=/opt/kafka-console
KAFKA_HOME=/opt/kafka
BUCKET=${local.app_bucket_id}
mkdir -p "$APP_DIR"

# ---- fetch the dashboard + API from S3 (instance role grants read) ----
for i in $(seq 1 30); do
  if aws s3 cp "s3://$BUCKET/dashboard.html" "$APP_DIR/dashboard.html" --region ${var.aws_region} && \
     aws s3 cp "s3://$BUCKET/kafka_api.py" "$APP_DIR/kafka_api.py" --region ${var.aws_region}; then
    break
  fi
  echo "waiting for S3 objects / credentials... ($i)"; sleep 5
done

# ---- ensure the Kafka python client is present (AMI already has it) ----
pip3 install --no-cache-dir confluent-kafka >/dev/null 2>&1 || true

# =========================================================================
# 1. Single-node Kafka in KRaft mode (no ZooKeeper)
# =========================================================================
KCFG="$KAFKA_HOME/config/server.properties"
cat > "$KCFG" <<'PROPS'
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=/var/lib/kafka-logs
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=true
PROPS

mkdir -p /var/lib/kafka-logs

# Format the storage directory once (idempotent guard).
# NOTE: because controller.quorum.voters is set (static quorum), we format
# WITHOUT --standalone. Kafka 4.2+ rejects combining the two.
if [ ! -f /var/lib/kafka-logs/meta.properties ]; then
  KAFKA_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"
  $KAFKA_HOME/bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c "$KCFG"
fi

# systemd unit for the broker
cat > /etc/systemd/system/kafka.service <<'UNIT'
[Unit]
Description=Apache Kafka (KRaft single node)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
UNIT

# =========================================================================
# 2. Dashboard + test/metrics API
# =========================================================================
cat > /etc/systemd/system/kafka-console.service <<'UNIT'
[Unit]
Description=zacamikafka health/test dashboard API
After=kafka.service
Wants=kafka.service

[Service]
Type=simple
Environment=KAFKA_BOOTSTRAP=localhost:9092
Environment=API_HOST=127.0.0.1
Environment=API_PORT=8080
WorkingDirectory=/opt/kafka-console
ExecStart=/usr/bin/python3 /opt/kafka-console/kafka_api.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now kafka.service
sleep 15
systemctl enable --now kafka-console.service
systemctl enable --now amazon-ssm-agent

touch /var/tmp/console-ready
EOF
}

###############################################################################
# The broker + dashboard instance, launched from the custom AMI
###############################################################################
resource "aws_instance" "kafka" {
  count                  = var.deploy_kafka_instance ? 1 : 0
  ami                    = aws_ami_from_instance.zacamikafka.id
  instance_type          = var.kafka_instance_type
  subnet_id              = local.builder_subnet
  iam_instance_profile   = aws_iam_instance_profile.ssm.name
  vpc_security_group_ids = [aws_security_group.kafka[0].id]
  user_data              = local.kafka_user_data

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  tags = {
    Name = "${var.ami_name}-broker"
    Role = "kafka-broker-dashboard"
  }

  # Ensure the app files and read policy exist before the instance boots
  # and tries to fetch them.
  depends_on = [
    aws_s3_object.dashboard,
    aws_s3_object.api,
    aws_iam_role_policy.app_read,
  ]
}

###############################################################################
# Outputs
###############################################################################
output "kafka_instance_id" {
  description = "Instance ID of the Kafka broker + dashboard"
  value       = var.deploy_kafka_instance ? aws_instance.kafka[0].id : null
}

output "dashboard_access_command" {
  description = "Port-forward the dashboard to your laptop via SSM, then open http://localhost:8080"
  value = var.deploy_kafka_instance ? format(
    "aws ssm start-session --target %s --document-name AWS-StartPortForwardingSession --parameters '{\"portNumber\":[\"8080\"],\"localPortNumber\":[\"8080\"]}'",
    aws_instance.kafka[0].id
  ) : "deploy_kafka_instance = false"
}

output "broker_shell_command" {
  description = "Open an interactive shell on the broker instance"
  value       = var.deploy_kafka_instance ? "aws ssm start-session --target ${aws_instance.kafka[0].id}" : "deploy_kafka_instance = false"
}
