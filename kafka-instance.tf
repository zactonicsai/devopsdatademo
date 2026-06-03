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
# Render the user-data bootstrap. The dashboard + API files are injected as
# base64 so their HTML/Python content needs no shell escaping.
###############################################################################
locals {
  dashboard_b64 = filebase64("${path.module}/dashboard.html")
  api_b64       = filebase64("${path.module}/kafka_api.py")

  # NOTE: this script is intentionally flush-left (no indentation). The inner
  # cat heredocs write config files verbatim, so leading whitespace would
  # corrupt server.properties and the systemd units. Keep it left-aligned.
  kafka_user_data = <<EOF
#!/bin/bash
set -euxo pipefail

APP_DIR=/opt/kafka-console
KAFKA_HOME=/opt/kafka
mkdir -p "$APP_DIR"

# ---- write the dashboard + API from the injected base64 blobs ----
echo "${local.dashboard_b64}" | base64 -d > "$APP_DIR/dashboard.html"
echo "${local.api_b64}" | base64 -d > "$APP_DIR/kafka_api.py"

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
