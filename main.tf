###############################################################################
# main.tf
# Builds a custom AMI ("zacamikafka") from the latest Amazon Linux 2023 base
# image, pre-installed with Python, Java, and all Kafka prerequisites, plus an
# IAM instance profile granting AWS Systems Manager (SSM) Session Manager access.
###############################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

###############################################################################
# Variables
###############################################################################
variable "aws_region" {
  description = "AWS region in which to build the AMI"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "Instance type used to build the image"
  type        = string
  default     = "t3.medium"
}

variable "ami_name" {
  description = "Name of the resulting AMI"
  type        = string
  default     = "zacamikafka"
}

variable "subnet_id" {
  description = "Subnet to launch the builder instance in (must have outbound internet for package installs). Leave empty to use the default VPC's default subnet."
  type        = string
  default     = ""
}

###############################################################################
# Data sources
###############################################################################

# Grab the latest Amazon Linux 2023 x86_64 base AMI.
data "aws_ami" "base" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Default VPC / subnet fallback so the config works out-of-the-box.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

locals {
  builder_subnet = var.subnet_id != "" ? var.subnet_id : data.aws_subnets.default.ids[0]
}

###############################################################################
# IAM: instance role + profile granting SSM access
###############################################################################
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ssm" {
  name               = "${var.ami_name}-ssm-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

# AWS-managed policy that enables SSM Session Manager + agent functionality.
resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.ssm.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ssm" {
  name = "${var.ami_name}-ssm-profile"
  role = aws_iam_role.ssm.name
}

###############################################################################
# Security group: no inbound needed (SSM uses outbound 443), allow all egress
###############################################################################
resource "aws_security_group" "builder" {
  name        = "${var.ami_name}-builder-sg"
  description = "Builder SG - egress only (SSM uses outbound 443)"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

###############################################################################
# Builder EC2 instance + provisioning via user_data
###############################################################################
resource "aws_instance" "builder" {
  ami                    = data.aws_ami.base.id
  instance_type          = var.instance_type
  subnet_id              = local.builder_subnet
  iam_instance_profile   = aws_iam_instance_profile.ssm.name
  vpc_security_group_ids = [aws_security_group.builder.id]

  # Provision Python, Java, and Kafka prerequisites at boot.
  user_data = <<-EOF
    #!/bin/bash
    set -euxo pipefail

    # System update
    dnf -y update

    # Python 3 + pip + dev tools
    dnf -y install python3 python3-pip python3-devel gcc make git tar wget

    # Java 17 (Corretto) - required by Kafka
    dnf -y install java-17-amazon-corretto java-17-amazon-corretto-devel

    # Networking / Kafka helper utilities
    dnf -y install nc telnet jq

    # Kafka prerequisites: download Apache Kafka binaries
    KAFKA_VERSION=3.7.1
    SCALA_VERSION=2.13
    cd /opt
    wget -q "https://downloads.apache.org/kafka/$${KAFKA_VERSION}/kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}.tgz"
    tar -xzf "kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}.tgz"
    ln -s "/opt/kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}" /opt/kafka
    rm -f "kafka_$${SCALA_VERSION}-$${KAFKA_VERSION}.tgz"

    # Useful Python libs for Kafka clients
    pip3 install --no-cache-dir kafka-python confluent-kafka

    # Ensure the SSM agent is enabled (preinstalled on AL2023)
    systemctl enable --now amazon-ssm-agent

    # Signal completion
    touch /var/tmp/provisioning-complete
  EOF

  tags = {
    Name = "${var.ami_name}-builder"
  }
}

###############################################################################
# Create the AMI from the provisioned instance
###############################################################################
resource "aws_ami_from_instance" "zacamikafka" {
  name               = var.ami_name
  source_instance_id = aws_instance.builder.id

  # Give user_data time to finish before snapshotting.
  # In production prefer Packer or an SSM RunCommand wait gate.
  depends_on = [aws_instance.builder]

  tags = {
    Name = var.ami_name
  }
}

###############################################################################
# Outputs
###############################################################################
output "base_ami_id" {
  description = "The base Amazon Linux 2023 AMI used"
  value       = data.aws_ami.base.id
}

output "new_ami_id" {
  description = "The newly created zacamikafka AMI ID"
  value       = aws_ami_from_instance.zacamikafka.id
}

output "ssm_instance_profile" {
  description = "Instance profile to attach to instances launched from this AMI for SSM access"
  value       = aws_iam_instance_profile.ssm.name
}
