# CloudWatch Module
# This module creates CloudWatch log groups and metric alarms

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# TODO: Add CloudWatch resource definitions
