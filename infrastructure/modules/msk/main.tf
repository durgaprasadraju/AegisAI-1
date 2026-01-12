# MSK (Kafka) Cluster Module
# This module creates an AWS MSK cluster

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# TODO: Add MSK cluster resource definitions
