# VPC Module
# This module creates a VPC with subnets, route tables, and internet gateway

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# TODO: Add VPC resource definitions
