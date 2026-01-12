# S3 Buckets Module
# This module creates S3 buckets for artifacts and UI hosting

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# TODO: Add S3 bucket resource definitions
