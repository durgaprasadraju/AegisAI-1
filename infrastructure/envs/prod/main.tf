# Production Environment Infrastructure
# This file orchestrates all infrastructure modules for the prod environment

terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# TODO: Add module calls for VPC, EKS, MSK, Lambda, API Gateway, S3, IAM, CloudWatch, DynamoDB
