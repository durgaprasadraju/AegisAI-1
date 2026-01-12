# EKS Cluster Module
# This module creates an EKS cluster with node groups

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# TODO: Add EKS cluster resource definitions
