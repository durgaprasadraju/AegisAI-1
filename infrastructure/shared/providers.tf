# Shared provider configurations
# This file can be included in environment configs if needed

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
