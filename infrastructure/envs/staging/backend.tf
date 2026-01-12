terraform {
  backend "s3" {
    bucket         = "aegisai-terraform-state-staging"
    key            = "staging/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "aegisai-terraform-state-lock-staging"
  }
}
