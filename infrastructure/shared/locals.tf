# Shared local values
# Common local values that can be reused across environments

locals {
  common_tags = {
    Project   = "AegisAI"
    ManagedBy = "Terraform"
  }
}
