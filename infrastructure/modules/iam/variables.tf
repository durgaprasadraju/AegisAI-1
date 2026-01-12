variable "role_name" {
  description = "Name of the IAM role"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "service_name" {
  description = "Name of the service this role is for"
  type        = string
}

variable "assume_role_policy" {
  description = "Assume role policy JSON"
  type        = string
}

variable "policy_documents" {
  description = "List of policy documents to attach"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
