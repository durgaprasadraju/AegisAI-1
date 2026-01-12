variable "table_name" {
  description = "Name of the DynamoDB table"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "hash_key" {
  description = "Hash key attribute name"
  type        = string
}

variable "hash_key_type" {
  description = "Hash key attribute type (S, N, B)"
  type        = string
  default     = "S"
}

variable "range_key" {
  description = "Range key attribute name (optional)"
  type        = string
  default     = ""
}

variable "range_key_type" {
  description = "Range key attribute type (S, N, B)"
  type        = string
  default     = "S"
}

variable "billing_mode" {
  description = "Billing mode (PROVISIONED or PAY_PER_REQUEST)"
  type        = string
  default     = "PAY_PER_REQUEST"
}

variable "read_capacity" {
  description = "Read capacity units (required if billing_mode is PROVISIONED)"
  type        = number
  default     = null
}

variable "write_capacity" {
  description = "Write capacity units (required if billing_mode is PROVISIONED)"
  type        = number
  default     = null
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
