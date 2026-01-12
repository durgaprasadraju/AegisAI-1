variable "api_name" {
  description = "Name of the API Gateway"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "stage_name" {
  description = "Name of the deployment stage"
  type        = string
  default     = "v1"
}

variable "cors_enabled" {
  description = "Enable CORS"
  type        = bool
  default     = true
}

variable "lambda_integrations" {
  description = "Map of Lambda function integrations"
  type = map(object({
    lambda_arn = string
    method     = string
    path       = string
  }))
  default = {}
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
