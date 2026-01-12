variable "log_group_name" {
  description = "Name of the CloudWatch log group"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "retention_in_days" {
  description = "Number of days to retain logs"
  type        = number
  default     = 7
}

variable "alarms" {
  description = "List of CloudWatch alarms to create"
  type = list(object({
    alarm_name          = string
    metric_name         = string
    namespace           = string
    statistic           = string
    threshold           = number
    comparison_operator = string
    evaluation_periods  = number
    period              = number
  }))
  default = []
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
