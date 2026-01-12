variable "cluster_name" {
  description = "Name of the MSK cluster"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "kafka_version" {
  description = "Kafka version"
  type        = string
  default     = "3.5.1"
}

variable "broker_instance_type" {
  description = "Instance type for Kafka brokers"
  type        = string
  default     = "kafka.m5.large"
}

variable "broker_count" {
  description = "Number of broker nodes"
  type        = number
  default     = 3
}

variable "vpc_id" {
  description = "VPC ID where the cluster will be created"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs for the cluster"
  type        = list(string)
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
