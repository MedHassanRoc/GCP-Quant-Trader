variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

# BigQuery/GCS location (multi-region like "US" or single region like "EU")
variable "location" {
  type    = string
  default = "US"
}
