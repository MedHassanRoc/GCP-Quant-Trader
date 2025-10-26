# GCS bucket for raw data
resource "google_storage_bucket" "raw" {
  name                        = "${var.project_id}-quantedge-raw"
  location                    = var.location
  uniform_bucket_level_access = true
  force_destroy               = true
  versioning { enabled = true }
}

# BigQuery dataset
resource "google_bigquery_dataset" "dw" {
  dataset_id  = "quantedge"
  location    = var.location
  description = "QuantEdge warehouse (bronze/silver/gold, predictions, ops)"
}

# Artifact Registry (Docker)
resource "google_artifact_registry_repository" "repo" {
  location      = var.region
  repository_id = "quantedge"
  description   = "Containers for QuantEdge"
  format        = "DOCKER"
}

# Service account to run Cloud Run
resource "google_service_account" "run_sa" {
  account_id   = "quantedge-run-sa"
  display_name = "QuantEdge Cloud Run SA"
}

# Cloud Run service (image to be deployed later from CI)
resource "google_cloud_run_v2_service" "api" {
  name     = "quantedge-api"
  location = var.region
  template {
    service_account = google_service_account.run_sa.email
    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # placeholder
      env {
        name  = "BQ_DATASET"
        value = google_bigquery_dataset.dw.dataset_id
      }
    }
  }
  ingress = "INGRESS_TRAFFIC_ALL" # allow public
}
