output "bucket_raw"   { value = google_storage_bucket.raw.name }
output "dataset_dw"   { value = google_bigquery_dataset.dw.dataset_id }
output "artifact_repo"{ value = google_artifact_registry_repository.repo.repository_id }
output "run_service"  { value = google_cloud_run_v2_service.api.name }
