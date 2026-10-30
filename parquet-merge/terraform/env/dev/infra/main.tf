module "merge_infra" {
  source = "../../../modules/merge-infra"

  name_prefix            = var.name_prefix
  force_destroy          = var.force_destroy
  staging_retention_days = var.staging_retention_days
  release_label          = var.emr_release_label
  queue_timeout_minutes  = var.emr_queue_timeout_minutes
}
