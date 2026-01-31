terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = "zoomcamp-terraform-485205"
  region      = "us-central1"

}

resource "google_storage_bucket" "zoomcamp" {
  name     = "zoomcamp-terraform-485205-bucket"
  location = "US"

  uniform_bucket_level_access = true
  force_destroy               = false
}