terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "sa-east-1"
}

resource "aws_s3_bucket" "entsoe" {
  bucket = var.s3_bucket_name
}

resource "aws_s3_bucket_versioning" "entsoe" {
  bucket = aws_s3_bucket.entsoe.id
  versioning_configuration {
    status = "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "entsoe" {
  bucket = aws_s3_bucket.entsoe.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "entsoe" {
  bucket                  = aws_s3_bucket.entsoe.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}