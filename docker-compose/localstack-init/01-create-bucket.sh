#!/bin/sh
set -eu

awslocal s3api head-bucket --bucket transferia-benchmark 2>/dev/null || \
  awslocal s3api create-bucket --bucket transferia-benchmark --region us-east-1
