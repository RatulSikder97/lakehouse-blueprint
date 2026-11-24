#!/usr/bin/env bash
# Idempotent MinIO bootstrap: warehouse bucket + a least-privilege identity for Lakekeeper.
# Lakekeeper uses this identity to call STS AssumeRole and vend short-lived,
# bucket-scoped credentials to query engines. Engines never see these keys.
set -euo pipefail

BUCKET="${LAKEHOUSE_BUCKET:-lakehouse}"
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

mc mb --ignore-existing "local/${BUCKET}"

cat > /tmp/lakekeeper-policy.json <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"],
      "Resource": ["arn:aws:s3:::${BUCKET}"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
      "Resource": ["arn:aws:s3:::${BUCKET}/*"]
    }
  ]
}
POLICY

mc admin policy create local lakehouse-warehouse-rw /tmp/lakekeeper-policy.json >/dev/null
mc admin user add local "$LAKEKEEPER_S3_ACCESS_KEY" "$LAKEKEEPER_S3_SECRET_KEY" >/dev/null
mc admin policy attach local lakehouse-warehouse-rw --user "$LAKEKEEPER_S3_ACCESS_KEY" >/dev/null 2>&1 || true

echo "minio-init: bucket '${BUCKET}' and user '${LAKEKEEPER_S3_ACCESS_KEY}' ready"
