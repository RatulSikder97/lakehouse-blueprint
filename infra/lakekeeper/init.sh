#!/bin/sh
# Idempotent Lakekeeper bootstrap: accept terms once, then create the `lakehouse` warehouse
# backed by MinIO with STS credential vending enabled.
set -eu

URL="${LAKEKEEPER_URL:-http://lakekeeper:8181}"

code=$(curl -s -o /tmp/bootstrap.out -w '%{http_code}' -X POST "$URL/management/v1/bootstrap" \
  -H 'Content-Type: application/json' --data '{"accept-terms-of-use": true}')
case "$code" in
  2*) echo "lakekeeper-init: server bootstrapped" ;;
  4*) echo "lakekeeper-init: already bootstrapped ($code)" ;;
  *) echo "lakekeeper-init: bootstrap failed ($code)"; cat /tmp/bootstrap.out; exit 1 ;;
esac

if curl -fsS "$URL/management/v1/warehouse" | grep -q '"name":"lakehouse"'; then
  echo "lakekeeper-init: warehouse 'lakehouse' already exists"
  exit 0
fi

cat > /tmp/warehouse.json <<JSON
{
  "warehouse-name": "lakehouse",
  "storage-profile": {
    "type": "s3",
    "bucket": "lakehouse",
    "key-prefix": "warehouse",
    "endpoint": "http://minio:9000",
    "sts-endpoint": "http://minio:9000",
    "region": "us-east-1",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": true
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "access-key-id": "${LAKEKEEPER_S3_ACCESS_KEY}",
    "secret-access-key": "${LAKEKEEPER_S3_SECRET_KEY}"
  },
  "delete-profile": {"type": "hard"}
}
JSON

code=$(curl -s -o /tmp/warehouse.out -w '%{http_code}' -X POST "$URL/management/v1/warehouse" \
  -H 'Content-Type: application/json' --data @/tmp/warehouse.json)
if [ "${code#2}" = "$code" ]; then
  echo "lakekeeper-init: warehouse creation failed ($code)"; cat /tmp/warehouse.out; exit 1
fi
echo "lakekeeper-init: warehouse 'lakehouse' created"
