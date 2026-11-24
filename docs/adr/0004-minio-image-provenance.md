# ADR 0004: MinIO image provenance

- Status: accepted
- Date: 2026-11-24

## Context

Upstream MinIO stopped publishing community container images and binaries to Docker Hub
and Quay in late 2025. `minio/minio` tags can no longer be pulled.

## Decision

Use Chainguard's source-built MinIO image, **pinned by digest**:
`cgr.dev/chainguard/minio@sha256:bd92a45b...` (MinIO `RELEASE.2026-11-23T00-10-37Z`,
AGPLv3). The same image provides `mc`, which the init container uses for the bucket, the
policy and the Lakekeeper user.

## Consequences

- Digest pinning makes builds reproducible. Chainguard's free tier only publishes `:latest`,
  so upgrading means resolving and pinning a new digest
  (`docker buildx imagetools inspect cgr.dev/chainguard/minio:latest`).
- MinIO is only the local S3 stand-in. The design does not depend on it (ADR 0002); any
  S3-compatible store with STS AssumeRole works, and on AWS it is S3 itself.
