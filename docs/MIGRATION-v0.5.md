# Migration Guide: v0.4.x → v0.5.0

This guide helps you upgrade from MetaFuse v0.4.x to v0.5.0.

---

## TL;DR

**Good news**: v0.5.0 has **no breaking changes**. All v0.4.x code continues to work unchanged.

**Quick upgrade:**

```bash
# Update dependencies
cargo update -p metafuse-catalog-api
cargo update -p metafuse-catalog-storage
cargo update -p metafuse-catalog-cli

# Build and test
cargo build
cargo test
```

---

## What's New in v0.5.0

### CI Enhancement & Cloud Testing

MetaFuse v0.5.0 focuses on CI infrastructure improvements:

- **Cloud Emulator Tests**: S3 tests run against MinIO in CI
- **GCS Emulator Support**: `GcsBackend` auto-detects `STORAGE_EMULATOR_HOST`
- **Improved Test Isolation**: Unique object keys per test prevent conflicts
- **CI Safety**: Docker checks, fork detection, job timeouts

### No Breaking Changes

v0.5.0 is fully backward compatible with v0.4.x. The upgrade path is:

1. Update dependencies
2. Build and test
3. Done!

---

## Cloud Emulator Testing

### Running S3 Tests Locally

```bash
# Start MinIO
docker run -d --name minio \
  -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:latest server /data

# Create test bucket
docker run --rm --network host \
  --entrypoint /bin/sh minio/mc:latest \
  -c "mc alias set local http://127.0.0.1:9000 minioadmin minioadmin && mc mb local/test-bucket"

# Run S3 tests
RUN_CLOUD_TESTS=1 cargo test --features s3 --test s3_emulator_tests
```

### GCS Tests (Currently Disabled)

GCS emulator tests are marked `#[ignore]` due to an incompatibility between
the `object_store` crate and `fake-gcs-server`:

- `object_store` uses XML API PUT for uploads
- `fake-gcs-server` only supports JSON API uploads
- Result: 405 Method Not Allowed errors

**Tracking Issues:**
- [fake-gcs-server#331](https://github.com/fsouza/fake-gcs-server/issues/331)
- [arrow-rs-object-store#167](https://github.com/apache/arrow-rs-object-store/issues/167)

Tests will be re-enabled when fake-gcs-server adds XML API support.

---

## Environment Variables

### New in v0.5.0

| Variable | Description | Default |
|----------|-------------|---------|
| `RUN_CLOUD_TESTS` | Enable cloud emulator tests | `0` (disabled) |

### Existing (unchanged)

| Variable | Description | Default |
|----------|-------------|---------|
| `METAFUSE_CACHE_TTL_SECS` | Cache TTL in seconds | `60` |
| `METAFUSE_CACHE_REVALIDATE` | Enable cache revalidation | `false` |
| `STORAGE_EMULATOR_HOST` | GCS emulator endpoint | (none) |
| `AWS_ENDPOINT` | S3-compatible endpoint | (none) |

---

## Testing Your Upgrade

### Unit Tests

```bash
# Test with default features
cargo test

# Test with cloud backends
cargo test --features cloud

# Test with security features
cargo test --features "rate-limiting,api-keys"
```

### Integration Tests

```bash
# Cloud emulator tests (requires Docker)
RUN_CLOUD_TESTS=1 cargo test --features cloud

# S3 tests only
RUN_CLOUD_TESTS=1 cargo test --features s3 --test s3_emulator_tests
```

### Manual Testing

```bash
# Test catalog operations
metafuse init --uri "file:///tmp/catalog.db"
metafuse list
metafuse show <dataset_name>
metafuse search "query"

# Test cloud backends (if configured)
metafuse init --uri "gs://my-bucket/catalog.db"
metafuse init --uri "s3://my-bucket/catalog.db?region=us-west-2"
```

---

## Getting Help

If you encounter issues:

1. **Check documentation**: [docs/](.)
2. **Search issues**: [GitHub Issues](https://github.com/ethan-tyler/MetaFuse/issues)
3. **Ask the community**: [GitHub Discussions](https://github.com/ethan-tyler/MetaFuse/discussions)

---

**Last Updated:** 2025-11-26
**Applies To:** MetaFuse v0.5.0
