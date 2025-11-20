# MetaFuse Architecture

## Overview

MetaFuse uses a novel **SQLite-on-object-storage** pattern that prioritizes simplicity, cost-effectiveness, and portability over complex distributed architectures.

## System Architecture

```
Data pipelines (DataFusion, dbt, etc.)
  -> Catalog Emitter (Rust crate)
  -> Catalog Backend (Local SQLite today; cloud backends later)
  -> SQLite catalog file (metafuse_catalog.db)
  -> Consumers (REST API, CLI, future web UI)
```

## Core Design Principles

### 1. SQLite-on-Object-Storage Pattern

Instead of running a dedicated database server, MetaFuse:

1. **Stores metadata in SQLite files** on object storage (GCS/S3) or local filesystem
2. **Readers download the catalog** and query locally (sub-millisecond latency)
3. **Writers use optimistic concurrency** with version-based locking
4. **No servers to maintain** - just object storage and your application code

**Benefits:**
- Zero infrastructure cost (beyond storage)
- Works offline and in restricted environments
- Easy to version, backup, and replicate
- Portable across environments

**Trade-offs:**
- Not ideal for extremely high write-throughput (>100 writes/sec)
- Download cost if catalog is very large (mitigated by caching)
- Optimistic concurrency can cause retries under contention

### 2. Storage Backend Abstraction

The `CatalogBackend` trait provides a unified interface:

```rust
pub trait CatalogBackend {
    fn exists(&self) -> CatalogResult<bool>;
    fn get_connection(&self) -> CatalogResult<rusqlite::Connection>;
}
```

**Implemented Backends:**
- `LocalSqliteBackend`: Direct filesystem access
- `GCSBackend` (planned): Google Cloud Storage with versioning
- `S3Backend` (planned): AWS S3 with ETags for optimistic locking

Each backend handles:
- Download/upload logic
- Connection caching
- Optimistic concurrency control via generation/version numbers

### 3. Optimistic Concurrency Control

MetaFuse uses a version counter in the `catalog_meta` table:

1. **Read**: Download catalog + current version number
2. **Modify**: Make changes in local SQLite
3. **Write**: Upload only if remote version matches expected version
4. **Retry**: If version mismatch, re-download and retry

```rust
// Pseudocode for write operation
let current_version = read_version_from_remote();
    let connection = backend.get_connection()?;

// Make changes...
connection.execute("UPDATE datasets SET ...")?;

// Increment version
let new_version = current_version + 1;
connection.execute("UPDATE catalog_meta SET version = ?", [new_version])?;

// Upload with precondition: version == current_version
backend.upload_if_version_matches(current_version)?;
```

This approach:
- Prevents lost updates without distributed locks
- Allows concurrent reads without contention
- Retries automatically on write conflicts
- Works across cloud backends (GCS generations, S3 ETags)

### 4. Schema Design

The SQLite schema is optimized for read-heavy workloads:

**Core Tables:**
- `datasets`: Dataset metadata (name, path, format, owner, tenant, domain)
- `fields`: Schema fields (name, type, nullable)
- `lineage`: Upstream/downstream relationships
- `tags`: Freeform labels for discovery
- `glossary_terms`: Business context and definitions
- `term_links`: Links between glossary terms
- `catalog_meta`: Version tracking for concurrency control

**Search Optimization:**
- `dataset_search`: FTS5 virtual table for full-text search
- Automatic triggers keep FTS index in sync
- Supports searching across name, path, domain, field names

**Indexes:**
- Primary keys on all tables
- Foreign key indexes for efficient joins
- Unique constraints on dataset names (per tenant)

### 5. DataFusion Integration

The `catalog-emitter` crate provides seamless integration:

```rust
use datafusion::prelude::*;
use metafuse_catalog_emitter::Emitter;

let ctx = SessionContext::new();
let df = ctx.read_parquet("input.parquet", Default::default()).await?;

// Transform
let result = df.filter(...)?.select(...)?;

// Write to storage
result.write_parquet("output.parquet", Default::default()).await?;

// Emit metadata (automatic schema extraction)
let emitter = Emitter::new(backend);
emitter.emit_dataset(
    "dataset_name",
    "output.parquet",
    "parquet",
    Some("tenant"),
    Some("domain"),
    Some("owner@example.com"),
    result.schema().inner().clone(),  // Arrow schema
    Some("Description"),
    None,
    vec!["input_dataset"],  // Upstream dependencies
    vec!["tag1", "tag2"],   // Tags
)?;
```

The emitter:
- Extracts schema from DataFusion's Arrow `SchemaRef`
- Converts Arrow types to SQL types for storage
- Tracks lineage edges automatically
- Handles multi-tenant metadata isolation

## Component Details

### catalog-core

**Purpose:** Core types, SQLite schema, error handling

**Key Types:**
- `DatasetMeta`: Dataset metadata struct
- `FieldMeta`: Field schema metadata
- `CatalogError`: Comprehensive error type
- `CatalogResult<T>`: Convenience type alias

**Responsibilities:**
- SQLite DDL schema initialization
- Version control utilities
- Type definitions shared across crates

### catalog-storage

**Purpose:** Storage backend abstraction

**Key Types:**
- `CatalogBackend` trait
- `LocalSqliteBackend` implementation
- `GCSBackend` and `S3Backend` (future)

**Responsibilities:**
- Abstract storage location (local, GCS, S3)
- Connection pooling and caching
- Download/upload with concurrency control
- Version/generation number tracking

### catalog-emitter

**Purpose:** DataFusion pipeline integration

**Key Types:**
- `Emitter<B: CatalogBackend>`

**Responsibilities:**
- Emit metadata from DataFusion pipelines
- Extract schema from Arrow `SchemaRef`
- Convert Arrow types to SQL types
- Write metadata to catalog backend
- Track lineage relationships
- Tag management

### catalog-api

**Purpose:** REST API server (Axum-based)

**Endpoints:**
- `GET /health`: Health check
- `GET /api/v1/datasets`: List all datasets
- `GET /api/v1/datasets/:name`: Get dataset details + lineage
- `GET /api/v1/search?q=query`: Full-text search

**Features:**
- CORS middleware for web UI access
- Structured error responses
- Environment-based configuration (port, catalog path)

### catalog-cli

**Purpose:** Command-line interface

**Commands:**
- `init`: Initialize new catalog
- `list`: List datasets (with filters)
- `show`: Show dataset details (with lineage)
- `search`: Full-text search
- `stats`: Catalog statistics

**Features:**
- Formatted output (tables, colors)
- Custom catalog path via `--catalog` flag
- JSON output support (future)

## Performance Characteristics

### Read Performance

- **Local queries:** Sub-millisecond (SQLite in-memory)
- **FTS5 search:** ~1-5ms for 10k datasets
- **Lineage traversal:** ~2-10ms (indexed joins)

### Write Performance

- **Single write:** ~10-50ms (local SQLite)
- **With cloud upload:** ~100-500ms (network overhead)
- **Optimistic retry:** +100-200ms per conflict

### Scalability

**Expected limits (single SQLite file):**
- Datasets: 10,000+
- Fields: 100,000+
- Lineage edges: 50,000+
- Catalog file size: <100MB
- 95th percentile API latency: <500ms

**Beyond these limits:**
- Consider migrating to DuckDB (larger datasets, OLAP)
- Consider migrating to PostgreSQL (high write contention)

## Future Enhancements

### Phase 2: Cloud Backends

- Implement `GCSBackend` with generation-based concurrency
- Implement `S3Backend` with ETag-based concurrency
- Add connection caching and download optimization

### Phase 3: Performance

- Add result caching layer (Redis/in-memory)
- Implement incremental catalog updates
- Add read replicas for horizontal scaling

### Phase 4: Advanced Features

- Column-level lineage tracking
- Query-level metadata (SQL queries that generated datasets)
- Integration with DataFusion's catalog abstraction
- Schema evolution tracking

## Security Considerations

### Current (v0.1.x)

- **No authentication/authorization** - API is open
- **SQL injection protection** - Prepared statements everywhere
- **File permissions** - User-managed (recommend `chmod 600`)

### Future

- API key authentication
- RBAC for dataset access
- SSO integration (OAuth2/OIDC)
- Audit logging for compliance

## Deployment Patterns

### Local Development

```bash
# Initialize catalog locally
metafuse init

# Run pipelines that emit metadata
cargo run --example simple_pipeline

# Query via CLI
metafuse list
```

### CI/CD Pipelines

```bash
# Download catalog from S3
aws s3 cp s3://my-bucket/catalog.db metafuse_catalog.db

# Run pipeline (emits metadata)
./my_datafusion_pipeline

# Upload updated catalog
aws s3 cp metafuse_catalog.db s3://my-bucket/catalog.db
```

### Cloud Functions (Serverless)

```python
# Google Cloud Function example
def run_pipeline(request):
    # Download catalog
    download_from_gcs("gs://my-bucket/catalog.db")

    # Run DataFusion pipeline (Rust binary)
    subprocess.run(["./pipeline"])

    # Upload catalog (with versioning)
    upload_to_gcs("metafuse_catalog.db", "gs://my-bucket/catalog.db")
```

### API Server Deployment

```dockerfile
# Dockerfile for REST API
FROM rust:1.83 as builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin metafuse-api

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /app/target/release/metafuse-api /usr/local/bin/
EXPOSE 8080
CMD ["metafuse-api"]
```

## References

- [SQLite Performance](https://www.sqlite.org/whentouse.html)
- [DataFusion Catalog Trait](https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html)
- [Optimistic Concurrency Control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control)
- [GCS Object Versioning](https://cloud.google.com/storage/docs/object-versioning)
- [S3 ETags](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)
