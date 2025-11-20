# Getting Started with MetaFuse

This tutorial will get you up and running with MetaFuse in approximately 10 minutes.

## Prerequisites

- Rust 1.75+ and Cargo installed
- Basic familiarity with command-line tools
- (Optional) DataFusion knowledge for pipeline integration

## Step 1: Install MetaFuse (2 minutes)

### Option A: Build from Source

```bash
# Clone the repository
git clone https://github.com/ethan-tyler/MetaFuse.git
cd MetaFuse

# Build all crates
cargo build --release

# Binaries will be in target/release/
# - metafuse (CLI)
# - metafuse-api (REST API server)
```

### Option B: Install via Cargo (Future)

```bash
# When published to crates.io
cargo install metafuse-cli
```

## Step 2: Initialize Your First Catalog (1 minute)

```bash
# Initialize a new catalog in the current directory
./target/release/metafuse init

# Or if installed globally
metafuse init
```

This creates `metafuse_catalog.db` in the current directory.

**Output:**
```
Initializing catalog at: metafuse_catalog.db
Catalog initialized successfully
```

**What just happened?**
- Created a SQLite database with MetaFuse schema
- Set up tables for datasets, fields, lineage, tags, glossary terms
- Initialized FTS5 full-text search index
- Set catalog version to 1

## Step 3: Run Your First Example (3 minutes)

MetaFuse provides a simple example that demonstrates DataFusion integration:

```bash
# Run the simple pipeline example
cargo run --example simple_pipeline

# Expected output:
# Creating sample data...
# Running DataFusion query...
# Emitting metadata to catalog...
# Metadata emitted: sample_dataset
```

**What the example does:**
1. Creates in-memory Arrow data
2. Runs a simple DataFusion query
3. Emits metadata to the catalog
4. Shows how to integrate MetaFuse into your pipeline

## Step 4: Query the Catalog via CLI (2 minutes)

Now let's explore the catalog using the CLI:

### List all datasets

```bash
metafuse list
```

**Output:**
```
Dataset               Tenant  Domain      Format   Owner                  Fields  Updated
sample_dataset        dev     analytics   parquet  example@metafuse.dev   3       2025-11-20 10:30:00
```

### Show dataset details

```bash
metafuse show sample_dataset
```

**Output:**
```
Dataset: sample_dataset
Path: /tmp/sample_data.parquet
Format: parquet
Tenant: dev
Domain: analytics
Owner: example@metafuse.dev
Description: Sample dataset for getting started

Fields:
  id (Int64, not null)
  name (Utf8, nullable)
  value (Float64, nullable)

Tags: [example, tutorial]

Created: 2025-11-20 10:30:00
Updated: 2025-11-20 10:30:00
```

### Show dataset lineage

```bash
metafuse show sample_dataset --lineage
```

**Output:**
```
Dataset: sample_dataset

Upstream Dependencies: (none)

Downstream Dependencies: (none)
```

### Search for datasets

```bash
metafuse search analytics
```

**Output:**
```
Found 1 dataset(s):

Dataset: sample_dataset
Domain: analytics
Owner: example@metafuse.dev
```

### View catalog statistics

```bash
metafuse stats
```

**Output:**
```
Catalog Statistics:

Datasets: 1
Fields: 3
Lineage Edges: 0
Tags: 2
Glossary Terms: 0

Catalog Version: 1
Last Updated: 2025-11-20 10:30:00
```

## Step 5: Start the REST API (1 minute)

Run the REST API server to access catalog data via HTTP:

```bash
# Default port: 8080
cargo run --bin metafuse-api

# Or specify a custom port
METAFUSE_PORT=3000 cargo run --bin metafuse-api
```

**Output:**
```
Starting MetaFuse API server...
Catalog path: metafuse_catalog.db
Listening on: http://0.0.0.0:8080
```

## Step 6: Query the API (1 minute)

Open a new terminal and test the API endpoints:

### Health check

```bash
curl http://localhost:8080/health
```

**Response:**
```json
{"status": "ok"}
```

### List datasets

```bash
curl http://localhost:8080/api/v1/datasets
```

**Response:**
```json
{
  "datasets": [
    {
      "name": "sample_dataset",
      "path": "/tmp/sample_data.parquet",
      "format": "parquet",
      "tenant": "dev",
      "domain": "analytics",
      "owner": "example@metafuse.dev",
      "description": "Sample dataset for getting started",
      "fields": 3,
      "created_at": "2025-11-20T10:30:00Z",
      "updated_at": "2025-11-20T10:30:00Z"
    }
  ]
}
```

### Get dataset details

```bash
curl http://localhost:8080/api/v1/datasets/sample_dataset
```

**Response:**
```json
{
  "name": "sample_dataset",
  "path": "/tmp/sample_data.parquet",
  "format": "parquet",
  "tenant": "dev",
  "domain": "analytics",
  "owner": "example@metafuse.dev",
  "description": "Sample dataset for getting started",
  "fields": [
    {"name": "id", "data_type": "Int64", "nullable": false},
    {"name": "name", "data_type": "Utf8", "nullable": true},
    {"name": "value", "data_type": "Float64", "nullable": true}
  ],
  "upstream": [],
  "downstream": [],
  "tags": ["example", "tutorial"],
  "created_at": "2025-11-20T10:30:00Z",
  "updated_at": "2025-11-20T10:30:00Z"
}
```

### Search datasets

```bash
curl "http://localhost:8080/api/v1/search?q=analytics"
```

**Response:**
```json
{
  "results": [
    {
      "name": "sample_dataset",
      "path": "/tmp/sample_data.parquet",
      "domain": "analytics",
      "owner": "example@metafuse.dev"
    }
  ]
}
```

## Next Steps

### Integrate with Your DataFusion Pipeline

Here's a minimal example to integrate MetaFuse into your own pipeline:

```rust
use datafusion::prelude::*;
use metafuse_catalog_emitter::Emitter;
use metafuse_catalog_storage::LocalSqliteBackend;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Your DataFusion pipeline
    let ctx = SessionContext::new();
    let df = ctx.read_parquet("input.parquet", Default::default()).await?;

    let result = df
        .filter(col("status").eq(lit("active")))?
        .select_columns(&["id", "name", "value"])?;

    // 2. Write output
    result.write_parquet("output.parquet", Default::default()).await?;

    // 3. Emit metadata to catalog
    let backend = LocalSqliteBackend::new("metafuse_catalog.db");
    let emitter = Emitter::new(backend);

    emitter.emit_dataset(
        "my_dataset",                     // Dataset name
        "output.parquet",                 // Path
        "parquet",                        // Format
        Some("prod"),                     // Tenant (optional)
        Some("analytics"),                // Domain (optional)
        Some("team@example.com"),         // Owner (optional)
        result.schema().inner().clone(),  // Arrow schema
        Some("Filtered active records"),  // Description (optional)
        None,                             // Row count (optional)
        vec!["input_dataset"],            // Upstream dependencies
        vec!["filtered", "active"],       // Tags
    )?;

    println!("Pipeline complete, metadata emitted");
    Ok(())
}
```

### Track Lineage in Multi-Stage Pipelines

```rust
// Stage 1: Load raw data
let raw = ctx.read_parquet("raw.parquet", Default::default()).await?;
emitter.emit_dataset("raw_data", "raw.parquet", "parquet", ..., vec![], ...)?;

// Stage 2: Clean data
let cleaned = raw.filter(...)?;
cleaned.write_parquet("cleaned.parquet", Default::default()).await?;
emitter.emit_dataset("cleaned_data", "cleaned.parquet", "parquet", ..., vec!["raw_data"], ...)?;

// Stage 3: Aggregate data
let aggregated = cleaned.aggregate(...)?;
aggregated.write_parquet("aggregated.parquet", Default::default()).await?;
emitter.emit_dataset("aggregated_data", "aggregated.parquet", "parquet", ..., vec!["cleaned_data"], ...)?;
```

Now when you run `metafuse show aggregated_data --lineage`, you'll see:

```
Upstream Dependencies:
  cleaned_data
    -> raw_data
```

### Explore Advanced Features

- **Multi-tenant metadata**: Isolate datasets by `tenant` (e.g., "prod", "dev", "customer-123")
- **Business glossary**: Add glossary terms to provide business context
- **Full-text search**: Search across dataset names, paths, domains, and field names
- **Custom catalog paths**: Use `--catalog /path/to/catalog.db` with CLI commands

## Common Operations

### Reset the catalog

```bash
# Delete and re-initialize (WARNING: deletes all metadata)
rm metafuse_catalog.db
metafuse init
```

### Force re-initialization

```bash
metafuse init --force
```

### Use a custom catalog location

```bash
# Specify catalog path
metafuse --catalog /data/my_catalog.db list

# Or set environment variable
export METAFUSE_CATALOG=/data/my_catalog.db
metafuse list
```

### Filter datasets

```bash
# Filter by tenant
metafuse list --tenant prod

# Filter by domain
metafuse list --domain analytics
```

## Troubleshooting

### "Catalog not found" error

```bash
# Ensure you've initialized the catalog
metafuse init

# Or specify the correct path
metafuse --catalog /path/to/catalog.db list
```

### API server won't start

```bash
# Check if port is already in use
lsof -i :8080

# Use a different port
METAFUSE_PORT=3000 metafuse-api
```

### "Database is locked" error

This occurs when multiple processes try to write simultaneously. MetaFuse uses optimistic concurrency, so retries should succeed. If the issue persists:

- Ensure only one process is writing at a time
- Check file permissions: `chmod 600 metafuse_catalog.db`

## Learn More

- [Architecture](architecture.md): Understand how MetaFuse works
- [API Reference](api-reference.md): Complete REST API documentation
- [Roadmap](roadmap.md): See what's coming next
- [Examples](../examples/README.md): More example projects

## Getting Help

- **GitHub Issues**: [Report bugs or request features](https://github.com/ethan-tyler/MetaFuse/issues)
- **GitHub Discussions**: [Ask questions](https://github.com/ethan-tyler/MetaFuse/discussions)
- **Documentation**: [Full docs](https://github.com/ethan-tyler/MetaFuse/tree/main/docs)

---

**Congratulations!** You've successfully set up MetaFuse and explored its core features. Now you're ready to integrate it into your DataFusion pipelines.
