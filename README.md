# MetaFuse

**MetaFuse** is a lightweight, serverless data catalog for **DataFusion** and modern **lakehouse** pipelines.

It captures dataset schemas, lineage, and operational metadata automatically from your data pipelines—without requiring Kafka, MySQL, or Elasticsearch. Just a SQLite file on object storage.

> 🚧 **Status**: Early MVP. APIs and layout will evolve.

## Vision

**Make enterprise-grade data cataloging accessible to every data team, regardless of size or budget.**

MetaFuse fills the gap between expensive, complex enterprise catalogs (DataHub, Collibra) and the needs of small-to-medium data teams. It provides:

- ✨ **Native DataFusion integration** - Emit metadata directly from pipelines
- 💰 **Serverless-friendly** - $0-$5/month on object storage (GCS/S3)
- 🚀 **Zero infrastructure** - No databases, no clusters to maintain
- 📊 **Automatic lineage capture** - Track data flow through transformations
- 🔍 **Full-text search** - Find datasets quickly with SQLite FTS5
- 🏷️ **Tags and glossary** - Organize with business context
- 🔐 **Multi-tenant support** - Isolate data by tenant/environment

## Goals

- Deploy a working catalog in under 30 minutes
- Automatically capture lineage from DataFusion pipelines with zero manual configuration
- Run on $0-$5/month infrastructure (object storage only)
- Support 10,000 datasets, 100,000 fields, 50,000 lineage edges
- Sub-500ms query latency for 95% of catalog API requests

## Architecture

MetaFuse uses a novel **SQLite-on-object-storage** pattern:

1. Metadata is stored in a SQLite file on GCS/S3/local storage
2. DataFusion pipelines emit metadata at write-time using the `catalog-emitter` library
3. Readers download the catalog, query locally, and benefit from local SQLite performance
4. Writers use optimistic concurrency control (version-based locking) to handle concurrent updates

This architecture provides:
- **Simplicity**: No database servers to manage
- **Cost-effectiveness**: Pay only for object storage (~$0.02/GB/month)
- **Scalability**: Start with SQLite, migrate to DuckDB or Postgres as you scale
- **Portability**: Works on laptops, CI pipelines, and cloud functions

## Workspace Layout

```
MetaFuse/
├── crates/
│   ├── catalog-core/       # Core types and SQLite schema
│   ├── catalog-storage/    # Local + cloud catalog backends
│   ├── catalog-emitter/    # DataFusion integration
│   ├── catalog-api/        # REST API (Axum)
│   └── catalog-cli/        # CLI for catalog operations
├── docs/                   # Documentation
├── examples/               # Usage examples
└── tests/                  # Integration tests
```

## Getting Started

### Prerequisites

- Rust 1.75+ and Cargo
- (Optional) DataFusion for pipeline integration

### Build from Source

```bash
git clone https://github.com/ethanurbanski/MetaFuse.git
cd MetaFuse
cargo build --release
```

### Initialize a Catalog

```bash
# Using the CLI
cargo run --bin metafuse -- init

# Or after installing
metafuse init
```

### Emit Metadata from DataFusion

```rust
use datafusion::prelude::*;
use metafuse_catalog_emitter::Emitter;
use metafuse_catalog_storage::LocalSqliteBackend;

#[tokio::main]
async fn main() -> Result<()> {
    // Your DataFusion pipeline
    let ctx = SessionContext::new();
    let df = ctx.read_parquet("data/input.parquet", Default::default()).await?;

    // Transform and write
    let result = df
        .filter(col("status").eq(lit("active")))?
        .select_columns(&["id", "name", "value"])?;

    result.write_parquet("data/output.parquet", Default::default()).await?;

    // Emit metadata to catalog
    let backend = LocalSqliteBackend::new("metafuse_catalog.db");
    let emitter = Emitter::new(backend);

    emitter.emit_dataset(
        "active_records",
        "data/output.parquet",
        "parquet",
        Some("prod"),
        Some("analytics"),
        Some("data-team@company.com"),
        result.schema().inner().clone(),
        None,
        None,
        vec!["raw_records".to_string()],
        vec!["active".to_string(), "filtered".to_string()],
    )?;

    Ok(())
}
```

### Query the Catalog

```bash
# List all datasets
metafuse list

# Show dataset details with lineage
metafuse show active_records --lineage

# Search datasets
metafuse search "analytics"

# Show catalog statistics
metafuse stats
```

### Run the API Server

```bash
cargo run --bin metafuse-api

# Query via REST API
curl http://localhost:8080/api/v1/datasets
curl http://localhost:8080/api/v1/datasets/active_records
curl http://localhost:8080/api/v1/search?q=analytics
```

## Use Cases

### Data Discovery
Stop wasting hours searching for datasets. Full-text search across dataset names, paths, domains, and field names.

### Data Lineage
Automatically track which datasets depend on each other. Understand impact analysis when making changes.

### Data Governance
Know what data exists, where it lives, who owns it, and when it was last updated. Essential for compliance.

### Team Collaboration
Share knowledge about datasets using tags, descriptions, and business glossary terms.

## Comparison with Alternatives

| Feature | MetaFuse | DataHub | Amundsen | AWS Glue |
|---------|----------|---------|----------|----------|
| **Cost** | $0-$5/mo | Self-hosted (high) | Self-hosted (high) | Pay-per-use |
| **Setup Time** | < 30 min | Days | Days | Hours |
| **Infrastructure** | None | Kafka, MySQL, ES | Neo4j, ES, Airflow | AWS only |
| **DataFusion Integration** | Native | Via connector | Via connector | No |
| **Local Development** | Yes | No | No | No |
| **Lineage** | Automatic | Manual/Scan | Manual/Scan | Limited |

## Roadmap

### Phase 1: MVP (Current)
- ✅ Core types and SQLite schema
- ✅ Local storage backend
- ✅ DataFusion emitter integration
- ✅ REST API
- ✅ CLI tool
- 🚧 Documentation and examples

### Phase 2: Open Source Launch
- Web UI with interactive lineage graphs
- GCS and S3 storage backends
- Comprehensive documentation
- Community infrastructure (Discord, GitHub Discussions)

### Phase 3: Enhanced Features
- Usage analytics (popular datasets, access patterns)
- Advanced search (fuzzy matching, column-level)
- Scale testing (10k+ datasets)
- Ecosystem integrations (dbt, Great Expectations, Airflow, Iceberg)

### Phase 4: Enterprise Features
- Multi-tenant hosted service
- SSO support (Google, Okta)
- RBAC and fine-grained permissions
- Audit logging
- Professional support

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MetaFuse is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## Community

- **GitHub Issues**: [Report bugs or request features](https://github.com/ethanurbanski/MetaFuse/issues)
- **Discussions**: [Ask questions and share ideas](https://github.com/ethanurbanski/MetaFuse/discussions)

## Acknowledgments

MetaFuse is built on the shoulders of giants:
- [Apache Arrow](https://arrow.apache.org/) and [DataFusion](https://datafusion.apache.org/)
- [SQLite](https://www.sqlite.org/)
- [Rust](https://www.rust-lang.org/)

Special thanks to the DataFusion community for inspiration and feedback.

---

**Built with ❤️ for data teams who want catalogs, not complexity.**
