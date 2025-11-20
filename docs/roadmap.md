# MetaFuse Roadmap

This document outlines the planned development phases for MetaFuse.

## Current Status: Phase 1 (MVP)

MetaFuse is currently in early MVP stage with core functionality implemented.

---

## Phase 1: MVP [done] (Q4 2025 - Q1 2026)

**Goal:** Build a working proof-of-concept with core features.

### Completed

- [done] **Core Data Model**
  - `DatasetMeta` and `FieldMeta` types
  - Comprehensive SQLite schema (datasets, fields, lineage, tags, glossary)
  - FTS5 full-text search integration
  - Version-based optimistic concurrency control

- [done] **Storage Backend**
  - `CatalogBackend` trait abstraction
  - `LocalSqliteBackend` implementation
  - Connection management and initialization
  - Error handling

- [done] **DataFusion Integration**
  - `Emitter` API for metadata capture
  - Automatic schema extraction from Arrow `SchemaRef`
  - Lineage tracking support
  - Tag and multi-tenant metadata

- [done] **REST API**
  - Axum-based HTTP server
  - List datasets endpoint (`/api/v1/datasets`)
  - Get dataset details endpoint (`/api/v1/datasets/:name`)
  - Search endpoint (`/api/v1/search`)
  - CORS support for web UIs

- [done] **CLI Tool**
  - `init` command (initialize catalog)
  - `list` command (list datasets with filters)
  - `show` command (dataset details + lineage)
  - `search` command (full-text search)
  - `stats` command (catalog statistics)
  - Formatted output with table display

- [done] **Documentation**
  - Comprehensive README
  - Apache 2.0 license
  - Basic project structure

### In Progress

- [in progress] **Examples and Testing**
  - Working examples (simple pipeline, lineage tracking)
  - Integration tests
  - Performance benchmarks

- [in progress] **Repository Infrastructure**
  - GitHub workflows (CI, lint, audit)
  - Issue and PR templates
  - Community policies (CoC, security, contributing)
  - Detailed documentation (architecture, getting started, API reference)

---

## Phase 2: Open Source Launch  (Q1-Q2 2026)

**Goal:** Prepare for public launch with complete documentation and cloud backend support.

### Planned Features

- **Cloud Storage Backends**
  - [done] Implement `GCSBackend` (Google Cloud Storage)
    - Generation-based optimistic concurrency
    - Download/upload with caching
    - Service account authentication
  - [done] Implement `S3Backend` (AWS S3)
    - ETag-based optimistic concurrency
    - IAM role authentication
    - Download/upload with caching

- **Web UI (Initial)**
  - [done] Basic dataset browser
  - [done] Interactive lineage visualization (DAG graph)
  - [done] Search interface
  - [done] Schema explorer
  - Tech stack: React/Vue + D3.js/Cytoscape.js for lineage graphs

- **Comprehensive Documentation**
  - [done] Architecture deep-dive
  - [done] Getting started guide (10-minute tutorial)
  - [done] API reference
  - [done] Deployment guides (Docker, Kubernetes, serverless)
  - [done] FAQ and troubleshooting
  - [done] Video tutorials (YouTube)

- **Community Infrastructure**
  - [done] GitHub Discussions enabled
  - [done] Discord or Slack community
  - [done] Contributing guidelines with good-first-issue labels
  - [done] Release process and versioning strategy

- **Testing and Quality**
  - [done] Integration test suite
  - [done] Performance benchmarks (throughput, latency)
  - [done] CI/CD with multi-platform testing
  - [done] Example projects (dbt integration, Airflow DAG, serverless pipeline)

---

## Phase 3: Enhanced Features  (Q3-Q4 2026)

**Goal:** Add advanced features requested by the community.

### Planned Features

- **Usage Analytics**
  - Track dataset access patterns
  - Identify popular datasets
  - Detect stale/unused datasets
  - Freshness monitoring (last updated time)
  - Dashboard for catalog health

- **Advanced Search**
  - Fuzzy matching (typo tolerance)
  - Column-level search (find datasets with specific field names)
  - Faceted search (filter by domain, owner, tags, format)
  - Search suggestions and autocomplete

- **Column-Level Lineage**
  - Track field-level dependencies (e.g., `output.revenue` depends on `input.sales * input.quantity`)
  - Visualize column-level transformations
  - Impact analysis at the field level

- **Scale Testing**
  - Validate performance with 10,000+ datasets
  - Optimize for 100,000+ fields
  - Test catalog file sizes >100MB
  - Identify bottlenecks and optimize

- **Ecosystem Integrations**
  - **dbt Integration**
    - Automatically ingest dbt models as datasets
    - Map dbt lineage to MetaFuse lineage
    - Sync dbt descriptions and tags
  - **Great Expectations Integration**
    - Attach data quality expectations to datasets
    - Track validation results over time
  - **Airflow Integration**
    - Emit metadata from Airflow DAGs
    - Visualize pipeline orchestration alongside lineage
  - **Apache Iceberg Integration**
    - Catalog Iceberg tables
    - Track Iceberg snapshot lineage

---

## Phase 4: Enterprise Features  (2027+)

**Goal:** Provide enterprise-ready features for larger organizations.

### Planned Features

- **Multi-Tenant Hosted Service**
  - SaaS offering with managed hosting
  - Tenant isolation and resource quotas
  - Subscription tiers (free, team, enterprise)

- **Authentication and Authorization**
  - API key authentication
  - OAuth2/OIDC SSO (Google, Okta, Azure AD)
  - Role-based access control (RBAC)
    - Reader, Writer, Admin roles
    - Dataset-level and domain-level permissions
  - Fine-grained access policies

- **Audit Logging**
  - Track all metadata changes (who, what, when)
  - Compliance reporting (GDPR, SOC2)
  - Retention policies for audit logs

- **Advanced Backend Options**
  - **PostgreSQL Backend**
    - For high write-throughput scenarios
    - Full ACID transactions
    - Horizontal scaling with read replicas
  - **DuckDB Backend**
    - For larger catalogs (>100MB)
    - Better OLAP query performance
    - Parquet-based storage

- **Professional Support**
  - SLA-backed support plans
  - Onboarding and training
  - Custom integrations and consulting

---

## Community Wishlist 

Features suggested by the community (not yet prioritized):

- **Data Contracts**
  - Define expected schemas for datasets
  - Validate incoming data against contracts
  - Alert on schema drift

- **Cost Tracking**
  - Attach cost metadata to datasets (storage cost, compute cost)
  - Identify expensive datasets
  - Budget alerts

- **Data Retention Policies**
  - Define TTLs for datasets
  - Automated archival or deletion
  - Compliance with retention regulations

- **Notifications and Alerts**
  - Slack/email notifications for schema changes
  - Alerts for stale datasets
  - Lineage impact notifications

- **Semantic Layer Integration**
  - Integrate with Cube.js, Looker, Tableau
  - Map semantic models to physical datasets

- **Query-Level Metadata**
  - Capture SQL queries that generated datasets
  - Link query execution logs to datasets
  - Enable query-level lineage

- **Schema Evolution Tracking**
  - Track schema changes over time
  - Diff schemas between versions
  - Alert on breaking changes

- **Data Profiling**
  - Capture min/max/avg/null-count statistics
  - Histograms and distribution data
  - Data quality scores

---

## Release Schedule (Tentative)

| Version | Target Date  | Focus                           |
|---------|--------------|---------------------------------|
| 0.1.0   | Q1 2026      | MVP (current)                   |
| 0.2.0   | Q2 2026      | Cloud backends + Web UI         |
| 0.3.0   | Q3 2026      | Usage analytics + advanced search |
| 0.4.0   | Q4 2026      | Ecosystem integrations          |
| 1.0.0   | Q1 2027      | Stable release + enterprise features |

---

## How to Influence the Roadmap

We welcome community input! Here's how you can help shape MetaFuse:

1. **Vote on Issues**:  the GitHub issues that matter most to you
2. **Submit RFCs**: Use the [RFC template](.github/ISSUE_TEMPLATE/rfc.yml) for significant proposals
3. **Join Discussions**: Participate in [GitHub Discussions](https://github.com/ethan-tyler/MetaFuse/discussions)
4. **Contribute Code**: See [CONTRIBUTING.md](../CONTRIBUTING.md) for how to contribute

---

## Versioning and Stability

- **v0.x**: Experimental. APIs may change without notice.
- **v1.0+**: Stable. Breaking changes will follow semantic versioning.
- **LTS versions** (future): Long-term support for enterprise users.

---

## Success Metrics

We'll track these metrics to measure progress:

- **Adoption**: GitHub stars, downloads, active users
- **Performance**: API latency, catalog size limits, query speed
- **Community**: Contributors, issues resolved, discussions activity
- **Quality**: Test coverage, CI pass rate, bug reports

---

## Questions?

- Open a [discussion](https://github.com/ethan-tyler/MetaFuse/discussions) to ask about the roadmap
- Submit a [feature request](https://github.com/ethan-tyler/MetaFuse/issues/new?template=feature_request.yml) for new ideas

---

**Last Updated:** 2025-11-20
