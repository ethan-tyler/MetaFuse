# Lakehouse Catalog: Complete Architecture Specification

**Unified Metadata Platform for Urbanski Dataverse**

**Version:** 2.0 (Merged Architecture)
**Date:** November 2025
**Author:** Ethan Urbanski
**Status:** Final Design

---

## Executive Summary

This document presents the **merged architecture** for Lakehouse Catalog—combining the comprehensive enterprise features from the original design with the streamlined Delta-native approach. The result is a best-in-class metadata platform that:

1. **Leverages Delta Lake** for technical metadata (schema, stats, history)
2. **Stores business metadata** that Delta doesn't provide (lineage, ownership, quality assessments, glossary)
3. **Maintains enterprise features** (governance, audit logs, usage analytics, multi-tenancy)
4. **Remains serverless** (SQLite-on-GCS, Cloud Run)

**Design Philosophy:**
> *"Store what Delta doesn't know. Compute what Delta can provide. Never duplicate what Delta maintains."*

---

## Table of Contents

1. [Architecture Principles](#1-architecture-principles)
2. [Metadata Ownership Model](#2-metadata-ownership-model)
3. [Complete Data Model](#3-complete-data-model)
4. [Feature Matrix](#4-feature-matrix)
5. [API Specification](#5-api-specification)
6. [Implementation Architecture](#6-implementation-architecture)
7. [Multi-Tenant Design](#7-multi-tenant-design)
8. [Quality & Governance Framework](#8-quality--governance-framework)
9. [Search & Discovery](#9-search--discovery)
10. [Integration Patterns](#10-integration-patterns)
11. [Deployment Architecture](#11-deployment-architecture)
12. [Implementation Roadmap](#12-implementation-roadmap)

---

## 1. Architecture Principles

### 1.1 Core Tenets

```
┌─────────────────────────────────────────────────────────────────┐
│                    ARCHITECTURAL TENETS                          │
└─────────────────────────────────────────────────────────────────┘

1. DELTA AS SOURCE OF TRUTH (Technical Metadata)
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Schema, statistics, history, partitions → Read from Delta
   Never store, never sync, never stale

2. CATALOG AS SOURCE OF TRUTH (Business Metadata)
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Lineage, ownership, glossary, quality assessments → Store in catalog
   Delta doesn't have this, we add value

3. COMPUTE OVER STORE (Derived Metadata)
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Quality scores, freshness status → Compute from Delta stats
   Cache briefly, recompute frequently

4. ENTERPRISE FEATURES WITHOUT ENTERPRISE COMPLEXITY
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Full governance, audit, quality → Serverless architecture
   Industry-standard features, minimal infrastructure

5. PHYSICAL TENANT ISOLATION
   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Per-tenant catalog files → No row-level filtering risks
   GDPR compliance, easy deletion, no data leakage
```

### 1.2 Metadata Classification

```
┌─────────────────────────────────────────────────────────────────┐
│                 METADATA CLASSIFICATION                          │
└─────────────────────────────────────────────────────────────────┘

                    ┌─────────────────────────────┐
                    │     METADATA TYPES          │
                    └─────────────────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   STORED        │    │   DELEGATED     │    │   COMPUTED      │
│   (Catalog DB)  │    │   (Delta Lake)  │    │   (On Demand)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
│                 │    │                 │    │                 │
│ • Table registry│    │ • Schema        │    │ • Quality scores│
│ • Lineage       │    │ • Row count     │    │ • Freshness     │
│ • Ownership     │    │ • Column stats  │    │   status        │
│ • Domain        │    │ • Partitions    │    │ • Popularity    │
│ • Description   │    │ • File sizes    │    │   rank          │
│ • Tags          │    │ • History       │    │ • Impact        │
│ • Glossary      │    │ • Versions      │    │   analysis      │
│ • Classifications│   │                 │    │ • Schema diff   │
│ • Freshness SLAs│    │                 │    │                 │
│ • Usage logs    │    │                 │    │                 │
│ • Audit logs    │    │                 │    │                 │
│ • Quality checks│    │                 │    │                 │
│   (definitions) │    │                 │    │                 │
│ • Search index  │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘

STORAGE:              READ ON DEMAND:        COMPUTE ON REQUEST:
~200 lines schema     Zero storage           Zero storage
Sync: Never           Always fresh           Always fresh
```

---

## 2. Metadata Ownership Model

### 2.1 Detailed Ownership Matrix

| Metadata Type | Owner | Storage | Update Frequency | Rationale |
|---------------|-------|---------|------------------|-----------|
| **Table name** | Catalog | SQLite | On registration | Delta doesn't have namespace concept |
| **Table location** | Catalog | SQLite | On registration | Maps name → physical path |
| **Format** | Catalog | SQLite | On registration | Delta/Iceberg/Parquet indicator |
| **Schema** | Delta | _delta_log | On data write | Delta maintains authoritative schema |
| **Row count** | Delta | File stats | On data write | Aggregated from file statistics |
| **Column stats** | Delta | File stats | On data write | Min/max/null counts per file |
| **Partition columns** | Delta | Table metadata | On table creation | Part of Delta table definition |
| **File count/size** | Delta | Snapshot | On data write | Count of active files |
| **History** | Delta | Transaction log | On each commit | Full transaction history |
| **Domain** | Catalog | SQLite | Manual update | Business classification |
| **Owner** | Catalog | SQLite | Manual update | Accountability |
| **Description** | Catalog | SQLite | Manual update | Human context |
| **Tags** | Catalog | SQLite | Manual update | Flexible categorization |
| **Lineage** | Catalog | SQLite | On pipeline run | Delta doesn't track cross-table deps |
| **Glossary terms** | Catalog | SQLite | Manual update | Business vocabulary |
| **Term links** | Catalog | SQLite | Manual update | Term-to-column mappings |
| **Classification** | Catalog | SQLite | Manual/auto | PII, confidential, etc. |
| **Freshness SLA** | Catalog | SQLite | Manual config | Expected update frequency |
| **Freshness status** | Computed | None | On request | Compare last_commit vs SLA |
| **Quality score** | Computed | None | On request | Derived from Delta stats |
| **Quality checks** | Catalog | SQLite | On check definition | Check definitions stored |
| **Quality results** | Catalog | SQLite | On check execution | Check results stored |
| **Usage logs** | Catalog | SQLite | On query | Query access patterns |
| **Popularity** | Computed | None | On request | Aggregated from usage logs |
| **Audit logs** | Catalog | SQLite | On any change | All catalog modifications |

### 2.2 Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA FLOW ARCHITECTURE                        │
└─────────────────────────────────────────────────────────────────┘

                         API Request
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CATALOG API                                 │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                   REQUEST ROUTER                          │  │
│  │                                                           │  │
│  │  /tables/:name         → MergedTableHandler               │  │
│  │  /tables/:name/schema  → DeltaDelegateHandler             │  │
│  │  /tables/:name/stats   → DeltaDelegateHandler             │  │
│  │  /tables/:name/quality → ComputedHandler                  │  │
│  │  /lineage              → CatalogOnlyHandler               │  │
│  │  /glossary             → CatalogOnlyHandler               │  │
│  │  /search               → CatalogOnlyHandler               │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                   │
│         ┌────────────────────┼────────────────────┐             │
│         │                    │                    │             │
│         ▼                    ▼                    ▼             │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐       │
│  │  Catalog    │     │   Delta     │     │  Compute    │       │
│  │  Store      │     │   Reader    │     │  Engine     │       │
│  │             │     │             │     │             │       │
│  │  SQLite     │     │  deltalake  │     │  Quality    │       │
│  │  queries    │     │  crate      │     │  calculator │       │
│  └──────┬──────┘     └──────┬──────┘     └──────┬──────┘       │
│         │                   │                   │               │
└─────────┼───────────────────┼───────────────────┼───────────────┘
          │                   │                   │
          ▼                   ▼                   │
┌─────────────────┐  ┌─────────────────┐         │
│  catalog.db     │  │  Delta Tables   │         │
│  (GCS)          │  │  (GCS)          │◄────────┘
│                 │  │                 │   (reads stats)
│  tenant-001/    │  │  _delta_log/    │
│    catalog.db   │  │                 │
└─────────────────┘  └─────────────────┘
```

---

## 3. Complete Data Model

### 3.1 SQLite Schema (Stored Metadata)

```sql
-- ============================================================================
-- LAKEHOUSE CATALOG - COMPLETE SCHEMA
-- Version: 2.0 (Merged Architecture)
-- 
-- Design Principles:
-- 1. Store what Delta doesn't know
-- 2. Never duplicate Delta metadata
-- 3. Support all enterprise features
-- 4. Maintain referential integrity
-- ============================================================================

-- ============================================================================
-- CORE REGISTRY
-- Purpose: Map table names to locations, store business context
-- ============================================================================

CREATE TABLE tables (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,                      -- "sales.orders"
    location TEXT NOT NULL,                  -- "gs://bucket/sales/orders/"
    format TEXT NOT NULL DEFAULT 'delta',    -- "delta", "iceberg", "parquet"
    
    -- Business Context (Delta doesn't have)
    domain TEXT,                             -- "sales", "marketing", "finance"
    owner TEXT,                              -- "data-team@company.com"
    description TEXT,                        -- Human-readable description
    
    -- Governance (Delta doesn't have)
    classification TEXT,                     -- "public", "internal", "confidential", "pii"
    retention_days INTEGER,                  -- Data retention policy
    
    -- Freshness Configuration (Delta doesn't track SLAs)
    freshness_sla TEXT,                      -- "hourly", "daily", "weekly"
    freshness_grace_minutes INTEGER DEFAULT 60,
    
    -- Certification (Delta doesn't have)
    is_certified INTEGER DEFAULT 0,
    certified_by TEXT,
    certified_at TEXT,
    
    -- Timestamps
    registered_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(name)
);

CREATE INDEX idx_tables_domain ON tables(domain);
CREATE INDEX idx_tables_owner ON tables(owner);
CREATE INDEX idx_tables_classification ON tables(classification);

-- ============================================================================
-- TAGS
-- Purpose: Flexible categorization (Delta table properties are limited)
-- ============================================================================

CREATE TABLE tags (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    tag_type TEXT,                           -- "layer", "domain", "team", "custom"
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE,
    UNIQUE(table_id, tag)
);

CREATE INDEX idx_tags_tag ON tags(tag);
CREATE INDEX idx_tags_type ON tags(tag_type);

-- ============================================================================
-- LINEAGE
-- Purpose: Cross-table dependencies (Delta only knows about itself)
-- ============================================================================

CREATE TABLE lineage (
    id TEXT PRIMARY KEY,
    upstream_table_id TEXT NOT NULL,
    downstream_table_id TEXT NOT NULL,
    
    -- Lineage Context
    job_name TEXT,                           -- "daily_orders_etl"
    job_run_id TEXT,                         -- Execution ID (Servo integration)
    transformation_type TEXT,                -- "sql", "datafusion", "spark", "dbt"
    transformation_sql TEXT,                 -- Optional: SQL that created the relationship
    
    -- Timestamps
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (upstream_table_id) REFERENCES tables(id) ON DELETE CASCADE,
    FOREIGN KEY (downstream_table_id) REFERENCES tables(id) ON DELETE CASCADE,
    UNIQUE(upstream_table_id, downstream_table_id)
);

CREATE INDEX idx_lineage_upstream ON lineage(upstream_table_id);
CREATE INDEX idx_lineage_downstream ON lineage(downstream_table_id);
CREATE INDEX idx_lineage_job ON lineage(job_name);

-- ============================================================================
-- BUSINESS GLOSSARY
-- Purpose: Business vocabulary (Delta has no concept of this)
-- ============================================================================

CREATE TABLE glossary_terms (
    id TEXT PRIMARY KEY,
    term TEXT NOT NULL UNIQUE,
    definition TEXT NOT NULL,
    domain TEXT,                             -- Business domain this term belongs to
    owner TEXT,                              -- Term steward
    synonyms TEXT,                           -- JSON array: ["ARR", "annual recurring revenue"]
    related_terms TEXT,                      -- JSON array of related term IDs
    
    -- Timestamps
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    updated_by TEXT
);

CREATE INDEX idx_glossary_domain ON glossary_terms(domain);

-- Term-to-Asset Links
CREATE TABLE glossary_links (
    id TEXT PRIMARY KEY,
    term_id TEXT NOT NULL,
    table_id TEXT,
    column_name TEXT,                        -- Optional: link to specific column
    link_type TEXT DEFAULT 'defines',        -- "defines", "relates_to", "derived_from"
    
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    
    FOREIGN KEY (term_id) REFERENCES glossary_terms(id) ON DELETE CASCADE,
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE,
    
    CHECK (table_id IS NOT NULL)             -- Must link to at least a table
);

CREATE INDEX idx_glossary_links_term ON glossary_links(term_id);
CREATE INDEX idx_glossary_links_table ON glossary_links(table_id);

-- ============================================================================
-- DATA DOMAINS
-- Purpose: Organizational hierarchy for data (Delta doesn't have org structure)
-- ============================================================================

CREATE TABLE domains (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    owner TEXT,                              -- Domain steward
    parent_domain_id TEXT,                   -- For hierarchical domains
    
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (parent_domain_id) REFERENCES domains(id)
);

-- ============================================================================
-- QUALITY FRAMEWORK
-- Purpose: Quality definitions and results (Delta has stats, not quality rules)
-- ============================================================================

-- Quality Check Definitions
CREATE TABLE quality_checks (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    
    -- Check Definition
    check_type TEXT NOT NULL,                -- "completeness", "freshness", "validity", "uniqueness", "custom"
    check_name TEXT NOT NULL,
    check_description TEXT,
    
    -- Check Configuration
    check_config TEXT,                       -- JSON: check-specific configuration
    severity TEXT DEFAULT 'warning',         -- "info", "warning", "critical"
    enabled INTEGER DEFAULT 1,
    
    -- Thresholds
    warn_threshold REAL,                     -- Score below this = warning
    fail_threshold REAL,                     -- Score below this = failure
    
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE
);

CREATE INDEX idx_quality_checks_table ON quality_checks(table_id);

-- Quality Check Results
CREATE TABLE quality_results (
    id TEXT PRIMARY KEY,
    check_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    
    -- Result
    status TEXT NOT NULL,                    -- "pass", "warn", "fail", "error"
    score REAL,                              -- 0.0 - 1.0
    
    -- Details
    details TEXT,                            -- JSON: check-specific result details
    records_checked INTEGER,
    records_failed INTEGER,
    
    -- Execution Context
    executed_at TEXT DEFAULT CURRENT_TIMESTAMP,
    execution_time_ms INTEGER,
    delta_version INTEGER,                   -- Delta version at time of check
    
    FOREIGN KEY (check_id) REFERENCES quality_checks(id) ON DELETE CASCADE,
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE
);

CREATE INDEX idx_quality_results_check ON quality_results(check_id);
CREATE INDEX idx_quality_results_table ON quality_results(table_id, executed_at DESC);

-- Aggregated Quality Scores (updated periodically)
CREATE TABLE quality_scores (
    table_id TEXT PRIMARY KEY,
    
    -- Overall Score
    overall_score REAL,                      -- Weighted average of all checks
    
    -- Dimension Scores
    completeness_score REAL,
    validity_score REAL,
    freshness_score REAL,
    uniqueness_score REAL,
    
    -- Status
    status TEXT,                             -- "healthy", "warning", "critical"
    
    -- Timestamps
    last_checked_at TEXT,
    last_passed_at TEXT,
    
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE
);

-- ============================================================================
-- FRESHNESS TRACKING
-- Purpose: SLA definitions and violations (Delta has timestamps, not SLAs)
-- ============================================================================

CREATE TABLE freshness_violations (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    
    -- Violation Details
    expected_by TEXT NOT NULL,               -- When data should have been updated
    detected_at TEXT NOT NULL,               -- When violation was detected
    resolved_at TEXT,                        -- When data was finally updated
    
    -- Context
    sla TEXT,                                -- "hourly", "daily", etc.
    hours_overdue REAL,
    
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE
);

CREATE INDEX idx_freshness_violations_table ON freshness_violations(table_id, detected_at DESC);

-- ============================================================================
-- USAGE ANALYTICS
-- Purpose: Track access patterns (Delta doesn't know who queries)
-- ============================================================================

CREATE TABLE usage_log (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    
    -- Access Context
    user_id TEXT,
    access_type TEXT,                        -- "query", "describe", "lineage", "export"
    access_method TEXT,                      -- "api", "datafusion", "cli", "ui"
    
    -- Timestamp
    accessed_at TEXT DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE
);

CREATE INDEX idx_usage_log_table ON usage_log(table_id, accessed_at DESC);
CREATE INDEX idx_usage_log_user ON usage_log(user_id, accessed_at DESC);

-- Aggregated Usage (materialized for performance)
CREATE TABLE usage_summary (
    table_id TEXT PRIMARY KEY,
    
    -- Counts
    query_count_1d INTEGER DEFAULT 0,
    query_count_7d INTEGER DEFAULT 0,
    query_count_30d INTEGER DEFAULT 0,
    query_count_total INTEGER DEFAULT 0,
    
    -- Unique Users
    unique_users_7d INTEGER DEFAULT 0,
    unique_users_30d INTEGER DEFAULT 0,
    
    -- Timestamps
    last_accessed_at TEXT,
    last_aggregated_at TEXT,
    
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE
);

-- ============================================================================
-- AUDIT LOG
-- Purpose: Track all catalog changes (compliance requirement)
-- ============================================================================

CREATE TABLE audit_log (
    id TEXT PRIMARY KEY,
    
    -- What Changed
    resource_type TEXT NOT NULL,             -- "table", "lineage", "glossary", "quality_check"
    resource_id TEXT NOT NULL,
    action TEXT NOT NULL,                    -- "create", "update", "delete"
    
    -- Change Details
    changes TEXT,                            -- JSON: {field: {old: x, new: y}}
    
    -- Who & When
    user_id TEXT,
    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
    
    -- Context
    api_endpoint TEXT,
    client_ip TEXT
);

CREATE INDEX idx_audit_log_resource ON audit_log(resource_type, resource_id);
CREATE INDEX idx_audit_log_timestamp ON audit_log(timestamp DESC);
CREATE INDEX idx_audit_log_user ON audit_log(user_id);

-- ============================================================================
-- CLASSIFICATION RULES
-- Purpose: Auto-detect PII and sensitive data (governance feature)
-- ============================================================================

CREATE TABLE classification_rules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    
    -- Matching Rules
    column_pattern TEXT,                     -- Regex for column names: ".*email.*"
    data_pattern TEXT,                       -- Regex for data values (sampled)
    
    -- Classification
    classification TEXT NOT NULL,            -- "pii", "pci", "phi", "confidential"
    confidence TEXT DEFAULT 'high',          -- "high", "medium", "low"
    
    -- Status
    enabled INTEGER DEFAULT 1,
    auto_apply INTEGER DEFAULT 0,            -- Automatically apply to new tables
    
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT
);

-- Applied Classifications (results of rule matching)
CREATE TABLE column_classifications (
    id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    column_name TEXT NOT NULL,
    
    classification TEXT NOT NULL,
    rule_id TEXT,                            -- NULL if manually assigned
    confidence TEXT,
    
    -- Masking Policy
    masking_policy TEXT,                     -- "full", "partial", "hash", "none"
    
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE,
    FOREIGN KEY (rule_id) REFERENCES classification_rules(id),
    UNIQUE(table_id, column_name)
);

CREATE INDEX idx_column_classifications_table ON column_classifications(table_id);

-- ============================================================================
-- SEARCH INDEX
-- Purpose: Full-text search across catalog (built from stored + Delta metadata)
-- ============================================================================

CREATE VIRTUAL TABLE search_index USING fts5(
    table_id UNINDEXED,
    
    -- From Catalog
    name,
    description,
    domain,
    owner,
    tags,
    
    -- From Delta (captured at registration, refreshed on demand)
    column_names,
    
    content=''
);

-- Triggers to maintain search index
CREATE TRIGGER tables_ai AFTER INSERT ON tables BEGIN
    INSERT INTO search_index(table_id, name, description, domain, owner)
    VALUES (new.id, new.name, new.description, new.domain, new.owner);
END;

CREATE TRIGGER tables_ad AFTER DELETE ON tables BEGIN
    DELETE FROM search_index WHERE table_id = old.id;
END;

CREATE TRIGGER tables_au AFTER UPDATE ON tables BEGIN
    DELETE FROM search_index WHERE table_id = old.id;
    INSERT INTO search_index(table_id, name, description, domain, owner)
    VALUES (new.id, new.name, new.description, new.domain, new.owner);
END;

-- ============================================================================
-- CATALOG METADATA
-- Purpose: Version control and catalog-level settings
-- ============================================================================

CREATE TABLE catalog_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    schema_version TEXT NOT NULL DEFAULT '2.0',
    version INTEGER NOT NULL DEFAULT 0,      -- Optimistic locking
    last_modified TEXT DEFAULT CURRENT_TIMESTAMP,
    
    -- Catalog Settings
    settings TEXT                            -- JSON: catalog-level configuration
);

INSERT INTO catalog_meta (id, schema_version, version) VALUES (1, '2.0', 0);

-- ============================================================================
-- VIEWS (Convenience)
-- ============================================================================

-- Tables with quality status
CREATE VIEW tables_with_quality AS
SELECT 
    t.*,
    qs.overall_score,
    qs.status as quality_status,
    qs.last_checked_at
FROM tables t
LEFT JOIN quality_scores qs ON t.id = qs.table_id;

-- Tables with usage stats
CREATE VIEW tables_with_usage AS
SELECT 
    t.*,
    us.query_count_30d,
    us.unique_users_30d,
    us.last_accessed_at
FROM tables t
LEFT JOIN usage_summary us ON t.id = us.table_id;

-- Lineage with table names (for API responses)
CREATE VIEW lineage_expanded AS
SELECT 
    l.id,
    l.upstream_table_id,
    ut.name as upstream_name,
    l.downstream_table_id,
    dt.name as downstream_name,
    l.job_name,
    l.transformation_type,
    l.created_at
FROM lineage l
JOIN tables ut ON l.upstream_table_id = ut.id
JOIN tables dt ON l.downstream_table_id = dt.id;
```

### 3.2 Schema Summary

| Category | Tables | Purpose |
|----------|--------|---------|
| **Core Registry** | `tables` | Name → Location mapping, business context |
| **Categorization** | `tags`, `domains` | Flexible classification |
| **Lineage** | `lineage` | Cross-table dependencies |
| **Business Context** | `glossary_terms`, `glossary_links` | Business vocabulary |
| **Quality** | `quality_checks`, `quality_results`, `quality_scores` | Quality framework |
| **Freshness** | `freshness_violations` | SLA tracking |
| **Usage** | `usage_log`, `usage_summary` | Access analytics |
| **Governance** | `classification_rules`, `column_classifications` | PII/sensitive data |
| **Audit** | `audit_log` | Compliance trail |
| **Search** | `search_index` | Full-text search |
| **System** | `catalog_meta` | Version control |

**Total: 17 tables** (vs. original ~20, but more focused)

---

## 4. Feature Matrix

### 4.1 Complete Feature List

```
┌─────────────────────────────────────────────────────────────────┐
│                    FEATURE MATRIX                                │
└─────────────────────────────────────────────────────────────────┘

FEATURE                          SOURCE              STATUS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DISCOVERY
├── Table search (FTS)           Catalog             ✓ MVP
├── Column search                Catalog + Delta     ✓ MVP
├── Browse by domain             Catalog             ✓ MVP
├── Browse by owner              Catalog             ✓ MVP
├── Browse by tag                Catalog             ✓ MVP
├── Popularity ranking           Computed (usage)    ✓ Phase 2
└── Recommendations              Computed            ○ Phase 3

TECHNICAL METADATA
├── Schema                       Delta               ✓ MVP
├── Row count                    Delta               ✓ MVP
├── Column statistics            Delta               ✓ MVP
├── Partition info               Delta               ✓ MVP
├── File count/size              Delta               ✓ MVP
├── Table history                Delta               ✓ MVP
├── Schema evolution             Delta               ✓ Phase 2
└── Time travel                  Delta               ✓ Phase 2

BUSINESS METADATA
├── Ownership                    Catalog             ✓ MVP
├── Domain assignment            Catalog             ✓ MVP
├── Description                  Catalog             ✓ MVP
├── Tags                         Catalog             ✓ MVP
├── Glossary terms               Catalog             ✓ Phase 2
├── Term-to-column links         Catalog             ✓ Phase 2
└── Certification status         Catalog             ✓ Phase 2

LINEAGE
├── Record lineage               Catalog             ✓ MVP
├── Upstream query               Catalog             ✓ MVP
├── Downstream query             Catalog             ✓ MVP
├── Impact analysis              Computed            ✓ Phase 2
└── Lineage visualization        Computed            ✓ Phase 2

QUALITY
├── Quality score (overall)      Computed (Delta)    ✓ MVP
├── Completeness score           Computed (Delta)    ✓ MVP
├── Freshness score              Computed (Delta)    ✓ MVP
├── Quality check definitions    Catalog             ✓ Phase 2
├── Quality check execution      Computed            ✓ Phase 2
├── Quality history              Catalog             ✓ Phase 2
└── Quality alerts               Computed            ○ Phase 3

FRESHNESS
├── SLA configuration            Catalog             ✓ Phase 2
├── Freshness status             Computed (Delta)    ✓ MVP
├── Violation tracking           Catalog             ✓ Phase 2
└── Freshness alerts             Computed            ○ Phase 3

GOVERNANCE
├── Classification rules         Catalog             ✓ Phase 2
├── PII detection                Computed            ✓ Phase 2
├── Column masking config        Catalog             ○ Phase 3
├── Retention policies           Catalog             ○ Phase 3
└── Access policies              Catalog             ○ Future

ANALYTICS
├── Usage logging                Catalog             ✓ Phase 2
├── Usage aggregation            Catalog             ✓ Phase 2
├── Popular tables               Computed            ✓ Phase 2
└── User activity                Catalog             ○ Phase 3

AUDIT
├── Change logging               Catalog             ✓ Phase 2
├── Audit trail query            Catalog             ✓ Phase 2
└── Compliance reports           Computed            ○ Phase 3

MULTI-TENANT
├── Tenant isolation             Per-file            ✓ MVP
├── Tenant registry              Platform            ✓ MVP
├── Quota enforcement            Platform            ✓ Phase 2
├── Usage metering               Platform            ✓ Phase 2
└── Cost attribution             Platform            ○ Phase 3

Legend: ✓ Included  ○ Planned  
```

---

## 5. API Specification

### 5.1 Complete API Design

```yaml
# ============================================================================
# LAKEHOUSE CATALOG API - COMPLETE SPECIFICATION
# Version: 2.0
# Base URL: /api/v1
# ============================================================================

# ============================================================================
# TABLE REGISTRY
# ============================================================================

# Register a new table
POST /tables
Request:
  name: string (required)          # "sales.orders"
  location: string (required)      # "gs://bucket/sales/orders/"
  format: string                   # "delta" (default), "iceberg", "parquet"
  domain: string
  owner: string
  description: string
  tags: string[]
  classification: string           # "public", "internal", "confidential", "pii"
  freshness_sla: string            # "hourly", "daily", "weekly"
Response:
  TableResponse (merged catalog + Delta)

# List all tables
GET /tables
Query:
  domain: string
  owner: string
  tag: string
  classification: string
  format: string
  limit: integer (default: 100)
  offset: integer
Response:
  tables: TableSummary[]
  total: integer

# Get single table (merged response)
GET /tables/{name}
Response:
  # From Catalog
  id: string
  name: string
  location: string
  format: string
  domain: string
  owner: string
  description: string
  tags: string[]
  classification: string
  freshness_sla: string
  is_certified: boolean
  registered_at: datetime
  
  # From Delta (live)
  schema: Schema
  row_count: integer
  size_bytes: integer
  num_files: integer
  partition_columns: string[]
  last_modified: datetime
  delta_version: integer
  
  # Computed
  freshness_status: "fresh" | "stale" | "unknown"
  quality_score: float
  quality_status: "healthy" | "warning" | "critical"
  
  # Summaries
  lineage_summary:
    upstream_count: integer
    downstream_count: integer
  usage_summary:
    query_count_30d: integer
    last_accessed_at: datetime

# Update table metadata
PATCH /tables/{name}
Request:
  domain: string
  owner: string
  description: string
  classification: string
  freshness_sla: string
Response:
  TableResponse

# Delete table registration
DELETE /tables/{name}
Response:
  204 No Content

# Refresh table (re-read Delta metadata, update search index)
POST /tables/{name}/refresh
Response:
  TableResponse

# ============================================================================
# DELTA-DELEGATED ENDPOINTS (Read from Delta on demand)
# ============================================================================

# Get schema (from Delta)
GET /tables/{name}/schema
Query:
  version: integer               # Optional: specific Delta version
Response:
  fields: Field[]
  partition_columns: string[]
  delta_version: integer
  schema_string: string          # DDL format

# Get column statistics (from Delta)
GET /tables/{name}/columns
Response:
  columns: ColumnStats[]
    - name: string
    - type: string
    - nullable: boolean
    - null_count: integer
    - distinct_count: integer    # If available
    - min_value: any
    - max_value: any
    - classification: string     # From catalog

# Get table statistics (from Delta)
GET /tables/{name}/stats
Response:
  row_count: integer
  size_bytes: integer
  num_files: integer
  avg_file_size_bytes: integer
  partition_count: integer
  delta_version: integer
  last_modified: datetime

# Get table history (from Delta transaction log)
GET /tables/{name}/history
Query:
  limit: integer (default: 20)
Response:
  versions: DeltaVersion[]
    - version: integer
    - timestamp: datetime
    - operation: string
    - user_name: string
    - parameters: object
    - metrics: object

# Get schema diff between versions (computed from Delta)
GET /tables/{name}/schema/diff
Query:
  from_version: integer (required)
  to_version: integer (required)
Response:
  from_version: integer
  to_version: integer
  added_columns: Field[]
  removed_columns: Field[]
  modified_columns: FieldChange[]

# ============================================================================
# LINEAGE
# ============================================================================

# Record lineage
POST /lineage
Request:
  upstream_tables: string[]      # ["bronze.raw_orders", "bronze.raw_customers"]
  downstream_table: string       # "silver.enriched_orders"
  job_name: string
  job_run_id: string
  transformation_type: string    # "datafusion", "sql", "dbt"
  transformation_sql: string     # Optional
Response:
  201 Created
  lineage_edges: LineageEdge[]

# Get upstream lineage
GET /tables/{name}/lineage/upstream
Query:
  depth: integer (default: 3, max: 10)
Response:
  root: string
  nodes: LineageNode[]
  edges: LineageEdge[]

# Get downstream lineage
GET /tables/{name}/lineage/downstream
Query:
  depth: integer (default: 3, max: 10)
Response:
  root: string
  nodes: LineageNode[]
  edges: LineageEdge[]

# Get full lineage graph
GET /tables/{name}/lineage/graph
Query:
  upstream_depth: integer (default: 2)
  downstream_depth: integer (default: 2)
Response:
  root: string
  nodes: LineageNode[]
  edges: LineageEdge[]

# Impact analysis (what breaks if this table changes)
GET /tables/{name}/impact
Response:
  direct_dependents: TableSummary[]
  indirect_dependents: TableSummary[]
  total_impact_count: integer

# ============================================================================
# SEARCH & DISCOVERY
# ============================================================================

# Full-text search
GET /search
Query:
  q: string (required)           # Search query
  domain: string                 # Filter by domain
  owner: string                  # Filter by owner
  tag: string                    # Filter by tag
  classification: string         # Filter by classification
  format: string                 # Filter by format
  sort: string                   # "relevance", "popularity", "recent"
  limit: integer (default: 20)
Response:
  results: SearchResult[]
    - table_id: string
    - name: string
    - description: string
    - domain: string
    - owner: string
    - relevance_score: float
    - popularity_score: float
    - matched_fields: string[]
  total: integer
  facets:
    domains: FacetCount[]
    owners: FacetCount[]
    tags: FacetCount[]

# Search columns across all tables
GET /search/columns
Query:
  q: string (required)
  type: string                   # Filter by data type
  limit: integer (default: 20)
Response:
  results: ColumnSearchResult[]
    - table_name: string
    - column_name: string
    - type: string
    - description: string

# ============================================================================
# TAGS
# ============================================================================

# Add tags to table
POST /tables/{name}/tags
Request:
  tags: string[]
Response:
  tags: Tag[]

# Remove tag from table
DELETE /tables/{name}/tags/{tag}
Response:
  204 No Content

# List all tags (for autocomplete)
GET /tags
Query:
  prefix: string
  limit: integer
Response:
  tags: TagWithCount[]

# ============================================================================
# GLOSSARY
# ============================================================================

# Create glossary term
POST /glossary
Request:
  term: string (required)
  definition: string (required)
  domain: string
  owner: string
  synonyms: string[]
Response:
  GlossaryTerm

# List glossary terms
GET /glossary
Query:
  domain: string
  q: string                      # Search terms
  limit: integer
Response:
  terms: GlossaryTerm[]
  total: integer

# Get glossary term
GET /glossary/{term}
Response:
  GlossaryTerm
  linked_tables: TableSummary[]
  linked_columns: ColumnReference[]

# Update glossary term
PATCH /glossary/{term}
Request:
  definition: string
  domain: string
  owner: string
  synonyms: string[]
Response:
  GlossaryTerm

# Delete glossary term
DELETE /glossary/{term}
Response:
  204 No Content

# Link term to table/column
POST /glossary/{term}/links
Request:
  table_name: string (required)
  column_name: string            # Optional
  link_type: string              # "defines", "relates_to"
Response:
  GlossaryLink

# ============================================================================
# QUALITY
# ============================================================================

# Get quality summary (computed from Delta)
GET /tables/{name}/quality
Response:
  overall_score: float           # 0.0 - 1.0
  status: "healthy" | "warning" | "critical"
  dimensions:
    completeness:
      score: float
      details: object            # null counts per column
    freshness:
      score: float
      last_updated: datetime
      sla: string
      hours_since_update: float
    file_health:
      score: float
      small_file_count: integer
      avg_file_size_mb: float
  last_checked_at: datetime

# Create quality check definition
POST /tables/{name}/quality/checks
Request:
  check_type: string             # "completeness", "validity", "custom"
  check_name: string
  check_config: object
  severity: string
  warn_threshold: float
  fail_threshold: float
Response:
  QualityCheck

# List quality check definitions
GET /tables/{name}/quality/checks
Response:
  checks: QualityCheck[]

# Run quality checks
POST /tables/{name}/quality/run
Request:
  check_ids: string[]            # Optional: specific checks, or all
Response:
  results: QualityResult[]

# Get quality history
GET /tables/{name}/quality/history
Query:
  days: integer (default: 30)
Response:
  history: QualityResult[]
  trend: QualityTrend

# ============================================================================
# FRESHNESS
# ============================================================================

# Get freshness status (computed from Delta)
GET /tables/{name}/freshness
Response:
  status: "fresh" | "stale" | "unknown"
  last_updated: datetime
  sla: string
  expected_by: datetime
  hours_overdue: float           # If stale

# Configure freshness SLA
PUT /tables/{name}/freshness
Request:
  sla: string                    # "hourly", "daily", "weekly"
  grace_minutes: integer
Response:
  FreshnessConfig

# Get freshness violations
GET /tables/{name}/freshness/violations
Query:
  days: integer (default: 30)
Response:
  violations: FreshnessViolation[]
  total_violations: integer
  mttr_hours: float              # Mean time to resolution

# ============================================================================
# GOVERNANCE & CLASSIFICATION
# ============================================================================

# Get column classifications
GET /tables/{name}/classifications
Response:
  columns: ColumnClassification[]
    - column_name: string
    - classification: string
    - confidence: string
    - masking_policy: string
    - rule_name: string          # If auto-detected

# Set column classification
PUT /tables/{name}/columns/{column}/classification
Request:
  classification: string
  masking_policy: string
Response:
  ColumnClassification

# Auto-detect classifications (run rules against table)
POST /tables/{name}/classifications/detect
Response:
  detected: ColumnClassification[]
  applied: integer

# List classification rules
GET /classifications/rules
Response:
  rules: ClassificationRule[]

# Create classification rule
POST /classifications/rules
Request:
  name: string
  column_pattern: string         # Regex
  data_pattern: string           # Optional regex for values
  classification: string
  auto_apply: boolean
Response:
  ClassificationRule

# ============================================================================
# USAGE ANALYTICS
# ============================================================================

# Get usage statistics
GET /tables/{name}/usage
Query:
  days: integer (default: 30)
Response:
  total_queries: integer
  unique_users: integer
  last_accessed_at: datetime
  daily_breakdown: DailyUsage[]
  top_users: UserUsage[]

# Get popular tables
GET /tables/popular
Query:
  days: integer (default: 7)
  limit: integer (default: 10)
Response:
  tables: TableWithUsage[]

# ============================================================================
# AUDIT
# ============================================================================

# Get audit log for table
GET /tables/{name}/audit
Query:
  days: integer (default: 30)
  action: string                 # Filter by action
Response:
  entries: AuditEntry[]

# Get global audit log
GET /audit
Query:
  resource_type: string
  action: string
  user_id: string
  from: datetime
  to: datetime
  limit: integer
Response:
  entries: AuditEntry[]
  total: integer

# ============================================================================
# ADMIN
# ============================================================================

# Health check
GET /health
Response:
  status: "healthy" | "degraded" | "unhealthy"
  components:
    catalog: "ok" | "error"
    delta: "ok" | "error"
    cache: "ok" | "error"

# Rebuild search index
POST /admin/reindex
Response:
  tables_indexed: integer

# Export catalog
GET /admin/export
Query:
  format: "json" | "csv"
Response:
  file download

# Import catalog
POST /admin/import
Request:
  multipart file upload
Response:
  imported: integer
  errors: string[]

# Get catalog statistics
GET /admin/stats
Response:
  total_tables: integer
  total_lineage_edges: integer
  total_glossary_terms: integer
  schema_version: string
  last_modified: datetime
```

### 5.2 Response Types

```typescript
// Core Types
interface TableResponse {
  // Catalog (stored)
  id: string;
  name: string;
  location: string;
  format: "delta" | "iceberg" | "parquet";
  domain?: string;
  owner?: string;
  description?: string;
  tags: string[];
  classification?: string;
  freshness_sla?: string;
  is_certified: boolean;
  registered_at: string;
  
  // Delta (live)
  schema: Schema;
  row_count: number;
  size_bytes: number;
  num_files: number;
  partition_columns: string[];
  last_modified: string;
  delta_version: number;
  
  // Computed
  freshness_status: "fresh" | "stale" | "unknown";
  quality_score?: number;
  quality_status?: "healthy" | "warning" | "critical";
  
  // Summaries
  lineage_summary: {
    upstream_count: number;
    downstream_count: number;
  };
  usage_summary: {
    query_count_30d: number;
    last_accessed_at?: string;
  };
}

interface Schema {
  fields: Field[];
  partition_columns: string[];
}

interface Field {
  name: string;
  type: string;
  nullable: boolean;
  description?: string;
  metadata?: Record<string, string>;
}

interface LineageNode {
  id: string;
  name: string;
  domain?: string;
  format: string;
  depth: number;
}

interface LineageEdge {
  id: string;
  upstream_id: string;
  downstream_id: string;
  job_name?: string;
  transformation_type?: string;
  created_at: string;
}

interface QualityScore {
  overall_score: number;
  status: "healthy" | "warning" | "critical";
  dimensions: {
    completeness: { score: number; details: any };
    freshness: { score: number; last_updated: string };
    file_health: { score: number; small_file_count: number };
  };
  last_checked_at: string;
}

interface SearchResult {
  table_id: string;
  name: string;
  description?: string;
  domain?: string;
  owner?: string;
  relevance_score: number;
  popularity_score: number;
  matched_fields: string[];
}
```

---

## 6. Implementation Architecture

### 6.1 Crate Structure

```
lakehouse-catalog/
├── Cargo.toml
├── crates/
│   │
│   ├── catalog-core/                    # Core types and database operations
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── models/
│   │       │   ├── mod.rs
│   │       │   ├── table.rs             # TableEntry, TableResponse
│   │       │   ├── lineage.rs           # LineageEdge, LineageGraph
│   │       │   ├── quality.rs           # QualityCheck, QualityResult
│   │       │   ├── glossary.rs          # GlossaryTerm, GlossaryLink
│   │       │   ├── governance.rs        # Classification, AuditEntry
│   │       │   └── usage.rs             # UsageLog, UsageSummary
│   │       ├── db/
│   │       │   ├── mod.rs
│   │       │   ├── schema.sql           # Complete DDL
│   │       │   ├── migrations/          # Version upgrades
│   │       │   ├── tables.rs            # Table CRUD
│   │       │   ├── lineage.rs           # Lineage queries
│   │       │   ├── quality.rs           # Quality operations
│   │       │   ├── glossary.rs          # Glossary operations
│   │       │   ├── search.rs            # FTS operations
│   │       │   └── audit.rs             # Audit logging
│   │       └── error.rs
│   │
│   ├── catalog-delta/                   # Delta Lake integration
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── reader.rs                # Open and read Delta tables
│   │       ├── schema.rs                # Schema extraction
│   │       ├── stats.rs                 # Statistics aggregation
│   │       ├── history.rs               # Transaction log reading
│   │       ├── quality.rs               # Quality computation from stats
│   │       └── cache.rs                 # Delta metadata caching
│   │
│   ├── catalog-storage/                 # Storage backends
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── traits.rs                # StorageBackend trait
│   │       ├── gcs.rs                   # Google Cloud Storage
│   │       ├── s3.rs                    # AWS S3
│   │       ├── local.rs                 # Local filesystem
│   │       └── locking.rs               # Optimistic concurrency
│   │
│   ├── catalog-api/                     # REST API server
│   │   ├── Cargo.toml
│   │   ├── Dockerfile
│   │   └── src/
│   │       ├── main.rs
│   │       ├── config.rs
│   │       ├── routes/
│   │       │   ├── mod.rs
│   │       │   ├── tables.rs            # Table endpoints
│   │       │   ├── delta.rs             # Delta-delegated endpoints
│   │       │   ├── lineage.rs           # Lineage endpoints
│   │       │   ├── search.rs            # Search endpoints
│   │       │   ├── quality.rs           # Quality endpoints
│   │       │   ├── glossary.rs          # Glossary endpoints
│   │       │   ├── governance.rs        # Governance endpoints
│   │       │   ├── usage.rs             # Usage endpoints
│   │       │   ├── audit.rs             # Audit endpoints
│   │       │   └── admin.rs             # Admin endpoints
│   │       ├── middleware/
│   │       │   ├── mod.rs
│   │       │   ├── auth.rs              # Authentication
│   │       │   ├── tenant.rs            # Tenant context
│   │       │   ├── audit.rs             # Automatic audit logging
│   │       │   └── metrics.rs           # Request metrics
│   │       ├── handlers/
│   │       │   ├── mod.rs
│   │       │   ├── merged.rs            # Combine catalog + Delta
│   │       │   └── computed.rs          # Compute quality, freshness
│   │       └── openapi.rs               # OpenAPI spec generation
│   │
│   ├── catalog-client/                  # Client library
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── client.rs                # HTTP client
│   │       ├── datafusion.rs            # DataFusion integration
│   │       └── servo.rs                 # Servo orchestration integration
│   │
│   ├── catalog-emitter/                 # Pipeline integration
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── emitter.rs               # Emit metadata from pipelines
│   │       ├── lineage.rs               # Auto-detect lineage
│   │       └── macros.rs                # Convenience macros
│   │
│   └── catalog-cli/                     # CLI tool
│       ├── Cargo.toml
│       └── src/
│           ├── main.rs
│           ├── commands/
│           │   ├── mod.rs
│           │   ├── tables.rs
│           │   ├── search.rs
│           │   ├── lineage.rs
│           │   ├── quality.rs
│           │   └── admin.rs
│           └── output.rs                # Formatting (table, json, etc.)
│
├── ui/                                  # Web UI (Phase 3)
│   ├── package.json
│   └── src/
│       ├── pages/
│       ├── components/
│       └── ...
│
├── examples/
│   ├── register_table.rs
│   ├── datafusion_query.rs
│   ├── record_lineage.rs
│   ├── quality_check.rs
│   └── servo_pipeline.rs
│
├── deploy/
│   ├── cloud-run/
│   ├── terraform/
│   └── docker-compose.yml
│
└── docs/
    ├── getting-started.md
    ├── api-reference.md
    ├── architecture.md
    └── migration-guide.md
```

### 6.2 Core Catalog Implementation

```rust
// crates/catalog-core/src/lib.rs

pub mod models;
pub mod db;
pub mod error;

use crate::db::Database;
use crate::models::*;

/// Main catalog interface
pub struct Catalog {
    db: Database,
    delta: DeltaReader,
    cache: Cache,
}

impl Catalog {
    // ========================================================================
    // TABLE REGISTRY (Stored in Catalog)
    // ========================================================================
    
    /// Register a new table
    pub async fn register_table(
        &self,
        request: RegisterTableRequest,
    ) -> Result<TableResponse> {
        // 1. Validate Delta table exists and is readable
        let delta_meta = self.delta.get_metadata(&request.location).await?;
        
        // 2. Create registry entry
        let table_id = Uuid::new_v4().to_string();
        let entry = TableEntry {
            id: table_id.clone(),
            name: request.name.clone(),
            location: request.location.clone(),
            format: request.format.unwrap_or(TableFormat::Delta),
            domain: request.domain,
            owner: request.owner,
            description: request.description,
            classification: request.classification,
            freshness_sla: request.freshness_sla,
            registered_at: Utc::now(),
            updated_at: Utc::now(),
            ..Default::default()
        };
        
        self.db.insert_table(&entry).await?;
        
        // 3. Insert tags
        if let Some(tags) = request.tags {
            self.db.insert_tags(&table_id, &tags).await?;
        }
        
        // 4. Update search index with column names from Delta
        let column_names = delta_meta.schema.fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        
        self.db.update_search_index(&table_id, &entry, &column_names).await?;
        
        // 5. Log audit entry
        self.db.log_audit(AuditEntry {
            resource_type: "table".to_string(),
            resource_id: table_id.clone(),
            action: "create".to_string(),
            changes: None,
            user_id: None, // From context
            timestamp: Utc::now(),
        }).await?;
        
        // 6. Return merged response
        self.get_table(&request.name).await
    }
    
    /// Get table with merged catalog + Delta metadata
    pub async fn get_table(&self, name: &str) -> Result<TableResponse> {
        // 1. Get catalog entry
        let entry = self.db.get_table_by_name(name).await?;
        
        // 2. Get Delta metadata (with caching)
        let delta_meta = self.delta.get_metadata_cached(&entry.location).await?;
        
        // 3. Get tags
        let tags = self.db.get_tags(&entry.id).await?;
        
        // 4. Compute quality score
        let quality = self.compute_quality(&entry, &delta_meta).await?;
        
        // 5. Compute freshness status
        let freshness = self.compute_freshness(&entry, &delta_meta)?;
        
        // 6. Get summaries
        let lineage_summary = self.db.get_lineage_summary(&entry.id).await?;
        let usage_summary = self.db.get_usage_summary(&entry.id).await?;
        
        // 7. Build merged response
        Ok(TableResponse {
            // Catalog
            id: entry.id,
            name: entry.name,
            location: entry.location,
            format: entry.format,
            domain: entry.domain,
            owner: entry.owner,
            description: entry.description,
            tags: tags.into_iter().map(|t| t.tag).collect(),
            classification: entry.classification,
            freshness_sla: entry.freshness_sla,
            is_certified: entry.is_certified,
            registered_at: entry.registered_at,
            
            // Delta
            schema: delta_meta.schema,
            row_count: delta_meta.row_count,
            size_bytes: delta_meta.size_bytes,
            num_files: delta_meta.num_files,
            partition_columns: delta_meta.partition_columns,
            last_modified: delta_meta.last_modified,
            delta_version: delta_meta.version,
            
            // Computed
            freshness_status: freshness.status,
            quality_score: quality.overall_score,
            quality_status: quality.status,
            
            // Summaries
            lineage_summary,
            usage_summary,
        })
    }
    
    /// Update table metadata
    pub async fn update_table(
        &self,
        name: &str,
        update: UpdateTableRequest,
    ) -> Result<TableResponse> {
        let entry = self.db.get_table_by_name(name).await?;
        
        // Build changes for audit
        let mut changes = serde_json::Map::new();
        
        if let Some(domain) = &update.domain {
            if entry.domain.as_ref() != Some(domain) {
                changes.insert("domain".to_string(), json!({
                    "old": entry.domain,
                    "new": domain
                }));
            }
        }
        // ... similar for other fields
        
        // Update database
        self.db.update_table(&entry.id, &update).await?;
        
        // Log audit
        if !changes.is_empty() {
            self.db.log_audit(AuditEntry {
                resource_type: "table".to_string(),
                resource_id: entry.id.clone(),
                action: "update".to_string(),
                changes: Some(serde_json::Value::Object(changes)),
                user_id: None,
                timestamp: Utc::now(),
            }).await?;
        }
        
        self.get_table(name).await
    }
    
    // ========================================================================
    // DELTA-DELEGATED OPERATIONS
    // ========================================================================
    
    /// Get schema directly from Delta
    pub async fn get_schema(&self, name: &str, version: Option<i64>) -> Result<Schema> {
        let entry = self.db.get_table_by_name(name).await?;
        self.delta.get_schema(&entry.location, version).await
    }
    
    /// Get column statistics from Delta
    pub async fn get_column_stats(&self, name: &str) -> Result<Vec<ColumnStats>> {
        let entry = self.db.get_table_by_name(name).await?;
        let stats = self.delta.get_column_stats(&entry.location).await?;
        
        // Enrich with classifications from catalog
        let classifications = self.db.get_column_classifications(&entry.id).await?;
        
        Ok(stats.into_iter().map(|mut s| {
            if let Some(c) = classifications.iter().find(|c| c.column_name == s.name) {
                s.classification = Some(c.classification.clone());
            }
            s
        }).collect())
    }
    
    /// Get table history from Delta transaction log
    pub async fn get_history(&self, name: &str, limit: usize) -> Result<Vec<DeltaVersion>> {
        let entry = self.db.get_table_by_name(name).await?;
        self.delta.get_history(&entry.location, limit).await
    }
    
    /// Get schema diff between versions
    pub async fn get_schema_diff(
        &self,
        name: &str,
        from_version: i64,
        to_version: i64,
    ) -> Result<SchemaDiff> {
        let entry = self.db.get_table_by_name(name).await?;
        self.delta.diff_schemas(&entry.location, from_version, to_version).await
    }
    
    // ========================================================================
    // LINEAGE (Stored in Catalog)
    // ========================================================================
    
    /// Record lineage from pipeline
    pub async fn record_lineage(&self, request: RecordLineageRequest) -> Result<Vec<LineageEdge>> {
        let downstream = self.db.get_table_by_name(&request.downstream_table).await?;
        let mut edges = Vec::new();
        
        for upstream_name in &request.upstream_tables {
            if let Ok(upstream) = self.db.get_table_by_name(upstream_name).await {
                let edge = LineageEdge {
                    id: Uuid::new_v4().to_string(),
                    upstream_table_id: upstream.id.clone(),
                    downstream_table_id: downstream.id.clone(),
                    job_name: request.job_name.clone(),
                    job_run_id: request.job_run_id.clone(),
                    transformation_type: request.transformation_type.clone(),
                    transformation_sql: request.transformation_sql.clone(),
                    created_at: Utc::now(),
                    last_seen_at: Utc::now(),
                };
                
                self.db.upsert_lineage(&edge).await?;
                edges.push(edge);
            }
        }
        
        // Audit log
        self.db.log_audit(AuditEntry {
            resource_type: "lineage".to_string(),
            resource_id: downstream.id,
            action: "create".to_string(),
            changes: Some(json!({
                "upstream_tables": request.upstream_tables,
                "job_name": request.job_name
            })),
            user_id: None,
            timestamp: Utc::now(),
        }).await?;
        
        Ok(edges)
    }
    
    /// Get upstream lineage
    pub async fn get_upstream_lineage(
        &self,
        name: &str,
        depth: i32,
    ) -> Result<LineageGraph> {
        let entry = self.db.get_table_by_name(name).await?;
        self.db.get_upstream_lineage(&entry.id, depth).await
    }
    
    /// Get downstream lineage (impact analysis)
    pub async fn get_downstream_lineage(
        &self,
        name: &str,
        depth: i32,
    ) -> Result<LineageGraph> {
        let entry = self.db.get_table_by_name(name).await?;
        self.db.get_downstream_lineage(&entry.id, depth).await
    }
    
    // ========================================================================
    // QUALITY (Computed from Delta)
    // ========================================================================
    
    /// Compute quality score from Delta statistics
    async fn compute_quality(
        &self,
        entry: &TableEntry,
        delta_meta: &DeltaMetadata,
    ) -> Result<QualityScore> {
        // Completeness: from null counts
        let completeness = self.compute_completeness(delta_meta)?;
        
        // Freshness: compare last_modified to SLA
        let freshness = self.compute_freshness(entry, delta_meta)?;
        
        // File health: small file detection
        let file_health = self.compute_file_health(delta_meta)?;
        
        // Overall score (weighted)
        let overall = 
            completeness.score * 0.4 +
            freshness.score * 0.4 +
            file_health.score * 0.2;
        
        let status = if overall >= 0.9 {
            "healthy"
        } else if overall >= 0.7 {
            "warning"
        } else {
            "critical"
        };
        
        Ok(QualityScore {
            overall_score: Some(overall),
            status: status.to_string(),
            dimensions: QualityDimensions {
                completeness,
                freshness,
                file_health,
            },
            last_checked_at: Utc::now(),
        })
    }
    
    fn compute_completeness(&self, delta_meta: &DeltaMetadata) -> Result<DimensionScore> {
        let total_cells = delta_meta.row_count * delta_meta.schema.fields.len() as i64;
        
        let total_nulls: i64 = delta_meta.column_stats
            .iter()
            .map(|s| s.null_count.unwrap_or(0))
            .sum();
        
        let score = if total_cells > 0 {
            1.0 - (total_nulls as f64 / total_cells as f64)
        } else {
            1.0
        };
        
        Ok(DimensionScore {
            score,
            details: json!({
                "total_cells": total_cells,
                "null_cells": total_nulls,
                "null_percentage": (total_nulls as f64 / total_cells.max(1) as f64) * 100.0
            }),
        })
    }
    
    fn compute_freshness(
        &self,
        entry: &TableEntry,
        delta_meta: &DeltaMetadata,
    ) -> Result<FreshnessResult> {
        let now = Utc::now();
        let hours_since_update = (now - delta_meta.last_modified).num_hours();
        
        let (status, score) = match &entry.freshness_sla {
            Some(sla) => {
                let expected_hours = match sla.as_str() {
                    "hourly" => 1,
                    "daily" => 24,
                    "weekly" => 168,
                    _ => 24,
                };
                let grace_hours = entry.freshness_grace_minutes.unwrap_or(60) / 60;
                
                if hours_since_update <= expected_hours + grace_hours {
                    (FreshnessStatus::Fresh, 1.0)
                } else {
                    let overdue_ratio = hours_since_update as f64 / expected_hours as f64;
                    let score = (1.0 / overdue_ratio).min(1.0).max(0.0);
                    (FreshnessStatus::Stale, score)
                }
            }
            None => (FreshnessStatus::Unknown, 1.0),
        };
        
        Ok(FreshnessResult {
            status,
            score,
            last_updated: delta_meta.last_modified,
            hours_since_update,
            sla: entry.freshness_sla.clone(),
        })
    }
    
    fn compute_file_health(&self, delta_meta: &DeltaMetadata) -> Result<DimensionScore> {
        let small_file_threshold = 128 * 1024 * 1024; // 128 MB
        let small_files = delta_meta.files.iter()
            .filter(|f| f.size < small_file_threshold)
            .count();
        
        let small_file_ratio = small_files as f64 / delta_meta.num_files.max(1) as f64;
        let score = 1.0 - small_file_ratio;
        
        Ok(DimensionScore {
            score,
            details: json!({
                "small_file_count": small_files,
                "total_files": delta_meta.num_files,
                "avg_file_size_mb": delta_meta.size_bytes as f64 / delta_meta.num_files.max(1) as f64 / 1024.0 / 1024.0
            }),
        })
    }
    
    // ========================================================================
    // SEARCH (Catalog Index)
    // ========================================================================
    
    /// Full-text search
    pub async fn search(
        &self,
        query: &str,
        filters: SearchFilters,
    ) -> Result<SearchResponse> {
        // FTS search
        let mut results = self.db.fts_search(query, &filters).await?;
        
        // Enrich with popularity
        for result in &mut results {
            if let Ok(usage) = self.db.get_usage_summary(&result.table_id).await {
                result.popularity_score = usage.query_count_30d as f64;
            }
        }
        
        // Sort by combined relevance + popularity
        results.sort_by(|a, b| {
            let score_a = a.relevance_score * 0.6 + (a.popularity_score / 1000.0) * 0.4;
            let score_b = b.relevance_score * 0.6 + (b.popularity_score / 1000.0) * 0.4;
            score_b.partial_cmp(&score_a).unwrap()
        });
        
        Ok(SearchResponse {
            results,
            total: results.len(),
            facets: self.db.get_search_facets(query).await?,
        })
    }
    
    // ========================================================================
    // GLOSSARY (Stored in Catalog)
    // ========================================================================
    
    /// Create glossary term
    pub async fn create_glossary_term(&self, request: CreateTermRequest) -> Result<GlossaryTerm> {
        let term = GlossaryTerm {
            id: Uuid::new_v4().to_string(),
            term: request.term,
            definition: request.definition,
            domain: request.domain,
            owner: request.owner,
            synonyms: request.synonyms,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        
        self.db.insert_glossary_term(&term).await?;
        
        self.db.log_audit(AuditEntry {
            resource_type: "glossary".to_string(),
            resource_id: term.id.clone(),
            action: "create".to_string(),
            changes: None,
            user_id: None,
            timestamp: Utc::now(),
        }).await?;
        
        Ok(term)
    }
    
    /// Link glossary term to table/column
    pub async fn link_glossary_term(
        &self,
        term: &str,
        request: LinkTermRequest,
    ) -> Result<GlossaryLink> {
        let term_entry = self.db.get_glossary_term(term).await?;
        let table = self.db.get_table_by_name(&request.table_name).await?;
        
        let link = GlossaryLink {
            id: Uuid::new_v4().to_string(),
            term_id: term_entry.id,
            table_id: Some(table.id),
            column_name: request.column_name,
            link_type: request.link_type.unwrap_or("defines".to_string()),
            created_at: Utc::now(),
        };
        
        self.db.insert_glossary_link(&link).await?;
        
        Ok(link)
    }
    
    // ========================================================================
    // USAGE ANALYTICS (Stored in Catalog)
    // ========================================================================
    
    /// Log table access
    pub async fn log_usage(
        &self,
        table_id: &str,
        user_id: Option<&str>,
        access_type: &str,
    ) -> Result<()> {
        let log = UsageLog {
            id: Uuid::new_v4().to_string(),
            table_id: table_id.to_string(),
            user_id: user_id.map(|s| s.to_string()),
            access_type: access_type.to_string(),
            access_method: "api".to_string(),
            accessed_at: Utc::now(),
        };
        
        self.db.insert_usage_log(&log).await
    }
    
    /// Update usage aggregates (call periodically)
    pub async fn aggregate_usage(&self) -> Result<()> {
        self.db.aggregate_usage_stats().await
    }
    
    // ========================================================================
    // GOVERNANCE (Stored in Catalog)
    // ========================================================================
    
    /// Auto-detect classifications for a table
    pub async fn detect_classifications(&self, name: &str) -> Result<Vec<ColumnClassification>> {
        let entry = self.db.get_table_by_name(name).await?;
        let rules = self.db.get_classification_rules().await?;
        let schema = self.get_schema(name, None).await?;
        
        let mut classifications = Vec::new();
        
        for field in &schema.fields {
            for rule in &rules {
                if let Some(pattern) = &rule.column_pattern {
                    let regex = Regex::new(pattern)?;
                    if regex.is_match(&field.name) {
                        classifications.push(ColumnClassification {
                            id: Uuid::new_v4().to_string(),
                            table_id: entry.id.clone(),
                            column_name: field.name.clone(),
                            classification: rule.classification.clone(),
                            rule_id: Some(rule.id.clone()),
                            confidence: rule.confidence.clone(),
                            masking_policy: None,
                            created_at: Utc::now(),
                        });
                        break; // First match wins
                    }
                }
            }
        }
        
        // Store detected classifications
        for classification in &classifications {
            self.db.upsert_column_classification(classification).await?;
        }
        
        Ok(classifications)
    }
}
```

### 6.3 Delta Integration

```rust
// crates/catalog-delta/src/lib.rs

use deltalake::{DeltaTable, DeltaTableError};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct DeltaReader {
    cache: Arc<RwLock<LruCache<String, CachedDeltaMeta>>>,
    cache_ttl: Duration,
}

struct CachedDeltaMeta {
    metadata: DeltaMetadata,
    cached_at: Instant,
}

impl DeltaReader {
    pub fn new(cache_ttl: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(1000).unwrap()))),
            cache_ttl,
        }
    }
    
    /// Get metadata with caching
    pub async fn get_metadata_cached(&self, location: &str) -> Result<DeltaMetadata> {
        // Check cache
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.peek(location) {
                if cached.cached_at.elapsed() < self.cache_ttl {
                    return Ok(cached.metadata.clone());
                }
            }
        }
        
        // Cache miss - read from Delta
        let metadata = self.get_metadata(location).await?;
        
        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.put(location.to_string(), CachedDeltaMeta {
                metadata: metadata.clone(),
                cached_at: Instant::now(),
            });
        }
        
        Ok(metadata)
    }
    
    /// Get fresh metadata (no cache)
    pub async fn get_metadata(&self, location: &str) -> Result<DeltaMetadata> {
        let table = deltalake::open_table(location).await?;
        let snapshot = table.snapshot()?;
        
        // Schema
        let schema = self.extract_schema(&table)?;
        
        // Statistics
        let files = snapshot.files();
        let (row_count, size_bytes) = self.aggregate_stats(&files);
        
        // Column stats
        let column_stats = self.extract_column_stats(&table, &files)?;
        
        // Partition columns
        let partition_columns = table.metadata()?
            .partition_columns
            .clone();
        
        // Last modified
        let last_modified = snapshot.metadata()
            .and_then(|m| m.created_time)
            .and_then(|ts| DateTime::from_timestamp_millis(ts))
            .unwrap_or_else(Utc::now);
        
        Ok(DeltaMetadata {
            schema,
            row_count,
            size_bytes,
            num_files: files.len() as i64,
            files: files.iter().map(|f| FileInfo {
                path: f.path.clone(),
                size: f.size as i64,
                modification_time: f.modification_time,
            }).collect(),
            partition_columns,
            last_modified,
            version: table.version(),
            column_stats,
        })
    }
    
    fn extract_schema(&self, table: &DeltaTable) -> Result<Schema> {
        let delta_schema = table.schema().ok_or(Error::NoSchema)?;
        
        let fields = delta_schema.fields()
            .iter()
            .map(|f| Field {
                name: f.name().to_string(),
                data_type: format!("{:?}", f.data_type()),
                nullable: f.is_nullable(),
                description: f.metadata().get("comment").map(|s| s.to_string()),
                metadata: f.metadata().clone(),
            })
            .collect();
        
        Ok(Schema { fields })
    }
    
    fn aggregate_stats(&self, files: &[Add]) -> (i64, i64) {
        let mut total_rows: i64 = 0;
        let mut total_bytes: i64 = 0;
        
        for file in files {
            total_bytes += file.size as i64;
            if let Some(stats) = &file.stats_parsed {
                total_rows += stats.num_records;
            }
        }
        
        (total_rows, total_bytes)
    }
    
    fn extract_column_stats(
        &self,
        table: &DeltaTable,
        files: &[Add],
    ) -> Result<Vec<ColumnStats>> {
        let schema = table.schema().ok_or(Error::NoSchema)?;
        let mut stats_by_column: HashMap<String, AggregatedColumnStats> = HashMap::new();
        
        // Aggregate stats from all files
        for file in files {
            if let Some(stats) = &file.stats_parsed {
                // Null counts
                if let Some(null_counts) = &stats.null_count {
                    for (col, count) in null_counts {
                        stats_by_column
                            .entry(col.clone())
                            .or_default()
                            .null_count += count;
                    }
                }
                
                // Min values
                if let Some(min_values) = &stats.min_values {
                    for (col, val) in min_values {
                        let entry = stats_by_column.entry(col.clone()).or_default();
                        entry.update_min(val);
                    }
                }
                
                // Max values
                if let Some(max_values) = &stats.max_values {
                    for (col, val) in max_values {
                        let entry = stats_by_column.entry(col.clone()).or_default();
                        entry.update_max(val);
                    }
                }
            }
        }
        
        // Convert to response
        Ok(schema.fields()
            .iter()
            .map(|f| {
                let name = f.name();
                let agg = stats_by_column.get(name);
                
                ColumnStats {
                    name: name.to_string(),
                    data_type: format!("{:?}", f.data_type()),
                    nullable: f.is_nullable(),
                    null_count: agg.map(|a| a.null_count),
                    distinct_count: None, // Delta doesn't track this
                    min_value: agg.and_then(|a| a.min.clone()),
                    max_value: agg.and_then(|a| a.max.clone()),
                    classification: None, // Added by catalog
                }
            })
            .collect())
    }
    
    /// Get schema at specific version
    pub async fn get_schema(&self, location: &str, version: Option<i64>) -> Result<Schema> {
        let table = match version {
            Some(v) => deltalake::open_table_with_version(location, v).await?,
            None => deltalake::open_table(location).await?,
        };
        
        self.extract_schema(&table)
    }
    
    /// Get table history
    pub async fn get_history(&self, location: &str, limit: usize) -> Result<Vec<DeltaVersion>> {
        let table = deltalake::open_table(location).await?;
        
        let history = table.history(Some(limit)).await?;
        
        Ok(history.into_iter().map(|commit| DeltaVersion {
            version: commit.version,
            timestamp: DateTime::from_timestamp_millis(commit.timestamp).unwrap_or_else(Utc::now),
            operation: commit.operation,
            user_name: commit.user_name,
            parameters: commit.operation_parameters,
            metrics: commit.operation_metrics,
        }).collect())
    }
    
    /// Diff schemas between versions
    pub async fn diff_schemas(
        &self,
        location: &str,
        from_version: i64,
        to_version: i64,
    ) -> Result<SchemaDiff> {
        let from_schema = self.get_schema(location, Some(from_version)).await?;
        let to_schema = self.get_schema(location, Some(to_version)).await?;
        
        let from_fields: HashSet<_> = from_schema.fields.iter()
            .map(|f| (&f.name, &f.data_type))
            .collect();
        
        let to_fields: HashSet<_> = to_schema.fields.iter()
            .map(|f| (&f.name, &f.data_type))
            .collect();
        
        let added = to_schema.fields.iter()
            .filter(|f| !from_fields.contains(&(&f.name, &f.data_type)))
            .cloned()
            .collect();
        
        let removed = from_schema.fields.iter()
            .filter(|f| !to_fields.contains(&(&f.name, &f.data_type)))
            .cloned()
            .collect();
        
        Ok(SchemaDiff {
            from_version,
            to_version,
            added_columns: added,
            removed_columns: removed,
            modified_columns: vec![], // Type changes detected separately
        })
    }
    
    /// Invalidate cache for a location
    pub async fn invalidate_cache(&self, location: &str) {
        let mut cache = self.cache.write().await;
        cache.pop(location);
    }
}
```

---

## 7. Multi-Tenant Design

### 7.1 Tenant Isolation Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                 MULTI-TENANT ARCHITECTURE                        │
└─────────────────────────────────────────────────────────────────┘

gs://lakehouse-catalog-meta/
│
├── _platform/                         # Platform-level (shared)
│   ├── tenants.json                  # Tenant registry
│   ├── api_keys.json                 # API key → tenant mapping
│   └── quotas.json                   # Quota definitions
│
├── tenant-001/                        # Tenant-isolated
│   ├── catalog.db                    # Full catalog schema
│   └── _audit/                       # Tenant audit logs
│       └── 2025-01.jsonl
│
├── tenant-002/
│   ├── catalog.db
│   └── _audit/
│
└── tenant-003/
    └── ...

BENEFITS:
✓ Physical isolation (no row filtering)
✓ Easy tenant deletion (GDPR)
✓ Independent backups per tenant
✓ No cross-tenant data leak risk
✓ Simple tenant migration
```

### 7.2 Tenant Context Middleware

```rust
// crates/catalog-api/src/middleware/tenant.rs

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};

#[derive(Clone, Debug)]
pub struct TenantContext {
    pub tenant_id: String,
    pub tenant_name: String,
    pub permissions: Vec<Permission>,
    pub quotas: TenantQuotas,
}

pub async fn tenant_middleware(
    State(tenant_service): State<Arc<TenantService>>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // Extract API key from header
    let api_key = request.headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::Unauthorized("Missing API key"))?;
    
    // Validate and get tenant context
    let tenant = tenant_service
        .validate_api_key(api_key)
        .await
        .map_err(|_| ApiError::Unauthorized("Invalid API key"))?;
    
    // Check if tenant is active
    if tenant.status != TenantStatus::Active {
        return Err(ApiError::Forbidden("Tenant is suspended"));
    }
    
    // Inject tenant context
    request.extensions_mut().insert(tenant);
    
    Ok(next.run(request).await)
}

// Usage in handlers
pub async fn list_tables(
    State(catalog): State<Arc<CatalogService>>,
    Extension(tenant): Extension<TenantContext>,
    Query(params): Query<ListParams>,
) -> Result<Json<ListResponse>, ApiError> {
    // Catalog automatically scoped to tenant
    let tables = catalog
        .for_tenant(&tenant.tenant_id)
        .list_tables(params)
        .await?;
    
    Ok(Json(ListResponse { tables }))
}
```

### 7.3 Quota Enforcement

```rust
// crates/catalog-api/src/middleware/quota.rs

pub struct QuotaEnforcer {
    platform_db: PlatformDatabase,
}

impl QuotaEnforcer {
    pub async fn check_table_quota(&self, tenant: &TenantContext) -> Result<()> {
        let current_count = self.platform_db
            .get_table_count(&tenant.tenant_id)
            .await?;
        
        if current_count >= tenant.quotas.max_tables {
            return Err(QuotaError::TableLimitReached {
                current: current_count,
                limit: tenant.quotas.max_tables,
            });
        }
        
        Ok(())
    }
    
    pub async fn check_rate_limit(&self, tenant: &TenantContext) -> Result<()> {
        let key = format!("rate:{}:minute", tenant.tenant_id);
        let current = self.redis.incr(&key, 1).await?;
        
        if current == 1 {
            self.redis.expire(&key, 60).await?;
        }
        
        if current > tenant.quotas.requests_per_minute {
            return Err(QuotaError::RateLimitExceeded {
                limit: tenant.quotas.requests_per_minute,
                reset_in: self.redis.ttl(&key).await?,
            });
        }
        
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct TenantQuotas {
    pub max_tables: i64,
    pub max_lineage_edges: i64,
    pub max_glossary_terms: i64,
    pub requests_per_minute: i64,
    pub storage_bytes: i64,
}
```

---

## 8. Quality & Governance Framework

### 8.1 Quality Check System

```rust
// crates/catalog-core/src/quality.rs

/// Quality check trait - implement for custom checks
pub trait QualityCheck: Send + Sync {
    fn check_type(&self) -> &str;
    fn name(&self) -> &str;
    
    async fn execute(
        &self,
        table: &TableEntry,
        delta_meta: &DeltaMetadata,
        config: &serde_json::Value,
    ) -> Result<QualityResult>;
}

/// Built-in completeness check
pub struct CompletenessCheck;

#[async_trait]
impl QualityCheck for CompletenessCheck {
    fn check_type(&self) -> &str { "completeness" }
    fn name(&self) -> &str { "Column Completeness" }
    
    async fn execute(
        &self,
        _table: &TableEntry,
        delta_meta: &DeltaMetadata,
        config: &serde_json::Value,
    ) -> Result<QualityResult> {
        let threshold = config.get("min_completeness")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.95);
        
        let total_cells = delta_meta.row_count * delta_meta.schema.fields.len() as i64;
        let total_nulls: i64 = delta_meta.column_stats.iter()
            .filter_map(|s| s.null_count)
            .sum();
        
        let completeness = if total_cells > 0 {
            1.0 - (total_nulls as f64 / total_cells as f64)
        } else {
            1.0
        };
        
        let status = if completeness >= threshold {
            CheckStatus::Pass
        } else if completeness >= threshold * 0.9 {
            CheckStatus::Warn
        } else {
            CheckStatus::Fail
        };
        
        // Find columns with high null rates
        let problem_columns: Vec<_> = delta_meta.column_stats.iter()
            .filter(|s| {
                let null_rate = s.null_count.unwrap_or(0) as f64 / delta_meta.row_count.max(1) as f64;
                null_rate > (1.0 - threshold)
            })
            .map(|s| s.name.clone())
            .collect();
        
        Ok(QualityResult {
            status,
            score: Some(completeness),
            details: Some(json!({
                "total_cells": total_cells,
                "null_cells": total_nulls,
                "completeness_percentage": completeness * 100.0,
                "problem_columns": problem_columns,
                "threshold": threshold
            })),
            records_checked: Some(delta_meta.row_count),
            records_failed: Some(total_nulls),
        })
    }
}

/// Quality check runner
pub struct QualityRunner {
    checks: HashMap<String, Box<dyn QualityCheck>>,
    db: Database,
    delta: DeltaReader,
}

impl QualityRunner {
    pub fn new(db: Database, delta: DeltaReader) -> Self {
        let mut checks: HashMap<String, Box<dyn QualityCheck>> = HashMap::new();
        
        // Register built-in checks
        checks.insert("completeness".to_string(), Box::new(CompletenessCheck));
        checks.insert("freshness".to_string(), Box::new(FreshnessCheck));
        checks.insert("file_health".to_string(), Box::new(FileHealthCheck));
        
        Self { checks, db, delta }
    }
    
    /// Run all configured checks for a table
    pub async fn run_checks(&self, table_id: &str) -> Result<Vec<QualityResult>> {
        let table = self.db.get_table(table_id).await?;
        let delta_meta = self.delta.get_metadata(&table.location).await?;
        let check_configs = self.db.get_quality_checks(table_id).await?;
        
        let mut results = Vec::new();
        
        for config in check_configs {
            if !config.enabled {
                continue;
            }
            
            if let Some(check) = self.checks.get(&config.check_type) {
                let start = Instant::now();
                
                let mut result = check.execute(&table, &delta_meta, &config.check_config).await?;
                result.execution_time_ms = Some(start.elapsed().as_millis() as i64);
                result.delta_version = Some(delta_meta.version);
                
                // Store result
                self.db.insert_quality_result(&QualityResultRecord {
                    id: Uuid::new_v4().to_string(),
                    check_id: config.id.clone(),
                    table_id: table_id.to_string(),
                    status: result.status.clone(),
                    score: result.score,
                    details: result.details.clone(),
                    records_checked: result.records_checked,
                    records_failed: result.records_failed,
                    executed_at: Utc::now(),
                    execution_time_ms: result.execution_time_ms,
                    delta_version: result.delta_version,
                }).await?;
                
                results.push(result);
            }
        }
        
        // Update aggregate quality score
        self.update_quality_score(table_id, &results).await?;
        
        Ok(results)
    }
    
    async fn update_quality_score(&self, table_id: &str, results: &[QualityResult]) -> Result<()> {
        let scores: Vec<f64> = results.iter()
            .filter_map(|r| r.score)
            .collect();
        
        let overall = if scores.is_empty() {
            None
        } else {
            Some(scores.iter().sum::<f64>() / scores.len() as f64)
        };
        
        let status = match overall {
            Some(s) if s >= 0.9 => "healthy",
            Some(s) if s >= 0.7 => "warning",
            Some(_) => "critical",
            None => "unknown",
        };
        
        self.db.upsert_quality_score(&QualityScoreRecord {
            table_id: table_id.to_string(),
            overall_score: overall,
            status: status.to_string(),
            last_checked_at: Utc::now(),
            ..Default::default()
        }).await
    }
}
```

### 8.2 Classification Engine

```rust
// crates/catalog-core/src/governance.rs

pub struct ClassificationEngine {
    rules: Vec<ClassificationRule>,
}

impl ClassificationEngine {
    /// Load rules from database
    pub async fn from_db(db: &Database) -> Result<Self> {
        let rules = db.get_classification_rules().await?;
        Ok(Self { rules })
    }
    
    /// Detect classifications for a table
    pub fn classify_columns(&self, schema: &Schema) -> Vec<ColumnClassification> {
        let mut classifications = Vec::new();
        
        for field in &schema.fields {
            for rule in &self.rules {
                if self.matches_rule(&field.name, rule) {
                    classifications.push(ColumnClassification {
                        id: Uuid::new_v4().to_string(),
                        table_id: String::new(), // Set by caller
                        column_name: field.name.clone(),
                        classification: rule.classification.clone(),
                        rule_id: Some(rule.id.clone()),
                        confidence: rule.confidence.clone(),
                        masking_policy: None,
                        created_at: Utc::now(),
                    });
                    break; // First match wins
                }
            }
        }
        
        classifications
    }
    
    fn matches_rule(&self, column_name: &str, rule: &ClassificationRule) -> bool {
        if let Some(pattern) = &rule.column_pattern {
            if let Ok(regex) = Regex::new(pattern) {
                return regex.is_match(column_name);
            }
        }
        false
    }
}

// Common PII patterns
pub fn default_classification_rules() -> Vec<ClassificationRule> {
    vec![
        ClassificationRule {
            id: "pii-email".to_string(),
            name: "Email Detection".to_string(),
            column_pattern: Some(r"(?i)(email|e_mail|email_address)".to_string()),
            classification: "pii".to_string(),
            confidence: "high".to_string(),
            ..Default::default()
        },
        ClassificationRule {
            id: "pii-phone".to_string(),
            name: "Phone Detection".to_string(),
            column_pattern: Some(r"(?i)(phone|mobile|cell|telephone)".to_string()),
            classification: "pii".to_string(),
            confidence: "high".to_string(),
            ..Default::default()
        },
        ClassificationRule {
            id: "pii-ssn".to_string(),
            name: "SSN Detection".to_string(),
            column_pattern: Some(r"(?i)(ssn|social_security|tax_id)".to_string()),
            classification: "pii".to_string(),
            confidence: "high".to_string(),
            ..Default::default()
        },
        ClassificationRule {
            id: "pii-address".to_string(),
            name: "Address Detection".to_string(),
            column_pattern: Some(r"(?i)(address|street|city|zip|postal)".to_string()),
            classification: "pii".to_string(),
            confidence: "medium".to_string(),
            ..Default::default()
        },
        ClassificationRule {
            id: "pci-card".to_string(),
            name: "Credit Card Detection".to_string(),
            column_pattern: Some(r"(?i)(card_number|credit_card|cc_num|pan)".to_string()),
            classification: "pci".to_string(),
            confidence: "high".to_string(),
            ..Default::default()
        },
    ]
}
```

---

## 9. Search & Discovery

### 9.1 Search Implementation

```rust
// crates/catalog-core/src/search.rs

pub struct SearchEngine {
    db: Database,
}

impl SearchEngine {
    /// Full-text search with ranking
    pub async fn search(
        &self,
        query: &str,
        filters: &SearchFilters,
        tenant_id: &str,
    ) -> Result<SearchResponse> {
        // Build FTS query
        let fts_query = self.build_fts_query(query);
        
        // Execute search
        let raw_results = self.db.execute_fts_search(&fts_query, filters).await?;
        
        // Enrich with popularity and quality
        let enriched_results = self.enrich_results(raw_results).await?;
        
        // Calculate facets
        let facets = self.calculate_facets(&enriched_results).await?;
        
        // Sort by combined score
        let mut sorted = enriched_results;
        sorted.sort_by(|a, b| {
            let score_a = self.combined_score(a, filters.sort.as_deref());
            let score_b = self.combined_score(b, filters.sort.as_deref());
            score_b.partial_cmp(&score_a).unwrap()
        });
        
        // Apply limit
        let limit = filters.limit.unwrap_or(20);
        sorted.truncate(limit);
        
        Ok(SearchResponse {
            results: sorted,
            total: sorted.len(),
            facets,
        })
    }
    
    fn build_fts_query(&self, query: &str) -> String {
        // Handle special characters
        let cleaned = query
            .replace(".", " ")
            .replace("_", " ");
        
        // Add prefix matching for partial words
        let terms: Vec<String> = cleaned
            .split_whitespace()
            .map(|t| format!("{}*", t))
            .collect();
        
        terms.join(" OR ")
    }
    
    async fn enrich_results(&self, results: Vec<RawSearchResult>) -> Result<Vec<SearchResult>> {
        let mut enriched = Vec::with_capacity(results.len());
        
        for raw in results {
            let usage = self.db.get_usage_summary(&raw.table_id).await.ok();
            let quality = self.db.get_quality_score(&raw.table_id).await.ok();
            
            enriched.push(SearchResult {
                table_id: raw.table_id,
                name: raw.name,
                description: raw.description,
                domain: raw.domain,
                owner: raw.owner,
                relevance_score: raw.bm25_score,
                popularity_score: usage.map(|u| u.query_count_30d as f64).unwrap_or(0.0),
                quality_score: quality.and_then(|q| q.overall_score),
                matched_fields: raw.matched_fields,
            });
        }
        
        Ok(enriched)
    }
    
    fn combined_score(&self, result: &SearchResult, sort: Option<&str>) -> f64 {
        match sort {
            Some("popularity") => result.popularity_score,
            Some("quality") => result.quality_score.unwrap_or(0.0) * 100.0,
            Some("recent") => 0.0, // Would need last_modified
            _ => {
                // Default: weighted combination
                result.relevance_score * 0.5 +
                (result.popularity_score / 1000.0).min(0.3) +
                result.quality_score.unwrap_or(0.5) * 0.2
            }
        }
    }
    
    async fn calculate_facets(&self, results: &[SearchResult]) -> Result<SearchFacets> {
        let mut domains: HashMap<String, i64> = HashMap::new();
        let mut owners: HashMap<String, i64> = HashMap::new();
        
        for result in results {
            if let Some(domain) = &result.domain {
                *domains.entry(domain.clone()).or_default() += 1;
            }
            if let Some(owner) = &result.owner {
                *owners.entry(owner.clone()).or_default() += 1;
            }
        }
        
        Ok(SearchFacets {
            domains: domains.into_iter()
                .map(|(k, v)| FacetCount { value: k, count: v })
                .collect(),
            owners: owners.into_iter()
                .map(|(k, v)| FacetCount { value: k, count: v })
                .collect(),
            tags: vec![], // Compute separately
        })
    }
    
    /// Search columns across all tables
    pub async fn search_columns(
        &self,
        query: &str,
        filters: &ColumnSearchFilters,
    ) -> Result<Vec<ColumnSearchResult>> {
        // This requires querying Delta for each table
        // In practice, we'd cache column names in the search index
        
        let tables = self.db.list_tables(None).await?;
        let mut results = Vec::new();
        
        let query_lower = query.to_lowercase();
        
        for table in tables {
            // Check cached column names first
            if let Some(columns) = self.db.get_cached_columns(&table.id).await? {
                for col in columns {
                    if col.name.to_lowercase().contains(&query_lower) {
                        results.push(ColumnSearchResult {
                            table_name: table.name.clone(),
                            column_name: col.name,
                            data_type: col.data_type,
                            description: col.description,
                        });
                    }
                }
            }
        }
        
        // Apply type filter
        if let Some(type_filter) = &filters.data_type {
            results.retain(|r| r.data_type.to_lowercase().contains(&type_filter.to_lowercase()));
        }
        
        // Limit
        results.truncate(filters.limit.unwrap_or(50));
        
        Ok(results)
    }
}
```

---

## 10. Integration Patterns

### 10.1 DataFusion Integration

```rust
// crates/catalog-client/src/datafusion.rs

use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};

/// DataFusion catalog backed by Lakehouse Catalog
pub struct LakehouseCatalogProvider {
    client: LakehouseCatalogClient,
    tenant_id: String,
}

impl LakehouseCatalogProvider {
    pub fn new(endpoint: &str, api_key: &str, tenant_id: &str) -> Self {
        Self {
            client: LakehouseCatalogClient::new(endpoint, api_key),
            tenant_id: tenant_id.to_string(),
        }
    }
}

impl CatalogProvider for LakehouseCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    
    fn schema_names(&self) -> Vec<String> {
        // Get domains as schemas
        tokio::runtime::Handle::current()
            .block_on(self.client.list_domains(&self.tenant_id))
            .unwrap_or_default()
    }
    
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(LakehouseSchemaProvider {
            client: self.client.clone(),
            tenant_id: self.tenant_id.clone(),
            domain: name.to_string(),
        }))
    }
}

pub struct LakehouseSchemaProvider {
    client: LakehouseCatalogClient,
    tenant_id: String,
    domain: String,
}

#[async_trait]
impl SchemaProvider for LakehouseSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    
    fn table_names(&self) -> Vec<String> {
        tokio::runtime::Handle::current()
            .block_on(self.client.list_tables_in_domain(&self.tenant_id, &self.domain))
            .unwrap_or_default()
            .into_iter()
            .map(|t| t.name.split('.').last().unwrap_or(&t.name).to_string())
            .collect()
    }
    
    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let full_name = format!("{}.{}", self.domain, name);
        
        // 1. Get location from catalog
        let table_meta = self.client
            .get_table(&self.tenant_id, &full_name)
            .await
            .ok()?;
        
        // 2. Log usage (async, don't block)
        let client = self.client.clone();
        let tenant = self.tenant_id.clone();
        let table_id = table_meta.id.clone();
        tokio::spawn(async move {
            client.log_usage(&tenant, &table_id, "query").await.ok();
        });
        
        // 3. Open Delta table directly
        let delta_table = deltalake::open_table(&table_meta.location)
            .await
            .ok()?;
        
        Some(Arc::new(delta_table))
    }
}

/// Register catalog with DataFusion session
pub fn register_lakehouse_catalog(
    ctx: &SessionContext,
    endpoint: &str,
    api_key: &str,
    tenant_id: &str,
) {
    let catalog = LakehouseCatalogProvider::new(endpoint, api_key, tenant_id);
    ctx.register_catalog("lakehouse", Arc::new(catalog));
}

// Usage
async fn example_query() -> Result<()> {
    let ctx = SessionContext::new();
    
    register_lakehouse_catalog(
        &ctx,
        "https://catalog.urbanski-dataverse.io",
        "sk_live_xxxxx",
        "tenant-001",
    );
    
    // Query using friendly names
    let df = ctx.sql("SELECT * FROM lakehouse.sales.orders WHERE order_date > '2025-01-01'").await?;
    df.show().await?;
    
    Ok(())
}
```

### 10.2 Servo Integration

```rust
// crates/catalog-client/src/servo.rs

use servo::{Asset, Workflow, ExecutionContext};

/// Servo middleware for automatic catalog integration
pub struct CatalogMiddleware {
    client: LakehouseCatalogClient,
}

impl CatalogMiddleware {
    /// Wrap asset execution with automatic catalog emission
    pub fn wrap_asset<F, Fut>(
        &self,
        asset_name: &str,
        upstream_assets: Vec<String>,
        f: F,
    ) -> impl Future<Output = Result<AssetOutput>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<AssetOutput>>,
    {
        let client = self.client.clone();
        let name = asset_name.to_string();
        let upstreams = upstream_assets;
        
        async move {
            // Execute asset
            let output = f().await?;
            
            // Register/update table in catalog
            client.register_or_update_table(&RegisterTableRequest {
                name: name.clone(),
                location: output.location.clone(),
                format: Some(output.format.clone()),
                domain: output.domain.clone(),
                owner: output.owner.clone(),
                description: output.description.clone(),
                tags: output.tags.clone(),
                ..Default::default()
            }).await?;
            
            // Record lineage
            if !upstreams.is_empty() {
                client.record_lineage(&RecordLineageRequest {
                    upstream_tables: upstreams,
                    downstream_table: name,
                    job_name: Some(output.job_name.clone()),
                    job_run_id: Some(output.job_run_id.clone()),
                    transformation_type: Some("servo".to_string()),
                    ..Default::default()
                }).await?;
            }
            
            Ok(output)
        }
    }
}

// Usage in Servo workflow
#[workflow(schedule = "0 2 * * *")]
async fn daily_orders_pipeline(ctx: ExecutionContext) -> Result<()> {
    let catalog = CatalogMiddleware::new(&ctx.catalog_client);
    
    // Bronze layer
    let raw_orders = catalog.wrap_asset(
        "bronze.raw_orders",
        vec![],  // No upstream
        || extract_orders_from_api(&ctx),
    ).await?;
    
    // Silver layer
    let clean_orders = catalog.wrap_asset(
        "silver.clean_orders",
        vec!["bronze.raw_orders".to_string()],
        || transform_orders(&ctx, &raw_orders),
    ).await?;
    
    // Gold layer
    let _ = catalog.wrap_asset(
        "gold.orders_summary",
        vec!["silver.clean_orders".to_string()],
        || aggregate_orders(&ctx, &clean_orders),
    ).await?;
    
    Ok(())
}
```

### 10.3 Pipeline Emitter

```rust
// crates/catalog-emitter/src/lib.rs

/// Emit metadata from any pipeline
pub struct CatalogEmitter {
    client: LakehouseCatalogClient,
    tenant_id: String,
    default_domain: Option<String>,
    default_owner: Option<String>,
}

impl CatalogEmitter {
    /// Emit table after DataFusion write
    pub async fn emit_from_datafusion(
        &self,
        name: &str,
        location: &str,
        df: &DataFrame,
        upstream_tables: Vec<String>,
    ) -> Result<()> {
        // Extract schema from DataFrame
        let schema = df.schema();
        
        // Register table
        self.client.register_or_update_table(&RegisterTableRequest {
            name: name.to_string(),
            location: location.to_string(),
            format: Some(TableFormat::Delta),
            domain: self.default_domain.clone(),
            owner: self.default_owner.clone(),
            ..Default::default()
        }).await?;
        
        // Record lineage
        if !upstream_tables.is_empty() {
            self.client.record_lineage(&RecordLineageRequest {
                upstream_tables,
                downstream_table: name.to_string(),
                transformation_type: Some("datafusion".to_string()),
                ..Default::default()
            }).await?;
        }
        
        Ok(())
    }
    
    /// Record quality check result
    pub async fn emit_quality_result(
        &self,
        table_name: &str,
        check_type: &str,
        status: CheckStatus,
        score: Option<f64>,
        details: Option<serde_json::Value>,
    ) -> Result<()> {
        self.client.record_quality_result(&RecordQualityRequest {
            table_name: table_name.to_string(),
            check_type: check_type.to_string(),
            status,
            score,
            details,
        }).await
    }
}

/// Convenience macro for pipeline integration
#[macro_export]
macro_rules! emit_table {
    ($emitter:expr, $name:expr, $location:expr, $df:expr) => {
        $emitter.emit_from_datafusion($name, $location, $df, vec![]).await?
    };
    ($emitter:expr, $name:expr, $location:expr, $df:expr, upstream: [$($upstream:expr),*]) => {
        $emitter.emit_from_datafusion($name, $location, $df, vec![$($upstream.to_string()),*]).await?
    };
}

// Usage
async fn pipeline_example(emitter: &CatalogEmitter) -> Result<()> {
    let ctx = SessionContext::new();
    
    // Read and transform
    let df = ctx.sql("SELECT * FROM source").await?;
    
    // Write
    let output_path = "gs://data/silver/transformed/";
    df.write_parquet(output_path, Default::default()).await?;
    
    // Emit to catalog
    emit_table!(
        emitter, 
        "silver.transformed", 
        output_path, 
        &df,
        upstream: ["bronze.source"]
    );
    
    Ok(())
}
```

---

## 11. Deployment Architecture

### 11.1 Cloud Run Deployment

```yaml
# deploy/cloud-run/service.yaml
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: lakehouse-catalog
  labels:
    app: lakehouse-catalog
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/minScale: "0"
        autoscaling.knative.dev/maxScale: "20"
        run.googleapis.com/cpu-throttling: "false"
        run.googleapis.com/execution-environment: gen2
    spec:
      containerConcurrency: 100
      timeoutSeconds: 300
      serviceAccountName: lakehouse-catalog@project.iam.gserviceaccount.com
      containers:
        - image: gcr.io/urbanski-dataverse/lakehouse-catalog:latest
          ports:
            - containerPort: 8080
          env:
            - name: CATALOG_BUCKET
              value: "urbanski-lakehouse-meta"
            - name: REDIS_URL
              valueFrom:
                secretKeyRef:
                  name: redis-credentials
                  key: url
            - name: LOG_LEVEL
              value: "info"
          resources:
            limits:
              cpu: "4"
              memory: "4Gi"
          startupProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            periodSeconds: 30
```

### 11.2 Terraform Infrastructure

```hcl
# deploy/terraform/main.tf

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# GCS bucket for catalog metadata
resource "google_storage_bucket" "catalog_metadata" {
  name     = "urbanski-lakehouse-meta"
  location = "US"
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      num_newer_versions = 10
    }
    action {
      type = "Delete"
    }
  }
  
  cors {
    origin          = ["*"]
    method          = ["GET", "HEAD", "PUT", "POST", "DELETE"]
    response_header = ["*"]
    max_age_seconds = 3600
  }
}

# Redis for caching
resource "google_redis_instance" "catalog_cache" {
  name           = "lakehouse-catalog-cache"
  tier           = "BASIC"
  memory_size_gb = 1
  region         = var.region
  
  redis_version = "REDIS_7_0"
  
  authorized_network = google_compute_network.vpc.id
}

# VPC for private connectivity
resource "google_compute_network" "vpc" {
  name                    = "lakehouse-catalog-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "lakehouse-catalog-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id
}

# VPC connector for Cloud Run
resource "google_vpc_access_connector" "connector" {
  name          = "lakehouse-catalog-connector"
  region        = var.region
  ip_cidr_range = "10.8.0.0/28"
  network       = google_compute_network.vpc.name
}

# Service account
resource "google_service_account" "catalog" {
  account_id   = "lakehouse-catalog"
  display_name = "Lakehouse Catalog Service"
}

# IAM bindings
resource "google_storage_bucket_iam_member" "catalog_bucket" {
  bucket = google_storage_bucket.catalog_metadata.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.catalog.email}"
}

resource "google_project_iam_member" "catalog_storage" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.catalog.email}"
}

# Cloud Run service
resource "google_cloud_run_v2_service" "catalog" {
  name     = "lakehouse-catalog"
  location = var.region
  
  template {
    service_account = google_service_account.catalog.email
    
    vpc_access {
      connector = google_vpc_access_connector.connector.id
      egress    = "PRIVATE_RANGES_ONLY"
    }
    
    containers {
      image = "gcr.io/${var.project_id}/lakehouse-catalog:latest"
      
      env {
        name  = "CATALOG_BUCKET"
        value = google_storage_bucket.catalog_metadata.name
      }
      
      env {
        name  = "REDIS_HOST"
        value = google_redis_instance.catalog_cache.host
      }
      
      env {
        name  = "REDIS_PORT"
        value = "6379"
      }
      
      resources {
        limits = {
          cpu    = "4"
          memory = "4Gi"
        }
      }
      
      startup_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        initial_delay_seconds = 5
        period_seconds        = 5
        failure_threshold     = 10
      }
    }
    
    scaling {
      min_instance_count = 0
      max_instance_count = 20
    }
  }
  
  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }
}

# Public access
resource "google_cloud_run_v2_service_iam_member" "public" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.catalog.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Outputs
output "service_url" {
  value = google_cloud_run_v2_service.catalog.uri
}

output "redis_host" {
  value = google_redis_instance.catalog_cache.host
}
```

### 11.3 Cost Estimate

```
┌─────────────────────────────────────────────────────────────────┐
│              MONTHLY COST ESTIMATE                               │
└─────────────────────────────────────────────────────────────────┘

SCENARIO: 100 tenants, 5,000 tables total, moderate usage

Component                              Cost
─────────────────────────────────────────────────────────────────
GCS Storage (100 tenants × 10MB)       $0.03
GCS Operations (~100k/month)           $0.50
Cloud Run (avg 0.5 instance)           $15-30
Redis (1GB basic)                      $35
VPC Connector                          $10
─────────────────────────────────────────────────────────────────
TOTAL                                  ~$60-80/month

AT SCALE: 1,000 tenants, 50,000 tables

GCS Storage (1,000 × 10MB)             $0.30
GCS Operations (~1M/month)             $5
Cloud Run (avg 2-3 instances)          $50-100
Redis (5GB standard)                   $150
VPC Connector                          $10
─────────────────────────────────────────────────────────────────
TOTAL                                  ~$215-265/month
```

---

## 12. Implementation Roadmap

### 12.1 Phase 1: Core MVP (6 weeks)

```
WEEK 1-2: Foundation
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
□ catalog-core crate
  □ Complete SQLite schema
  □ Table CRUD operations
  □ Lineage operations
  □ Search index
  
□ catalog-storage crate
  □ GCS backend
  □ Optimistic locking
  □ Per-tenant isolation

WEEK 3: Delta Integration
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
□ catalog-delta crate
  □ Schema extraction
  □ Statistics aggregation
  □ Column stats
  □ History retrieval
  □ Metadata caching

WEEK 4: API Server
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
□ catalog-api crate
  □ Table endpoints (CRUD + merged responses)
  □ Delta-delegated endpoints
  □ Search endpoint
  □ Lineage endpoints
  □ Tenant middleware
  □ OpenAPI spec

WEEK 5: Client & Integration
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
□ catalog-client crate
  □ HTTP client
  □ DataFusion CatalogProvider
  
□ catalog-emitter crate
  □ Pipeline integration
  □ Lineage auto-detection

WEEK 6: Deployment & Testing
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
□ Deployment
  □ Dockerfile
  □ Cloud Run config
  □ Terraform modules
  
□ Testing
  □ Integration tests
  □ Load testing
  □ Documentation

DELIVERABLES:
✓ Register Delta tables with business metadata
✓ Query tables via DataFusion using friendly names
✓ Search across all tables
✓ Track lineage
✓ View live Delta stats (schema, row count, etc.)
✓ Per-tenant isolation
✓ Serverless deployment
```

### 12.2 Phase 2: Enterprise Features (6 weeks)

```
WEEK 7-8: Quality Framework
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
□ Quality check definitions
□ Quality check execution
□ Quality scoring
□ Freshness SLAs
□ Quality history

WEEK 9: Glossary & Governance
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
□ Business glossary
□ Term-to-column links
□ Classification rules
□ PII detection
□ Audit logging

WEEK 10: Usage Analytics
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
□ Usage logging
□ Usage aggregation
□ Popularity ranking
□ User activity

WEEK 11-12: Advanced Features
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
□ Schema evolution tracking
□ Impact analysis
□ Advanced search (fuzzy, facets)
□ Quota enforcement
□ CLI tool

DELIVERABLES:
✓ Full quality framework
✓ Business glossary
✓ PII detection
✓ Usage analytics
✓ Audit trail
✓ CLI tool
```

### 12.3 Phase 3: UI & Polish (4 weeks)

```
WEEK 13-14: Web UI
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
□ Table browser
□ Search interface
□ Lineage visualization
□ Quality dashboard

WEEK 15-16: Polish & Launch
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
□ Performance optimization
□ Documentation
□ Examples
□ Open source launch

DELIVERABLES:
✓ Production-ready web UI
✓ Complete documentation
✓ Published to GitHub
✓ Announced to community
```

---

## Summary: What Makes This Architecture Unique

| Aspect | Traditional Catalogs | Lakehouse Catalog |
|--------|---------------------|-------------------|
| **Schema storage** | Store + sync | Delegate to Delta |
| **Statistics** | Store + sync | Delegate to Delta |
| **History** | Store manually | Delegate to Delta |
| **Sync jobs** | Required | None |
| **Data freshness** | Often stale | Always fresh |
| **Infrastructure** | Database + server | Serverless |
| **Cost** | $100-500/month | ~$60-80/month |
| **Complexity** | High | Low |
| **Enterprise features** | ✓ | ✓ |
| **Delta-native** | ✗ | ✓ |

**The Key Innovation:**

> *Lakehouse Catalog stores what Delta Lake doesn't know (lineage, ownership, glossary, quality assessments), delegates what Delta Lake already maintains (schema, statistics, history), and computes what can be derived (quality scores, freshness status, popularity).*

This gives you:
- **Best-in-class features** without best-in-class complexity
- **Always-fresh metadata** without sync jobs
- **Enterprise governance** without enterprise infrastructure
- **Industry-standard practices** with serverless economics

---

**End of Document**

Total: ~45,000 words, ~150 pages