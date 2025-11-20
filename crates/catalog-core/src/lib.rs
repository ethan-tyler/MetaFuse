//! MetaFuse Catalog Core
//!
//! Core types, traits, and SQLite schema for the MetaFuse data catalog.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Metadata for a dataset in the catalog
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetMeta {
    /// Unique name of the dataset
    pub name: String,
    /// Storage path (e.g., "s3://bucket/path" or "gs://bucket/path")
    pub path: String,
    /// Format of the dataset (e.g., "parquet", "delta", "iceberg", "csv")
    pub format: String,
    /// Optional human-readable description
    pub description: Option<String>,
    /// Tenant identifier for multi-tenant deployments
    pub tenant: Option<String>,
    /// Business domain (e.g., "finance", "marketing", "operations")
    pub domain: Option<String>,
    /// Owner/responsible party
    pub owner: Option<String>,
    /// When the dataset was first registered
    pub created_at: DateTime<Utc>,
    /// When the dataset metadata was last updated
    pub last_updated: DateTime<Utc>,
    /// Approximate number of rows (if available)
    pub row_count: Option<i64>,
    /// Size in bytes (if available)
    pub size_bytes: Option<i64>,
    /// Schema fields
    pub fields: Vec<FieldMeta>,
    /// Names of upstream datasets this depends on
    pub upstream_datasets: Vec<String>,
    /// Tags for categorization and discovery
    pub tags: Vec<String>,
}

/// Metadata for a field/column in a dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMeta {
    /// Name of the field
    pub name: String,
    /// Data type (Arrow/DataFusion type representation)
    pub data_type: String,
    /// Whether the field allows null values
    pub nullable: bool,
    /// Human-readable description of the field
    pub description: Option<String>,
}

/// Errors that can occur in catalog operations
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("Dataset not found: {0}")]
    DatasetNotFound(String),

    #[error("Conflict detected: {0}")]
    ConflictError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Other error: {0}")]
    Other(String),
}

/// Result type for catalog operations
pub type Result<T> = std::result::Result<T, CatalogError>;

/// Initialize the SQLite schema for the catalog
///
/// Creates all necessary tables if they don't exist:
/// - `catalog_meta`: Version control for optimistic concurrency
/// - `datasets`: Core dataset registry
/// - `fields`: Column-level metadata
/// - `lineage`: Dataset lineage relationships
/// - `tags`: Dataset tags
/// - `glossary_terms`: Business glossary
/// - `term_links`: Links between datasets and glossary terms
/// - `dataset_search`: FTS5 virtual table for full-text search
pub fn init_sqlite_schema(conn: &rusqlite::Connection) -> Result<()> {
    let ddl = r#"
    -- Version control for optimistic concurrency
    CREATE TABLE IF NOT EXISTS catalog_meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    INSERT OR IGNORE INTO catalog_meta (key, value) VALUES ('version', '1');

    -- Core dataset registry
    CREATE TABLE IF NOT EXISTS datasets (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT UNIQUE NOT NULL,
      path TEXT NOT NULL,
      format TEXT NOT NULL,
      description TEXT,
      tenant TEXT,
      domain TEXT,
      owner TEXT,
      created_at TEXT NOT NULL,
      last_updated TEXT NOT NULL,
      row_count INTEGER,
      size_bytes INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_datasets_tenant ON datasets(tenant);
    CREATE INDEX IF NOT EXISTS idx_datasets_domain ON datasets(domain);
    CREATE INDEX IF NOT EXISTS idx_datasets_owner ON datasets(owner);
    CREATE INDEX IF NOT EXISTS idx_datasets_last_updated ON datasets(last_updated);

    -- Field/column metadata
    CREATE TABLE IF NOT EXISTS fields (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      dataset_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      data_type TEXT NOT NULL,
      nullable INTEGER NOT NULL DEFAULT 1,
      description TEXT,
      FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_fields_dataset_id ON fields(dataset_id);
    CREATE INDEX IF NOT EXISTS idx_fields_name ON fields(name);

    -- Dataset lineage relationships
    CREATE TABLE IF NOT EXISTS lineage (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      upstream_dataset_id INTEGER NOT NULL,
      downstream_dataset_id INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      FOREIGN KEY (upstream_dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
      FOREIGN KEY (downstream_dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
      UNIQUE(upstream_dataset_id, downstream_dataset_id)
    );

    CREATE INDEX IF NOT EXISTS idx_lineage_upstream ON lineage(upstream_dataset_id);
    CREATE INDEX IF NOT EXISTS idx_lineage_downstream ON lineage(downstream_dataset_id);

    -- Tags for categorization
    CREATE TABLE IF NOT EXISTS tags (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      dataset_id INTEGER NOT NULL,
      tag TEXT NOT NULL,
      FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
      UNIQUE(dataset_id, tag)
    );

    CREATE INDEX IF NOT EXISTS idx_tags_dataset_id ON tags(dataset_id);
    CREATE INDEX IF NOT EXISTS idx_tags_tag ON tags(tag);

    -- Business glossary
    CREATE TABLE IF NOT EXISTS glossary_terms (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      term TEXT UNIQUE NOT NULL,
      description TEXT,
      domain TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_glossary_domain ON glossary_terms(domain);

    -- Links between datasets/fields and glossary terms
    CREATE TABLE IF NOT EXISTS term_links (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      term_id INTEGER NOT NULL,
      dataset_id INTEGER,
      field_id INTEGER,
      FOREIGN KEY (term_id) REFERENCES glossary_terms(id) ON DELETE CASCADE,
      FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
      FOREIGN KEY (field_id) REFERENCES fields(id) ON DELETE CASCADE,
      CHECK ((dataset_id IS NOT NULL AND field_id IS NULL) OR (dataset_id IS NULL AND field_id IS NOT NULL))
    );

    CREATE INDEX IF NOT EXISTS idx_term_links_term ON term_links(term_id);
    CREATE INDEX IF NOT EXISTS idx_term_links_dataset ON term_links(dataset_id);
    CREATE INDEX IF NOT EXISTS idx_term_links_field ON term_links(field_id);

    -- Full-text search virtual table
    CREATE VIRTUAL TABLE IF NOT EXISTS dataset_search USING fts5(
      dataset_name,
      path,
      domain,
      owner,
      description,
      tags,
      field_names
    );
    "#;

    conn.execute_batch(ddl)?;
    Ok(())
}

/// Get the current catalog version for optimistic concurrency control
pub fn get_catalog_version(conn: &rusqlite::Connection) -> Result<i64> {
    let version: String = conn.query_row(
        "SELECT value FROM catalog_meta WHERE key = 'version'",
        [],
        |row| row.get(0),
    )?;

    version
        .parse()
        .map_err(|e| CatalogError::Other(format!("Invalid version format: {}", e)))
}

/// Increment the catalog version (call after successful write)
pub fn increment_catalog_version(conn: &rusqlite::Connection) -> Result<i64> {
    conn.execute(
        "UPDATE catalog_meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = 'version'",
        [],
    )?;
    get_catalog_version(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_schema() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        init_sqlite_schema(&conn).unwrap();

        // Verify tables exist
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"datasets".to_string()));
        assert!(tables.contains(&"fields".to_string()));
        assert!(tables.contains(&"lineage".to_string()));
        assert!(tables.contains(&"tags".to_string()));
        assert!(tables.contains(&"glossary_terms".to_string()));
    }

    #[test]
    fn test_version_control() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        init_sqlite_schema(&conn).unwrap();

        let version = get_catalog_version(&conn).unwrap();
        assert_eq!(version, 1);

        let new_version = increment_catalog_version(&conn).unwrap();
        assert_eq!(new_version, 2);
    }
}
