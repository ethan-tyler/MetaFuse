//! MetaFuse Catalog Storage
//!
//! Storage backend abstraction for the MetaFuse catalog.
//! Supports local SQLite with future extensions for GCS/S3.

use metafuse_catalog_core::{init_sqlite_schema, CatalogError, Result};
use rusqlite::Connection;
use std::path::{Path, PathBuf};

/// Backend abstraction for catalog storage
///
/// Implementations handle different storage mechanisms:
/// - Local filesystem (SQLite file)
/// - GCS (SQLite on Google Cloud Storage)
/// - S3 (SQLite on AWS S3)
pub trait CatalogBackend: Send + Sync {
    /// Get a connection to the catalog database
    ///
    /// For local backends, this opens a direct connection.
    /// For cloud backends, this downloads the catalog file,
    /// opens it locally, and tracks the version for optimistic concurrency.
    fn get_connection(&self) -> Result<Connection>;

    /// Check if the catalog exists
    fn exists(&self) -> Result<bool>;

    /// Initialize a new catalog (create the database file)
    fn initialize(&self) -> Result<()>;
}

/// Local filesystem SQLite backend
///
/// Stores the catalog as a SQLite file on the local filesystem.
/// This is the primary backend for MVP and local development.
#[derive(Clone, Debug)]
pub struct LocalSqliteBackend {
    /// Path to the SQLite database file
    path: PathBuf,
}

impl LocalSqliteBackend {
    /// Create a new local SQLite backend
    ///
    /// # Arguments
    /// * `path` - Path to the SQLite database file
    ///
    /// # Example
    /// ```
    /// use metafuse_catalog_storage::LocalSqliteBackend;
    ///
    /// let backend = LocalSqliteBackend::new("catalog.db");
    /// ```
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    /// Get the path to the database file
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl CatalogBackend for LocalSqliteBackend {
    fn get_connection(&self) -> Result<Connection> {
        let conn = Connection::open(&self.path)?;

        // Enable foreign key constraints
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;

        // Initialize schema if needed
        init_sqlite_schema(&conn)?;

        Ok(conn)
    }

    fn exists(&self) -> Result<bool> {
        Ok(self.path.exists())
    }

    fn initialize(&self) -> Result<()> {
        if self.exists()? {
            return Err(CatalogError::Other(format!(
                "Catalog already exists at {:?}",
                self.path
            )));
        }

        let conn = Connection::open(&self.path)?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        init_sqlite_schema(&conn)?;

        Ok(())
    }
}

/// GCS backend for catalog storage (future implementation)
///
/// This will implement the SQLite-on-object-storage pattern:
/// 1. Download catalog.db from GCS bucket
/// 2. Open local connection
/// 3. Perform operations
/// 4. Upload back to GCS with generation number check (optimistic concurrency)
#[allow(dead_code)]
pub struct GcsBackend {
    bucket: String,
    path: String,
}

/// S3 backend for catalog storage (future implementation)
///
/// Similar to GCS backend but using AWS S3.
#[allow(dead_code)]
pub struct S3Backend {
    bucket: String,
    path: String,
    region: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_local_backend_initialize() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Remove the file so we can test initialization
        std::fs::remove_file(path).unwrap();

        let backend = LocalSqliteBackend::new(path);
        assert!(!backend.exists().unwrap());

        backend.initialize().unwrap();
        assert!(backend.exists().unwrap());

        let conn = backend.get_connection().unwrap();
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"datasets".to_string()));
    }

    #[test]
    fn test_local_backend_double_initialize() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        std::fs::remove_file(path).unwrap();

        let backend = LocalSqliteBackend::new(path);
        backend.initialize().unwrap();

        // Second initialize should fail
        assert!(backend.initialize().is_err());
    }

    #[test]
    fn test_local_backend_connection() {
        let temp_file = NamedTempFile::new().unwrap();
        let backend = LocalSqliteBackend::new(temp_file.path());

        let conn = backend.get_connection().unwrap();

        // Test that foreign keys are enabled
        let fk_enabled: i32 = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fk_enabled, 1);
    }
}
