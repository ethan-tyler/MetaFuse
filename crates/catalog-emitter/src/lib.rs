//! MetaFuse Catalog Emitter
//!
//! DataFusion integration for automatic metadata capture from pipelines.

use chrono::Utc;
use datafusion::arrow::datatypes::SchemaRef;
use metafuse_catalog_core::{DatasetMeta, FieldMeta, Result};
use metafuse_catalog_storage::CatalogBackend;

/// Emitter API for capturing metadata from DataFusion pipelines
///
/// Use this to automatically register datasets, capture lineage,
/// and track metadata as data flows through your pipeline.
///
/// # Example
/// ```ignore
/// use metafuse_catalog_emitter::Emitter;
/// use metafuse_catalog_storage::LocalSqliteBackend;
///
/// let backend = LocalSqliteBackend::new("catalog.db");
/// let emitter = Emitter::new(backend);
///
/// // After writing a dataset with DataFusion:
/// emitter.emit_dataset(
///     "my_dataset",
///     "s3://bucket/path/to/data",
///     "parquet",
///     Some("prod-tenant"),
///     Some("analytics"),
///     Some("data-team@company.com"),
///     schema,
///     Some(1_000_000),
///     Some(50_000_000),
///     vec!["upstream_dataset_1".to_string()],
///     vec!["pii".to_string(), "daily".to_string()],
/// )?;
/// ```
pub struct Emitter<B: CatalogBackend> {
    backend: B,
}

impl<B: CatalogBackend> Emitter<B> {
    /// Create a new emitter with the given backend
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    /// Emit metadata for a dataset
    ///
    /// This registers a dataset in the catalog with its schema, lineage, and tags.
    /// Call this after successfully writing a dataset in your pipeline.
    ///
    /// # Arguments
    /// * `name` - Unique name for the dataset
    /// * `path` - Storage path (e.g., "s3://bucket/path" or "gs://bucket/path")
    /// * `format` - Format type ("parquet", "delta", "iceberg", "csv", etc.)
    /// * `description` - Optional human-readable description
    /// * `tenant` - Optional tenant identifier for multi-tenant deployments
    /// * `domain` - Optional business domain ("finance", "marketing", etc.)
    /// * `owner` - Optional owner/responsible party
    /// * `schema` - DataFusion Arrow schema
    /// * `row_count` - Optional approximate row count
    /// * `size_bytes` - Optional size in bytes
    /// * `upstream_datasets` - List of upstream dataset names this depends on
    /// * `tags` - Tags for categorization and discovery
    #[allow(clippy::too_many_arguments)]
    pub fn emit_dataset(
        &self,
        name: &str,
        path: &str,
        format: &str,
        description: Option<&str>,
        tenant: Option<&str>,
        domain: Option<&str>,
        owner: Option<&str>,
        schema: SchemaRef,
        row_count: Option<i64>,
        size_bytes: Option<i64>,
        upstream_datasets: Vec<String>,
        tags: Vec<String>,
    ) -> Result<()> {
        // Convert Arrow schema to FieldMeta
        let fields = schema
            .fields()
            .iter()
            .map(|f| FieldMeta {
                name: f.name().to_string(),
                data_type: format!("{:?}", f.data_type()),
                nullable: f.is_nullable(),
                description: None,
            })
            .collect();

        let now = Utc::now();

        let dataset = DatasetMeta {
            name: name.to_string(),
            path: path.to_string(),
            format: format.to_string(),
            description: description.map(|s| s.to_string()),
            tenant: tenant.map(|s| s.to_string()),
            domain: domain.map(|s| s.to_string()),
            owner: owner.map(|s| s.to_string()),
            created_at: now,
            last_updated: now,
            row_count,
            size_bytes,
            fields,
            upstream_datasets,
            tags,
        };

        self.write_dataset(&dataset)?;

        Ok(())
    }

    /// Write dataset metadata to the catalog
    fn write_dataset(&self, dataset: &DatasetMeta) -> Result<()> {
        let mut conn = self.backend.get_connection()?;

        let tx = conn.transaction()?;

        // Insert or update dataset
        tx.execute(
            r#"
            INSERT INTO datasets (name, path, format, description, tenant, domain, owner, created_at, last_updated, row_count, size_bytes)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(name) DO UPDATE SET
                path = excluded.path,
                format = excluded.format,
                description = excluded.description,
                tenant = excluded.tenant,
                domain = excluded.domain,
                owner = excluded.owner,
                last_updated = excluded.last_updated,
                row_count = excluded.row_count,
                size_bytes = excluded.size_bytes
            "#,
            rusqlite::params![
                dataset.name,
                dataset.path,
                dataset.format,
                dataset.description,
                dataset.tenant,
                dataset.domain,
                dataset.owner,
                dataset.created_at.to_rfc3339(),
                dataset.last_updated.to_rfc3339(),
                dataset.row_count,
                dataset.size_bytes,
            ],
        )?;

        // Get dataset ID
        let dataset_id: i64 = tx.query_row(
            "SELECT id FROM datasets WHERE name = ?1",
            [&dataset.name],
            |row| row.get(0),
        )?;

        // Delete existing fields and insert new ones
        tx.execute("DELETE FROM fields WHERE dataset_id = ?1", [dataset_id])?;

        for field in &dataset.fields {
            tx.execute(
                "INSERT INTO fields (dataset_id, name, data_type, nullable, description) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    dataset_id,
                    field.name,
                    field.data_type,
                    field.nullable as i32,
                    field.description,
                ],
            )?;
        }

        // Delete existing lineage and insert new ones
        tx.execute(
            "DELETE FROM lineage WHERE downstream_dataset_id = ?1",
            [dataset_id],
        )?;

        for upstream_name in &dataset.upstream_datasets {
            // Get or skip if upstream doesn't exist
            let upstream_id: Option<i64> = tx
                .query_row(
                    "SELECT id FROM datasets WHERE name = ?1",
                    [upstream_name],
                    |row| row.get(0),
                )
                .ok();

            if let Some(upstream_id) = upstream_id {
                tx.execute(
                    "INSERT OR IGNORE INTO lineage (upstream_dataset_id, downstream_dataset_id, created_at) VALUES (?1, ?2, ?3)",
                    rusqlite::params![
                        upstream_id,
                        dataset_id,
                        Utc::now().to_rfc3339(),
                    ],
                )?;
            }
        }

        // Delete existing tags and insert new ones
        tx.execute("DELETE FROM tags WHERE dataset_id = ?1", [dataset_id])?;

        for tag in &dataset.tags {
            tx.execute(
                "INSERT OR IGNORE INTO tags (dataset_id, tag) VALUES (?1, ?2)",
                rusqlite::params![dataset_id, tag],
            )?;
        }

        // Refresh FTS index entry for this dataset
        tx.execute(
            "DELETE FROM dataset_search WHERE dataset_name = ?1",
            [&dataset.name],
        )?;

        let field_names = dataset
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join(" ");

        let tag_string = dataset.tags.join(" ");

        tx.execute(
            r#"
            INSERT INTO dataset_search (dataset_name, path, domain, owner, description, tags, field_names)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            rusqlite::params![
                dataset.name,
                dataset.path,
                dataset.domain,
                dataset.owner,
                dataset.description,
                tag_string,
                field_names
            ],
        )?;

        // Commit transaction
        tx.commit()?;

        Ok(())
    }

    /// Get a reference to the backend
    pub fn backend(&self) -> &B {
        &self.backend
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use metafuse_catalog_storage::LocalSqliteBackend;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[test]
    fn test_emit_dataset() {
        let temp_file = NamedTempFile::new().unwrap();
        let backend = LocalSqliteBackend::new(temp_file.path());
        let emitter = Emitter::new(backend);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));

        emitter
            .emit_dataset(
                "test_dataset",
                "s3://test-bucket/data",
                "parquet",
                Some("Test dataset for emitter"),
                Some("test-tenant"),
                Some("analytics"),
                Some("test@example.com"),
                schema,
                Some(1000),
                Some(50000),
                vec![],
                vec!["test".to_string(), "sample".to_string()],
            )
            .unwrap();

        // Verify dataset was written
        let conn = emitter.backend().get_connection().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM datasets", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);

        // Verify fields were written
        let field_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM fields", [], |row| row.get(0))
            .unwrap();
        assert_eq!(field_count, 3);

        // Verify tags were written
        let tag_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM tags", [], |row| row.get(0))
            .unwrap();
        assert_eq!(tag_count, 2);
    }

    #[test]
    fn test_emit_dataset_with_lineage() {
        let temp_file = NamedTempFile::new().unwrap();
        let backend = LocalSqliteBackend::new(temp_file.path());
        let emitter = Emitter::new(backend);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        // Create upstream dataset
        emitter
            .emit_dataset(
                "upstream",
                "s3://bucket/upstream",
                "parquet",
                Some("Upstream dataset"),
                None,
                None,
                None,
                schema.clone(),
                None,
                None,
                vec![],
                vec![],
            )
            .unwrap();

        // Create downstream dataset with lineage
        emitter
            .emit_dataset(
                "downstream",
                "s3://bucket/downstream",
                "parquet",
                Some("Downstream dataset"),
                None,
                None,
                None,
                schema,
                None,
                None,
                vec!["upstream".to_string()],
                vec![],
            )
            .unwrap();

        // Verify lineage was created
        let conn = emitter.backend().get_connection().unwrap();
        let lineage_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM lineage", [], |row| row.get(0))
            .unwrap();
        assert_eq!(lineage_count, 1);
    }
}
