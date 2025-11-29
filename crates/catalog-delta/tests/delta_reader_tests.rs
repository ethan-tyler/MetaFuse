//! Integration tests for DeltaReader.
//!
//! These tests create sample Delta tables and verify that DeltaReader
//! can correctly extract metadata, schema, and statistics.

use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
use metafuse_catalog_delta::DeltaReader;
use std::time::Duration;
use tempfile::TempDir;

/// Create a simple test Delta table with basic columns.
async fn create_test_delta_table(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let fields = vec![
        StructField::new("id", DataType::Primitive(PrimitiveType::Integer), false),
        StructField::new("name", DataType::Primitive(PrimitiveType::String), true),
        StructField::new("amount", DataType::Primitive(PrimitiveType::Double), true),
        StructField::new(
            "created_at",
            DataType::Primitive(PrimitiveType::Timestamp),
            true,
        ),
    ];

    let _ = CreateBuilder::new()
        .with_location(path)
        .with_columns(fields)
        .await?;

    Ok(())
}

/// Create a partitioned Delta table.
async fn create_partitioned_delta_table(
    path: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let fields = vec![
        StructField::new("id", DataType::Primitive(PrimitiveType::Integer), false),
        StructField::new(
            "category",
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new("value", DataType::Primitive(PrimitiveType::Double), true),
        StructField::new("date", DataType::Primitive(PrimitiveType::Date), false),
    ];

    let _ = CreateBuilder::new()
        .with_location(path)
        .with_columns(fields)
        .with_partition_columns(vec!["category", "date"])
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_read_schema_from_delta_table() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = format!("file://{}", temp_dir.path().to_str().unwrap());

    // Create a test Delta table
    create_test_delta_table(&table_path).await.unwrap();

    // Read schema using DeltaReader
    let reader = DeltaReader::new(Duration::from_secs(60));
    let schema = reader.get_schema(&table_path, None).await.unwrap();

    // Verify schema
    assert_eq!(schema.fields.len(), 4);
    assert!(schema.partition_columns.is_empty());

    // Check field names
    let field_names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"amount"));
    assert!(field_names.contains(&"created_at"));

    // Check nullable flags
    let id_field = schema.fields.iter().find(|f| f.name == "id").unwrap();
    assert!(!id_field.nullable);

    let name_field = schema.fields.iter().find(|f| f.name == "name").unwrap();
    assert!(name_field.nullable);
}

#[tokio::test]
async fn test_read_partitioned_schema() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = format!("file://{}", temp_dir.path().to_str().unwrap());

    // Create a partitioned Delta table
    create_partitioned_delta_table(&table_path).await.unwrap();

    // Read schema using DeltaReader
    let reader = DeltaReader::new(Duration::from_secs(60));
    let schema = reader.get_schema(&table_path, None).await.unwrap();

    // Verify partition columns
    assert_eq!(schema.partition_columns.len(), 2);
    assert!(schema.partition_columns.contains(&"category".to_string()));
    assert!(schema.partition_columns.contains(&"date".to_string()));
}

#[tokio::test]
async fn test_get_metadata_empty_table() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = format!("file://{}", temp_dir.path().to_str().unwrap());

    // Create a test Delta table
    create_test_delta_table(&table_path).await.unwrap();

    // Get metadata
    let reader = DeltaReader::new(Duration::from_secs(60));
    let metadata = reader.get_metadata(&table_path).await.unwrap();

    // Verify metadata for empty table
    assert_eq!(metadata.row_count, 0);
    assert_eq!(metadata.num_files, 0);
    assert_eq!(metadata.size_bytes, 0);
    assert_eq!(metadata.version, 0);
    assert_eq!(metadata.schema.fields.len(), 4);
}

#[tokio::test]
async fn test_cache_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = format!("file://{}", temp_dir.path().to_str().unwrap());

    // Create a test Delta table
    create_test_delta_table(&table_path).await.unwrap();

    // Create reader with caching
    let reader = DeltaReader::new(Duration::from_secs(60));

    // First read (cache miss)
    let meta1 = reader.get_metadata_cached(&table_path).await.unwrap();

    // Second read (cache hit)
    let meta2 = reader.get_metadata_cached(&table_path).await.unwrap();

    // Both should be the same
    assert_eq!(meta1.version, meta2.version);
    assert_eq!(meta1.schema, meta2.schema);

    // Invalidate cache
    reader.invalidate_cache(&table_path).await;

    // Third read (cache miss after invalidation)
    let meta3 = reader.get_metadata_cached(&table_path).await.unwrap();
    assert_eq!(meta1.version, meta3.version);
}

#[tokio::test]
async fn test_cache_disabled() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = format!("file://{}", temp_dir.path().to_str().unwrap());

    // Create a test Delta table
    create_test_delta_table(&table_path).await.unwrap();

    // Create reader with caching disabled
    let reader = DeltaReader::new(Duration::ZERO);

    // Both reads should work (always fresh)
    let meta1 = reader.get_metadata_cached(&table_path).await.unwrap();
    let meta2 = reader.get_metadata_cached(&table_path).await.unwrap();

    assert_eq!(meta1.version, meta2.version);
}

#[tokio::test]
async fn test_invalid_table_path() {
    let reader = DeltaReader::new(Duration::from_secs(60));

    // Try to read from non-existent path
    let result = reader.get_metadata("file:///nonexistent/path/table").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_invalid_url_format() {
    let reader = DeltaReader::new(Duration::from_secs(60));

    // Try with invalid URL
    let result = reader.get_metadata("not-a-valid-url").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_history() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = format!("file://{}", temp_dir.path().to_str().unwrap());

    // Create a test Delta table
    create_test_delta_table(&table_path).await.unwrap();

    // Get history
    let reader = DeltaReader::new(Duration::from_secs(60));
    let history = reader.get_history(&table_path, 10).await.unwrap();

    // Should have at least one entry (the CREATE operation)
    assert!(!history.is_empty());
    assert_eq!(history[0].version, 0);
    assert!(!history[0].operation.is_empty());
}

#[tokio::test]
async fn test_custom_cache_capacity() {
    let reader = DeltaReader::with_capacity(Duration::from_secs(60), 10);

    // Just verify it doesn't panic
    let temp_dir = TempDir::new().unwrap();
    let table_path = format!("file://{}", temp_dir.path().to_str().unwrap());
    create_test_delta_table(&table_path).await.unwrap();

    let _ = reader.get_metadata_cached(&table_path).await.unwrap();
}

#[tokio::test]
async fn test_get_metadata_at_version() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = format!("file://{}", temp_dir.path().to_str().unwrap());

    // Create a test Delta table
    create_test_delta_table(&table_path).await.unwrap();

    // Get metadata at version 0
    let reader = DeltaReader::new(Duration::from_secs(60));
    let metadata = reader
        .get_metadata_at_version(&table_path, 0)
        .await
        .unwrap();

    // Verify metadata at version 0
    assert_eq!(metadata.version, 0);
    assert_eq!(metadata.schema.fields.len(), 4);
}

#[tokio::test]
async fn test_absolute_path_normalization() {
    let temp_dir = TempDir::new().unwrap();
    // Use absolute path without file:// scheme
    let table_path = temp_dir.path().to_str().unwrap().to_string();
    let table_url = format!("file://{}", &table_path);

    // Create table using file:// URL
    create_test_delta_table(&table_url).await.unwrap();

    // Read using absolute path (should be auto-normalized to file:// URL)
    let reader = DeltaReader::new(Duration::from_secs(60));
    let metadata = reader.get_metadata(&table_path).await.unwrap();

    assert_eq!(metadata.version, 0);
    assert_eq!(metadata.schema.fields.len(), 4);
}
