//! MetaFuse Catalog API Server
//!
//! REST API for querying the MetaFuse catalog.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use metafuse_catalog_storage::{CatalogBackend, LocalSqliteBackend};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing_subscriber::EnvFilter;

/// Application state shared across handlers
#[derive(Clone)]
struct AppState<B: CatalogBackend> {
    backend: Arc<B>,
}

/// Dataset response structure
#[derive(Debug, Serialize, Deserialize)]
struct DatasetResponse {
    id: i64,
    name: String,
    path: String,
    format: String,
    tenant: Option<String>,
    domain: Option<String>,
    owner: Option<String>,
    created_at: String,
    last_updated: String,
    row_count: Option<i64>,
    size_bytes: Option<i64>,
}

/// Field response structure
#[derive(Debug, Serialize, Deserialize)]
struct FieldResponse {
    name: String,
    data_type: String,
    nullable: bool,
    description: Option<String>,
}

/// Dataset detail response with fields
#[derive(Debug, Serialize, Deserialize)]
struct DatasetDetailResponse {
    #[serde(flatten)]
    dataset: DatasetResponse,
    fields: Vec<FieldResponse>,
    tags: Vec<String>,
    upstream_datasets: Vec<String>,
    downstream_datasets: Vec<String>,
}

/// Error response
#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Get catalog path from environment or use default
    let catalog_path =
        std::env::var("METAFUSE_CATALOG_PATH").unwrap_or_else(|_| "metafuse_catalog.db".to_string());

    tracing::info!("Using catalog at: {}", catalog_path);

    let backend = LocalSqliteBackend::new(&catalog_path);

    // Check if catalog exists
    if !backend.exists().unwrap_or(false) {
        tracing::warn!("Catalog does not exist, initializing new catalog");
        backend.initialize().expect("Failed to initialize catalog");
    }

    let state = AppState {
        backend: Arc::new(backend),
    };

    // Build router
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/v1/datasets", get(list_datasets))
        .route("/api/v1/datasets/:name", get(get_dataset))
        .route("/api/v1/search", get(search_datasets))
        .layer(CorsLayer::permissive())
        .with_state(state);

    // Get port from environment or use default
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .expect("PORT must be a valid number");

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("MetaFuse API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// Health check endpoint
async fn health_check() -> &'static str {
    "ok"
}

/// List all datasets
async fn list_datasets<B: CatalogBackend>(
    State(state): State<AppState<B>>,
) -> Result<Json<Vec<DatasetResponse>>, (StatusCode, Json<ErrorResponse>)> {
    let conn = state
        .backend
        .get_connection()
        .map_err(|e| internal_error(e.to_string()))?;

    let mut stmt = conn
        .prepare(
            r#"
            SELECT id, name, path, format, tenant, domain, owner,
                   created_at, last_updated, row_count, size_bytes
            FROM datasets
            ORDER BY last_updated DESC
            "#,
        )
        .map_err(|e| internal_error(e.to_string()))?;

    let datasets = stmt
        .query_map([], |row| {
            Ok(DatasetResponse {
                id: row.get(0)?,
                name: row.get(1)?,
                path: row.get(2)?,
                format: row.get(3)?,
                tenant: row.get(4)?,
                domain: row.get(5)?,
                owner: row.get(6)?,
                created_at: row.get(7)?,
                last_updated: row.get(8)?,
                row_count: row.get(9)?,
                size_bytes: row.get(10)?,
            })
        })
        .map_err(|e| internal_error(e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| internal_error(e.to_string()))?;

    Ok(Json(datasets))
}

/// Get a specific dataset by name
async fn get_dataset<B: CatalogBackend>(
    State(state): State<AppState<B>>,
    Path(name): Path<String>,
) -> Result<Json<DatasetDetailResponse>, (StatusCode, Json<ErrorResponse>)> {
    let conn = state
        .backend
        .get_connection()
        .map_err(|e| internal_error(e.to_string()))?;

    // Get dataset
    let dataset: DatasetResponse = conn
        .query_row(
            r#"
            SELECT id, name, path, format, tenant, domain, owner,
                   created_at, last_updated, row_count, size_bytes
            FROM datasets
            WHERE name = ?1
            "#,
            [&name],
            |row| {
                Ok(DatasetResponse {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    path: row.get(2)?,
                    format: row.get(3)?,
                    tenant: row.get(4)?,
                    domain: row.get(5)?,
                    owner: row.get(6)?,
                    created_at: row.get(7)?,
                    last_updated: row.get(8)?,
                    row_count: row.get(9)?,
                    size_bytes: row.get(10)?,
                })
            },
        )
        .map_err(|_| not_found(format!("Dataset '{}' not found", name)))?;

    // Get fields
    let mut stmt = conn
        .prepare("SELECT name, data_type, nullable, description FROM fields WHERE dataset_id = ?1")
        .map_err(|e| internal_error(e.to_string()))?;

    let fields = stmt
        .query_map([dataset.id], |row| {
            Ok(FieldResponse {
                name: row.get(0)?,
                data_type: row.get(1)?,
                nullable: row.get::<_, i32>(2)? != 0,
                description: row.get(3)?,
            })
        })
        .map_err(|e| internal_error(e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| internal_error(e.to_string()))?;

    // Get tags
    let mut stmt = conn
        .prepare("SELECT tag FROM tags WHERE dataset_id = ?1")
        .map_err(|e| internal_error(e.to_string()))?;

    let tags = stmt
        .query_map([dataset.id], |row| row.get::<_, String>(0))
        .map_err(|e| internal_error(e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| internal_error(e.to_string()))?;

    // Get upstream datasets
    let mut stmt = conn
        .prepare(
            r#"
            SELECT d.name
            FROM lineage l
            JOIN datasets d ON l.upstream_dataset_id = d.id
            WHERE l.downstream_dataset_id = ?1
            "#,
        )
        .map_err(|e| internal_error(e.to_string()))?;

    let upstream_datasets = stmt
        .query_map([dataset.id], |row| row.get::<_, String>(0))
        .map_err(|e| internal_error(e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| internal_error(e.to_string()))?;

    // Get downstream datasets
    let mut stmt = conn
        .prepare(
            r#"
            SELECT d.name
            FROM lineage l
            JOIN datasets d ON l.downstream_dataset_id = d.id
            WHERE l.upstream_dataset_id = ?1
            "#,
        )
        .map_err(|e| internal_error(e.to_string()))?;

    let downstream_datasets = stmt
        .query_map([dataset.id], |row| row.get::<_, String>(0))
        .map_err(|e| internal_error(e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| internal_error(e.to_string()))?;

    Ok(Json(DatasetDetailResponse {
        dataset,
        fields,
        tags,
        upstream_datasets,
        downstream_datasets,
    }))
}

/// Search datasets using FTS
async fn search_datasets<B: CatalogBackend>(
    State(state): State<AppState<B>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<DatasetResponse>>, (StatusCode, Json<ErrorResponse>)> {
    let query = params
        .get("q")
        .ok_or_else(|| bad_request("Missing 'q' parameter".to_string()))?;

    let conn = state
        .backend
        .get_connection()
        .map_err(|e| internal_error(e.to_string()))?;

    let mut stmt = conn
        .prepare(
            r#"
            SELECT d.id, d.name, d.path, d.format, d.tenant, d.domain, d.owner,
                   d.created_at, d.last_updated, d.row_count, d.size_bytes
            FROM datasets d
            JOIN dataset_search s ON d.id = s.rowid
            WHERE dataset_search MATCH ?1
            ORDER BY rank
            "#,
        )
        .map_err(|e| internal_error(e.to_string()))?;

    let datasets = stmt
        .query_map([query], |row| {
            Ok(DatasetResponse {
                id: row.get(0)?,
                name: row.get(1)?,
                path: row.get(2)?,
                format: row.get(3)?,
                tenant: row.get(4)?,
                domain: row.get(5)?,
                owner: row.get(6)?,
                created_at: row.get(7)?,
                last_updated: row.get(8)?,
                row_count: row.get(9)?,
                size_bytes: row.get(10)?,
            })
        })
        .map_err(|e| internal_error(e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| internal_error(e.to_string()))?;

    Ok(Json(datasets))
}

/// Helper function to create internal error response
fn internal_error(message: String) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: message }),
    )
}

/// Helper function to create not found error response
fn not_found(message: String) -> (StatusCode, Json<ErrorResponse>) {
    (StatusCode::NOT_FOUND, Json(ErrorResponse { error: message }))
}

/// Helper function to create bad request error response
fn bad_request(message: String) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse { error: message }),
    )
}
