//! Integration tests for MetaFuse Catalog API
//!
//! These tests verify the complete request/response cycle for API endpoints.
//! They test write operations, the `?include` parameter, and response formats.
//!
//! Note: These tests require a running server or use mock setup.
//! For now, we test the request/response structures and validation logic.

use axum::{
    body::Body,
    extract::{Extension, Path, Query},
    http::{Request, StatusCode},
    middleware,
    response::Response,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

// =============================================================================
// Test Helper Types (matching main.rs structures)
// =============================================================================

#[derive(Debug, Clone)]
struct RequestId(String);

#[derive(Debug, Serialize, Deserialize)]
struct ErrorResponse {
    error: String,
    request_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct DatasetResponse {
    id: i64,
    name: String,
    path: String,
    format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta_location: Option<String>,
    description: Option<String>,
    tenant: Option<String>,
    domain: Option<String>,
    owner: Option<String>,
    created_at: String,
    last_updated: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateDatasetRequest {
    name: String,
    path: String,
    format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta_location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tenant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    domain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upstream_datasets: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct DatasetQueryParams {
    include: Option<String>,
}

// =============================================================================
// Test Helper Functions
// =============================================================================

async fn request_id_middleware(mut req: Request<Body>, next: middleware::Next) -> Response {
    let request_id = RequestId(Uuid::new_v4().to_string());
    req.extensions_mut().insert(request_id.clone());
    next.run(req).await
}

async fn extract_json_body(response: Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("Failed to read response body");
    serde_json::from_slice(&body).expect("Failed to parse JSON")
}

fn bad_request(message: String, request_id: String) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: message,
            request_id,
        }),
    )
}

// =============================================================================
// Mock Handlers for Testing
// =============================================================================

/// Mock create dataset handler
async fn mock_create_dataset(
    Extension(request_id): Extension<RequestId>,
    Json(req): Json<CreateDatasetRequest>,
) -> Result<(StatusCode, Json<DatasetResponse>), (StatusCode, Json<ErrorResponse>)> {
    // Validate name format (alphanumeric, underscore, hyphen, dot)
    if req.name.is_empty() {
        return Err(bad_request(
            "Dataset name cannot be empty".to_string(),
            request_id.0.clone(),
        ));
    }

    if !req
        .name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(bad_request(
            "Dataset name contains invalid characters".to_string(),
            request_id.0.clone(),
        ));
    }

    // Validate tags if provided
    if let Some(ref tags) = req.tags {
        for tag in tags {
            if tag.is_empty() {
                return Err(bad_request(
                    "Tag cannot be empty".to_string(),
                    request_id.0.clone(),
                ));
            }
            if !tag
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == ':')
            {
                return Err(bad_request(
                    format!("Tag '{}' contains invalid characters", tag),
                    request_id.0.clone(),
                ));
            }
        }
    }

    Ok((
        StatusCode::CREATED,
        Json(DatasetResponse {
            id: 1,
            name: req.name,
            path: req.path,
            format: req.format,
            delta_location: req.delta_location,
            description: req.description,
            tenant: req.tenant,
            domain: req.domain,
            owner: req.owner,
            created_at: "2025-11-27T00:00:00Z".to_string(),
            last_updated: "2025-11-27T00:00:00Z".to_string(),
        }),
    ))
}

/// Valid include values (matching main.rs)
const VALID_INCLUDE_VALUES: &[&str] = &["delta", "quality", "lineage"];

/// Mock get dataset handler with ?include support
async fn mock_get_dataset(
    Extension(request_id): Extension<RequestId>,
    Path(name): Path<String>,
    Query(params): Query<DatasetQueryParams>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    // Validate name
    if name.is_empty() {
        return Err(bad_request(
            "Dataset name cannot be empty".to_string(),
            request_id.0.clone(),
        ));
    }

    // Parse and validate include options
    let include = params.include.unwrap_or_default();
    let include_parts: Vec<&str> = include
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .map(|s| s.leak() as &str) // Convert to static for comparison
        .collect();

    // Validate all include values are known
    let invalid: Vec<&&str> = include_parts
        .iter()
        .filter(|p| !VALID_INCLUDE_VALUES.contains(*p))
        .collect();
    if !invalid.is_empty() {
        return Err(bad_request(
            format!(
                "Invalid include value(s): {}. Valid values: {}",
                invalid
                    .iter()
                    .map(|s| format!("'{}'", s))
                    .collect::<Vec<_>>()
                    .join(", "),
                VALID_INCLUDE_VALUES.join(", ")
            ),
            request_id.0.clone(),
        ));
    }

    let include_delta = include_parts.iter().any(|&p| p == "delta");
    let include_quality = include_parts.iter().any(|&p| p == "quality");
    let include_lineage = include_parts.iter().any(|&p| p == "lineage");

    // Simulate a dataset without delta_location for testing
    let has_delta_location = !name.contains("no_delta");

    // Build response
    let mut response = json!({
        "id": 1,
        "name": name,
        "path": "s3://bucket/data.parquet",
        "format": "parquet",
        "description": "Test dataset",
        "tenant": "test",
        "domain": "analytics",
        "owner": "test@example.com",
        "created_at": "2025-11-27T00:00:00Z",
        "last_updated": "2025-11-27T00:00:00Z",
        "fields": [],
        "tags": ["test"],
        "upstream_datasets": [],
        "downstream_datasets": []
    });

    // Only include delta_location if the dataset has one
    if has_delta_location {
        response["delta_location"] = json!("s3://bucket/delta");
    }

    // Add optional includes
    if include_delta {
        if !has_delta_location {
            return Err(bad_request(
                format!(
                    "Cannot include delta metadata for dataset '{}': delta_location is not configured",
                    name
                ),
                request_id.0.clone(),
            ));
        }
        response["delta"] = json!({
            "version": 10,
            "row_count": 1000,
            "size_bytes": 50000,
            "num_files": 5,
            "partition_columns": ["date"],
            "last_modified": "2025-11-27T00:00:00Z"
        });
    }

    if include_quality {
        response["quality"] = json!({
            "overall_score": 0.95,
            "completeness_score": 0.98,
            "freshness_score": 1.0,
            "file_health_score": 0.87,
            "last_computed": "2025-11-27T00:00:00Z"
        });
    }

    if include_lineage {
        response["lineage"] = json!({
            "upstream": ["raw_data"],
            "downstream": ["aggregated_data"]
        });
    }

    Ok(Json(response))
}

// =============================================================================
// Delta-Delegated Mock Handlers
// =============================================================================

#[derive(Debug, Deserialize)]
struct SchemaQueryParams {
    version: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct HistoryQueryParams {
    limit: Option<usize>,
}

/// Mock get schema handler (Delta-delegated)
async fn mock_get_schema(
    Extension(request_id): Extension<RequestId>,
    Path(name): Path<String>,
    Query(params): Query<SchemaQueryParams>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    // Validate name
    if name.is_empty() {
        return Err(bad_request(
            "Dataset name cannot be empty".to_string(),
            request_id.0.clone(),
        ));
    }

    // Simulate dataset without delta_location
    if name.contains("no_delta") {
        return Err(bad_request(
            format!("Dataset '{}' does not have a delta_location", name),
            request_id.0.clone(),
        ));
    }

    // Simulate dataset not found
    if name.contains("nonexistent") {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Dataset '{}' not found", name),
                request_id: request_id.0,
            }),
        ));
    }

    let version = params.version.unwrap_or(10);
    Ok(Json(json!({
        "dataset_name": name,
        "delta_version": version,
        "schema": {
            "fields": [
                {"name": "id", "type": "Int64", "nullable": false},
                {"name": "name", "type": "Utf8", "nullable": true},
                {"name": "amount", "type": "Float64", "nullable": true}
            ]
        },
        "partition_columns": ["date"]
    })))
}

/// Mock get stats handler (Delta-delegated)
async fn mock_get_stats(
    Extension(request_id): Extension<RequestId>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    // Validate name
    if name.is_empty() {
        return Err(bad_request(
            "Dataset name cannot be empty".to_string(),
            request_id.0.clone(),
        ));
    }

    // Simulate dataset without delta_location
    if name.contains("no_delta") {
        return Err(bad_request(
            format!("Dataset '{}' does not have a delta_location", name),
            request_id.0.clone(),
        ));
    }

    // Simulate dataset not found
    if name.contains("nonexistent") {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Dataset '{}' not found", name),
                request_id: request_id.0,
            }),
        ));
    }

    Ok(Json(json!({
        "dataset_name": name,
        "delta_version": 10,
        "row_count": 150000,
        "size_bytes": 45000000,
        "num_files": 24,
        "last_modified": "2025-11-27T00:00:00Z"
    })))
}

/// Mock get history handler (Delta-delegated)
async fn mock_get_history(
    Extension(request_id): Extension<RequestId>,
    Path(name): Path<String>,
    Query(params): Query<HistoryQueryParams>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    // Validate name
    if name.is_empty() {
        return Err(bad_request(
            "Dataset name cannot be empty".to_string(),
            request_id.0.clone(),
        ));
    }

    // Simulate dataset without delta_location
    if name.contains("no_delta") {
        return Err(bad_request(
            format!("Dataset '{}' does not have a delta_location", name),
            request_id.0.clone(),
        ));
    }

    // Simulate dataset not found
    if name.contains("nonexistent") {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Dataset '{}' not found", name),
                request_id: request_id.0,
            }),
        ));
    }

    let limit = params.limit.unwrap_or(10);
    let versions: Vec<Value> = (0..limit.min(3))
        .rev()
        .map(|i| {
            json!({
                "version": 10 - i as i64,
                "timestamp": format!("2025-11-{}T00:00:00Z", 27 - i),
                "operation": if i == 0 { "WRITE" } else { "MERGE" }
            })
        })
        .collect();

    Ok(Json(json!({
        "dataset_name": name,
        "versions": versions
    })))
}

/// Mock add tags handler
async fn mock_add_tags(
    Extension(request_id): Extension<RequestId>,
    Path(name): Path<String>,
    Json(req): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let tags = req
        .get("tags")
        .and_then(|t| t.as_array())
        .ok_or_else(|| bad_request("Missing 'tags' array".to_string(), request_id.0.clone()))?;

    // Validate tags
    for tag in tags {
        let tag_str = tag
            .as_str()
            .ok_or_else(|| bad_request("Tag must be a string".to_string(), request_id.0.clone()))?;

        if tag_str.is_empty() {
            return Err(bad_request(
                "Tag cannot be empty".to_string(),
                request_id.0.clone(),
            ));
        }

        if !tag_str
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == ':')
        {
            return Err(bad_request(
                format!("Tag '{}' contains invalid characters", tag_str),
                request_id.0,
            ));
        }
    }

    Ok(Json(json!({
        "name": name,
        "tags": tags
    })))
}

fn create_test_app() -> Router {
    Router::new()
        .route("/api/v1/datasets", post(mock_create_dataset))
        .route("/api/v1/datasets/{name}", get(mock_get_dataset))
        .route("/api/v1/datasets/{name}/tags", post(mock_add_tags))
        .route("/api/v1/datasets/{name}/schema", get(mock_get_schema))
        .route("/api/v1/datasets/{name}/stats", get(mock_get_stats))
        .route("/api/v1/datasets/{name}/history", get(mock_get_history))
        .layer(middleware::from_fn(request_id_middleware))
}

// =============================================================================
// Write Endpoint Tests
// =============================================================================

#[tokio::test]
async fn test_create_dataset_success() {
    let app = create_test_app();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/datasets")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "name": "test_dataset",
                "path": "s3://bucket/data.parquet",
                "format": "parquet",
                "delta_location": "s3://bucket/delta",
                "description": "Test dataset for integration tests",
                "tenant": "test",
                "domain": "analytics"
            })
            .to_string(),
        ))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let json = extract_json_body(response).await;
    assert_eq!(json["name"].as_str(), Some("test_dataset"));
    assert_eq!(json["format"].as_str(), Some("parquet"));
    assert_eq!(json["delta_location"].as_str(), Some("s3://bucket/delta"));
}

#[tokio::test]
async fn test_create_dataset_with_tags() {
    let app = create_test_app();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/datasets")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "name": "tagged_dataset",
                "path": "s3://bucket/data.parquet",
                "format": "parquet",
                "tags": ["production", "analytics", "tier:gold"]
            })
            .to_string(),
        ))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_create_dataset_invalid_name_empty() {
    let app = create_test_app();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/datasets")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "name": "",
                "path": "s3://bucket/data.parquet",
                "format": "parquet"
            })
            .to_string(),
        ))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let json = extract_json_body(response).await;
    assert!(json["error"].as_str().unwrap().contains("empty"));
    assert!(json.get("request_id").is_some());
}

#[tokio::test]
async fn test_create_dataset_invalid_name_special_chars() {
    let app = create_test_app();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/datasets")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "name": "invalid@name!",
                "path": "s3://bucket/data.parquet",
                "format": "parquet"
            })
            .to_string(),
        ))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let json = extract_json_body(response).await;
    assert!(json["error"].as_str().unwrap().contains("invalid"));
}

#[tokio::test]
async fn test_create_dataset_invalid_tag() {
    let app = create_test_app();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/datasets")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "name": "test_dataset",
                "path": "s3://bucket/data.parquet",
                "format": "parquet",
                "tags": ["valid-tag", "invalid tag with spaces"]
            })
            .to_string(),
        ))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let json = extract_json_body(response).await;
    assert!(json["error"].as_str().unwrap().contains("invalid"));
}

// =============================================================================
// Include Parameter Tests
// =============================================================================

#[tokio::test]
async fn test_get_dataset_without_include() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/test_dataset")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = extract_json_body(response).await;

    // Should have base fields but no optional includes
    assert!(json.get("name").is_some());
    assert!(json.get("delta").is_none());
    assert!(json.get("quality").is_none());
    assert!(json.get("lineage").is_none());
}

#[tokio::test]
async fn test_get_dataset_with_include_delta() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/test_dataset?include=delta")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = extract_json_body(response).await;

    // Should have delta info
    assert!(json.get("delta").is_some());
    let delta = json.get("delta").unwrap();
    assert!(delta.get("version").is_some());
    assert!(delta.get("row_count").is_some());
    assert!(delta.get("size_bytes").is_some());
    assert!(delta.get("num_files").is_some());

    // Should NOT have other optionals
    assert!(json.get("quality").is_none());
    assert!(json.get("lineage").is_none());
}

#[tokio::test]
async fn test_get_dataset_with_include_quality() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/test_dataset?include=quality")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = extract_json_body(response).await;

    // Should have quality info
    assert!(json.get("quality").is_some());
    let quality = json.get("quality").unwrap();
    assert!(quality.get("overall_score").is_some());
    assert!(quality.get("completeness_score").is_some());
    assert!(quality.get("freshness_score").is_some());
    assert!(quality.get("file_health_score").is_some());

    // Should NOT have other optionals
    assert!(json.get("delta").is_none());
    assert!(json.get("lineage").is_none());
}

#[tokio::test]
async fn test_get_dataset_with_include_lineage() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/test_dataset?include=lineage")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = extract_json_body(response).await;

    // Should have lineage info
    assert!(json.get("lineage").is_some());
    let lineage = json.get("lineage").unwrap();
    assert!(lineage.get("upstream").is_some());
    assert!(lineage.get("downstream").is_some());

    // Should NOT have other optionals
    assert!(json.get("delta").is_none());
    assert!(json.get("quality").is_none());
}

#[tokio::test]
async fn test_get_dataset_with_include_multiple() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/test_dataset?include=delta,quality,lineage")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = extract_json_body(response).await;

    // Should have ALL optional includes
    assert!(json.get("delta").is_some());
    assert!(json.get("quality").is_some());
    assert!(json.get("lineage").is_some());
}

#[tokio::test]
async fn test_get_dataset_with_include_case_insensitive() {
    let app = create_test_app();

    // Test uppercase
    let req = Request::builder()
        .uri("/api/v1/datasets/test_dataset?include=DELTA,QUALITY")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Mock handler doesn't implement case-insensitivity, but the actual handler does
    // This test documents expected behavior
}

// =============================================================================
// Tag Management Tests
// =============================================================================

#[tokio::test]
async fn test_add_tags_success() {
    let app = create_test_app();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/datasets/test_dataset/tags")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "tags": ["production", "validated"]
            })
            .to_string(),
        ))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_add_tags_invalid_format() {
    let app = create_test_app();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/datasets/test_dataset/tags")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "tags": ["valid-tag", "invalid tag!@#"]
            })
            .to_string(),
        ))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_add_tags_empty_tag() {
    let app = create_test_app();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/datasets/test_dataset/tags")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "tags": ["valid", ""]
            })
            .to_string(),
        ))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let json = extract_json_body(response).await;
    assert!(json["error"].as_str().unwrap().contains("empty"));
}

#[tokio::test]
async fn test_add_tags_missing_tags_field() {
    let app = create_test_app();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/datasets/test_dataset/tags")
        .header("content-type", "application/json")
        .body(Body::from(json!({}).to_string()))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let json = extract_json_body(response).await;
    assert!(json["error"].as_str().unwrap().contains("tags"));
}

// =============================================================================
// Error Response Format Tests
// =============================================================================

#[tokio::test]
async fn test_error_response_includes_request_id() {
    let app = create_test_app();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/datasets")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "name": "",
                "path": "s3://bucket/data.parquet",
                "format": "parquet"
            })
            .to_string(),
        ))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    let json = extract_json_body(response).await;

    // All error responses MUST include request_id
    assert!(
        json.get("request_id").is_some(),
        "Error response missing 'request_id' field"
    );
    let request_id = json["request_id"].as_str().unwrap();
    assert!(
        Uuid::parse_str(request_id).is_ok(),
        "request_id should be a valid UUID"
    );
}

#[tokio::test]
async fn test_error_response_format_consistency() {
    let app = create_test_app();

    // Test multiple error conditions
    let error_requests = vec![
        (
            "empty name",
            json!({"name": "", "path": "s3://x", "format": "parquet"}),
        ),
        (
            "invalid name",
            json!({"name": "bad@name", "path": "s3://x", "format": "parquet"}),
        ),
        (
            "invalid tag",
            json!({"name": "good_name", "path": "s3://x", "format": "parquet", "tags": ["bad tag!"]}),
        ),
    ];

    for (case, body) in error_requests {
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/datasets")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();

        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "Expected 400 for case: {}",
            case
        );

        let json = extract_json_body(response).await;

        // All error responses should have exactly 2 fields
        let obj = json.as_object().expect("Response should be an object");
        assert!(
            obj.contains_key("error"),
            "{} response missing 'error' field",
            case
        );
        assert!(
            obj.contains_key("request_id"),
            "{} response missing 'request_id' field",
            case
        );
        assert_eq!(
            obj.len(),
            2,
            "{} should have exactly 2 fields (error, request_id), got: {:?}",
            case,
            obj.keys().collect::<Vec<_>>()
        );
    }
}

// =============================================================================
// Include Parameter Validation Tests
// =============================================================================

#[tokio::test]
async fn test_get_dataset_with_invalid_include_value() {
    let app = create_test_app();

    // Test single invalid value
    let req = Request::builder()
        .uri("/api/v1/datasets/test_dataset?include=foo")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Unknown include value 'foo' should return 400"
    );

    let json = extract_json_body(response).await;
    assert!(
        json["error"]
            .as_str()
            .unwrap()
            .contains("Invalid include value"),
        "Error message should mention invalid include value"
    );
    assert!(
        json["error"].as_str().unwrap().contains("foo"),
        "Error message should include the invalid value 'foo'"
    );
    assert!(
        json.get("request_id").is_some(),
        "Response must include request_id"
    );
}

#[tokio::test]
async fn test_get_dataset_with_multiple_invalid_include_values() {
    let app = create_test_app();

    // Test multiple invalid values mixed with valid
    let req = Request::builder()
        .uri("/api/v1/datasets/test_dataset?include=delta,foo,bar,quality")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Unknown include values should return 400"
    );

    let json = extract_json_body(response).await;
    let error_msg = json["error"].as_str().unwrap();
    assert!(
        error_msg.contains("foo") && error_msg.contains("bar"),
        "Error message should list all invalid values"
    );
}

#[tokio::test]
async fn test_get_dataset_include_delta_without_delta_location() {
    let app = create_test_app();

    // Use dataset name containing "no_delta" to trigger missing delta_location case
    let req = Request::builder()
        .uri("/api/v1/datasets/test_dataset_no_delta?include=delta")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Requesting include=delta on dataset without delta_location should return 400"
    );

    let json = extract_json_body(response).await;
    let error_msg = json["error"].as_str().unwrap();
    assert!(
        error_msg.contains("delta_location is not configured"),
        "Error message should explain delta_location is missing: {}",
        error_msg
    );
    assert!(
        json.get("request_id").is_some(),
        "Response must include request_id"
    );
}

#[tokio::test]
async fn test_get_dataset_quality_without_delta_location_succeeds() {
    let app = create_test_app();

    // Requesting quality/lineage without delta_location should work fine
    let req = Request::builder()
        .uri("/api/v1/datasets/test_dataset_no_delta?include=quality,lineage")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Requesting include=quality,lineage on dataset without delta_location should succeed"
    );

    let json = extract_json_body(response).await;
    assert!(
        json.get("quality").is_some(),
        "Quality info should be present"
    );
    assert!(
        json.get("lineage").is_some(),
        "Lineage info should be present"
    );
    assert!(
        json.get("delta").is_none(),
        "Delta info should NOT be present"
    );
}

// =============================================================================
// Delta-Delegated Endpoint Tests
// =============================================================================

#[tokio::test]
async fn test_get_schema_success() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/sales_data/schema")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = extract_json_body(response).await;
    assert_eq!(json["dataset_name"].as_str(), Some("sales_data"));
    assert!(json.get("delta_version").is_some());
    assert!(json.get("schema").is_some());
    assert!(json.get("partition_columns").is_some());
}

#[tokio::test]
async fn test_get_schema_with_version() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/sales_data/schema?version=5")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = extract_json_body(response).await;
    assert_eq!(json["delta_version"].as_i64(), Some(5));
}

#[tokio::test]
async fn test_get_schema_without_delta_location() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/dataset_no_delta/schema")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Schema endpoint should return 400 for dataset without delta_location"
    );

    let json = extract_json_body(response).await;
    assert!(json["error"]
        .as_str()
        .unwrap()
        .contains("does not have a delta_location"));
}

#[tokio::test]
async fn test_get_schema_dataset_not_found() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/nonexistent_dataset/schema")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let json = extract_json_body(response).await;
    assert!(json["error"].as_str().unwrap().contains("not found"));
}

#[tokio::test]
async fn test_get_stats_success() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/sales_data/stats")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = extract_json_body(response).await;
    assert_eq!(json["dataset_name"].as_str(), Some("sales_data"));
    assert!(json.get("delta_version").is_some());
    assert!(json.get("row_count").is_some());
    assert!(json.get("size_bytes").is_some());
    assert!(json.get("num_files").is_some());
    assert!(json.get("last_modified").is_some());
}

#[tokio::test]
async fn test_get_stats_without_delta_location() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/dataset_no_delta/stats")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Stats endpoint should return 400 for dataset without delta_location"
    );
}

#[tokio::test]
async fn test_get_history_success() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/sales_data/history")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = extract_json_body(response).await;
    assert_eq!(json["dataset_name"].as_str(), Some("sales_data"));
    assert!(json.get("versions").is_some());
    let versions = json["versions"].as_array().unwrap();
    assert!(!versions.is_empty());

    // Verify version structure
    let first_version = &versions[0];
    assert!(first_version.get("version").is_some());
    assert!(first_version.get("timestamp").is_some());
    assert!(first_version.get("operation").is_some());
}

#[tokio::test]
async fn test_get_history_with_limit() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/sales_data/history?limit=2")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = extract_json_body(response).await;
    let versions = json["versions"].as_array().unwrap();
    assert!(
        versions.len() <= 2,
        "History should respect limit parameter"
    );
}

#[tokio::test]
async fn test_get_history_without_delta_location() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/api/v1/datasets/dataset_no_delta/history")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "History endpoint should return 400 for dataset without delta_location"
    );
}

#[tokio::test]
async fn test_delta_delegated_endpoints_include_request_id_on_error() {
    let app = create_test_app();

    // Test all three endpoints return request_id on error
    let endpoints = vec![
        "/api/v1/datasets/nonexistent_dataset/schema",
        "/api/v1/datasets/nonexistent_dataset/stats",
        "/api/v1/datasets/nonexistent_dataset/history",
    ];

    for endpoint in endpoints {
        let req = Request::builder()
            .uri(endpoint)
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "Endpoint {} should return 404",
            endpoint
        );

        let json = extract_json_body(response).await;
        assert!(
            json.get("request_id").is_some(),
            "Error response for {} should include request_id",
            endpoint
        );
    }
}
