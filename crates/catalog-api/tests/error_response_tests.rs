//! Tests for API error response shapes
//!
//! Verifies that all error responses include the expected fields:
//! - `error`: Human-readable error message
//! - `request_id`: UUID for request correlation and debugging
//!
//! These tests ensure consistency across all error types (400, 404, 500).

use axum::{
    body::Body,
    extract::Extension,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

/// Request ID for tracking requests through the system
#[derive(Debug, Clone)]
struct RequestId(String);

/// Error response structure (must match API implementation)
#[derive(Debug, Serialize, Deserialize)]
struct ErrorResponse {
    error: String,
    request_id: String,
}

/// Helper to extract JSON body from response
async fn extract_json_body(response: Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("Failed to read response body");
    serde_json::from_slice(&body).expect("Failed to parse JSON")
}

/// Middleware to add request ID to every request
async fn request_id_middleware(mut req: Request<Body>, next: Next) -> Response {
    let request_id = RequestId(Uuid::new_v4().to_string());
    req.extensions_mut().insert(request_id.clone());
    next.run(req).await
}

/// Helper function to create bad request error response
fn bad_request(message: String, request_id: String) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: message,
            request_id,
        }),
    )
}

/// Helper function to create not found error response
fn not_found(message: String, request_id: String) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: message,
            request_id,
        }),
    )
}

/// Helper function to create internal error response
fn internal_error(_message: String, request_id: String) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: "Internal server error. Please contact support with the request ID.".to_string(),
            request_id,
        }),
    )
}

// Test handlers that simulate various error conditions
async fn handler_bad_request(
    Extension(request_id): Extension<RequestId>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    Err(bad_request(
        "Invalid input: name cannot be empty".to_string(),
        request_id.0,
    ))
}

async fn handler_not_found(
    Extension(request_id): Extension<RequestId>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    Err(not_found(
        "Dataset 'nonexistent' not found".to_string(),
        request_id.0,
    ))
}

async fn handler_internal_error(
    Extension(request_id): Extension<RequestId>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    Err(internal_error(
        "Database connection failed".to_string(),
        request_id.0,
    ))
}

async fn handler_success() -> Json<Value> {
    Json(serde_json::json!({"status": "ok"}))
}

fn create_test_app() -> Router {
    Router::new()
        .route("/bad_request", get(handler_bad_request))
        .route("/not_found", get(handler_not_found))
        .route("/internal_error", get(handler_internal_error))
        .route("/success", get(handler_success))
        .layer(middleware::from_fn(request_id_middleware))
}

#[tokio::test]
async fn test_bad_request_includes_request_id() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/bad_request")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let json = extract_json_body(response).await;

    // Verify error field exists and contains a message
    assert!(
        json.get("error").is_some(),
        "400 response missing 'error' field: {:?}",
        json
    );
    assert!(
        json["error"].as_str().is_some(),
        "400 'error' field should be a string: {:?}",
        json
    );

    // Verify request_id field exists and is a valid UUID
    assert!(
        json.get("request_id").is_some(),
        "400 response missing 'request_id' field: {:?}",
        json
    );
    let request_id = json["request_id"].as_str().unwrap();
    assert!(
        Uuid::parse_str(request_id).is_ok(),
        "400 'request_id' should be a valid UUID: {}",
        request_id
    );
}

#[tokio::test]
async fn test_not_found_includes_request_id() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/not_found")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let json = extract_json_body(response).await;

    // Verify error field exists
    assert!(
        json.get("error").is_some(),
        "404 response missing 'error' field: {:?}",
        json
    );
    assert!(json["error"].as_str().unwrap().contains("not found"));

    // Verify request_id field exists and is a valid UUID
    assert!(
        json.get("request_id").is_some(),
        "404 response missing 'request_id' field: {:?}",
        json
    );
    let request_id = json["request_id"].as_str().unwrap();
    assert!(
        Uuid::parse_str(request_id).is_ok(),
        "404 'request_id' should be a valid UUID: {}",
        request_id
    );
}

#[tokio::test]
async fn test_internal_error_includes_request_id() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/internal_error")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let json = extract_json_body(response).await;

    // Verify error field exists and is generic (no sensitive info)
    assert!(
        json.get("error").is_some(),
        "500 response missing 'error' field: {:?}",
        json
    );
    let error_msg = json["error"].as_str().unwrap();
    assert!(
        error_msg.contains("Internal server error"),
        "500 error should be generic: {}",
        error_msg
    );
    assert!(
        !error_msg.contains("Database"),
        "500 error should NOT leak implementation details: {}",
        error_msg
    );

    // Verify request_id field exists and is a valid UUID
    assert!(
        json.get("request_id").is_some(),
        "500 response missing 'request_id' field: {:?}",
        json
    );
    let request_id = json["request_id"].as_str().unwrap();
    assert!(
        Uuid::parse_str(request_id).is_ok(),
        "500 'request_id' should be a valid UUID: {}",
        request_id
    );
}

#[tokio::test]
async fn test_success_does_not_have_error_shape() {
    let app = create_test_app();

    let req = Request::builder()
        .uri("/success")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = extract_json_body(response).await;

    // Success response should NOT have error/request_id fields
    assert!(
        json.get("error").is_none(),
        "200 response should NOT have 'error' field: {:?}",
        json
    );
    assert!(
        json.get("status").is_some(),
        "200 response should have 'status' field: {:?}",
        json
    );
}

#[tokio::test]
async fn test_error_response_has_consistent_shape() {
    let app = create_test_app();

    // Test all error types have the same shape
    let error_endpoints = vec!["/bad_request", "/not_found", "/internal_error"];
    let expected_statuses = vec![
        StatusCode::BAD_REQUEST,
        StatusCode::NOT_FOUND,
        StatusCode::INTERNAL_SERVER_ERROR,
    ];

    for (endpoint, expected_status) in error_endpoints.iter().zip(expected_statuses.iter()) {
        let req = Request::builder()
            .uri(*endpoint)
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), *expected_status);

        let json = extract_json_body(response).await;

        // All error responses should have exactly two fields: error and request_id
        let obj = json.as_object().expect("Response should be an object");
        assert!(
            obj.contains_key("error"),
            "{} missing 'error' field",
            endpoint
        );
        assert!(
            obj.contains_key("request_id"),
            "{} missing 'request_id' field",
            endpoint
        );
        assert_eq!(
            obj.len(),
            2,
            "{} should have exactly 2 fields (error, request_id), got: {:?}",
            endpoint,
            obj.keys().collect::<Vec<_>>()
        );
    }
}
