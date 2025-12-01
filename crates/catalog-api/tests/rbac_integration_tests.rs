//! Integration tests for RBAC (Role-Based Access Control) enforcement
//!
//! These tests verify:
//! 1. Permission checks work correctly for different roles (Reader, Editor, Admin)
//! 2. The require_tenant_middleware enforces tenant context
//! 3. Write/delete operations are properly guarded
//!
//! Run with: `cargo test -p metafuse-catalog-api --features "api-keys,test-utils" --test rbac_integration_tests`

#[cfg(all(feature = "api-keys", feature = "test-utils"))]
mod rbac_tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        middleware,
        response::Response,
        routing::get,
        Router,
    };
    use metafuse_catalog_api::{
        control_plane::TenantRole,
        multi_tenant::{require_delete_permission, require_write_permission, RbacErrorResponse},
        tenant_resolver::{ResolvedTenant, TenantSource},
    };
    use serde_json::Value;
    use tower::ServiceExt;

    /// Helper to create a ResolvedTenant for testing
    fn create_test_tenant(
        tenant_id: &str,
        role: TenantRole,
        source: TenantSource,
    ) -> ResolvedTenant {
        ResolvedTenant::for_testing(tenant_id, Some(role), source)
    }

    /// Helper to create a header-only ResolvedTenant (no role)
    fn create_header_tenant(tenant_id: &str) -> ResolvedTenant {
        ResolvedTenant::for_testing(tenant_id, None, TenantSource::Header)
    }

    /// Helper to extract JSON body from response
    async fn extract_json_body(response: Response) -> Value {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("Failed to read response body");
        serde_json::from_slice(&body).expect("Failed to parse JSON")
    }

    // ==========================================================================
    // Permission Check Function Tests
    // ==========================================================================

    #[test]
    fn test_require_write_permission_allows_admin() {
        let tenant = create_test_tenant("tenant-1", TenantRole::Admin, TenantSource::ApiKey);
        let result = require_write_permission(Some(&tenant), "req-123");
        assert!(result.is_ok(), "Admin should have write permission");
    }

    #[test]
    fn test_require_write_permission_allows_editor() {
        let tenant = create_test_tenant("tenant-1", TenantRole::Editor, TenantSource::ApiKey);
        let result = require_write_permission(Some(&tenant), "req-123");
        assert!(result.is_ok(), "Editor should have write permission");
    }

    #[test]
    fn test_require_write_permission_denies_viewer() {
        let tenant = create_test_tenant("tenant-1", TenantRole::Viewer, TenantSource::ApiKey);
        let result = require_write_permission(Some(&tenant), "req-123");

        assert!(result.is_err(), "Viewer should NOT have write permission");
        let (status, json) = result.unwrap_err();
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(json.error.contains("Write permission denied"));
        assert_eq!(json.request_id, "req-123");
    }

    #[test]
    fn test_require_write_permission_allows_none_tenant() {
        // When no tenant is present (single-tenant mode), permission is granted
        let result = require_write_permission(None, "req-123");
        assert!(
            result.is_ok(),
            "No tenant context should allow write (backward compatibility)"
        );
    }

    #[test]
    fn test_require_delete_permission_allows_admin() {
        let tenant = create_test_tenant("tenant-1", TenantRole::Admin, TenantSource::ApiKey);
        let result = require_delete_permission(Some(&tenant), "req-123");
        assert!(result.is_ok(), "Admin should have delete permission");
    }

    #[test]
    fn test_require_delete_permission_denies_editor() {
        let tenant = create_test_tenant("tenant-1", TenantRole::Editor, TenantSource::ApiKey);
        let result = require_delete_permission(Some(&tenant), "req-123");

        assert!(result.is_err(), "Editor should NOT have delete permission");
        let (status, json) = result.unwrap_err();
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(json.error.contains("Delete permission denied"));
    }

    #[test]
    fn test_require_delete_permission_denies_viewer() {
        let tenant = create_test_tenant("tenant-1", TenantRole::Viewer, TenantSource::ApiKey);
        let result = require_delete_permission(Some(&tenant), "req-123");

        assert!(result.is_err(), "Viewer should NOT have delete permission");
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[test]
    fn test_require_delete_permission_allows_none_tenant() {
        // When no tenant is present (single-tenant mode), permission is granted
        let result = require_delete_permission(None, "req-123");
        assert!(
            result.is_ok(),
            "No tenant context should allow delete (backward compatibility)"
        );
    }

    // ==========================================================================
    // ResolvedTenant Permission Tests
    // ==========================================================================

    #[test]
    fn test_resolved_tenant_viewer_permissions() {
        let tenant = create_test_tenant("tenant-1", TenantRole::Viewer, TenantSource::ApiKey);

        assert!(tenant.can_read(), "Viewer should be able to read");
        assert!(!tenant.can_write(), "Viewer should NOT be able to write");
        assert!(!tenant.can_delete(), "Viewer should NOT be able to delete");
        assert!(
            !tenant.can_manage_keys(),
            "Viewer should NOT be able to manage keys"
        );
    }

    #[test]
    fn test_resolved_tenant_editor_permissions() {
        let tenant = create_test_tenant("tenant-1", TenantRole::Editor, TenantSource::ApiKey);

        assert!(tenant.can_read(), "Editor should be able to read");
        assert!(tenant.can_write(), "Editor should be able to write");
        assert!(!tenant.can_delete(), "Editor should NOT be able to delete");
        assert!(
            !tenant.can_manage_keys(),
            "Editor should NOT be able to manage keys"
        );
    }

    #[test]
    fn test_resolved_tenant_admin_permissions() {
        let tenant = create_test_tenant("tenant-1", TenantRole::Admin, TenantSource::ApiKey);

        assert!(tenant.can_read(), "Admin should be able to read");
        assert!(tenant.can_write(), "Admin should be able to write");
        assert!(tenant.can_delete(), "Admin should be able to delete");
        assert!(
            tenant.can_manage_keys(),
            "Admin should be able to manage keys"
        );
    }

    #[test]
    fn test_resolved_tenant_header_only_is_readonly() {
        // Header-only resolution (no API key) should use effective_role which defaults to Viewer
        let tenant = create_header_tenant("tenant-1");

        assert!(tenant.can_read(), "Header-only should be able to read");
        assert!(
            !tenant.can_write(),
            "Header-only should NOT be able to write"
        );
        assert!(
            !tenant.can_delete(),
            "Header-only should NOT be able to delete"
        );
    }

    // ==========================================================================
    // Middleware Integration Tests
    // ==========================================================================

    /// Mock handler for testing middleware
    async fn mock_handler() -> &'static str {
        "OK"
    }

    #[tokio::test]
    async fn test_require_tenant_middleware_rejects_unauthenticated() {
        use metafuse_catalog_api::tenant_resolver::require_tenant_middleware;

        let app = Router::new()
            .route("/test", get(mock_handler))
            .layer(middleware::from_fn(require_tenant_middleware));

        // Request without ResolvedTenant extension
        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = app.clone().oneshot(req).await.unwrap();

        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "Request without tenant context should be rejected with 401"
        );

        // Verify error response format
        let json = extract_json_body(response).await;
        assert!(
            json.get("error").is_some(),
            "Error response should have error field"
        );
        // Note: request_id is optional when no RequestId extension is present
        assert!(
            json.get("message").is_some(),
            "Error response should have message field"
        );
    }

    #[tokio::test]
    async fn test_require_tenant_middleware_allows_authenticated() {
        use metafuse_catalog_api::tenant_resolver::require_tenant_middleware;

        let app = Router::new()
            .route("/test", get(mock_handler))
            .layer(middleware::from_fn(require_tenant_middleware));

        // Create request with ResolvedTenant extension
        let tenant = create_test_tenant("tenant-1", TenantRole::Viewer, TenantSource::ApiKey);
        let mut req = Request::builder().uri("/test").body(Body::empty()).unwrap();
        req.extensions_mut().insert(tenant);

        let response = app.clone().oneshot(req).await.unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "Request with tenant context should succeed"
        );
    }

    // ==========================================================================
    // Error Response Format Tests
    // ==========================================================================

    #[test]
    fn test_rbac_error_response_serialization() {
        let error = RbacErrorResponse {
            error: "Write permission denied".to_string(),
            request_id: "req-abc-123".to_string(),
        };

        let json = serde_json::to_string(&error).expect("Should serialize");
        let parsed: Value = serde_json::from_str(&json).expect("Should parse");

        assert_eq!(parsed["error"], "Write permission denied");
        assert_eq!(parsed["request_id"], "req-abc-123");
    }

    #[test]
    fn test_permission_check_error_includes_required_role() {
        let tenant = create_test_tenant("tenant-1", TenantRole::Viewer, TenantSource::ApiKey);

        // Write permission error
        let write_result = require_write_permission(Some(&tenant), "req-1");
        let (_, json) = write_result.unwrap_err();
        assert!(
            json.error.contains("Editor") || json.error.contains("Admin"),
            "Write error should mention required role"
        );

        // Delete permission error
        let delete_result = require_delete_permission(Some(&tenant), "req-2");
        let (_, json) = delete_result.unwrap_err();
        assert!(
            json.error.contains("Admin"),
            "Delete error should mention Admin role"
        );
    }

    // ==========================================================================
    // Role Hierarchy Tests
    // ==========================================================================

    #[test]
    fn test_role_hierarchy_viewer_is_minimum() {
        // Viewer is the minimum role - can only read
        let viewer = create_test_tenant("tenant-1", TenantRole::Viewer, TenantSource::ApiKey);
        assert!(viewer.can_read());
        assert!(!viewer.can_write());
        assert!(!viewer.can_delete());
        assert!(!viewer.can_manage_keys());
    }

    #[test]
    fn test_role_hierarchy_editor_includes_viewer() {
        // Editor can do everything Reader can, plus write
        let editor = create_test_tenant("tenant-1", TenantRole::Editor, TenantSource::ApiKey);
        assert!(editor.can_read()); // Reader capability
        assert!(editor.can_write()); // Editor capability
        assert!(!editor.can_delete()); // Admin only
        assert!(!editor.can_manage_keys()); // Admin only
    }

    #[test]
    fn test_role_hierarchy_admin_includes_all() {
        // Admin can do everything
        let admin = create_test_tenant("tenant-1", TenantRole::Admin, TenantSource::ApiKey);
        assert!(admin.can_read()); // Reader capability
        assert!(admin.can_write()); // Editor capability
        assert!(admin.can_delete()); // Admin capability
        assert!(admin.can_manage_keys()); // Admin capability
    }

    // ==========================================================================
    // Source-Based Permission Tests
    // ==========================================================================

    #[test]
    fn test_api_key_source_has_full_permissions() {
        // API key source with Admin role has full permissions
        let tenant = create_test_tenant("tenant-1", TenantRole::Admin, TenantSource::ApiKey);
        assert!(tenant.can_read());
        assert!(tenant.can_write());
        assert!(tenant.can_delete());
        assert!(tenant.can_manage_keys());
    }

    #[test]
    fn test_header_source_is_read_only() {
        // Header-only source defaults to Viewer (read-only)
        let tenant = create_header_tenant("tenant-1");
        assert!(tenant.can_read());
        assert!(!tenant.can_write());
        assert!(!tenant.can_delete());
        assert!(!tenant.can_manage_keys());
    }

    #[test]
    fn test_both_source_with_admin_has_full_permissions() {
        // Both source (API key + header) with Admin role has full permissions
        let tenant =
            ResolvedTenant::for_testing("tenant-1", Some(TenantRole::Admin), TenantSource::Both);
        assert!(tenant.can_read());
        assert!(tenant.can_write());
        assert!(tenant.can_delete());
        assert!(tenant.can_manage_keys());
    }
}

// Feature gate message for tests
#[cfg(not(all(feature = "api-keys", feature = "test-utils")))]
#[test]
fn rbac_tests_require_api_keys_feature() {
    eprintln!("RBAC integration tests require 'api-keys' and 'test-utils' features");
    eprintln!(
        "Run with: cargo test --features \"api-keys,test-utils\" --test rbac_integration_tests"
    );
}
