//! Tenant Resolution Middleware
//!
//! Resolves the current tenant from API key or `X-Tenant-ID` header and attaches
//! the tenant context to request extensions for downstream handlers.
//!
//! # Resolution Chain
//!
//! 1. **API Key**: If a tenant API key is provided (Bearer token starting with `mft_`),
//!    validate it and extract the tenant_id from the key.
//! 2. **Header**: If `X-Tenant-ID` header is present, verify the tenant exists and is
//!    active, then attach with Viewer role.
//! 3. **Conflict**: If both are present and the tenant_ids don't match, reject with 403.
//!
//! # Security Model
//!
//! - Tenant API keys (prefix `mft_`) are validated against the control plane database
//! - Global API keys (prefix `mf_`) do NOT provide tenant identity
//! - The resolved tenant role determines what operations are permitted
//! - **Header-only resolution** verifies tenant exists and is Active, but only grants
//!   Viewer (read-only) access. For write operations, use a tenant API key.
//!
//! # Security Considerations
//!
//! - Header-only resolution (`X-Tenant-ID` without API key) is useful for read-only
//!   access in trusted internal networks but should be used with caution in public APIs.
//! - Always use the `require_write_permission` or `require_admin_permission` guards
//!   for mutating operations to ensure proper authorization.
//! - Tenant status is verified for both API key and header resolution - suspended or
//!   deleted tenants cannot be accessed.
//!
//! # Usage
//!
//! ```rust,ignore
//! use metafuse_catalog_api::tenant_resolver::{tenant_resolver_middleware, ResolvedTenant};
//!
//! // Add middleware to router
//! let app = Router::new()
//!     .route("/datasets", get(list_datasets))
//!     .layer(middleware::from_fn(tenant_resolver_middleware));
//!
//! // In handlers, extract the resolved tenant
//! async fn list_datasets(
//!     Extension(tenant): Extension<ResolvedTenant>,
//! ) -> impl IntoResponse {
//!     // tenant.tenant_id() returns the resolved tenant
//!     // tenant.role() returns the TenantRole (Admin, Editor, Viewer)
//! }
//! ```

use crate::control_plane::{ControlPlane, TenantRole, ValidatedTenantKey};
use axum::{
    extract::{Extension, Request},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use metafuse_catalog_storage::TenantContext;
use serde::Serialize;
use std::sync::Arc;
use tracing::{debug, warn};

/// Configuration for the tenant resolver middleware.
///
/// Controls security-sensitive behavior like header-only resolution.
#[derive(Debug, Clone, Default)]
pub struct TenantResolverConfig {
    /// Allow header-only tenant resolution (X-Tenant-ID without API key).
    ///
    /// **Security Warning**: When enabled, any request can specify a tenant via header,
    /// which is only safe in trusted internal networks. In production with external
    /// traffic, this should be `false` to require API key authentication.
    ///
    /// Default: `false`
    pub allow_header_only_resolution: bool,
}

impl TenantResolverConfig {
    /// Create a config that allows header-only resolution.
    ///
    /// **Warning**: Only use in trusted internal networks.
    pub fn with_header_resolution() -> Self {
        Self {
            allow_header_only_resolution: true,
        }
    }
}

/// Header name for explicit tenant ID
pub const TENANT_ID_HEADER: &str = "X-Tenant-ID";

/// Prefix for tenant-scoped API keys
const TENANT_API_KEY_PREFIX: &str = "mft_";

/// Resolved tenant information attached to request extensions.
///
/// Contains the validated tenant context and the role derived from the API key
/// (or a default viewer role if resolved via header only).
#[derive(Debug, Clone)]
pub struct ResolvedTenant {
    /// Validated tenant context
    context: TenantContext,
    /// Role from API key (or None if resolved via header)
    role: Option<TenantRole>,
    /// Source of resolution
    source: TenantSource,
}

/// How the tenant was resolved
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantSource {
    /// Resolved from tenant API key
    ApiKey,
    /// Resolved from X-Tenant-ID header
    Header,
    /// Resolved from both (matched)
    Both,
}

impl ResolvedTenant {
    /// Create a new resolved tenant from API key validation
    pub fn from_api_key(key: &ValidatedTenantKey) -> Result<Self, String> {
        let context = TenantContext::new(&key.tenant_id)
            .map_err(|e| format!("Invalid tenant ID from API key: {}", e))?;
        Ok(Self {
            context,
            role: Some(key.role),
            source: TenantSource::ApiKey,
        })
    }

    /// Create a new resolved tenant from header
    pub fn from_header(tenant_id: &str) -> Result<Self, String> {
        let context = TenantContext::new(tenant_id)
            .map_err(|e| format!("Invalid tenant ID in header: {}", e))?;
        Ok(Self {
            context,
            role: None, // No role when resolved via header only
            source: TenantSource::Header,
        })
    }

    /// Create a new resolved tenant from both API key and header (matched)
    pub fn from_both(key: &ValidatedTenantKey) -> Result<Self, String> {
        let context =
            TenantContext::new(&key.tenant_id).map_err(|e| format!("Invalid tenant ID: {}", e))?;
        Ok(Self {
            context,
            role: Some(key.role),
            source: TenantSource::Both,
        })
    }

    /// Create a new resolved tenant for testing purposes
    #[cfg(any(test, feature = "test-utils"))]
    pub fn for_testing(tenant_id: &str, role: Option<TenantRole>, source: TenantSource) -> Self {
        Self {
            context: TenantContext::new(tenant_id).expect("Invalid tenant ID for testing"),
            role,
            source,
        }
    }

    /// Get the tenant ID
    pub fn tenant_id(&self) -> &str {
        self.context.tenant_id()
    }

    /// Get the tenant context
    pub fn context(&self) -> &TenantContext {
        &self.context
    }

    /// Get the role (if resolved via API key)
    pub fn role(&self) -> Option<TenantRole> {
        self.role
    }

    /// Get the effective role (defaults to Viewer if no API key)
    pub fn effective_role(&self) -> TenantRole {
        self.role.unwrap_or(TenantRole::Viewer)
    }

    /// Get the resolution source
    pub fn source(&self) -> TenantSource {
        self.source
    }

    /// Check if user can read data
    pub fn can_read(&self) -> bool {
        self.effective_role().can_read()
    }

    /// Check if user can write data
    pub fn can_write(&self) -> bool {
        self.effective_role().can_write()
    }

    /// Check if user can delete data
    pub fn can_delete(&self) -> bool {
        self.effective_role().can_delete()
    }

    /// Check if user can manage API keys
    pub fn can_manage_keys(&self) -> bool {
        self.effective_role().can_manage_keys()
    }
}

impl std::fmt::Display for ResolvedTenant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({})",
            self.tenant_id(),
            match self.source {
                TenantSource::ApiKey => "api_key",
                TenantSource::Header => "header",
                TenantSource::Both => "both",
            }
        )
    }
}

/// Error response for tenant resolution failures
#[derive(Debug, Serialize)]
struct TenantErrorResponse {
    error: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}

/// Extract request ID from extensions if available
fn get_request_id(req: &Request) -> Option<String> {
    req.extensions()
        .get::<uuid::Uuid>()
        .map(|id| id.to_string())
}

/// Extract API key from Authorization header
fn extract_api_key(req: &Request) -> Option<String> {
    req.headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.trim().to_string())
}

/// Extract tenant ID from X-Tenant-ID header
fn extract_tenant_header(req: &Request) -> Option<String> {
    req.headers()
        .get(TENANT_ID_HEADER)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Extract client IP from request headers
#[allow(dead_code)] // Used in future audit logging integration
fn extract_client_ip(req: &Request) -> Option<String> {
    // Try X-Forwarded-For first (may contain multiple IPs, take the first)
    if let Some(forwarded) = req.headers().get("x-forwarded-for") {
        if let Ok(value) = forwarded.to_str() {
            if let Some(first_ip) = value.split(',').next() {
                let ip = first_ip.trim();
                if !ip.is_empty() {
                    return Some(ip.to_string());
                }
            }
        }
    }

    // Try X-Real-IP
    if let Some(real_ip) = req.headers().get("x-real-ip") {
        if let Ok(value) = real_ip.to_str() {
            let ip = value.trim();
            if !ip.is_empty() {
                return Some(ip.to_string());
            }
        }
    }

    None
}

/// Tenant resolution middleware
///
/// Resolves the current tenant from API key or `X-Tenant-ID` header and attaches
/// `ResolvedTenant` to request extensions.
///
/// # Resolution Rules
///
/// 1. If tenant API key (`mft_*`) is present:
///    - Validate the key
///    - Extract tenant from the key
///    - If `X-Tenant-ID` header is also present and doesn't match, return 403
///
/// 2. If only `X-Tenant-ID` header is present AND `allow_header_only_resolution` is true:
///    - Validate the tenant ID format
///    - Verify tenant exists and is active
///    - Attach with Viewer role (no API key = read-only access)
///
/// 3. If neither is present:
///    - Pass through without tenant context (endpoints decide if required)
///
/// # Security
///
/// By default, header-only resolution is DISABLED. Pass `TenantResolverConfig` as an
/// Extension to enable it (only recommended for trusted internal networks).
///
/// # Error Responses
///
/// - **401 Unauthorized**: Invalid or expired API key
/// - **403 Forbidden**: Tenant ID mismatch between API key and header
/// - **400 Bad Request**: Invalid tenant ID format
#[cfg(feature = "api-keys")]
pub async fn tenant_resolver_middleware(
    Extension(control_plane): Extension<Arc<ControlPlane>>,
    config: Option<Extension<TenantResolverConfig>>,
    mut req: Request,
    next: Next,
) -> Response {
    let resolver_config = config.map(|c| c.0).unwrap_or_default();
    let request_id = get_request_id(&req);
    let api_key = extract_api_key(&req);
    let header_tenant = extract_tenant_header(&req);

    // Check if this is a tenant API key
    let is_tenant_key = api_key
        .as_ref()
        .map(|k| k.starts_with(TENANT_API_KEY_PREFIX))
        .unwrap_or(false);

    // Case 1: Tenant API key present
    if is_tenant_key {
        let api_key = api_key.unwrap();

        // Validate the tenant API key
        match control_plane.validate_tenant_api_key(&api_key).await {
            Ok(Some(validated_key)) => {
                // Check for tenant ID conflict
                if let Some(ref header_id) = header_tenant {
                    if header_id != &validated_key.tenant_id {
                        warn!(
                            api_key_tenant = %validated_key.tenant_id,
                            header_tenant = %header_id,
                            "Tenant ID mismatch between API key and header"
                        );
                        return (
                            StatusCode::FORBIDDEN,
                            Json(TenantErrorResponse {
                                error: "Forbidden".to_string(),
                                message: format!(
                                    "Tenant ID mismatch: API key belongs to '{}' but header specifies '{}'",
                                    validated_key.tenant_id, header_id
                                ),
                                request_id,
                            }),
                        )
                            .into_response();
                    }

                    // Both match - use API key tenant with Both source
                    match ResolvedTenant::from_both(&validated_key) {
                        Ok(resolved) => {
                            debug!(
                                tenant = %resolved.tenant_id(),
                                role = %resolved.effective_role(),
                                source = "both",
                                "Resolved tenant from API key + header"
                            );
                            req.extensions_mut().insert(resolved);
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to create tenant context");
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(TenantErrorResponse {
                                    error: "Internal Server Error".to_string(),
                                    message: e,
                                    request_id,
                                }),
                            )
                                .into_response();
                        }
                    }
                } else {
                    // Only API key - use that
                    match ResolvedTenant::from_api_key(&validated_key) {
                        Ok(resolved) => {
                            debug!(
                                tenant = %resolved.tenant_id(),
                                role = %resolved.effective_role(),
                                source = "api_key",
                                "Resolved tenant from API key"
                            );
                            req.extensions_mut().insert(resolved);
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to create tenant context from API key");
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(TenantErrorResponse {
                                    error: "Internal Server Error".to_string(),
                                    message: e,
                                    request_id,
                                }),
                            )
                                .into_response();
                        }
                    }
                }
            }
            Ok(None) => {
                // Invalid API key
                warn!("Invalid tenant API key");
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(TenantErrorResponse {
                        error: "Unauthorized".to_string(),
                        message: "Invalid or expired API key".to_string(),
                        request_id,
                    }),
                )
                    .into_response();
            }
            Err(e) => {
                // Validation error
                warn!(error = %e, "Error validating tenant API key");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(TenantErrorResponse {
                        error: "Internal Server Error".to_string(),
                        message: "Failed to validate API key".to_string(),
                        request_id,
                    }),
                )
                    .into_response();
            }
        }
    }
    // Case 2: Only X-Tenant-ID header present (no tenant API key)
    else if let Some(tenant_id) = header_tenant {
        // Check if header-only resolution is allowed
        if !resolver_config.allow_header_only_resolution {
            warn!(
                tenant_id = %tenant_id,
                "Header-only tenant resolution is disabled; API key required"
            );
            return (
                StatusCode::UNAUTHORIZED,
                Json(TenantErrorResponse {
                    error: "Unauthorized".to_string(),
                    message: "Tenant API key required. Header-only resolution is disabled."
                        .to_string(),
                    request_id,
                }),
            )
                .into_response();
        }

        // First, validate the tenant ID format
        if let Err(e) = TenantContext::new(&tenant_id) {
            warn!(tenant_id = %tenant_id, error = %e, "Invalid tenant ID format in header");
            return (
                StatusCode::BAD_REQUEST,
                Json(TenantErrorResponse {
                    error: "Bad Request".to_string(),
                    message: format!("Invalid tenant ID format: {}", e),
                    request_id,
                }),
            )
                .into_response();
        }

        // Verify tenant exists and is active in control plane
        match control_plane.get_tenant(&tenant_id).await {
            Ok(Some(tenant)) => {
                if !tenant.is_operational() {
                    warn!(
                        tenant_id = %tenant_id,
                        status = %tenant.status,
                        "Tenant is not active"
                    );
                    return (
                        StatusCode::FORBIDDEN,
                        Json(TenantErrorResponse {
                            error: "Forbidden".to_string(),
                            message: format!(
                                "Tenant '{}' is not active (status: {})",
                                tenant_id, tenant.status
                            ),
                            request_id,
                        }),
                    )
                        .into_response();
                }

                // Tenant is active - create resolved tenant with Viewer role
                match ResolvedTenant::from_header(&tenant_id) {
                    Ok(resolved) => {
                        debug!(
                            tenant = %resolved.tenant_id(),
                            source = "header",
                            "Resolved tenant from header (read-only)"
                        );
                        req.extensions_mut().insert(resolved);
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create tenant context from header");
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(TenantErrorResponse {
                                error: "Internal Server Error".to_string(),
                                message: e,
                                request_id,
                            }),
                        )
                            .into_response();
                    }
                }
            }
            Ok(None) => {
                warn!(tenant_id = %tenant_id, "Tenant not found");
                return (
                    StatusCode::NOT_FOUND,
                    Json(TenantErrorResponse {
                        error: "Not Found".to_string(),
                        message: format!("Tenant '{}' not found", tenant_id),
                        request_id,
                    }),
                )
                    .into_response();
            }
            Err(e) => {
                warn!(error = %e, "Error looking up tenant");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(TenantErrorResponse {
                        error: "Internal Server Error".to_string(),
                        message: "Failed to verify tenant".to_string(),
                        request_id,
                    }),
                )
                    .into_response();
            }
        }
    }
    // Case 3: Neither API key nor header - pass through
    // Downstream handlers decide if tenant is required

    next.run(req).await
}

/// Non-blocking tenant resolver (attaches identity without enforcing)
///
/// Similar to `tenant_resolver_middleware` but never returns errors.
/// Invalid keys or headers are logged but request proceeds.
/// Use this when tenant context is optional.
///
/// Respects `TenantResolverConfig.allow_header_only_resolution` - if disabled,
/// header-only resolution is silently skipped.
#[cfg(feature = "api-keys")]
pub async fn tenant_identity_middleware(
    Extension(control_plane): Extension<Arc<ControlPlane>>,
    config: Option<Extension<TenantResolverConfig>>,
    mut req: Request,
    next: Next,
) -> Response {
    let resolver_config = config.map(|c| c.0).unwrap_or_default();
    let api_key = extract_api_key(&req);
    let header_tenant = extract_tenant_header(&req);

    // Check if this is a tenant API key
    let is_tenant_key = api_key
        .as_ref()
        .map(|k| k.starts_with(TENANT_API_KEY_PREFIX))
        .unwrap_or(false);

    if is_tenant_key {
        let api_key = api_key.unwrap();

        if let Ok(Some(validated_key)) = control_plane.validate_tenant_api_key(&api_key).await {
            // Check for tenant ID conflict
            if let Some(ref header_id) = header_tenant {
                if header_id != &validated_key.tenant_id {
                    warn!(
                        api_key_tenant = %validated_key.tenant_id,
                        header_tenant = %header_id,
                        "Tenant ID mismatch (not attaching identity)"
                    );
                    // Don't attach identity on conflict
                    return next.run(req).await;
                }
            }

            if let Ok(resolved) = ResolvedTenant::from_api_key(&validated_key) {
                debug!(
                    tenant = %resolved.tenant_id(),
                    role = %resolved.effective_role(),
                    "Attached tenant identity from API key"
                );
                req.extensions_mut().insert(resolved);
            }
        }
    } else if let Some(tenant_id) = header_tenant {
        // Only allow header-only resolution if explicitly enabled
        if !resolver_config.allow_header_only_resolution {
            debug!(
                tenant_id = %tenant_id,
                "Header-only resolution disabled, skipping"
            );
            return next.run(req).await;
        }

        // Verify tenant exists and is active before attaching
        if let Ok(Some(tenant)) = control_plane.get_tenant(&tenant_id).await {
            if tenant.is_operational() {
                if let Ok(resolved) = ResolvedTenant::from_header(&tenant_id) {
                    debug!(
                        tenant = %resolved.tenant_id(),
                        "Attached tenant identity from header"
                    );
                    req.extensions_mut().insert(resolved);
                }
            } else {
                debug!(
                    tenant_id = %tenant_id,
                    status = %tenant.status,
                    "Tenant not operational, not attaching identity"
                );
            }
        } else {
            debug!(
                tenant_id = %tenant_id,
                "Tenant not found or error, not attaching identity"
            );
        }
    }

    next.run(req).await
}

/// Middleware to require tenant context (returns 401 if not resolved)
///
/// Use after `tenant_resolver_middleware` or `tenant_identity_middleware`
/// to enforce that a tenant must be present.
pub async fn require_tenant_middleware(req: Request, next: Next) -> Response {
    if req.extensions().get::<ResolvedTenant>().is_none() {
        let request_id = get_request_id(&req);
        return (
            StatusCode::UNAUTHORIZED,
            Json(TenantErrorResponse {
                error: "Unauthorized".to_string(),
                message: "Tenant context required. Provide tenant API key or X-Tenant-ID header."
                    .to_string(),
                request_id,
            }),
        )
            .into_response();
    }

    next.run(req).await
}

/// Middleware to require write permission
///
/// Use after `tenant_resolver_middleware` to enforce write permission.
pub async fn require_write_permission(req: Request, next: Next) -> Response {
    match req.extensions().get::<ResolvedTenant>() {
        Some(tenant) if tenant.can_write() => next.run(req).await,
        Some(tenant) => {
            let request_id = get_request_id(&req);
            warn!(
                tenant = %tenant.tenant_id(),
                role = %tenant.effective_role(),
                "Write permission denied"
            );
            (
                StatusCode::FORBIDDEN,
                Json(TenantErrorResponse {
                    error: "Forbidden".to_string(),
                    message: format!(
                        "Write permission required. Current role: {}",
                        tenant.effective_role()
                    ),
                    request_id,
                }),
            )
                .into_response()
        }
        None => {
            let request_id = get_request_id(&req);
            (
                StatusCode::UNAUTHORIZED,
                Json(TenantErrorResponse {
                    error: "Unauthorized".to_string(),
                    message: "Tenant context required for write operations.".to_string(),
                    request_id,
                }),
            )
                .into_response()
        }
    }
}

/// Middleware to require admin permission
///
/// Use after `tenant_resolver_middleware` to enforce admin permission.
pub async fn require_admin_permission(req: Request, next: Next) -> Response {
    match req.extensions().get::<ResolvedTenant>() {
        Some(tenant) if tenant.can_manage_keys() => next.run(req).await,
        Some(tenant) => {
            let request_id = get_request_id(&req);
            warn!(
                tenant = %tenant.tenant_id(),
                role = %tenant.effective_role(),
                "Admin permission denied"
            );
            (
                StatusCode::FORBIDDEN,
                Json(TenantErrorResponse {
                    error: "Forbidden".to_string(),
                    message: format!(
                        "Admin permission required. Current role: {}",
                        tenant.effective_role()
                    ),
                    request_id,
                }),
            )
                .into_response()
        }
        None => {
            let request_id = get_request_id(&req);
            (
                StatusCode::UNAUTHORIZED,
                Json(TenantErrorResponse {
                    error: "Unauthorized".to_string(),
                    message: "Tenant context required for admin operations.".to_string(),
                    request_id,
                }),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolved_tenant_permissions() {
        // Admin role
        let admin = ResolvedTenant {
            context: TenantContext::new("test-tenant").unwrap(),
            role: Some(TenantRole::Admin),
            source: TenantSource::ApiKey,
        };
        assert!(admin.can_read());
        assert!(admin.can_write());
        assert!(admin.can_delete());
        assert!(admin.can_manage_keys());

        // Editor role
        let editor = ResolvedTenant {
            context: TenantContext::new("test-tenant").unwrap(),
            role: Some(TenantRole::Editor),
            source: TenantSource::ApiKey,
        };
        assert!(editor.can_read());
        assert!(editor.can_write());
        assert!(!editor.can_delete()); // Only Admin can delete
        assert!(!editor.can_manage_keys());

        // Viewer role
        let viewer = ResolvedTenant {
            context: TenantContext::new("test-tenant").unwrap(),
            role: Some(TenantRole::Viewer),
            source: TenantSource::ApiKey,
        };
        assert!(viewer.can_read());
        assert!(!viewer.can_write());
        assert!(!viewer.can_delete());
        assert!(!viewer.can_manage_keys());

        // Header only (defaults to Viewer)
        let header_only = ResolvedTenant {
            context: TenantContext::new("test-tenant").unwrap(),
            role: None,
            source: TenantSource::Header,
        };
        assert!(header_only.can_read());
        assert!(!header_only.can_write());
        assert!(!header_only.can_delete());
        assert!(!header_only.can_manage_keys());
    }

    #[test]
    fn test_resolved_tenant_display() {
        let tenant = ResolvedTenant {
            context: TenantContext::new("acme-corp").unwrap(),
            role: Some(TenantRole::Admin),
            source: TenantSource::ApiKey,
        };
        assert_eq!(format!("{}", tenant), "acme-corp(api_key)");

        let tenant = ResolvedTenant {
            context: TenantContext::new("acme-corp").unwrap(),
            role: None,
            source: TenantSource::Header,
        };
        assert_eq!(format!("{}", tenant), "acme-corp(header)");

        let tenant = ResolvedTenant {
            context: TenantContext::new("acme-corp").unwrap(),
            role: Some(TenantRole::Editor),
            source: TenantSource::Both,
        };
        assert_eq!(format!("{}", tenant), "acme-corp(both)");
    }

    #[test]
    fn test_effective_role() {
        // With explicit role
        let with_role = ResolvedTenant {
            context: TenantContext::new("test").unwrap(),
            role: Some(TenantRole::Admin),
            source: TenantSource::ApiKey,
        };
        assert_eq!(with_role.effective_role(), TenantRole::Admin);

        // Without role (defaults to Viewer)
        let without_role = ResolvedTenant {
            context: TenantContext::new("test").unwrap(),
            role: None,
            source: TenantSource::Header,
        };
        assert_eq!(without_role.effective_role(), TenantRole::Viewer);
    }

    #[test]
    fn test_tenant_source_equality() {
        assert_eq!(TenantSource::ApiKey, TenantSource::ApiKey);
        assert_ne!(TenantSource::ApiKey, TenantSource::Header);
        assert_ne!(TenantSource::Header, TenantSource::Both);
    }

    #[test]
    fn test_resolved_tenant_accessors() {
        let tenant = ResolvedTenant {
            context: TenantContext::new("my-tenant").unwrap(),
            role: Some(TenantRole::Editor),
            source: TenantSource::Both,
        };

        assert_eq!(tenant.tenant_id(), "my-tenant");
        assert_eq!(tenant.context().tenant_id(), "my-tenant");
        assert_eq!(tenant.role(), Some(TenantRole::Editor));
        assert_eq!(tenant.source(), TenantSource::Both);
    }

    #[test]
    fn test_resolved_tenant_from_api_key() {
        use crate::control_plane::ValidatedTenantKey;

        let key = ValidatedTenantKey {
            key_hash: "hash123".to_string(),
            tenant_id: "valid-tenant".to_string(),
            name: "Test Key".to_string(),
            role: TenantRole::Admin,
        };

        let resolved = ResolvedTenant::from_api_key(&key).unwrap();
        assert_eq!(resolved.tenant_id(), "valid-tenant");
        assert_eq!(resolved.role(), Some(TenantRole::Admin));
        assert_eq!(resolved.source(), TenantSource::ApiKey);
    }

    #[test]
    fn test_resolved_tenant_from_header() {
        // Valid tenant ID
        let resolved = ResolvedTenant::from_header("valid-tenant").unwrap();
        assert_eq!(resolved.tenant_id(), "valid-tenant");
        assert_eq!(resolved.role(), None);
        assert_eq!(resolved.source(), TenantSource::Header);

        // Invalid tenant ID (too short)
        let result = ResolvedTenant::from_header("ab");
        assert!(result.is_err());

        // Invalid tenant ID (bad characters)
        let result = ResolvedTenant::from_header("Invalid Tenant");
        assert!(result.is_err());
    }

    #[test]
    fn test_resolved_tenant_from_both() {
        use crate::control_plane::ValidatedTenantKey;

        let key = ValidatedTenantKey {
            key_hash: "hash456".to_string(),
            tenant_id: "both-tenant".to_string(),
            name: "Both Key".to_string(),
            role: TenantRole::Viewer,
        };

        let resolved = ResolvedTenant::from_both(&key).unwrap();
        assert_eq!(resolved.tenant_id(), "both-tenant");
        assert_eq!(resolved.role(), Some(TenantRole::Viewer));
        assert_eq!(resolved.source(), TenantSource::Both);
    }

    #[test]
    fn test_header_only_is_read_only() {
        // Key point: header-only resolution grants Viewer (read-only) access
        let header_tenant = ResolvedTenant::from_header("read-only-tenant").unwrap();

        // Can read
        assert!(header_tenant.can_read());

        // Cannot write, delete, or manage keys
        assert!(!header_tenant.can_write());
        assert!(!header_tenant.can_delete());
        assert!(!header_tenant.can_manage_keys());

        // Effective role is Viewer
        assert_eq!(header_tenant.effective_role(), TenantRole::Viewer);
    }

    #[test]
    fn test_resolver_config_default_is_secure() {
        let config = TenantResolverConfig::default();
        assert!(
            !config.allow_header_only_resolution,
            "Default config should NOT allow header-only resolution for security"
        );
    }

    #[test]
    fn test_resolver_config_with_header_resolution() {
        let config = TenantResolverConfig::with_header_resolution();
        assert!(
            config.allow_header_only_resolution,
            "with_header_resolution should enable header-only resolution"
        );
    }

    // Note: Full middleware integration tests would require mocking the ControlPlane
    // and are better placed in integration tests. Here we test the core logic
    // that doesn't require async context.
}
