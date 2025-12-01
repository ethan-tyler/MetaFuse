//! Prometheus metrics for the MetaFuse catalog API
//!
//! This module is only compiled when the `metrics` feature is enabled.
//!
//! ## Global Metrics
//! - `http_requests_total` - Counter for total HTTP requests
//! - `http_request_duration_seconds` - Histogram for request latencies
//! - `catalog_operations_total` - Counter for catalog operations
//! - `catalog_datasets_total` - Gauge for total datasets in catalog
//!
//! ## Multi-Tenant Metrics
//!
//! When multi-tenant mode is enabled, additional tenant-labeled metrics are exposed:
//! - `tenant_http_requests_total` - Counter per tenant
//! - `tenant_http_request_duration_seconds` - Histogram per tenant
//! - `tenant_api_calls_total` - Counter per tenant and tier
//! - `tenant_rate_limit_hits_total` - Counter for rate limit rejections per tenant
//! - `tenant_backend_cache_hits_total` - Counter for backend cache hits/misses
//!
//! ## Connection Pool Metrics
//!
//! - `tenant_connection_wait_seconds` - Histogram for permit wait times
//! - `tenant_active_connections` - Gauge for active connections per tenant
//! - `tenant_connection_timeouts_total` - Counter for acquire timeouts
//! - `tenant_circuit_breaker_state` - Gauge for circuit breaker state (0=closed, 1=open)
//! - `tenant_circuit_breaker_trips_total` - Counter for circuit breaker trips
//!
//! ## Cardinality Warning
//!
//! Per-tenant metrics (those with `tenant_id` label) create a new Prometheus time series
//! for each unique tenant. In deployments with a large number of tenants (>1000), this
//! can lead to high memory usage and slow metric queries. Consider:
//! - Aggregating metrics at the tier level instead of tenant level
//! - Using metric relabeling to drop high-cardinality labels
//! - Implementing tenant metric rotation for inactive tenants

use axum::{
    extract::{Extension, MatchedPath, Request},
    http::StatusCode,
    middleware::Next,
    response::IntoResponse,
};
use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec, CounterVec,
    Encoder, Gauge, GaugeVec, HistogramVec, TextEncoder,
};
use std::time::Instant;

lazy_static! {
    // ==========================================================================
    // Global Metrics (backward compatible)
    // ==========================================================================

    /// Counter for total HTTP requests by method, path, and status
    pub static ref HTTP_REQUESTS_TOTAL: CounterVec = register_counter_vec!(
        "http_requests_total",
        "Total number of HTTP requests",
        &["method", "path", "status"]
    )
    .unwrap();

    /// Histogram for HTTP request duration in seconds
    pub static ref HTTP_REQUEST_DURATION_SECONDS: HistogramVec = register_histogram_vec!(
        "http_request_duration_seconds",
        "HTTP request latency in seconds",
        &["method", "path"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    )
    .unwrap();

    /// Counter for catalog operations (emit_dataset, search, etc.)
    pub static ref CATALOG_OPERATIONS_TOTAL: CounterVec = register_counter_vec!(
        "catalog_operations_total",
        "Total number of catalog operations",
        &["operation", "status"]
    )
    .unwrap();

    /// Gauge for total number of datasets in the catalog
    pub static ref CATALOG_DATASETS_TOTAL: Gauge = register_gauge!(
        "catalog_datasets_total",
        "Total number of datasets in the catalog"
    )
    .unwrap();

    // ==========================================================================
    // Multi-Tenant Metrics
    // ==========================================================================

    /// Counter for HTTP requests per tenant
    pub static ref TENANT_HTTP_REQUESTS_TOTAL: CounterVec = register_counter_vec!(
        "tenant_http_requests_total",
        "Total HTTP requests per tenant",
        &["tenant_id", "method", "path", "status"]
    )
    .unwrap();

    /// Histogram for HTTP request duration per tenant
    pub static ref TENANT_HTTP_REQUEST_DURATION_SECONDS: HistogramVec = register_histogram_vec!(
        "tenant_http_request_duration_seconds",
        "HTTP request latency per tenant in seconds",
        &["tenant_id", "method", "path"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    )
    .unwrap();

    /// Counter for API calls per tenant and tier
    pub static ref TENANT_API_CALLS_TOTAL: CounterVec = register_counter_vec!(
        "tenant_api_calls_total",
        "Total API calls per tenant with tier label",
        &["tenant_id", "tier", "operation"]
    )
    .unwrap();

    /// Counter for rate limit hits per tenant
    pub static ref TENANT_RATE_LIMIT_HITS_TOTAL: CounterVec = register_counter_vec!(
        "tenant_rate_limit_hits_total",
        "Rate limit rejections per tenant",
        &["tenant_id", "tier"]
    )
    .unwrap();

    /// Counter for tenant backend cache operations
    pub static ref TENANT_BACKEND_CACHE_TOTAL: CounterVec = register_counter_vec!(
        "tenant_backend_cache_total",
        "Tenant backend cache hits and misses",
        &["result"]  // "hit" or "miss"
    )
    .unwrap();

    /// Gauge for current number of cached tenant backends
    pub static ref TENANT_BACKEND_CACHE_SIZE: Gauge = register_gauge!(
        "tenant_backend_cache_size",
        "Current number of cached tenant backends"
    )
    .unwrap();

    /// Gauge for datasets per tenant
    pub static ref TENANT_DATASETS_TOTAL: GaugeVec = register_gauge_vec!(
        "tenant_datasets_total",
        "Total datasets per tenant",
        &["tenant_id"]
    )
    .unwrap();

    /// Counter for tenant lifecycle events
    pub static ref TENANT_LIFECYCLE_EVENTS_TOTAL: CounterVec = register_counter_vec!(
        "tenant_lifecycle_events_total",
        "Tenant lifecycle events",
        &["event"]  // "created", "suspended", "reactivated", "deleted", "purged"
    )
    .unwrap();

    // ==========================================================================
    // Connection Pool Metrics
    // ==========================================================================

    /// Histogram for connection permit wait times per tenant
    pub static ref TENANT_CONNECTION_WAIT_SECONDS: HistogramVec = register_histogram_vec!(
        "tenant_connection_wait_seconds",
        "Time spent waiting for a connection permit",
        &["tenant_id"],
        vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0]
    )
    .unwrap();

    /// Gauge for active connections per tenant
    pub static ref TENANT_ACTIVE_CONNECTIONS: GaugeVec = register_gauge_vec!(
        "tenant_active_connections",
        "Current number of active connections per tenant",
        &["tenant_id"]
    )
    .unwrap();

    /// Counter for connection acquire timeouts per tenant
    pub static ref TENANT_CONNECTION_TIMEOUTS_TOTAL: CounterVec = register_counter_vec!(
        "tenant_connection_timeouts_total",
        "Total connection acquire timeouts per tenant",
        &["tenant_id"]
    )
    .unwrap();

    /// Gauge for circuit breaker state per tenant (0=closed, 1=open)
    pub static ref TENANT_CIRCUIT_BREAKER_STATE: GaugeVec = register_gauge_vec!(
        "tenant_circuit_breaker_state",
        "Circuit breaker state per tenant (0=closed, 1=open)",
        &["tenant_id"]
    )
    .unwrap();

    /// Counter for circuit breaker trips per tenant
    pub static ref TENANT_CIRCUIT_BREAKER_TRIPS_TOTAL: CounterVec = register_counter_vec!(
        "tenant_circuit_breaker_trips_total",
        "Total circuit breaker trips per tenant",
        &["tenant_id"]
    )
    .unwrap();
}

// =============================================================================
// Cardinality Configuration
// =============================================================================

/// Configuration for tenant metrics cardinality control.
///
/// Controls whether per-tenant metrics include the actual `tenant_id` label
/// or use an aggregated placeholder to prevent cardinality explosion.
///
/// # Cardinality Warning
///
/// Setting `include_tenant_id = true` will create a new Prometheus time series
/// for each unique tenant. With many tenants (>1000), this can cause:
/// - High memory usage in Prometheus
/// - Slow metric queries
/// - Storage bloat
///
/// Only enable per-tenant labels in environments with:
/// - Small number of tenants
/// - Appropriate metric retention policies
/// - Sufficient Prometheus resources
#[derive(Clone, Debug)]
pub struct TenantMetricsConfig {
    /// When true, include actual tenant_id in metric labels.
    /// When false (default), use "aggregated" placeholder for cardinality safety.
    pub include_tenant_id: bool,
}

impl Default for TenantMetricsConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl TenantMetricsConfig {
    /// Create configuration from environment variables.
    ///
    /// # Environment Variables
    ///
    /// - `METAFUSE_TENANT_METRICS_INCLUDE_ID`: Set to "true" to enable per-tenant labels.
    ///   Default: "false" (tier-level aggregation only)
    pub fn from_env() -> Self {
        Self {
            include_tenant_id: std::env::var("METAFUSE_TENANT_METRICS_INCLUDE_ID")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false),
        }
    }

    /// Create configuration with per-tenant labels enabled.
    ///
    /// **Warning**: This can cause cardinality explosion with many tenants.
    #[allow(dead_code)]
    pub fn with_tenant_labels() -> Self {
        Self {
            include_tenant_id: true,
        }
    }

    /// Create configuration with tier-level aggregation (no per-tenant labels).
    #[allow(dead_code)]
    pub fn aggregated() -> Self {
        Self {
            include_tenant_id: false,
        }
    }
}

// =============================================================================
// Tenant Metrics Info
// =============================================================================

/// Tenant info extracted from request for metrics.
///
/// Used to label Prometheus metrics with tenant information.
/// The actual tenant_id vs "aggregated" placeholder is controlled by
/// `TenantMetricsConfig`.
#[derive(Clone, Debug)]
pub struct TenantMetricsInfo {
    pub tenant_id: String,
    pub tier: String,
}

impl TenantMetricsInfo {
    /// Create tenant metrics info with cardinality-aware tenant_id.
    ///
    /// If `config.include_tenant_id` is false, uses "aggregated" placeholder
    /// instead of the actual tenant_id to prevent metric cardinality explosion.
    pub fn new(tenant_id: &str, tier: &str, config: &TenantMetricsConfig) -> Self {
        Self {
            tenant_id: if config.include_tenant_id {
                tenant_id.to_string()
            } else {
                "aggregated".to_string()
            },
            tier: tier.to_string(),
        }
    }

    /// Create tenant metrics info with explicit tenant_id (no cardinality control).
    ///
    /// Use this only when you need the actual tenant_id regardless of config.
    #[allow(dead_code)]
    pub fn with_explicit_tenant_id(tenant_id: &str, tier: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            tier: tier.to_string(),
        }
    }
}

// =============================================================================
// Tenant Metrics Injection Middleware
// =============================================================================

/// Middleware to inject `TenantMetricsInfo` into request extensions.
///
/// This middleware extracts tenant information from `ResolvedTenant` (if present)
/// and creates `TenantMetricsInfo` for downstream metrics recording.
///
/// **Must run AFTER** `tenant_resolver_middleware` which populates `ResolvedTenant`.
///
/// # Cardinality Control
///
/// Uses `TenantMetricsConfig` to control whether the actual `tenant_id` or
/// "aggregated" placeholder is used in metrics labels.
///
/// # Example
///
/// ```ignore
/// // In main.rs middleware stack (layers execute in reverse order):
/// app.layer(middleware::from_fn(track_metrics))
///    .layer(middleware::from_fn(tenant_metrics_middleware))  // <-- This middleware
///    .layer(middleware::from_fn(tenant_resolver_middleware))
/// ```
#[cfg(feature = "api-keys")]
pub async fn tenant_metrics_middleware(
    config: Option<Extension<TenantMetricsConfig>>,
    mut req: Request,
    next: Next,
) -> impl IntoResponse {
    use crate::tenant_resolver::ResolvedTenant;

    let metrics_config = config.map(|Extension(c)| c).unwrap_or_default();

    // Extract tenant info from ResolvedTenant if present
    if let Some(resolved) = req.extensions().get::<ResolvedTenant>() {
        let tier_str = resolved
            .tier()
            .map(|t| format!("{:?}", t).to_lowercase())
            .unwrap_or_else(|| "unknown".to_string());

        let metrics_info = TenantMetricsInfo::new(resolved.tenant_id(), &tier_str, &metrics_config);

        tracing::debug!(
            tenant_id = %resolved.tenant_id(),
            tier = %tier_str,
            metrics_tenant_id = %metrics_info.tenant_id,
            "Injected TenantMetricsInfo for metrics recording"
        );

        req.extensions_mut().insert(metrics_info);
    }

    next.run(req).await
}

/// Axum middleware to track HTTP request metrics
///
/// Records both global and tenant-specific metrics when tenant info is available.
pub async fn track_metrics(req: Request, next: Next) -> impl IntoResponse {
    let start = Instant::now();
    let method = req.method().to_string();
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());

    // Extract tenant info if present
    let tenant_info = req.extensions().get::<TenantMetricsInfo>().cloned();

    let response = next.run(req).await;
    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    // Record global metrics (always)
    HTTP_REQUESTS_TOTAL
        .with_label_values(&[&method, &path, &status])
        .inc();

    HTTP_REQUEST_DURATION_SECONDS
        .with_label_values(&[&method, &path])
        .observe(duration);

    // Record tenant-specific metrics if tenant info is available
    if let Some(info) = tenant_info {
        TENANT_HTTP_REQUESTS_TOTAL
            .with_label_values(&[&info.tenant_id, &method, &path, &status])
            .inc();

        TENANT_HTTP_REQUEST_DURATION_SECONDS
            .with_label_values(&[&info.tenant_id, &method, &path])
            .observe(duration);
    }

    response
}

/// Handler for the `/metrics` endpoint
pub async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];

    match encoder.encode(&metric_families, &mut buffer) {
        Ok(_) => (
            StatusCode::OK,
            [("content-type", encoder.format_type())],
            buffer,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to encode metrics: {}", e),
        )
            .into_response(),
    }
}

/// Record a catalog operation metric
pub fn record_catalog_operation(operation: &str, status: &str) {
    CATALOG_OPERATIONS_TOTAL
        .with_label_values(&[operation, status])
        .inc();
}

/// Update the total datasets gauge
#[allow(dead_code)]
pub fn update_datasets_total(count: i64) {
    CATALOG_DATASETS_TOTAL.set(count as f64);
}

// =============================================================================
// Multi-Tenant Metrics Helper Functions
// =============================================================================

/// Record a tenant API call
pub fn record_tenant_api_call(tenant_id: &str, tier: &str, operation: &str) {
    TENANT_API_CALLS_TOTAL
        .with_label_values(&[tenant_id, tier, operation])
        .inc();
}

/// Record a tenant rate limit hit
pub fn record_tenant_rate_limit_hit(tenant_id: &str, tier: &str) {
    TENANT_RATE_LIMIT_HITS_TOTAL
        .with_label_values(&[tenant_id, tier])
        .inc();
}

/// Record a tenant backend cache hit
pub fn record_tenant_backend_cache_hit() {
    TENANT_BACKEND_CACHE_TOTAL.with_label_values(&["hit"]).inc();
}

/// Record a tenant backend cache miss
pub fn record_tenant_backend_cache_miss() {
    TENANT_BACKEND_CACHE_TOTAL
        .with_label_values(&["miss"])
        .inc();
}

/// Update the tenant backend cache size gauge
pub fn update_tenant_backend_cache_size(size: usize) {
    TENANT_BACKEND_CACHE_SIZE.set(size as f64);
}

/// Update the datasets count for a specific tenant
pub fn update_tenant_datasets_total(tenant_id: &str, count: i64) {
    TENANT_DATASETS_TOTAL
        .with_label_values(&[tenant_id])
        .set(count as f64);
}

/// Record a tenant lifecycle event
pub fn record_tenant_lifecycle_event(event: &str) {
    TENANT_LIFECYCLE_EVENTS_TOTAL
        .with_label_values(&[event])
        .inc();
}

/// Convenience function to record tenant creation
pub fn record_tenant_created() {
    record_tenant_lifecycle_event("created");
}

/// Convenience function to record tenant suspension
pub fn record_tenant_suspended() {
    record_tenant_lifecycle_event("suspended");
}

/// Convenience function to record tenant reactivation
pub fn record_tenant_reactivated() {
    record_tenant_lifecycle_event("reactivated");
}

/// Convenience function to record tenant deletion
pub fn record_tenant_deleted() {
    record_tenant_lifecycle_event("deleted");
}

/// Convenience function to record tenant purge
pub fn record_tenant_purged() {
    record_tenant_lifecycle_event("purged");
}

// =============================================================================
// Connection Pool Metrics Helper Functions
// =============================================================================

/// Record time spent waiting for a connection permit
pub fn record_connection_wait_time(tenant_id: &str, duration_secs: f64) {
    TENANT_CONNECTION_WAIT_SECONDS
        .with_label_values(&[tenant_id])
        .observe(duration_secs);
}

/// Update the active connection count for a tenant
pub fn update_active_connections(tenant_id: &str, count: usize) {
    TENANT_ACTIVE_CONNECTIONS
        .with_label_values(&[tenant_id])
        .set(count as f64);
}

/// Record a connection acquire timeout
pub fn record_connection_timeout(tenant_id: &str) {
    TENANT_CONNECTION_TIMEOUTS_TOTAL
        .with_label_values(&[tenant_id])
        .inc();
}

/// Update circuit breaker state for a tenant (false=closed, true=open)
pub fn update_circuit_breaker_state(tenant_id: &str, is_open: bool) {
    TENANT_CIRCUIT_BREAKER_STATE
        .with_label_values(&[tenant_id])
        .set(if is_open { 1.0 } else { 0.0 });
}

/// Record a circuit breaker trip
pub fn record_circuit_breaker_trip(tenant_id: &str) {
    TENANT_CIRCUIT_BREAKER_TRIPS_TOTAL
        .with_label_values(&[tenant_id])
        .inc();
    update_circuit_breaker_state(tenant_id, true);
}
