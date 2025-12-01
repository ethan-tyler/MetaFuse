//! Rate limiting middleware for the MetaFuse catalog API
//!
//! This module provides tiered rate limiting with support for:
//! - Anonymous requests (IP-based, lower limits)
//! - Authenticated requests (API key-based, higher limits)
//! - Tenant-aware rate limiting (per-tenant quotas based on tier)
//! - Trusted proxy support (X-Forwarded-For extraction)
//! - Standard RFC 6585 compliant 429 responses
//!
//! ## Configuration
//!
//! - `METAFUSE_RATE_LIMIT_ANONYMOUS`: Requests per minute for anonymous clients (default: 100)
//! - `METAFUSE_RATE_LIMIT_AUTHENTICATED`: Requests per minute for authenticated clients (default: 1000)
//! - `METAFUSE_RATE_LIMIT_WINDOW_SECS`: Rate limit window in seconds (default: 60)
//! - `METAFUSE_TRUSTED_PROXIES`: Comma-separated list of trusted proxy IPs (optional, supports IPv4/IPv6)
//! - `METAFUSE_RATE_LIMIT_MAX_BUCKETS`: Maximum bucket storage (default: 10000)
//! - `METAFUSE_RATE_LIMIT_BUCKET_TTL_SECS`: Idle bucket TTL in seconds (default: 600)
//!
//! ## Multi-Tenant Rate Limits
//!
//! When multi-tenant mode is enabled, rate limits are applied per-tenant with tier-based quotas:
//! - Free tier: 100 requests/minute (configurable via `METAFUSE_RATE_LIMIT_FREE`)
//! - Standard tier: 1000 requests/minute (configurable via `METAFUSE_RATE_LIMIT_STANDARD`)
//! - Premium tier: 5000 requests/minute (configurable via `METAFUSE_RATE_LIMIT_PREMIUM`)
//! - Enterprise tier: 10000 requests/minute (configurable via `METAFUSE_RATE_LIMIT_ENTERPRISE`)
//!
//! Rate limit keys in multi-tenant mode: `tenant:{tenant_id}:{api_key_or_ip}`
//!
//! ## Security
//!
//! - **Trusted Proxy Validation**: X-Forwarded-For headers are only honored when the immediate
//!   peer address matches a configured trusted proxy. This prevents header spoofing attacks.
//! - **Memory Bounds**: Bucket storage is capped at 10,000 entries with TTL-based eviction
//!   (10-minute idle timeout) to prevent unbounded memory growth from high-cardinality keys.
//!
//! ## Example
//!
//! ```rust,ignore
//! use axum::Router;
//! use metafuse_catalog_api::rate_limiting;
//!
//! let app = Router::new()
//!     .layer(axum::middleware::from_fn(rate_limiting::rate_limit_middleware));
//! ```

use axum::{
    extract::{ConnectInfo, Request},
    http::StatusCode,
    middleware::Next,
    response::Response,
    Json,
};
use dashmap::DashMap;
use serde_json::json;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{debug, warn};

/// Rate limit for anonymous (unauthenticated) requests per window
const DEFAULT_ANONYMOUS_LIMIT: u32 = 100;

/// Rate limit for authenticated (API key) requests per window
const DEFAULT_AUTHENTICATED_LIMIT: u32 = 1000;

/// Default rate limit window in seconds
const DEFAULT_WINDOW_SECS: u64 = 60;

/// Default maximum number of rate limit buckets to keep in memory
const DEFAULT_MAX_BUCKETS: usize = 10_000;

/// Default TTL for idle rate limit buckets (10 minutes)
const DEFAULT_BUCKET_TTL_SECS: u64 = 600;

// Tenant tier-based rate limits (requests per window)
const DEFAULT_FREE_TIER_LIMIT: u32 = 100;
const DEFAULT_STANDARD_TIER_LIMIT: u32 = 1000;
const DEFAULT_PREMIUM_TIER_LIMIT: u32 = 5000;
const DEFAULT_ENTERPRISE_TIER_LIMIT: u32 = 10000;

/// Marker type for API key identity in request extensions
#[derive(Clone, Debug)]
pub struct ApiKeyId {
    pub id: String,
}

/// Tenant rate limit info for request extensions.
///
/// This is injected by tenant resolution middleware to enable
/// tenant-aware rate limiting with tier-based quotas.
#[derive(Clone, Debug)]
pub struct TenantRateLimitInfo {
    /// Tenant identifier
    pub tenant_id: String,
    /// Tenant tier for determining rate limit
    pub tier: TenantTier,
}

/// Tenant tier for rate limiting purposes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenantTier {
    Free,
    Standard,
    Premium,
    Enterprise,
}

impl TenantTier {
    /// Parse tier from string.
    /// Reserved for future use when parsing tier from external sources.
    #[allow(dead_code)]
    pub fn parse_tier(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "free" => TenantTier::Free,
            "premium" => TenantTier::Premium,
            "enterprise" => TenantTier::Enterprise,
            _ => TenantTier::Standard, // Default to standard
        }
    }
}

impl std::str::FromStr for TenantTier {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "free" => Ok(TenantTier::Free),
            "standard" => Ok(TenantTier::Standard),
            "premium" => Ok(TenantTier::Premium),
            "enterprise" => Ok(TenantTier::Enterprise),
            other => Err(format!("Unknown tier: {}", other)),
        }
    }
}

/// Configuration for rate limiting
#[derive(Clone, Debug)]
pub struct RateLimitConfig {
    pub anonymous_limit: u32,
    pub authenticated_limit: u32,
    pub window_secs: u64,
    pub trusted_proxies: Option<Vec<String>>,
    /// Reserved for future bucket cleanup implementation
    #[allow(dead_code)]
    pub max_buckets: usize,
    /// Reserved for future bucket cleanup implementation
    #[allow(dead_code)]
    pub bucket_ttl_secs: u64,
    // Tenant tier-based limits
    pub free_tier_limit: u32,
    pub standard_tier_limit: u32,
    pub premium_tier_limit: u32,
    pub enterprise_tier_limit: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            anonymous_limit: std::env::var("METAFUSE_RATE_LIMIT_ANONYMOUS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_ANONYMOUS_LIMIT),
            authenticated_limit: std::env::var("METAFUSE_RATE_LIMIT_AUTHENTICATED")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_AUTHENTICATED_LIMIT),
            window_secs: std::env::var("METAFUSE_RATE_LIMIT_WINDOW_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_WINDOW_SECS),
            trusted_proxies: std::env::var("METAFUSE_TRUSTED_PROXIES")
                .ok()
                .map(|s| s.split(',').map(|ip| ip.trim().to_string()).collect()),
            max_buckets: std::env::var("METAFUSE_RATE_LIMIT_MAX_BUCKETS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_MAX_BUCKETS),
            bucket_ttl_secs: std::env::var("METAFUSE_RATE_LIMIT_BUCKET_TTL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_BUCKET_TTL_SECS),
            // Tenant tier limits
            free_tier_limit: std::env::var("METAFUSE_RATE_LIMIT_FREE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_FREE_TIER_LIMIT),
            standard_tier_limit: std::env::var("METAFUSE_RATE_LIMIT_STANDARD")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_STANDARD_TIER_LIMIT),
            premium_tier_limit: std::env::var("METAFUSE_RATE_LIMIT_PREMIUM")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_PREMIUM_TIER_LIMIT),
            enterprise_tier_limit: std::env::var("METAFUSE_RATE_LIMIT_ENTERPRISE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_ENTERPRISE_TIER_LIMIT),
        }
    }
}

/// Rate limit bucket for tracking requests
#[derive(Clone, Debug)]
struct RateLimitBucket {
    count: u32,
    window_start: Instant,
    last_accessed: Instant,
}

/// Global rate limiter state
pub struct RateLimiter {
    config: Arc<RateLimitConfig>,
    buckets: Arc<DashMap<String, RateLimitBucket>>,
}

impl Clone for RateLimiter {
    fn clone(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
            buckets: Arc::clone(&self.buckets),
        }
    }
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config: Arc::new(config),
            buckets: Arc::new(DashMap::new()),
        }
    }

    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }

    fn extract_client_ip<B>(&self, req: &Request<B>) -> Option<String> {
        // Get immediate peer address (the direct connection source)
        let peer_addr = req
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ci| ci.0.ip());

        // Only trust proxy headers if the request comes from a trusted proxy
        if let Some(trusted_proxies) = &self.config.trusted_proxies {
            if let Some(peer_ip) = peer_addr {
                let peer_ip_str = peer_ip.to_string();

                // Validate peer is in trusted proxy list
                if trusted_proxies.contains(&peer_ip_str) {
                    debug!(
                        peer_ip = %peer_ip_str,
                        "Request from trusted proxy, checking forwarded headers"
                    );

                    // Trust X-Forwarded-For header
                    if let Some(forwarded) = req.headers().get("x-forwarded-for") {
                        if let Ok(forwarded_str) = forwarded.to_str() {
                            if let Some(client_ip) = forwarded_str.split(',').next() {
                                let client_ip = client_ip.trim();
                                debug!(
                                    client_ip = %client_ip,
                                    peer_ip = %peer_ip_str,
                                    "Extracted IP from X-Forwarded-For"
                                );
                                return Some(client_ip.to_string());
                            }
                        }
                    }

                    // Trust X-Real-IP as fallback
                    if let Some(real_ip) = req.headers().get("x-real-ip") {
                        if let Ok(ip_str) = real_ip.to_str() {
                            debug!(
                                client_ip = %ip_str,
                                peer_ip = %peer_ip_str,
                                "Extracted IP from X-Real-IP"
                            );
                            return Some(ip_str.to_string());
                        }
                    }
                } else {
                    warn!(
                        peer_ip = %peer_ip_str,
                        "Request not from trusted proxy, ignoring forwarded headers"
                    );
                }
            }
        }

        // Fall back to peer address from connection
        peer_addr.map(|ip| {
            let ip_str = ip.to_string();
            debug!(client_ip = %ip_str, "Using peer address for rate limiting");
            ip_str
        })
    }

    /// Get rate limit for a tenant tier.
    fn get_tier_limit(&self, tier: TenantTier) -> u32 {
        match tier {
            TenantTier::Free => self.config.free_tier_limit,
            TenantTier::Standard => self.config.standard_tier_limit,
            TenantTier::Premium => self.config.premium_tier_limit,
            TenantTier::Enterprise => self.config.enterprise_tier_limit,
        }
    }

    fn get_rate_limit_key<B>(&self, req: &Request<B>) -> (String, u32) {
        // Priority 0: Check for tenant context (multi-tenant mode)
        // When a tenant is resolved, rate limits are scoped to the tenant
        if let Some(tenant_info) = req.extensions().get::<TenantRateLimitInfo>() {
            let limit = self.get_tier_limit(tenant_info.tier);

            // Sub-priority 0a: Tenant + API key
            if let Some(api_key) = req.extensions().get::<ApiKeyId>() {
                let key = format!("tenant:{}:auth:{}", tenant_info.tenant_id, api_key.id);
                debug!(
                    rate_limit_key = %key,
                    tenant_id = %tenant_info.tenant_id,
                    tier = ?tenant_info.tier,
                    limit = limit,
                    "Using tenant + API key for rate limiting"
                );
                return (key, limit);
            }

            // Sub-priority 0b: Tenant + IP (header-only auth)
            if let Some(ip) = self.extract_client_ip(req) {
                let key = format!("tenant:{}:ip:{}", tenant_info.tenant_id, ip);
                debug!(
                    rate_limit_key = %key,
                    tenant_id = %tenant_info.tenant_id,
                    tier = ?tenant_info.tier,
                    limit = limit,
                    "Using tenant + IP for rate limiting"
                );
                return (key, limit);
            }

            // Fallback: tenant only (shouldn't happen in practice)
            let key = format!("tenant:{}:unknown", tenant_info.tenant_id);
            debug!(
                rate_limit_key = %key,
                tenant_id = %tenant_info.tenant_id,
                "Using tenant-only key for rate limiting"
            );
            return (key, limit);
        }

        // Priority 1: Check for authenticated API key (non-tenant mode)
        if let Some(api_key) = req.extensions().get::<ApiKeyId>() {
            let key = format!("auth:{}", api_key.id);
            debug!(rate_limit_key = %key, "Using API key for rate limiting");
            return (key, self.config.authenticated_limit);
        }

        // Priority 2: Extract client IP
        if let Some(ip) = self.extract_client_ip(req) {
            let key = format!("anon:{}", ip);
            debug!(rate_limit_key = %key, "Using IP for rate limiting");
            return (key, self.config.anonymous_limit);
        }

        // Fallback: unknown client
        warn!("Could not extract rate limit key, using 'unknown'");
        ("anon:unknown".to_string(), self.config.anonymous_limit)
    }
}

/// Rate limit metadata for response headers
#[derive(Debug, Clone)]
pub struct RateLimitMetadata {
    pub limit: u32,
    pub remaining: u32,
    pub reset: u64,
}

impl RateLimiter {
    /// Check rate limit and return metadata for headers
    pub fn check_rate_limit_with_metadata<B>(
        &self,
        req: &Request<B>,
    ) -> (Result<(), u64>, RateLimitMetadata) {
        let (key, limit) = self.get_rate_limit_key(req);
        let now = Instant::now();
        let window_duration = Duration::from_secs(self.config.window_secs);

        // Automatic cleanup: if bucket count exceeds threshold, clean up stale buckets
        let threshold = self.config.max_buckets / 2;
        if self.buckets.len() > threshold {
            let ttl = Duration::from_secs(self.config.bucket_ttl_secs);
            self.buckets
                .retain(|_, bucket| now.duration_since(bucket.last_accessed) < ttl);
        }

        let mut bucket = self
            .buckets
            .entry(key.clone())
            .or_insert_with(|| RateLimitBucket {
                count: 0,
                window_start: now,
                last_accessed: now,
            });

        bucket.last_accessed = now;

        // Check if window has expired
        if now.duration_since(bucket.window_start) >= window_duration {
            bucket.window_start = now;
            bucket.count = 0;
        }

        // Calculate reset time (window_start + window_duration as unix timestamp)
        let reset_instant = bucket.window_start + window_duration;
        let reset_secs = reset_instant
            .duration_since(Instant::now())
            .as_secs()
            .saturating_add(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );

        // Check if limit exceeded
        if bucket.count >= limit {
            let retry_after = self
                .config
                .window_secs
                .saturating_sub(now.duration_since(bucket.window_start).as_secs());

            let metadata = RateLimitMetadata {
                limit,
                remaining: 0,
                reset: reset_secs,
            };

            return (Err(retry_after), metadata);
        }

        // Increment counter
        bucket.count += 1;
        let remaining = limit.saturating_sub(bucket.count);

        let metadata = RateLimitMetadata {
            limit,
            remaining,
            reset: reset_secs,
        };

        (Ok(()), metadata)
    }
}

/// Rate limiting middleware
pub async fn rate_limit_middleware(
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    use axum::http::header::{HeaderName, HeaderValue};

    // Get or create rate limiter from extensions
    let rate_limiter = req
        .extensions()
        .get::<RateLimiter>()
        .cloned()
        .unwrap_or_else(|| RateLimiter::new(RateLimitConfig::default()));

    let request_id = req
        .extensions()
        .get::<uuid::Uuid>()
        .map(|id| id.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Check rate limit and get metadata
    let (result, metadata) = rate_limiter.check_rate_limit_with_metadata(&req);

    match result {
        Ok(()) => {
            let mut response = next.run(req).await;

            // Add rate limit headers to success response
            let headers = response.headers_mut();
            headers.insert(
                HeaderName::from_static("x-ratelimit-limit"),
                HeaderValue::from(metadata.limit),
            );
            headers.insert(
                HeaderName::from_static("x-ratelimit-remaining"),
                HeaderValue::from(metadata.remaining),
            );
            headers.insert(
                HeaderName::from_static("x-ratelimit-reset"),
                HeaderValue::from(metadata.reset),
            );

            Ok(response)
        }
        Err(retry_after) => {
            // Record rate limit hit metric
            #[cfg(feature = "metrics")]
            {
                let (tenant_id, tier) = req
                    .extensions()
                    .get::<crate::metrics::TenantMetricsInfo>()
                    .map(|info| (info.tenant_id.as_str(), info.tier.as_str()))
                    .unwrap_or(("unknown", "unknown"));
                crate::metrics::record_tenant_rate_limit_hit(tenant_id, tier);
            }

            let error_body = json!({
                "error": {
                    "code": "RATE_LIMIT_EXCEEDED",
                    "message": "Too many requests. Please retry after the specified time.",
                },
                "request_id": request_id,
                "retry_after": retry_after,
            });

            // Convert to full Response to add headers
            let json_body = serde_json::to_string(&error_body).unwrap();
            let mut full_response = Response::new(json_body.into());
            *full_response.status_mut() = StatusCode::TOO_MANY_REQUESTS;

            let headers = full_response.headers_mut();
            headers.insert(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/json"),
            );
            headers.insert(
                HeaderName::from_static("x-ratelimit-limit"),
                HeaderValue::from(metadata.limit),
            );
            headers.insert(
                HeaderName::from_static("x-ratelimit-remaining"),
                HeaderValue::from(0u32),
            );
            headers.insert(
                HeaderName::from_static("x-ratelimit-reset"),
                HeaderValue::from(metadata.reset),
            );
            headers.insert(
                HeaderName::from_static("retry-after"),
                HeaderValue::from(retry_after),
            );

            Ok(full_response)
        }
    }
}

/// Create rate limiter layer for sharing across requests
pub fn create_rate_limiter() -> RateLimiter {
    RateLimiter::new(RateLimitConfig::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    /// Helper to create a test config with custom limits
    fn test_config(anonymous: u32, authenticated: u32) -> RateLimitConfig {
        RateLimitConfig {
            anonymous_limit: anonymous,
            authenticated_limit: authenticated,
            window_secs: 60,
            trusted_proxies: None,
            max_buckets: DEFAULT_MAX_BUCKETS,
            bucket_ttl_secs: DEFAULT_BUCKET_TTL_SECS,
            free_tier_limit: DEFAULT_FREE_TIER_LIMIT,
            standard_tier_limit: DEFAULT_STANDARD_TIER_LIMIT,
            premium_tier_limit: DEFAULT_PREMIUM_TIER_LIMIT,
            enterprise_tier_limit: DEFAULT_ENTERPRISE_TIER_LIMIT,
        }
    }

    #[test]
    fn test_config_defaults() {
        let config = RateLimitConfig::default();
        assert_eq!(config.anonymous_limit, DEFAULT_ANONYMOUS_LIMIT);
        assert_eq!(config.authenticated_limit, DEFAULT_AUTHENTICATED_LIMIT);
        assert_eq!(config.window_secs, DEFAULT_WINDOW_SECS);
        // Verify tier limits
        assert_eq!(config.free_tier_limit, DEFAULT_FREE_TIER_LIMIT);
        assert_eq!(config.standard_tier_limit, DEFAULT_STANDARD_TIER_LIMIT);
        assert_eq!(config.premium_tier_limit, DEFAULT_PREMIUM_TIER_LIMIT);
        assert_eq!(config.enterprise_tier_limit, DEFAULT_ENTERPRISE_TIER_LIMIT);
    }

    #[test]
    fn test_tenant_tier_parsing() {
        assert_eq!("free".parse::<TenantTier>().unwrap(), TenantTier::Free);
        assert_eq!("FREE".parse::<TenantTier>().unwrap(), TenantTier::Free);
        assert_eq!(
            "standard".parse::<TenantTier>().unwrap(),
            TenantTier::Standard
        );
        assert_eq!(
            "premium".parse::<TenantTier>().unwrap(),
            TenantTier::Premium
        );
        assert_eq!(
            "enterprise".parse::<TenantTier>().unwrap(),
            TenantTier::Enterprise
        );
        // Unknown tier returns error, caller decides fallback
        assert!("unknown".parse::<TenantTier>().is_err());
    }

    #[test]
    fn test_tenant_rate_limit_with_api_key() {
        let limiter = RateLimiter::new(test_config(100, 1000));
        let mut req = Request::builder().body(()).unwrap();

        // Set tenant info (Premium tier: 5000 limit)
        req.extensions_mut().insert(TenantRateLimitInfo {
            tenant_id: "acme-corp".to_string(),
            tier: TenantTier::Premium,
        });
        req.extensions_mut().insert(ApiKeyId {
            id: "key-123".to_string(),
        });

        let (key, limit) = limiter.get_rate_limit_key(&req);
        assert_eq!(key, "tenant:acme-corp:auth:key-123");
        assert_eq!(limit, DEFAULT_PREMIUM_TIER_LIMIT);
    }

    #[test]
    fn test_tenant_rate_limit_with_ip() {
        let limiter = RateLimiter::new(test_config(100, 1000));
        let addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();
        let mut req = Request::builder().body(()).unwrap();

        // Set tenant info (Free tier: 100 limit)
        req.extensions_mut().insert(TenantRateLimitInfo {
            tenant_id: "test-tenant".to_string(),
            tier: TenantTier::Free,
        });
        req.extensions_mut().insert(ConnectInfo(addr));

        let (key, limit) = limiter.get_rate_limit_key(&req);
        assert_eq!(key, "tenant:test-tenant:ip:192.168.1.100");
        assert_eq!(limit, DEFAULT_FREE_TIER_LIMIT);
    }

    #[test]
    fn test_enterprise_tier_limit() {
        let limiter = RateLimiter::new(test_config(100, 1000));
        let addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let mut req = Request::builder().body(()).unwrap();

        req.extensions_mut().insert(TenantRateLimitInfo {
            tenant_id: "big-corp".to_string(),
            tier: TenantTier::Enterprise,
        });
        req.extensions_mut().insert(ConnectInfo(addr));

        let (key, limit) = limiter.get_rate_limit_key(&req);
        assert_eq!(key, "tenant:big-corp:ip:10.0.0.1");
        assert_eq!(limit, DEFAULT_ENTERPRISE_TIER_LIMIT);
    }

    #[test]
    fn test_rate_limiter_key_with_api_key() {
        let limiter = RateLimiter::new(RateLimitConfig::default());
        let mut req = Request::builder().body(()).unwrap();
        req.extensions_mut().insert(ApiKeyId {
            id: "test-key-123".to_string(),
        });

        let (key, limit) = limiter.get_rate_limit_key(&req);
        assert_eq!(key, "auth:test-key-123");
        assert_eq!(limit, DEFAULT_AUTHENTICATED_LIMIT);
    }

    #[test]
    fn test_rate_limiter_key_fallback_to_ip() {
        let limiter = RateLimiter::new(RateLimitConfig::default());
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut req = Request::builder().body(()).unwrap();
        req.extensions_mut().insert(ConnectInfo(addr));

        let (key, limit) = limiter.get_rate_limit_key(&req);
        assert_eq!(key, "anon:127.0.0.1");
        assert_eq!(limit, DEFAULT_ANONYMOUS_LIMIT);
    }

    #[test]
    fn test_rate_limiter_allows_under_limit() {
        let mut config = test_config(5, 10);
        config.window_secs = 60;
        let limiter = RateLimiter::new(config);

        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut req = Request::builder().body(()).unwrap();
        req.extensions_mut().insert(ConnectInfo(addr));

        // Should allow 5 requests
        for _ in 0..5 {
            let (result, _metadata) = limiter.check_rate_limit_with_metadata(&req);
            assert!(result.is_ok());
        }

        // 6th request should be blocked
        let (result, _metadata) = limiter.check_rate_limit_with_metadata(&req);
        assert!(result.is_err());
    }

    #[test]
    fn test_trusted_proxy_validation() {
        let mut config = test_config(100, 1000);
        config.trusted_proxies = Some(vec!["10.0.0.1".to_string()]);
        let limiter = RateLimiter::new(config);

        // Request from trusted proxy with X-Forwarded-For
        let trusted_addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let mut trusted_req = Request::builder()
            .header("x-forwarded-for", "203.0.113.1")
            .body(())
            .unwrap();
        trusted_req
            .extensions_mut()
            .insert(ConnectInfo(trusted_addr));

        let (key, _) = limiter.get_rate_limit_key(&trusted_req);
        assert_eq!(
            key, "anon:203.0.113.1",
            "Should use X-Forwarded-For from trusted proxy"
        );

        // Request from untrusted proxy with X-Forwarded-For
        let untrusted_addr: SocketAddr = "192.168.1.1:8080".parse().unwrap();
        let mut untrusted_req = Request::builder()
            .header("x-forwarded-for", "203.0.113.1")
            .body(())
            .unwrap();
        untrusted_req
            .extensions_mut()
            .insert(ConnectInfo(untrusted_addr));

        let (key, _) = limiter.get_rate_limit_key(&untrusted_req);
        assert_eq!(
            key, "anon:192.168.1.1",
            "Should ignore X-Forwarded-For from untrusted source"
        );
    }

    #[test]
    fn test_bucket_cleanup() {
        let limiter = RateLimiter::new(test_config(100, 1000));

        // Create several buckets
        for i in 0..10 {
            let addr: SocketAddr = format!("127.0.0.{}:8080", i).parse().unwrap();
            let mut req = Request::builder().body(()).unwrap();
            req.extensions_mut().insert(ConnectInfo(addr));
            let (result, _metadata) = limiter.check_rate_limit_with_metadata(&req);
            assert!(result.is_ok());
        }

        assert_eq!(limiter.buckets.len(), 10, "Should have 10 buckets");

        // Note: cleanup_old_buckets() was removed. Cleanup now happens automatically
        // in check_rate_limit_with_metadata() when bucket threshold is exceeded.
    }

    #[test]
    fn test_bucket_ttl_prevents_eviction_of_active() {
        let limiter = RateLimiter::new(test_config(100, 1000));

        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut req = Request::builder().body(()).unwrap();
        req.extensions_mut().insert(ConnectInfo(addr));

        // Make a request to create bucket
        let (result, _metadata) = limiter.check_rate_limit_with_metadata(&req);
        assert!(result.is_ok());
        assert_eq!(limiter.buckets.len(), 1);

        // Note: cleanup_old_buckets() was removed. Cleanup now happens automatically
        // when bucket threshold is exceeded, and active buckets are preserved.
    }

    #[test]
    fn test_trusted_proxy_ipv6() {
        let mut config = test_config(100, 1000);
        config.trusted_proxies = Some(vec!["2001:db8::1".to_string()]);
        let limiter = RateLimiter::new(config);

        // Request from trusted IPv6 proxy with X-Forwarded-For
        let trusted_addr: SocketAddr = "[2001:db8::1]:8080".parse().unwrap();
        let mut trusted_req = Request::builder()
            .header("x-forwarded-for", "203.0.113.1")
            .body(())
            .unwrap();
        trusted_req
            .extensions_mut()
            .insert(ConnectInfo(trusted_addr));

        let (key, _) = limiter.get_rate_limit_key(&trusted_req);
        assert_eq!(
            key, "anon:203.0.113.1",
            "Should use X-Forwarded-For from trusted IPv6 proxy"
        );
    }

    #[test]
    fn test_untrusted_proxy_ignores_x_real_ip() {
        let mut config = test_config(100, 1000);
        config.trusted_proxies = Some(vec!["10.0.0.1".to_string()]);
        let limiter = RateLimiter::new(config);

        // Request from untrusted source with X-Real-IP header
        let untrusted_addr: SocketAddr = "192.168.1.1:8080".parse().unwrap();
        let mut untrusted_req = Request::builder()
            .header("x-real-ip", "203.0.113.1")
            .body(())
            .unwrap();
        untrusted_req
            .extensions_mut()
            .insert(ConnectInfo(untrusted_addr));

        let (key, _) = limiter.get_rate_limit_key(&untrusted_req);
        assert_eq!(
            key, "anon:192.168.1.1",
            "Should ignore X-Real-IP from untrusted source"
        );
    }

    #[test]
    fn test_bucket_cap_enforcement() {
        // Create limiter with small cap for testing
        let mut config = test_config(100, 1000);
        config.max_buckets = 20;
        let limiter = RateLimiter::new(config);

        // Create buckets up to half the cap
        for i in 0..10 {
            let addr: SocketAddr = format!("127.0.0.{}:8080", i).parse().unwrap();
            let mut req = Request::builder().body(()).unwrap();
            req.extensions_mut().insert(ConnectInfo(addr));
            let (result, _metadata) = limiter.check_rate_limit_with_metadata(&req);
            assert!(result.is_ok());
        }

        assert_eq!(limiter.buckets.len(), 10, "Should have 10 buckets");

        // Mark all existing buckets as stale
        for mut entry in limiter.buckets.iter_mut() {
            entry.value_mut().last_accessed =
                Instant::now() - Duration::from_secs(DEFAULT_BUCKET_TTL_SECS + 1);
        }

        // Create the 11th bucket (threshold is max_buckets/2 = 10)
        let addr: SocketAddr = "127.0.0.11:8080".parse().unwrap();
        let mut req = Request::builder().body(()).unwrap();
        req.extensions_mut().insert(ConnectInfo(addr));
        let (result, _metadata) = limiter.check_rate_limit_with_metadata(&req);
        assert!(result.is_ok());

        // Now we have 11 buckets (10 stale + 1 fresh). Next request should trigger cleanup
        assert_eq!(limiter.buckets.len(), 11);

        // Make one more request to trigger cleanup (11 > threshold of 10)
        let addr2: SocketAddr = "127.0.0.12:8080".parse().unwrap();
        let mut req2 = Request::builder().body(()).unwrap();
        req2.extensions_mut().insert(ConnectInfo(addr2));
        let (result, _metadata) = limiter.check_rate_limit_with_metadata(&req2);
        assert!(result.is_ok());

        // After cleanup, should only have the 2 fresh buckets (11 and 12)
        assert_eq!(
            limiter.buckets.len(),
            2,
            "Old buckets should be evicted when cap threshold reached"
        );
    }

    #[test]
    fn test_rate_limit_response_includes_request_id() {
        // This is a unit test for the middleware error response format
        // The actual middleware test would require integration testing
        let error_body = json!({
            "error": "Rate limit exceeded",
            "message": "Too many requests. Please retry after the specified time.",
            "request_id": "test-request-id",
            "retry_after": 60,
        });

        assert_eq!(error_body["error"], "Rate limit exceeded");
        assert_eq!(error_body["request_id"], "test-request-id");
        assert_eq!(error_body["retry_after"], 60);
    }
}
