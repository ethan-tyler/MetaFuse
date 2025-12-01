//! Connection pool configuration for tenant backends.
//!
//! Provides configuration for per-tenant connection limiting via semaphores.
//! Uses a global limit for all tenants (simplified approach).

use std::time::Duration;

/// Default maximum connections per tenant.
pub const DEFAULT_MAX_CONNECTIONS: usize = 10;

/// Default timeout for acquiring a connection permit.
pub const DEFAULT_ACQUIRE_TIMEOUT_SECS: u64 = 5;

/// Circuit breaker configuration defaults.
pub const DEFAULT_CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
pub const DEFAULT_CIRCUIT_BREAKER_RESET_SECS: u64 = 30;

/// Connection pool configuration.
///
/// Controls per-tenant connection limiting and circuit breaker behavior.
///
/// # Environment Variables
///
/// | Variable | Default | Description |
/// |----------|---------|-------------|
/// | `METAFUSE_CONN_LIMIT` | 10 | Max connections per tenant |
/// | `METAFUSE_CONN_ACQUIRE_TIMEOUT_SECS` | 5 | Timeout waiting for permit |
/// | `METAFUSE_CIRCUIT_BREAKER_THRESHOLD` | 5 | Failures before circuit opens |
/// | `METAFUSE_CIRCUIT_BREAKER_RESET_SECS` | 30 | Time before circuit resets |
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// Maximum concurrent connections per tenant.
    pub max_connections_per_tenant: usize,

    /// Timeout for acquiring a connection permit.
    pub acquire_timeout: Duration,

    /// Enable Prometheus metrics for connection pool.
    pub enable_metrics: bool,

    /// Circuit breaker configuration.
    pub circuit_breaker: CircuitBreakerConfig,
}

/// Circuit breaker configuration for tenant backends.
///
/// When a tenant backend experiences repeated failures, the circuit breaker
/// opens to prevent cascading failures and allow recovery.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before circuit opens.
    pub failure_threshold: u32,

    /// Duration before circuit resets from open to half-open state.
    pub reset_timeout: Duration,

    /// Enable circuit breaker functionality.
    pub enabled: bool,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: DEFAULT_CIRCUIT_BREAKER_THRESHOLD,
            reset_timeout: Duration::from_secs(DEFAULT_CIRCUIT_BREAKER_RESET_SECS),
            enabled: true,
        }
    }
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_tenant: DEFAULT_MAX_CONNECTIONS,
            acquire_timeout: Duration::from_secs(DEFAULT_ACQUIRE_TIMEOUT_SECS),
            enable_metrics: true,
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

impl ConnectionPoolConfig {
    /// Create a new configuration with specified max connections.
    ///
    /// # Panics
    ///
    /// Panics if `max_connections_per_tenant` is 0.
    pub fn new(max_connections_per_tenant: usize) -> Self {
        assert!(
            max_connections_per_tenant > 0,
            "max_connections_per_tenant must be > 0"
        );
        Self {
            max_connections_per_tenant,
            ..Default::default()
        }
    }

    /// Validate configuration values.
    ///
    /// Returns an error if any values are invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_connections_per_tenant == 0 {
            return Err("max_connections_per_tenant must be > 0".to_string());
        }
        if self.acquire_timeout.is_zero() {
            return Err("acquire_timeout must be > 0".to_string());
        }
        if self.circuit_breaker.enabled {
            if self.circuit_breaker.failure_threshold == 0 {
                return Err("circuit_breaker.failure_threshold must be > 0".to_string());
            }
            if self.circuit_breaker.reset_timeout.is_zero() {
                return Err("circuit_breaker.reset_timeout must be > 0".to_string());
            }
        }
        Ok(())
    }

    /// Create configuration from environment variables.
    ///
    /// Falls back to defaults for any unset variables.
    pub fn from_env() -> Self {
        Self {
            max_connections_per_tenant: env_parse("METAFUSE_CONN_LIMIT", DEFAULT_MAX_CONNECTIONS),
            acquire_timeout: Duration::from_secs(env_parse(
                "METAFUSE_CONN_ACQUIRE_TIMEOUT_SECS",
                DEFAULT_ACQUIRE_TIMEOUT_SECS,
            )),
            enable_metrics: env_parse("METAFUSE_CONN_METRICS_ENABLED", true),
            circuit_breaker: CircuitBreakerConfig {
                failure_threshold: env_parse(
                    "METAFUSE_CIRCUIT_BREAKER_THRESHOLD",
                    DEFAULT_CIRCUIT_BREAKER_THRESHOLD,
                ),
                reset_timeout: Duration::from_secs(env_parse(
                    "METAFUSE_CIRCUIT_BREAKER_RESET_SECS",
                    DEFAULT_CIRCUIT_BREAKER_RESET_SECS,
                )),
                enabled: env_parse("METAFUSE_CIRCUIT_BREAKER_ENABLED", true),
            },
        }
    }

    /// Set maximum connections per tenant.
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections_per_tenant = max;
        self
    }

    /// Set acquire timeout.
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Enable or disable metrics.
    pub fn with_metrics(mut self, enabled: bool) -> Self {
        self.enable_metrics = enabled;
        self
    }

    /// Set circuit breaker configuration.
    pub fn with_circuit_breaker(mut self, config: CircuitBreakerConfig) -> Self {
        self.circuit_breaker = config;
        self
    }

    /// Disable circuit breaker.
    pub fn without_circuit_breaker(mut self) -> Self {
        self.circuit_breaker.enabled = false;
        self
    }
}

/// Parse an environment variable with a default fallback.
fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ConnectionPoolConfig::default();
        assert_eq!(config.max_connections_per_tenant, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(
            config.acquire_timeout,
            Duration::from_secs(DEFAULT_ACQUIRE_TIMEOUT_SECS)
        );
        assert!(config.enable_metrics);
        assert!(config.circuit_breaker.enabled);
    }

    #[test]
    fn test_builder_pattern() {
        let config = ConnectionPoolConfig::default()
            .with_max_connections(20)
            .with_acquire_timeout(Duration::from_secs(10))
            .with_metrics(false)
            .without_circuit_breaker();

        assert_eq!(config.max_connections_per_tenant, 20);
        assert_eq!(config.acquire_timeout, Duration::from_secs(10));
        assert!(!config.enable_metrics);
        assert!(!config.circuit_breaker.enabled);
    }

    #[test]
    fn test_new_with_max_connections() {
        let config = ConnectionPoolConfig::new(50);
        assert_eq!(config.max_connections_per_tenant, 50);
        // Other values should be default
        assert!(config.circuit_breaker.enabled);
    }

    #[test]
    fn test_validate_valid_config() {
        let config = ConnectionPoolConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_zero_max_connections() {
        let config = ConnectionPoolConfig {
            max_connections_per_tenant: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("max_connections_per_tenant"));
    }

    #[test]
    fn test_validate_zero_acquire_timeout() {
        let config = ConnectionPoolConfig {
            acquire_timeout: Duration::ZERO,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("acquire_timeout"));
    }

    #[test]
    fn test_validate_circuit_breaker_zero_threshold() {
        let config = ConnectionPoolConfig {
            circuit_breaker: CircuitBreakerConfig {
                failure_threshold: 0,
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failure_threshold"));
    }

    #[test]
    fn test_validate_circuit_breaker_zero_reset_timeout() {
        let config = ConnectionPoolConfig {
            circuit_breaker: CircuitBreakerConfig {
                reset_timeout: Duration::ZERO,
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("reset_timeout"));
    }

    #[test]
    fn test_validate_disabled_circuit_breaker_allows_zero_values() {
        let config = ConnectionPoolConfig {
            circuit_breaker: CircuitBreakerConfig {
                failure_threshold: 0,
                reset_timeout: Duration::ZERO,
                enabled: false,
            },
            ..Default::default()
        };
        // When circuit breaker is disabled, its values are not validated
        assert!(config.validate().is_ok());
    }
}
