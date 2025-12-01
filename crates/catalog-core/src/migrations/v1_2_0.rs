//! Migration v1.2.0: Multi-Tenant Control Plane.
//!
//! This migration adds the control plane schema for multi-tenant support:
//! - `tenants` table for tenant registry and lifecycle management
//! - `tenant_api_keys` table for tenant-scoped API key authentication
//! - `tenant_audit_log` table for control plane operations audit trail
//!
//! # Architecture
//!
//! The control plane database is separate from per-tenant data catalogs.
//! It manages:
//! - Tenant lifecycle (create, suspend, delete, purge)
//! - Tenant-scoped API keys with RBAC
//! - Control plane audit logging
//!
//! Per-tenant data catalogs continue to use the existing schema (datasets,
//! fields, lineage, etc.) but are stored at tenant-specific paths.

use super::Migration;

/// Version number: 1_002_000 represents v1.2.0
/// Format: MAJOR * 1_000_000 + MINOR * 1_000 + PATCH
pub const VERSION: i64 = 1_002_000;

/// No columns to add to existing tables in this migration.
/// All changes are new tables for the control plane.
const ADD_COLUMNS: &[(&str, &str, &str)] = &[];

pub fn migration() -> Migration {
    Migration {
        version: VERSION,
        description: "v1.2.0: Multi-Tenant Control Plane",
        sql: SQL,
        add_columns: ADD_COLUMNS,
    }
}

const SQL: &str = r#"
-- ============================================================================
-- MetaFuse v1.2.0 Schema Migration
-- Multi-Tenant Control Plane
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. TENANTS TABLE
-- Core tenant registry with lifecycle management
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Unique tenant identifier (URL-safe, immutable after creation)
    -- Format: lowercase alphanumeric with hyphen/underscore, 3-63 chars
    tenant_id TEXT UNIQUE NOT NULL,
    -- Human-readable display name
    display_name TEXT NOT NULL,
    -- Tenant lifecycle status
    status TEXT NOT NULL DEFAULT 'active',
    -- Pricing/feature tier
    tier TEXT NOT NULL DEFAULT 'standard',
    -- Storage URI template for tenant catalog
    -- Example: gs://bucket/tenants/{tenant_id}/catalog.db
    storage_uri TEXT NOT NULL,
    -- Quota: maximum number of datasets
    quota_max_datasets INTEGER NOT NULL DEFAULT 10000,
    -- Quota: maximum storage size in bytes (default 10GB)
    quota_max_storage_bytes INTEGER NOT NULL DEFAULT 10737418240,
    -- Quota: maximum API calls per hour
    quota_max_api_calls_per_hour INTEGER NOT NULL DEFAULT 10000,
    -- Primary contact email for the tenant
    admin_email TEXT NOT NULL,
    -- Timestamps
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Lifecycle timestamps
    suspended_at TEXT,
    deleted_at TEXT,
    -- Constraints
    CHECK (status IN ('active', 'suspended', 'pending_deletion', 'deleted')),
    CHECK (tier IN ('free', 'standard', 'premium', 'enterprise')),
    CHECK (tenant_id GLOB '[a-z0-9][a-z0-9_-]*'),
    CHECK (length(tenant_id) >= 3 AND length(tenant_id) <= 63),
    CHECK (quota_max_datasets > 0),
    CHECK (quota_max_storage_bytes > 0),
    CHECK (quota_max_api_calls_per_hour > 0)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_tenants_status ON tenants(status);
CREATE INDEX IF NOT EXISTS idx_tenants_tier ON tenants(tier);
CREATE INDEX IF NOT EXISTS idx_tenants_status_tier ON tenants(status, tier);
CREATE INDEX IF NOT EXISTS idx_tenants_created_at ON tenants(created_at);

-- ----------------------------------------------------------------------------
-- 2. TENANT API KEYS TABLE
-- Tenant-scoped API keys with RBAC
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Tenant this key belongs to
    tenant_id TEXT NOT NULL,
    -- bcrypt hash of the API key (same format as existing api_keys table)
    key_hash TEXT UNIQUE NOT NULL,
    -- Human-readable name for the key
    name TEXT NOT NULL,
    -- Role determines permissions: admin, editor, viewer
    role TEXT NOT NULL DEFAULT 'viewer',
    -- Timestamps
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Revocation timestamp (null = active)
    revoked_at TEXT,
    -- Last usage timestamp for monitoring
    last_used_at TEXT,
    -- Optional expiration timestamp
    expires_at TEXT,
    -- Foreign key to tenants table (with CASCADE delete)
    FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    -- Constraints
    CHECK (role IN ('admin', 'editor', 'viewer'))
);

-- Indexes for API key lookup and management
CREATE INDEX IF NOT EXISTS idx_tenant_api_keys_tenant ON tenant_api_keys(tenant_id);
CREATE INDEX IF NOT EXISTS idx_tenant_api_keys_tenant_role ON tenant_api_keys(tenant_id, role);
CREATE INDEX IF NOT EXISTS idx_tenant_api_keys_revoked ON tenant_api_keys(revoked_at);
CREATE INDEX IF NOT EXISTS idx_tenant_api_keys_expires ON tenant_api_keys(expires_at);

-- ----------------------------------------------------------------------------
-- 3. TENANT AUDIT LOG TABLE
-- Audit trail for control plane operations
-- Separate from per-tenant audit_log (data plane operations)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- When the action occurred
    timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Action type: create, update, suspend, reactivate, delete, purge,
    --              key_create, key_revoke, export_request
    action TEXT NOT NULL,
    -- Tenant affected by the action
    tenant_id TEXT NOT NULL,
    -- Actor who performed the action (email, API key name, or 'system')
    actor TEXT NOT NULL,
    -- Additional details as JSON (optional)
    details TEXT,
    -- Request ID for correlation with API logs
    request_id TEXT,
    -- Client IP address
    client_ip TEXT
);

-- Indexes for audit log queries
CREATE INDEX IF NOT EXISTS idx_tenant_audit_log_tenant ON tenant_audit_log(tenant_id);
CREATE INDEX IF NOT EXISTS idx_tenant_audit_log_timestamp ON tenant_audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_tenant_audit_log_action ON tenant_audit_log(action);
CREATE INDEX IF NOT EXISTS idx_tenant_audit_log_actor ON tenant_audit_log(actor);

-- ----------------------------------------------------------------------------
-- 4. TRIGGER: Update tenant updated_at on modification
-- ----------------------------------------------------------------------------
CREATE TRIGGER IF NOT EXISTS tenant_updated_at
AFTER UPDATE ON tenants
BEGIN
    UPDATE tenants SET updated_at = datetime('now') WHERE id = NEW.id;
END;
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::run_migrations;
    use rusqlite::Connection;

    #[test]
    fn test_migration_version() {
        assert_eq!(VERSION, 1_002_000);
    }

    #[test]
    fn test_migration_description() {
        let m = migration();
        assert!(m.description.contains("v1.2.0"));
        assert!(m.description.contains("Multi-Tenant"));
    }

    #[test]
    fn test_tenants_table_created() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        run_migrations(&conn).unwrap();

        // Verify tenants table exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='tenants'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // Verify we can insert a tenant
        conn.execute(
            "INSERT INTO tenants (tenant_id, display_name, storage_uri, admin_email)
             VALUES ('acme-corp', 'Acme Corporation', 'gs://bucket/tenants/acme-corp/catalog.db', 'admin@acme.com')",
            [],
        )
        .unwrap();

        // Verify the tenant was inserted with defaults
        let (status, tier): (String, String) = conn
            .query_row(
                "SELECT status, tier FROM tenants WHERE tenant_id = 'acme-corp'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "active");
        assert_eq!(tier, "standard");
    }

    #[test]
    fn test_tenant_id_validation() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        run_migrations(&conn).unwrap();

        // Valid tenant_id patterns
        assert!(conn
            .execute(
                "INSERT INTO tenants (tenant_id, display_name, storage_uri, admin_email)
                 VALUES ('valid-tenant', 'Valid', 'uri', 'a@b.com')",
                [],
            )
            .is_ok());

        assert!(conn
            .execute(
                "INSERT INTO tenants (tenant_id, display_name, storage_uri, admin_email)
                 VALUES ('tenant_123', 'Valid', 'uri', 'a@b.com')",
                [],
            )
            .is_ok());

        // Invalid tenant_id patterns should fail
        // Too short (< 3 chars)
        assert!(conn
            .execute(
                "INSERT INTO tenants (tenant_id, display_name, storage_uri, admin_email)
                 VALUES ('ab', 'Invalid', 'uri', 'a@b.com')",
                [],
            )
            .is_err());

        // Starts with non-alphanumeric
        assert!(conn
            .execute(
                "INSERT INTO tenants (tenant_id, display_name, storage_uri, admin_email)
                 VALUES ('-invalid', 'Invalid', 'uri', 'a@b.com')",
                [],
            )
            .is_err());

        // Contains invalid characters (uppercase not matched by GLOB)
        assert!(conn
            .execute(
                "INSERT INTO tenants (tenant_id, display_name, storage_uri, admin_email)
                 VALUES ('Invalid', 'Invalid', 'uri', 'a@b.com')",
                [],
            )
            .is_err());
    }

    #[test]
    fn test_tenant_api_keys_table() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        run_migrations(&conn).unwrap();

        // First create a tenant
        conn.execute(
            "INSERT INTO tenants (tenant_id, display_name, storage_uri, admin_email)
             VALUES ('test-tenant', 'Test', 'uri', 'a@b.com')",
            [],
        )
        .unwrap();

        // Create an API key for the tenant
        conn.execute(
            "INSERT INTO tenant_api_keys (tenant_id, key_hash, name, role)
             VALUES ('test-tenant', 'hash123', 'My Key', 'editor')",
            [],
        )
        .unwrap();

        // Verify the key was created
        let (name, role): (String, String) = conn
            .query_row(
                "SELECT name, role FROM tenant_api_keys WHERE tenant_id = 'test-tenant'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(name, "My Key");
        assert_eq!(role, "editor");
    }

    #[test]
    fn test_tenant_api_keys_cascade_delete() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        run_migrations(&conn).unwrap();

        // Create tenant and API key
        conn.execute(
            "INSERT INTO tenants (tenant_id, display_name, storage_uri, admin_email)
             VALUES ('delete-test', 'Delete Test', 'uri', 'a@b.com')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO tenant_api_keys (tenant_id, key_hash, name, role)
             VALUES ('delete-test', 'hash456', 'Test Key', 'viewer')",
            [],
        )
        .unwrap();

        // Verify key exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = 'delete-test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // Delete tenant - should cascade delete API keys
        conn.execute("DELETE FROM tenants WHERE tenant_id = 'delete-test'", [])
            .unwrap();

        // Verify key was deleted
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = 'delete-test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_tenant_audit_log() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        run_migrations(&conn).unwrap();

        // Insert audit log entry
        conn.execute(
            "INSERT INTO tenant_audit_log (action, tenant_id, actor, details)
             VALUES ('create', 'new-tenant', 'admin@example.com', '{\"tier\": \"premium\"}')",
            [],
        )
        .unwrap();

        // Verify entry was created
        let (action, actor): (String, String) = conn
            .query_row(
                "SELECT action, actor FROM tenant_audit_log WHERE tenant_id = 'new-tenant'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(action, "create");
        assert_eq!(actor, "admin@example.com");
    }

    #[test]
    fn test_role_constraint() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        run_migrations(&conn).unwrap();

        // Create tenant first
        conn.execute(
            "INSERT INTO tenants (tenant_id, display_name, storage_uri, admin_email)
             VALUES ('role-test', 'Role Test', 'uri', 'a@b.com')",
            [],
        )
        .unwrap();

        // Valid roles should work
        for role in &["admin", "editor", "viewer"] {
            conn.execute(
                &format!(
                    "INSERT INTO tenant_api_keys (tenant_id, key_hash, name, role)
                     VALUES ('role-test', 'hash_{}', 'Key', '{}')",
                    role, role
                ),
                [],
            )
            .unwrap();
        }

        // Invalid role should fail
        let result = conn.execute(
            "INSERT INTO tenant_api_keys (tenant_id, key_hash, name, role)
             VALUES ('role-test', 'hash_invalid', 'Key', 'superuser')",
            [],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_sqlite_schema(&conn).unwrap();

        // Run migrations twice
        let count1 = run_migrations(&conn).unwrap();
        assert!(count1 > 0);

        let count2 = run_migrations(&conn).unwrap();
        assert_eq!(count2, 0, "Second run should apply no migrations");
    }
}
