//! Migration v1.0.0: Delta-native Lakehouse Catalog schema expansion.
//!
//! This migration adds tables for:
//! - Owners with contact information
//! - Quality metrics (completeness, freshness, file health)
//! - Freshness SLA configuration
//! - Governance rules (PII detection patterns)
//! - Column classifications (PII, sensitivity levels)
//! - Audit logging for compliance
//! - Usage statistics for analytics
//!
//! It also adds the `delta_location` column to the datasets table.

use super::Migration;

/// Version number: 1_000_000 represents v1.0.0
/// Format: MAJOR * 1_000_000 + MINOR * 1_000 + PATCH
pub const VERSION: i64 = 1_000_000;

/// Columns to add to existing tables.
/// Format: (table_name, column_name, column_type)
const ADD_COLUMNS: &[(&str, &str, &str)] = &[
    // Add delta_location to datasets for Delta-native integration
    ("datasets", "delta_location", "TEXT"),
];

pub fn migration() -> Migration {
    Migration {
        version: VERSION,
        description: "v1.0.0: Delta-native Lakehouse Catalog schema expansion",
        sql: SQL,
        add_columns: ADD_COLUMNS,
    }
}

const SQL: &str = r#"
-- ============================================================================
-- MetaFuse v1.0.0 Schema Migration
-- Delta-native Lakehouse Catalog
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. OWNERS TABLE
-- Stores owner/team information with contact details
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS owners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Unique identifier (email, team name, or service account)
    owner_id TEXT UNIQUE NOT NULL,
    -- Display name
    name TEXT NOT NULL,
    -- Owner type: 'user', 'team', 'service'
    owner_type TEXT NOT NULL DEFAULT 'user',
    -- Contact email (may differ from owner_id for teams)
    email TEXT,
    -- Slack channel or handle
    slack_channel TEXT,
    -- Additional contact info as JSON
    contact_info TEXT,
    -- When the owner was registered
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- When the owner info was last updated
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CHECK (owner_type IN ('user', 'team', 'service'))
);

CREATE INDEX IF NOT EXISTS idx_owners_type ON owners(owner_type);
CREATE INDEX IF NOT EXISTS idx_owners_email ON owners(email);

-- ----------------------------------------------------------------------------
-- 2. QUALITY METRICS TABLE
-- Stores quality scores computed from Delta statistics
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quality_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id INTEGER NOT NULL,
    -- When this metric was computed
    computed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Completeness score (0.0-1.0): derived from null counts
    completeness_score REAL,
    -- Freshness score (0.0-1.0): based on last_modified vs SLA
    freshness_score REAL,
    -- File health score (0.0-1.0): based on file size distribution
    file_health_score REAL,
    -- Overall quality score (weighted average)
    overall_score REAL,
    -- Number of total rows at time of computation
    row_count INTEGER,
    -- Number of files at time of computation
    file_count INTEGER,
    -- Total size in bytes
    size_bytes INTEGER,
    -- Number of small files (below threshold)
    small_file_count INTEGER,
    -- Average file size in bytes
    avg_file_size INTEGER,
    -- Detailed metrics as JSON
    details TEXT,

    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_quality_metrics_dataset ON quality_metrics(dataset_id);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_computed_at ON quality_metrics(computed_at);

-- ----------------------------------------------------------------------------
-- 3. FRESHNESS CONFIG TABLE
-- SLA-based freshness configuration per dataset
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS freshness_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id INTEGER UNIQUE NOT NULL,
    -- Expected update frequency in seconds
    expected_interval_secs INTEGER NOT NULL,
    -- Grace period before marking stale (seconds)
    grace_period_secs INTEGER NOT NULL DEFAULT 0,
    -- Timezone for schedule-based freshness (e.g., 'America/New_York')
    timezone TEXT DEFAULT 'UTC',
    -- Cron expression for expected update schedule (optional)
    cron_schedule TEXT,
    -- Whether to alert when stale
    alert_on_stale INTEGER NOT NULL DEFAULT 1,
    -- Alert channels as JSON array
    alert_channels TEXT,
    -- When this config was created
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- When this config was last updated
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
);

-- ----------------------------------------------------------------------------
-- 4. GOVERNANCE RULES TABLE
-- Data governance rules including PII detection patterns
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Rule name (unique identifier)
    name TEXT UNIQUE NOT NULL,
    -- Rule type: 'pii_detection', 'retention', 'access_control'
    rule_type TEXT NOT NULL,
    -- Description of what the rule does
    description TEXT,
    -- JSON configuration for the rule
    -- For PII detection: {"patterns": [...], "column_names": [...]}
    config TEXT NOT NULL,
    -- Priority (lower = higher priority)
    priority INTEGER NOT NULL DEFAULT 100,
    -- Whether the rule is active
    is_active INTEGER NOT NULL DEFAULT 1,
    -- When the rule was created
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- When the rule was last updated
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CHECK (rule_type IN ('pii_detection', 'retention', 'access_control', 'custom'))
);

CREATE INDEX IF NOT EXISTS idx_governance_rules_type ON governance_rules(rule_type);
CREATE INDEX IF NOT EXISTS idx_governance_rules_active ON governance_rules(is_active);

-- Insert default PII detection rules
INSERT OR IGNORE INTO governance_rules (name, rule_type, description, config, priority) VALUES
    ('email_detection', 'pii_detection', 'Detect email addresses',
     '{"patterns": ["^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"], "column_names": ["email", "e_mail", "email_address", "user_email"]}', 10),
    ('ssn_detection', 'pii_detection', 'Detect US Social Security Numbers',
     '{"patterns": ["^\\d{3}-\\d{2}-\\d{4}$", "^\\d{9}$"], "column_names": ["ssn", "social_security", "social_security_number"]}', 10),
    ('phone_detection', 'pii_detection', 'Detect phone numbers',
     '{"patterns": ["^\\+?1?[-.\\s]?\\(?\\d{3}\\)?[-.\\s]?\\d{3}[-.\\s]?\\d{4}$"], "column_names": ["phone", "phone_number", "mobile", "cell"]}', 20),
    ('credit_card_detection', 'pii_detection', 'Detect credit card numbers',
     '{"patterns": ["^\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}$"], "column_names": ["credit_card", "card_number", "cc_number"]}', 10),
    ('ip_address_detection', 'pii_detection', 'Detect IP addresses',
     '{"patterns": ["^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$"], "column_names": ["ip", "ip_address", "client_ip", "source_ip"]}', 30);

-- ----------------------------------------------------------------------------
-- 5. COLUMN CLASSIFICATIONS TABLE
-- Column-level PII and sensitivity classifications
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS column_classifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    field_id INTEGER NOT NULL,
    -- Classification type: 'pii', 'sensitive', 'confidential', 'public'
    classification TEXT NOT NULL,
    -- Specific category within classification (e.g., 'email', 'ssn', 'phone')
    category TEXT,
    -- Confidence score (0.0-1.0) if auto-detected
    confidence REAL,
    -- How the classification was determined: 'auto', 'manual', 'rule'
    source TEXT NOT NULL DEFAULT 'auto',
    -- Rule that triggered this classification (if source='rule')
    rule_id INTEGER,
    -- Whether this classification has been verified by a human
    verified INTEGER NOT NULL DEFAULT 0,
    -- User who verified (if verified)
    verified_by TEXT,
    -- When verified
    verified_at TEXT,
    -- When the classification was created
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- When the classification was last updated
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (field_id) REFERENCES fields(id) ON DELETE CASCADE,
    FOREIGN KEY (rule_id) REFERENCES governance_rules(id) ON DELETE SET NULL,
    CHECK (classification IN ('pii', 'sensitive', 'confidential', 'public', 'unknown')),
    CHECK (source IN ('auto', 'manual', 'rule'))
);

CREATE INDEX IF NOT EXISTS idx_column_class_field ON column_classifications(field_id);
CREATE INDEX IF NOT EXISTS idx_column_class_classification ON column_classifications(classification);
CREATE INDEX IF NOT EXISTS idx_column_class_category ON column_classifications(category);
CREATE INDEX IF NOT EXISTS idx_column_class_verified ON column_classifications(verified);

-- ----------------------------------------------------------------------------
-- 6. AUDIT LOG TABLE
-- Comprehensive audit trail for all catalog mutations
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Timestamp of the action
    timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Action type: 'create', 'update', 'delete', 'read', 'search'
    action TEXT NOT NULL,
    -- Entity type: 'dataset', 'field', 'tag', 'lineage', 'owner', etc.
    entity_type TEXT NOT NULL,
    -- Entity identifier (e.g., dataset name or ID)
    entity_id TEXT,
    -- User or service that performed the action
    actor TEXT,
    -- Actor type: 'user', 'service', 'system'
    actor_type TEXT DEFAULT 'user',
    -- API key ID used (if applicable)
    api_key_id INTEGER,
    -- Request ID for correlation
    request_id TEXT,
    -- Client IP address
    client_ip TEXT,
    -- Old values (for updates/deletes) as JSON
    old_values TEXT,
    -- New values (for creates/updates) as JSON
    new_values TEXT,
    -- Additional context as JSON
    context TEXT,

    CHECK (action IN ('create', 'update', 'delete', 'read', 'search', 'export', 'import')),
    CHECK (actor_type IN ('user', 'service', 'system', 'anonymous'))
);

CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON audit_log(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_actor ON audit_log(actor);
CREATE INDEX IF NOT EXISTS idx_audit_log_request_id ON audit_log(request_id);

-- ----------------------------------------------------------------------------
-- 7. USAGE STATS TABLE
-- Track dataset access patterns for analytics
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS usage_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id INTEGER NOT NULL,
    -- Date for this stat record (YYYY-MM-DD)
    stat_date TEXT NOT NULL,
    -- Number of read operations
    read_count INTEGER NOT NULL DEFAULT 0,
    -- Number of distinct users who accessed
    unique_users INTEGER NOT NULL DEFAULT 0,
    -- Number of search appearances
    search_appearances INTEGER NOT NULL DEFAULT 0,
    -- Number of times included in lineage queries
    lineage_queries INTEGER NOT NULL DEFAULT 0,
    -- Number of API calls
    api_calls INTEGER NOT NULL DEFAULT 0,
    -- Breakdown by access method as JSON
    access_methods TEXT,
    -- When this record was last updated
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    UNIQUE(dataset_id, stat_date)
);

CREATE INDEX IF NOT EXISTS idx_usage_stats_dataset ON usage_stats(dataset_id);
CREATE INDEX IF NOT EXISTS idx_usage_stats_date ON usage_stats(stat_date);

-- ----------------------------------------------------------------------------
-- 8. ADDITIONAL INDEXES for performance
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_datasets_format ON datasets(format);
CREATE INDEX IF NOT EXISTS idx_datasets_path ON datasets(path);

-- ----------------------------------------------------------------------------
-- 10. UPDATE FTS to include new searchable fields
-- Note: FTS5 tables can't be altered, so we leave the existing one
-- The application can rebuild if needed
-- ----------------------------------------------------------------------------
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_migration_sql_valid() {
        let conn = Connection::open_in_memory().unwrap();

        // First init the base schema
        crate::init_sqlite_schema(&conn).unwrap();

        // Then apply the migration
        conn.execute_batch(SQL).unwrap();

        // Verify new tables exist
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"owners".to_string()));
        assert!(tables.contains(&"quality_metrics".to_string()));
        assert!(tables.contains(&"freshness_config".to_string()));
        assert!(tables.contains(&"governance_rules".to_string()));
        assert!(tables.contains(&"column_classifications".to_string()));
        assert!(tables.contains(&"audit_log".to_string()));
        assert!(tables.contains(&"usage_stats".to_string()));
    }

    #[test]
    fn test_migration_idempotent() {
        let conn = Connection::open_in_memory().unwrap();

        // Init base schema
        crate::init_sqlite_schema(&conn).unwrap();

        // Apply migration twice - should not error
        conn.execute_batch(SQL).unwrap();
        conn.execute_batch(SQL).unwrap();
    }

    #[test]
    fn test_default_governance_rules() {
        let conn = Connection::open_in_memory().unwrap();

        // Init base schema and migration
        crate::init_sqlite_schema(&conn).unwrap();
        conn.execute_batch(SQL).unwrap();

        // Verify default PII rules were inserted
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM governance_rules WHERE rule_type = 'pii_detection'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert!(count >= 5, "Expected at least 5 default PII rules");
    }

    #[test]
    fn test_owners_table_constraints() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        conn.execute_batch(SQL).unwrap();

        // Valid insert
        conn.execute(
            "INSERT INTO owners (owner_id, name, owner_type, email) VALUES ('alice', 'Alice', 'user', 'alice@example.com')",
            [],
        ).unwrap();

        // Duplicate owner_id should fail
        let result = conn.execute(
            "INSERT INTO owners (owner_id, name, owner_type) VALUES ('alice', 'Another Alice', 'user')",
            [],
        );
        assert!(result.is_err());

        // Invalid owner_type should fail
        let result = conn.execute(
            "INSERT INTO owners (owner_id, name, owner_type) VALUES ('bob', 'Bob', 'invalid')",
            [],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_audit_log_constraints() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        conn.execute_batch(SQL).unwrap();

        // Valid insert
        conn.execute(
            "INSERT INTO audit_log (action, entity_type, entity_id, actor) VALUES ('create', 'dataset', 'my_dataset', 'alice')",
            [],
        ).unwrap();

        // Invalid action should fail
        let result = conn.execute(
            "INSERT INTO audit_log (action, entity_type) VALUES ('invalid_action', 'dataset')",
            [],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_quality_metrics_foreign_key() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("PRAGMA foreign_keys = ON", []).unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        conn.execute_batch(SQL).unwrap();

        // Insert a dataset
        conn.execute(
            "INSERT INTO datasets (name, path, format, created_at, last_updated) VALUES ('test', '/test', 'delta', datetime('now'), datetime('now'))",
            [],
        ).unwrap();

        let dataset_id: i64 = conn
            .query_row("SELECT id FROM datasets WHERE name = 'test'", [], |row| {
                row.get(0)
            })
            .unwrap();

        // Insert quality metrics for valid dataset
        conn.execute(
            "INSERT INTO quality_metrics (dataset_id, completeness_score, freshness_score, file_health_score, overall_score) VALUES (?1, 0.95, 1.0, 0.8, 0.92)",
            [dataset_id],
        ).unwrap();

        // Foreign key violation should fail (invalid dataset_id)
        let result = conn.execute(
            "INSERT INTO quality_metrics (dataset_id, overall_score) VALUES (9999, 0.5)",
            [],
        );
        assert!(result.is_err());
    }
}
