//! Migration v1.1.0: Domain Management and Glossary Enhancements.
//!
//! This migration adds:
//! - Domains table for organizing datasets by business domain
//! - Glossary enhancements (owner_id, status, timestamps)
//! - domain_id foreign key to datasets table

use super::Migration;

/// Version number: 1_001_000 represents v1.1.0
/// Format: MAJOR * 1_000_000 + MINOR * 1_000 + PATCH
pub const VERSION: i64 = 1_001_000;

/// Columns to add to existing tables.
/// Format: (table_name, column_name, column_type)
const ADD_COLUMNS: &[(&str, &str, &str)] = &[
    // Add domain_id foreign key to datasets
    ("datasets", "domain_id", "INTEGER"),
    // Glossary term enhancements
    ("glossary_terms", "owner_id", "TEXT"),
    ("glossary_terms", "status", "TEXT DEFAULT 'draft'"),
    (
        "glossary_terms",
        "created_at",
        "TEXT DEFAULT CURRENT_TIMESTAMP",
    ),
    (
        "glossary_terms",
        "updated_at",
        "TEXT DEFAULT CURRENT_TIMESTAMP",
    ),
];

pub fn migration() -> Migration {
    Migration {
        version: VERSION,
        description: "v1.1.0: Domain Management and Glossary Enhancements",
        sql: SQL,
        add_columns: ADD_COLUMNS,
    }
}

const SQL: &str = r#"
-- ============================================================================
-- MetaFuse v1.1.0 Schema Migration
-- Domain Management and Glossary Enhancements
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. DOMAINS TABLE
-- Business domains for organizing datasets
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Unique domain identifier (slug format, e.g., "finance", "marketing")
    name TEXT UNIQUE NOT NULL,
    -- Human-readable display name
    display_name TEXT NOT NULL,
    -- Description of what this domain covers
    description TEXT,
    -- Owner/team responsible for this domain (references owners.owner_id)
    owner_id TEXT,
    -- Whether the domain is active (soft delete support)
    is_active INTEGER NOT NULL DEFAULT 1,
    -- Timestamps
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CHECK (name GLOB '[a-z0-9_-]*')
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_domains_name ON domains(name);
CREATE INDEX IF NOT EXISTS idx_domains_owner ON domains(owner_id);
CREATE INDEX IF NOT EXISTS idx_domains_active ON domains(is_active);

-- ----------------------------------------------------------------------------
-- 2. GLOSSARY TERM STATUS INDEX
-- Index for filtering by status (draft, approved, deprecated)
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_glossary_terms_domain ON glossary_terms(domain);
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::{column_exists, run_migrations};
    use rusqlite::Connection;

    #[test]
    fn test_migration_version() {
        assert_eq!(VERSION, 1_001_000);
    }

    #[test]
    fn test_migration_description() {
        let m = migration();
        assert!(m.description.contains("v1.1.0"));
        assert!(m.description.contains("Domain"));
    }

    #[test]
    fn test_domains_table_created() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        run_migrations(&conn).unwrap();

        // Verify domains table exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='domains'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // Verify we can insert a domain
        conn.execute(
            "INSERT INTO domains (name, display_name, description) VALUES ('finance', 'Finance', 'Financial data')",
            [],
        )
        .unwrap();

        // Verify the check constraint for name format
        let result = conn.execute(
            "INSERT INTO domains (name, display_name) VALUES ('Invalid Name!', 'Invalid')",
            [],
        );
        assert!(result.is_err(), "Should reject invalid domain names");
    }

    #[test]
    fn test_datasets_domain_id_column() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        run_migrations(&conn).unwrap();

        // Verify domain_id column exists in datasets
        assert!(column_exists(&conn, "datasets", "domain_id").unwrap());
    }

    #[test]
    fn test_glossary_terms_enhancements() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_sqlite_schema(&conn).unwrap();
        run_migrations(&conn).unwrap();

        // Verify new columns exist
        assert!(column_exists(&conn, "glossary_terms", "owner_id").unwrap());
        assert!(column_exists(&conn, "glossary_terms", "status").unwrap());
        assert!(column_exists(&conn, "glossary_terms", "created_at").unwrap());
        assert!(column_exists(&conn, "glossary_terms", "updated_at").unwrap());

        // Verify we can insert with new columns
        conn.execute(
            "INSERT INTO glossary_terms (term, description, domain, owner_id, status) VALUES ('revenue', 'Total income', 'finance', 'user@example.com', 'approved')",
            [],
        )
        .unwrap();
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
