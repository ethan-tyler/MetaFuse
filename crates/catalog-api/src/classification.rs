//! Classification Engine Module
//!
//! This module provides PII and sensitivity classification for MetaFuse columns:
//! - **Rule-based detection**: Pattern matching using regex
//! - **Column name hints**: Detection based on column naming conventions
//! - **Manual overrides**: Support for human-verified classifications
//! - **Confidence scoring**: Confidence levels for auto-detected classifications
//!
//! # Architecture
//!
//! Classification rules are loaded from the `governance_rules` table and compiled
//! into regex patterns. The engine scans columns by name and optionally by data type.
//!
//! Classifications are stored in the `column_classifications` table with their
//! source (auto, manual, rule) and confidence level.

#![allow(dead_code)]

use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Classification types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Classification {
    /// Personally Identifiable Information
    Pii,
    /// Sensitive business data
    Sensitive,
    /// Confidential data
    Confidential,
    /// Public data
    Public,
    /// Unknown classification
    Unknown,
}

impl Classification {
    pub fn as_str(&self) -> &'static str {
        match self {
            Classification::Pii => "pii",
            Classification::Sensitive => "sensitive",
            Classification::Confidential => "confidential",
            Classification::Public => "public",
            Classification::Unknown => "unknown",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "pii" => Classification::Pii,
            "sensitive" => Classification::Sensitive,
            "confidential" => Classification::Confidential,
            "public" => Classification::Public,
            _ => Classification::Unknown,
        }
    }
}

/// How a classification was determined
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClassificationSource {
    /// Automatically detected
    Auto,
    /// Manually set by user
    Manual,
    /// Detected by governance rule
    Rule,
}

impl ClassificationSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            ClassificationSource::Auto => "auto",
            ClassificationSource::Manual => "manual",
            ClassificationSource::Rule => "rule",
        }
    }
}

/// A compiled governance rule for classification
#[derive(Debug)]
pub struct CompiledRule {
    /// Database ID of the rule
    pub rule_id: i64,
    /// Rule name
    pub name: String,
    /// Category (e.g., "email", "ssn")
    pub category: String,
    /// Classification to apply
    pub classification: Classification,
    /// Pre-compiled regex patterns
    pub patterns: Vec<Regex>,
    /// Column name hints (lowercase)
    pub column_names: Vec<String>,
    /// Priority (lower = higher priority)
    pub priority: i32,
}

/// Result of classifying a single column
#[derive(Debug, Clone, Serialize)]
pub struct ColumnClassification {
    /// Column name
    pub column_name: String,
    /// Detected classification
    pub classification: Classification,
    /// Category within classification (e.g., "email")
    pub category: Option<String>,
    /// Confidence score (0.0-1.0)
    pub confidence: f64,
    /// How the classification was determined
    pub source: ClassificationSource,
    /// Rule ID if detected by rule
    pub rule_id: Option<i64>,
}

/// The classification engine with compiled rules
pub struct ClassificationEngine {
    rules: Vec<CompiledRule>,
}

impl ClassificationEngine {
    /// Create a new classification engine from database rules
    pub fn load_from_db(conn: &rusqlite::Connection) -> Result<Self, ClassificationError> {
        let rules = load_and_compile_rules(conn)?;
        info!(
            rule_count = rules.len(),
            "Classification engine initialized"
        );
        Ok(Self { rules })
    }

    /// Create an engine with pre-compiled rules (for testing)
    pub fn with_rules(rules: Vec<CompiledRule>) -> Self {
        Self { rules }
    }

    /// Get the number of loaded rules
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// Classify a single column by name and optionally data type
    pub fn classify_column(&self, column_name: &str, data_type: &str) -> ColumnClassification {
        let column_lower = column_name.to_lowercase();

        // Check rules in priority order
        for rule in &self.rules {
            // Check column name hints first (highest confidence)
            for hint in &rule.column_names {
                if column_lower.contains(hint) {
                    return ColumnClassification {
                        column_name: column_name.to_string(),
                        classification: rule.classification,
                        category: Some(rule.category.clone()),
                        confidence: 0.9, // High confidence for name match
                        source: ClassificationSource::Rule,
                        rule_id: Some(rule.rule_id),
                    };
                }
            }
        }

        // For string columns, could do pattern matching on sample data
        // For now, we only use column name matching
        if !data_type.to_lowercase().contains("string") {
            // Non-string columns are unlikely to be PII
            return ColumnClassification {
                column_name: column_name.to_string(),
                classification: Classification::Unknown,
                category: None,
                confidence: 0.5,
                source: ClassificationSource::Auto,
                rule_id: None,
            };
        }

        // Default to unknown
        ColumnClassification {
            column_name: column_name.to_string(),
            classification: Classification::Unknown,
            category: None,
            confidence: 0.5,
            source: ClassificationSource::Auto,
            rule_id: None,
        }
    }

    /// Scan all columns and return classifications
    pub fn scan_columns(
        &self,
        columns: &[(String, String)], // (name, data_type)
    ) -> Vec<ColumnClassification> {
        columns
            .iter()
            .map(|(name, dtype)| self.classify_column(name, dtype))
            .collect()
    }
}

/// Maximum allowed length for a regex pattern (protect against oversized patterns)
const MAX_REGEX_PATTERN_LEN: usize = 1000;

/// Maximum compiled regex size limit (bytes) - protects against complex patterns
const MAX_REGEX_SIZE_LIMIT: usize = 10 * 1024; // 10KB

/// Load and compile rules from the governance_rules table
fn load_and_compile_rules(
    conn: &rusqlite::Connection,
) -> Result<Vec<CompiledRule>, ClassificationError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, name, config, priority
        FROM governance_rules
        WHERE rule_type = 'pii_detection' AND is_active = 1
        ORDER BY priority ASC
        "#,
    )?;

    let mut rules = Vec::new();
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i32>(3)?,
        ))
    })?;

    for row_result in rows {
        let (rule_id, name, config_str, priority) = row_result?;

        // Parse the JSON config
        let config: RuleConfig = match serde_json::from_str(&config_str) {
            Ok(c) => c,
            Err(e) => {
                warn!(rule_id, name = %name, error = %e, "Failed to parse rule config");
                continue;
            }
        };

        // Compile regex patterns with safety limits
        let mut patterns = Vec::new();
        for pattern in &config.patterns {
            // Check pattern length
            if pattern.len() > MAX_REGEX_PATTERN_LEN {
                warn!(
                    rule_id,
                    name = %name,
                    pattern_len = pattern.len(),
                    max_len = MAX_REGEX_PATTERN_LEN,
                    "Regex pattern exceeds maximum length, skipping"
                );
                continue;
            }

            // Compile with size limit to prevent ReDoS
            match regex::RegexBuilder::new(pattern)
                .size_limit(MAX_REGEX_SIZE_LIMIT)
                .build()
            {
                Ok(re) => patterns.push(re),
                Err(e) => {
                    warn!(rule_id, name = %name, pattern = %pattern, error = %e, "Failed to compile regex");
                    // Continue without this pattern
                }
            }
        }

        // Determine category from rule name
        let category = name.strip_suffix("_detection").unwrap_or(&name).to_string();

        rules.push(CompiledRule {
            rule_id,
            name,
            category,
            classification: Classification::Pii, // PII detection rules
            patterns,
            column_names: config
                .column_names
                .iter()
                .map(|s| s.to_lowercase())
                .collect(),
            priority,
        });
    }

    Ok(rules)
}

/// JSON config structure for governance rules
#[derive(Debug, Deserialize)]
struct RuleConfig {
    #[serde(default)]
    patterns: Vec<String>,
    #[serde(default)]
    column_names: Vec<String>,
}

/// Classification engine errors
#[derive(Debug)]
pub enum ClassificationError {
    /// Database error
    DatabaseError(rusqlite::Error),
    /// JSON parsing error
    JsonError(serde_json::Error),
}

impl std::fmt::Display for ClassificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClassificationError::DatabaseError(e) => write!(f, "Database error: {}", e),
            ClassificationError::JsonError(e) => write!(f, "JSON error: {}", e),
        }
    }
}

impl std::error::Error for ClassificationError {}

impl From<rusqlite::Error> for ClassificationError {
    fn from(e: rusqlite::Error) -> Self {
        ClassificationError::DatabaseError(e)
    }
}

impl From<serde_json::Error> for ClassificationError {
    fn from(e: serde_json::Error) -> Self {
        ClassificationError::JsonError(e)
    }
}

// =============================================================================
// Database Operations
// =============================================================================

/// Store a column classification in the database
pub fn store_classification(
    conn: &rusqlite::Connection,
    field_id: i64,
    classification: &ColumnClassification,
) -> Result<i64, rusqlite::Error> {
    // First, check if a classification exists for this field
    let existing: Option<i64> = conn
        .query_row(
            "SELECT id FROM column_classifications WHERE field_id = ?1",
            [field_id],
            |row| row.get(0),
        )
        .ok();

    if let Some(existing_id) = existing {
        // Update existing classification
        conn.execute(
            r#"
            UPDATE column_classifications SET
                classification = ?1,
                category = ?2,
                confidence = ?3,
                source = ?4,
                rule_id = ?5,
                updated_at = datetime('now')
            WHERE id = ?6
            "#,
            rusqlite::params![
                classification.classification.as_str(),
                classification.category,
                classification.confidence,
                classification.source.as_str(),
                classification.rule_id,
                existing_id,
            ],
        )?;
        Ok(existing_id)
    } else {
        // Insert new classification
        conn.execute(
            r#"
            INSERT INTO column_classifications (
                field_id, classification, category, confidence, source, rule_id,
                verified, created_at, updated_at
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6,
                0, datetime('now'), datetime('now')
            )
            "#,
            rusqlite::params![
                field_id,
                classification.classification.as_str(),
                classification.category,
                classification.confidence,
                classification.source.as_str(),
                classification.rule_id,
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }
}

/// Get classifications for a dataset's fields
pub fn get_dataset_classifications(
    conn: &rusqlite::Connection,
    dataset_id: i64,
) -> Result<Vec<ColumnClassificationEntry>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        r#"
        SELECT
            f.name as field_name,
            c.classification,
            c.category,
            c.confidence,
            c.source,
            c.rule_id,
            c.verified,
            c.verified_by,
            c.verified_at
        FROM fields f
        LEFT JOIN column_classifications c ON c.field_id = f.id
        WHERE f.dataset_id = ?1
        ORDER BY f.id
        "#,
    )?;

    let entries = stmt
        .query_map([dataset_id], |row| {
            let classification_str: Option<String> = row.get(1)?;
            Ok(ColumnClassificationEntry {
                field_name: row.get(0)?,
                classification: classification_str
                    .map(|s| Classification::parse(&s))
                    .unwrap_or(Classification::Unknown),
                category: row.get(2)?,
                confidence: row.get(3)?,
                source: row.get::<_, Option<String>>(4)?.map(|s| match s.as_str() {
                    "manual" => ClassificationSource::Manual,
                    "rule" => ClassificationSource::Rule,
                    _ => ClassificationSource::Auto,
                }),
                rule_id: row.get(5)?,
                verified: row
                    .get::<_, Option<i32>>(6)?
                    .map(|v| v == 1)
                    .unwrap_or(false),
                verified_by: row.get(7)?,
                verified_at: row.get(8)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(entries)
}

/// Get all columns with PII classification
pub fn get_pii_columns(
    conn: &rusqlite::Connection,
) -> Result<Vec<PiiColumnEntry>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        r#"
        SELECT
            d.name as dataset_name,
            f.name as field_name,
            c.category,
            c.confidence,
            c.verified
        FROM column_classifications c
        JOIN fields f ON f.id = c.field_id
        JOIN datasets d ON d.id = f.dataset_id
        WHERE c.classification = 'pii'
        ORDER BY d.name, f.name
        "#,
    )?;

    let entries = stmt
        .query_map([], |row| {
            Ok(PiiColumnEntry {
                dataset_name: row.get(0)?,
                field_name: row.get(1)?,
                category: row.get(2)?,
                confidence: row.get::<_, Option<f64>>(3)?,
                verified: row.get::<_, i32>(4)? == 1,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(entries)
}

/// Update a classification manually
pub fn set_manual_classification(
    conn: &rusqlite::Connection,
    field_id: i64,
    classification: Classification,
    category: Option<&str>,
    verified_by: &str,
) -> Result<(), rusqlite::Error> {
    // Check if a classification exists for this field
    let existing: Option<i64> = conn
        .query_row(
            "SELECT id FROM column_classifications WHERE field_id = ?1",
            [field_id],
            |row| row.get(0),
        )
        .ok();

    if let Some(existing_id) = existing {
        // Update existing classification
        conn.execute(
            r#"
            UPDATE column_classifications SET
                classification = ?1,
                category = ?2,
                confidence = 1.0,
                source = 'manual',
                verified = 1,
                verified_by = ?3,
                verified_at = datetime('now'),
                updated_at = datetime('now')
            WHERE id = ?4
            "#,
            rusqlite::params![classification.as_str(), category, verified_by, existing_id],
        )?;
    } else {
        // Insert new manual classification
        conn.execute(
            r#"
            INSERT INTO column_classifications (
                field_id, classification, category, confidence, source,
                verified, verified_by, verified_at, created_at, updated_at
            ) VALUES (
                ?1, ?2, ?3, 1.0, 'manual',
                1, ?4, datetime('now'), datetime('now'), datetime('now')
            )
            "#,
            rusqlite::params![field_id, classification.as_str(), category, verified_by],
        )?;
    }

    Ok(())
}

// =============================================================================
// Response Types
// =============================================================================

/// Entry in dataset classifications response
#[derive(Debug, Clone, Serialize)]
pub struct ColumnClassificationEntry {
    pub field_name: String,
    pub classification: Classification,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<ClassificationSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_id: Option<i64>,
    pub verified: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_at: Option<String>,
}

/// Response for dataset classifications endpoint
#[derive(Debug, Clone, Serialize)]
pub struct DatasetClassificationsResponse {
    pub dataset_id: i64,
    pub dataset_name: String,
    pub classifications: Vec<ColumnClassificationEntry>,
    pub pii_count: usize,
    pub unclassified_count: usize,
}

/// Entry in PII columns response
#[derive(Debug, Clone, Serialize)]
pub struct PiiColumnEntry {
    pub dataset_name: String,
    pub field_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    pub verified: bool,
}

/// Response for PII columns endpoint
#[derive(Debug, Clone, Serialize)]
pub struct PiiColumnsResponse {
    pub total_pii_columns: usize,
    pub verified_count: usize,
    pub columns: Vec<PiiColumnEntry>,
}

/// Request to set manual classification
#[derive(Debug, Clone, Deserialize)]
pub struct SetClassificationRequest {
    pub classification: String,
    pub category: Option<String>,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classification_as_str() {
        assert_eq!(Classification::Pii.as_str(), "pii");
        assert_eq!(Classification::Sensitive.as_str(), "sensitive");
        assert_eq!(Classification::Confidential.as_str(), "confidential");
        assert_eq!(Classification::Public.as_str(), "public");
        assert_eq!(Classification::Unknown.as_str(), "unknown");
    }

    #[test]
    fn test_classification_from_str() {
        assert_eq!(Classification::parse("pii"), Classification::Pii);
        assert_eq!(Classification::parse("PII"), Classification::Pii);
        assert_eq!(Classification::parse("invalid"), Classification::Unknown);
    }

    #[test]
    fn test_classification_source_as_str() {
        assert_eq!(ClassificationSource::Auto.as_str(), "auto");
        assert_eq!(ClassificationSource::Manual.as_str(), "manual");
        assert_eq!(ClassificationSource::Rule.as_str(), "rule");
    }

    #[test]
    fn test_engine_classify_email() {
        let rules = vec![CompiledRule {
            rule_id: 1,
            name: "email_detection".to_string(),
            category: "email".to_string(),
            classification: Classification::Pii,
            patterns: vec![],
            column_names: vec!["email".to_string(), "e_mail".to_string()],
            priority: 10,
        }];

        let engine = ClassificationEngine::with_rules(rules);

        // Should match email
        let result = engine.classify_column("user_email", "STRING");
        assert_eq!(result.classification, Classification::Pii);
        assert_eq!(result.category, Some("email".to_string()));
        assert!(result.confidence > 0.8);

        // Should not match random column
        let result = engine.classify_column("user_name", "STRING");
        assert_eq!(result.classification, Classification::Unknown);
    }

    #[test]
    fn test_engine_classify_ssn() {
        let rules = vec![CompiledRule {
            rule_id: 2,
            name: "ssn_detection".to_string(),
            category: "ssn".to_string(),
            classification: Classification::Pii,
            patterns: vec![],
            column_names: vec!["ssn".to_string(), "social_security".to_string()],
            priority: 10,
        }];

        let engine = ClassificationEngine::with_rules(rules);

        let result = engine.classify_column("customer_ssn", "STRING");
        assert_eq!(result.classification, Classification::Pii);
        assert_eq!(result.category, Some("ssn".to_string()));
    }

    #[test]
    fn test_engine_classify_phone() {
        let rules = vec![CompiledRule {
            rule_id: 3,
            name: "phone_detection".to_string(),
            category: "phone".to_string(),
            classification: Classification::Pii,
            patterns: vec![],
            column_names: vec![
                "phone".to_string(),
                "mobile".to_string(),
                "cell".to_string(),
            ],
            priority: 20,
        }];

        let engine = ClassificationEngine::with_rules(rules);

        let result = engine.classify_column("phone_number", "STRING");
        assert_eq!(result.classification, Classification::Pii);

        let result = engine.classify_column("mobile", "STRING");
        assert_eq!(result.classification, Classification::Pii);
    }

    #[test]
    fn test_engine_non_string_column() {
        let rules = vec![CompiledRule {
            rule_id: 1,
            name: "email_detection".to_string(),
            category: "email".to_string(),
            classification: Classification::Pii,
            patterns: vec![],
            column_names: vec!["email".to_string()],
            priority: 10,
        }];

        let engine = ClassificationEngine::with_rules(rules);

        // Non-string column that matches name still gets classified
        let result = engine.classify_column("email_count", "INTEGER");
        assert_eq!(result.classification, Classification::Pii);
    }

    #[test]
    fn test_engine_scan_columns() {
        let rules = vec![
            CompiledRule {
                rule_id: 1,
                name: "email_detection".to_string(),
                category: "email".to_string(),
                classification: Classification::Pii,
                patterns: vec![],
                column_names: vec!["email".to_string()],
                priority: 10,
            },
            CompiledRule {
                rule_id: 2,
                name: "phone_detection".to_string(),
                category: "phone".to_string(),
                classification: Classification::Pii,
                patterns: vec![],
                column_names: vec!["phone".to_string()],
                priority: 20,
            },
        ];

        let engine = ClassificationEngine::with_rules(rules);

        let columns = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "STRING".to_string()),
            ("email".to_string(), "STRING".to_string()),
            ("phone".to_string(), "STRING".to_string()),
        ];

        let results = engine.scan_columns(&columns);

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].classification, Classification::Unknown);
        assert_eq!(results[1].classification, Classification::Unknown);
        assert_eq!(results[2].classification, Classification::Pii);
        assert_eq!(results[3].classification, Classification::Pii);
    }

    #[test]
    fn test_load_rules_from_db() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        // Load engine - should have default rules from migration
        let engine = ClassificationEngine::load_from_db(&conn).unwrap();

        // Default migration inserts 5 PII detection rules
        assert!(engine.rule_count() >= 5);
    }

    #[test]
    fn test_store_and_get_classification() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        // Create a dataset and field
        conn.execute(
            "INSERT INTO datasets (name, path, format, created_at, last_updated)
             VALUES ('test_ds', '/test', 'delta', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();

        let dataset_id: i64 = conn
            .query_row(
                "SELECT id FROM datasets WHERE name = 'test_ds'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        conn.execute(
            "INSERT INTO fields (dataset_id, name, data_type, nullable)
             VALUES (?1, 'user_email', 'STRING', 1)",
            [dataset_id],
        )
        .unwrap();

        let field_id: i64 = conn
            .query_row(
                "SELECT id FROM fields WHERE dataset_id = ?1 AND name = 'user_email'",
                [dataset_id],
                |row| row.get(0),
            )
            .unwrap();

        // Store classification
        let classification = ColumnClassification {
            column_name: "user_email".to_string(),
            classification: Classification::Pii,
            category: Some("email".to_string()),
            confidence: 0.9,
            source: ClassificationSource::Rule,
            rule_id: Some(1),
        };

        store_classification(&conn, field_id, &classification).unwrap();

        // Get classifications
        let results = get_dataset_classifications(&conn, dataset_id).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].field_name, "user_email");
        assert_eq!(results[0].classification, Classification::Pii);
        assert_eq!(results[0].category, Some("email".to_string()));
    }

    #[test]
    fn test_set_manual_classification() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        // Create dataset and field
        conn.execute(
            "INSERT INTO datasets (name, path, format, created_at, last_updated)
             VALUES ('test_ds', '/test', 'delta', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();

        let dataset_id: i64 = conn
            .query_row(
                "SELECT id FROM datasets WHERE name = 'test_ds'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        conn.execute(
            "INSERT INTO fields (dataset_id, name, data_type, nullable)
             VALUES (?1, 'secret_key', 'STRING', 1)",
            [dataset_id],
        )
        .unwrap();

        let field_id: i64 = conn
            .query_row(
                "SELECT id FROM fields WHERE dataset_id = ?1 AND name = 'secret_key'",
                [dataset_id],
                |row| row.get(0),
            )
            .unwrap();

        // Set manual classification
        set_manual_classification(
            &conn,
            field_id,
            Classification::Confidential,
            Some("api_key"),
            "admin@example.com",
        )
        .unwrap();

        // Verify
        let results = get_dataset_classifications(&conn, dataset_id).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].classification, Classification::Confidential);
        assert!(results[0].verified);
        assert_eq!(
            results[0].verified_by,
            Some("admin@example.com".to_string())
        );
    }

    #[test]
    fn test_get_pii_columns() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        // Create datasets and fields
        conn.execute(
            "INSERT INTO datasets (name, path, format, created_at, last_updated)
             VALUES ('users', '/users', 'delta', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();

        let users_id: i64 = conn
            .query_row("SELECT id FROM datasets WHERE name = 'users'", [], |row| {
                row.get(0)
            })
            .unwrap();

        conn.execute(
            "INSERT INTO fields (dataset_id, name, data_type, nullable) VALUES (?1, 'email', 'STRING', 1)",
            [users_id],
        ).unwrap();

        let email_field_id: i64 = conn
            .query_row(
                "SELECT id FROM fields WHERE dataset_id = ?1 AND name = 'email'",
                [users_id],
                |row| row.get(0),
            )
            .unwrap();

        // Store PII classification
        let classification = ColumnClassification {
            column_name: "email".to_string(),
            classification: Classification::Pii,
            category: Some("email".to_string()),
            confidence: 0.9,
            source: ClassificationSource::Rule,
            rule_id: Some(1),
        };
        store_classification(&conn, email_field_id, &classification).unwrap();

        // Get all PII columns
        let pii_columns = get_pii_columns(&conn).unwrap();

        assert_eq!(pii_columns.len(), 1);
        assert_eq!(pii_columns[0].dataset_name, "users");
        assert_eq!(pii_columns[0].field_name, "email");
    }
}
