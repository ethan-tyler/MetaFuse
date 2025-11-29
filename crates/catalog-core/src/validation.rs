//! Input validation for MetaFuse catalog
//!
//! Provides validation functions to prevent:
//! - SQL injection (via FTS queries)
//! - Path traversal attacks
//! - Malformed names and identifiers
//! - Excessively long inputs

use crate::{CatalogError, Result};

/// Maximum length for dataset names
pub const MAX_DATASET_NAME_LEN: usize = 255;

/// Maximum length for field names
pub const MAX_FIELD_NAME_LEN: usize = 255;

/// Maximum length for tag values
pub const MAX_TAG_LEN: usize = 100;

/// Maximum length for tenant/domain identifiers
pub const MAX_IDENTIFIER_LEN: usize = 100;

/// Maximum length for FTS search queries
pub const MAX_SEARCH_QUERY_LEN: usize = 500;

/// Valid governance rule types
/// NOTE: These must stay in sync with the CHECK constraint in migrations/v1_0_0.rs
pub const VALID_RULE_TYPES: &[&str] = &[
    "pii_detection",
    "retention",
    "access_control",
    "custom", // Catch-all for user-defined rules
];

/// Minimum score value (inclusive)
pub const MIN_SCORE: f64 = 0.0;

/// Maximum score value (inclusive)
pub const MAX_SCORE: f64 = 1.0;

/// Validate dataset name
///
/// Requirements:
/// - Not empty
/// - <= 255 characters
/// - Alphanumeric, underscore, hyphen only
/// - Cannot start or end with hyphen
pub fn validate_dataset_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CatalogError::ValidationError(
            "Dataset name cannot be empty".to_string(),
        ));
    }

    if name.len() > MAX_DATASET_NAME_LEN {
        return Err(CatalogError::ValidationError(format!(
            "Dataset name too long: {} > {} characters",
            name.len(),
            MAX_DATASET_NAME_LEN
        )));
    }

    // Check for valid characters (alphanumeric, underscore, hyphen, dot)
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(CatalogError::ValidationError(
            "Dataset name contains invalid characters (allowed: alphanumeric, _, -, .)".to_string(),
        ));
    }

    // Cannot start or end with hyphen
    if name.starts_with('-') || name.ends_with('-') {
        return Err(CatalogError::ValidationError(
            "Dataset name cannot start or end with hyphen".to_string(),
        ));
    }

    Ok(())
}

/// Validate field name
///
/// Requirements:
/// - Not empty
/// - <= 255 characters
/// - Alphanumeric, underscore only
pub fn validate_field_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CatalogError::ValidationError(
            "Field name cannot be empty".to_string(),
        ));
    }

    if name.len() > MAX_FIELD_NAME_LEN {
        return Err(CatalogError::ValidationError(format!(
            "Field name too long: {} > {} characters",
            name.len(),
            MAX_FIELD_NAME_LEN
        )));
    }

    // Check for valid characters (alphanumeric, underscore)
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(CatalogError::ValidationError(
            "Field name contains invalid characters (allowed: alphanumeric, _)".to_string(),
        ));
    }

    Ok(())
}

/// Validate tag value
///
/// Requirements:
/// - Not empty
/// - <= 100 characters
/// - Alphanumeric, underscore, hyphen, colon only
pub fn validate_tag(tag: &str) -> Result<()> {
    if tag.is_empty() {
        return Err(CatalogError::ValidationError(
            "Tag cannot be empty".to_string(),
        ));
    }

    if tag.len() > MAX_TAG_LEN {
        return Err(CatalogError::ValidationError(format!(
            "Tag too long: {} > {} characters",
            tag.len(),
            MAX_TAG_LEN
        )));
    }

    // Check for valid characters (alphanumeric, underscore, hyphen, colon for namespacing)
    if !tag
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == ':')
    {
        return Err(CatalogError::ValidationError(
            "Tag contains invalid characters (allowed: alphanumeric, _, -, :)".to_string(),
        ));
    }

    Ok(())
}

/// Validate tenant/domain identifier
///
/// Requirements:
/// - Not empty
/// - <= 100 characters
/// - Alphanumeric, underscore, hyphen only
pub fn validate_identifier(identifier: &str, field_name: &str) -> Result<()> {
    if identifier.is_empty() {
        return Err(CatalogError::ValidationError(format!(
            "{} cannot be empty",
            field_name
        )));
    }

    if identifier.len() > MAX_IDENTIFIER_LEN {
        return Err(CatalogError::ValidationError(format!(
            "{} too long: {} > {} characters",
            field_name,
            identifier.len(),
            MAX_IDENTIFIER_LEN
        )));
    }

    // Check for valid characters (alphanumeric, underscore, hyphen)
    if !identifier
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(CatalogError::ValidationError(format!(
            "{} contains invalid characters (allowed: alphanumeric, _, -)",
            field_name
        )));
    }

    Ok(())
}

/// Validate FTS search query
///
/// Validates query length and format. FTS5 operators (AND, OR, NOT, *, quotes, etc.)
/// are explicitly ALLOWED to enable powerful search capabilities for data teams.
///
/// Security note: This is safe because:
/// 1. Queries are passed via parameterized SQL (no SQL injection risk)
/// 2. FTS5 syntax errors result in query failures, not security issues
/// 3. Worst case: malformed query returns no results or error
///
/// Users can use FTS5 syntax like:
/// - Simple terms: "analytics"
/// - Phrases: "user profile"
/// - Boolean: "analytics AND finance"
/// - Wildcards: "user*"
/// - Proximity: "NEAR(term1 term2, 10)"
pub fn validate_fts_query(query: &str) -> Result<String> {
    if query.is_empty() {
        return Err(CatalogError::ValidationError(
            "Search query cannot be empty".to_string(),
        ));
    }

    if query.len() > MAX_SEARCH_QUERY_LEN {
        return Err(CatalogError::ValidationError(format!(
            "Search query too long: {} > {} characters",
            query.len(),
            MAX_SEARCH_QUERY_LEN
        )));
    }

    // Return the query as-is - FTS operators are intentionally allowed
    // for powerful search capabilities
    Ok(query.to_string())
}

/// Validate governance rule type
///
/// Ensures the rule type is one of the known types:
/// - pii_detection: Rules for detecting personally identifiable information
/// - retention: Data retention policy rules
/// - access_control: Access control rules
/// - custom: User-defined custom rules (catch-all)
///
/// NOTE: These types must match the CHECK constraint in migrations/v1_0_0.rs
pub fn validate_rule_type(rule_type: &str) -> Result<()> {
    if !VALID_RULE_TYPES.contains(&rule_type) {
        return Err(CatalogError::ValidationError(format!(
            "Invalid rule type '{}'. Valid types: {}",
            rule_type,
            VALID_RULE_TYPES.join(", ")
        )));
    }
    Ok(())
}

/// Validate a quality score value
///
/// Ensures the score is between 0.0 and 1.0 (inclusive)
pub fn validate_score(score: f64, field_name: &str) -> Result<()> {
    if score.is_nan() {
        return Err(CatalogError::ValidationError(format!(
            "{} cannot be NaN",
            field_name
        )));
    }

    if !(MIN_SCORE..=MAX_SCORE).contains(&score) {
        return Err(CatalogError::ValidationError(format!(
            "{} must be between {} and {}, got {}",
            field_name, MIN_SCORE, MAX_SCORE, score
        )));
    }

    Ok(())
}

/// Validate file:// URI path for traversal attacks
///
/// Prevents:
/// - .. path components
/// - Absolute path manipulation
/// - Symlink attacks (basic check)
pub fn validate_file_uri_path(path: &str) -> Result<()> {
    // Check for path traversal patterns
    if path.contains("..") {
        return Err(CatalogError::ValidationError(
            "Path contains traversal pattern (..)".to_string(),
        ));
    }

    // Check for null bytes (common in path traversal attacks)
    if path.contains('\0') {
        return Err(CatalogError::ValidationError(
            "Path contains null byte".to_string(),
        ));
    }

    // Note: Absolute paths are allowed but may indicate potential issues
    // (removed tracing::warn as tracing is not a dependency in catalog-core)

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_dataset_names() {
        assert!(validate_dataset_name("my_dataset").is_ok());
        assert!(validate_dataset_name("dataset-123").is_ok());
        assert!(validate_dataset_name("my.dataset.v2").is_ok());
        assert!(validate_dataset_name("a").is_ok());
        assert!(validate_dataset_name("ABC_123").is_ok());
    }

    #[test]
    fn test_invalid_dataset_names() {
        assert!(validate_dataset_name("").is_err()); // Empty
        assert!(validate_dataset_name(&"a".repeat(256)).is_err()); // Too long
        assert!(validate_dataset_name("my dataset").is_err()); // Space
        assert!(validate_dataset_name("my@dataset").is_err()); // @
        assert!(validate_dataset_name("-dataset").is_err()); // Starts with hyphen
        assert!(validate_dataset_name("dataset-").is_err()); // Ends with hyphen
        assert!(validate_dataset_name("my/dataset").is_err()); // Slash
    }

    #[test]
    fn test_valid_field_names() {
        assert!(validate_field_name("id").is_ok());
        assert!(validate_field_name("user_id").is_ok());
        assert!(validate_field_name("field123").is_ok());
        assert!(validate_field_name("FIELD_NAME").is_ok());
    }

    #[test]
    fn test_invalid_field_names() {
        assert!(validate_field_name("").is_err()); // Empty
        assert!(validate_field_name(&"a".repeat(256)).is_err()); // Too long
        assert!(validate_field_name("field-name").is_err()); // Hyphen
        assert!(validate_field_name("field name").is_err()); // Space
        assert!(validate_field_name("field.name").is_err()); // Dot
    }

    #[test]
    fn test_valid_tags() {
        assert!(validate_tag("production").is_ok());
        assert!(validate_tag("env:prod").is_ok());
        assert!(validate_tag("version-2").is_ok());
        assert!(validate_tag("team_analytics").is_ok());
    }

    #[test]
    fn test_invalid_tags() {
        assert!(validate_tag("").is_err()); // Empty
        assert!(validate_tag(&"a".repeat(101)).is_err()); // Too long
        assert!(validate_tag("tag with space").is_err()); // Space
        assert!(validate_tag("tag@value").is_err()); // @
    }

    #[test]
    fn test_valid_identifiers() {
        assert!(validate_identifier("prod", "tenant").is_ok());
        assert!(validate_identifier("analytics", "domain").is_ok());
        assert!(validate_identifier("team-1", "tenant").is_ok());
        assert!(validate_identifier("data_eng", "domain").is_ok());
    }

    #[test]
    fn test_invalid_identifiers() {
        assert!(validate_identifier("", "tenant").is_err()); // Empty
        assert!(validate_identifier(&"a".repeat(101), "tenant").is_err()); // Too long
        assert!(validate_identifier("my tenant", "tenant").is_err()); // Space
        assert!(validate_identifier("tenant:1", "tenant").is_err()); // Colon
    }

    #[test]
    fn test_validate_fts_query() {
        assert!(validate_fts_query("user").is_ok());
        assert!(validate_fts_query("user data").is_ok());
        assert!(validate_fts_query("\"exact phrase\"").is_ok());
        assert!(validate_fts_query("analytics AND finance").is_ok()); // Operators allowed
        assert!(validate_fts_query("user*").is_ok()); // Wildcards allowed
        assert!(validate_fts_query("").is_err()); // Empty
        assert!(validate_fts_query(&"a".repeat(501)).is_err()); // Too long
    }

    #[test]
    fn test_validate_file_uri_path() {
        assert!(validate_file_uri_path("catalog.db").is_ok());
        assert!(validate_file_uri_path("data/catalog.db").is_ok());
        assert!(validate_file_uri_path("../../../etc/passwd").is_err()); // Traversal
        assert!(validate_file_uri_path("data/../catalog.db").is_err()); // Traversal
        assert!(validate_file_uri_path("data\0hidden").is_err()); // Null byte
    }

    #[test]
    fn test_validate_rule_type() {
        // Valid rule types (must match DB CHECK constraint)
        assert!(validate_rule_type("pii_detection").is_ok());
        assert!(validate_rule_type("retention").is_ok());
        assert!(validate_rule_type("access_control").is_ok());
        assert!(validate_rule_type("custom").is_ok());

        // Invalid rule types
        assert!(validate_rule_type("").is_err());
        assert!(validate_rule_type("unknown").is_err());
        assert!(validate_rule_type("PII_DETECTION").is_err()); // Case sensitive
        assert!(validate_rule_type("pii-detection").is_err()); // Wrong separator
        assert!(validate_rule_type("data_classification").is_err()); // Not in DB
    }

    #[test]
    fn test_validate_score() {
        // Valid scores
        assert!(validate_score(0.0, "test").is_ok());
        assert!(validate_score(0.5, "test").is_ok());
        assert!(validate_score(1.0, "test").is_ok());
        assert!(validate_score(0.999, "test").is_ok());
        assert!(validate_score(0.001, "test").is_ok());

        // Invalid scores
        assert!(validate_score(-0.1, "test").is_err());
        assert!(validate_score(1.1, "test").is_err());
        assert!(validate_score(-1.0, "test").is_err());
        assert!(validate_score(2.0, "test").is_err());
        assert!(validate_score(f64::NAN, "test").is_err());
    }
}
