// Infrastructure for quality framework - async QualityCalculator not yet wired to handlers
#![allow(dead_code)]

//! Quality Framework Module
//!
//! This module provides data quality computation and tracking for MetaFuse datasets:
//! - **Completeness Score**: Based on null counts from Delta column statistics
//! - **Freshness Score**: Based on last_modified vs configured SLA
//! - **File Health Score**: Based on small file ratio and file size distribution
//! - **Overall Score**: Weighted combination of the above
//!
//! # Architecture
//!
//! Quality scores are computed on-demand from Delta metadata and stored in the
//! `quality_metrics` table. Each computation creates a new row (history is preserved).
//!
//! Partial failures are handled gracefully - if one score can't be computed,
//! the others are still calculated and returned.

use serde::Serialize;
use tracing::{debug, info, warn};

/// Minimum file size in bytes considered "healthy" (128 MB)
const SMALL_FILE_THRESHOLD_BYTES: i64 = 128 * 1024 * 1024;

/// Default weight for completeness in overall score
const WEIGHT_COMPLETENESS: f64 = 0.4;

/// Default weight for freshness in overall score
const WEIGHT_FRESHNESS: f64 = 0.4;

/// Default weight for file health in overall score
const WEIGHT_FILE_HEALTH: f64 = 0.2;

/// Quality scores computed from Delta metadata
#[derive(Debug, Clone, Serialize)]
pub struct QualityScores {
    /// Completeness score (0.0-1.0): 1.0 - (null_cells / total_cells)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completeness_score: Option<f64>,

    /// Freshness score (0.0-1.0): Based on SLA from freshness_config
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness_score: Option<f64>,

    /// File health score (0.0-1.0): Based on small file ratio
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_health_score: Option<f64>,

    /// Overall quality score (weighted average)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overall_score: Option<f64>,

    /// Detailed breakdown of quality metrics
    pub details: QualityDetails,
}

/// Detailed quality metrics
#[derive(Debug, Clone, Serialize, Default)]
pub struct QualityDetails {
    /// Total number of rows
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<i64>,

    /// Total number of files
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_count: Option<i64>,

    /// Total size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<i64>,

    /// Number of small files (below threshold)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub small_file_count: Option<i64>,

    /// Average file size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_file_size: Option<i64>,

    /// Total null cell count across all columns
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_null_count: Option<i64>,

    /// Expected freshness interval in seconds (from SLA)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness_sla_secs: Option<i64>,

    /// Actual staleness in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staleness_secs: Option<i64>,

    /// Last modification timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
}

/// Response for quality endpoint
#[derive(Debug, Clone, Serialize)]
pub struct QualityResponse {
    pub dataset_id: i64,
    pub dataset_name: String,
    pub computed_at: String,
    #[serde(flatten)]
    pub scores: QualityScores,
}

/// Response for unhealthy datasets endpoint
#[derive(Debug, Clone, Serialize)]
pub struct UnhealthyDatasetsResponse {
    pub threshold: f64,
    pub datasets: Vec<UnhealthyDatasetEntry>,
}

/// Entry in unhealthy datasets list
#[derive(Debug, Clone, Serialize)]
pub struct UnhealthyDatasetEntry {
    pub dataset_id: i64,
    pub dataset_name: String,
    pub overall_score: f64,
    pub completeness_score: Option<f64>,
    pub freshness_score: Option<f64>,
    pub file_health_score: Option<f64>,
    pub computed_at: String,
}

/// Quality calculator using Delta metadata
pub struct QualityCalculator {
    delta_reader: std::sync::Arc<metafuse_catalog_delta::DeltaReader>,
}

impl QualityCalculator {
    /// Create a new quality calculator
    pub fn new(delta_reader: std::sync::Arc<metafuse_catalog_delta::DeltaReader>) -> Self {
        Self { delta_reader }
    }

    /// Compute quality scores for a dataset
    ///
    /// Partial failures are handled gracefully - each score is computed independently.
    pub async fn compute(
        &self,
        conn: &rusqlite::Connection,
        dataset_id: i64,
        delta_location: &str,
    ) -> Result<QualityScores, QualityError> {
        debug!(dataset_id, delta_location, "Computing quality scores");

        // Get Delta metadata
        let metadata = self
            .delta_reader
            .get_metadata_cached(delta_location)
            .await
            .map_err(|e| QualityError::DeltaError(e.to_string()))?;

        // Initialize details
        let mut details = QualityDetails {
            row_count: Some(metadata.row_count),
            file_count: Some(metadata.num_files),
            size_bytes: Some(metadata.size_bytes),
            last_modified: Some(metadata.last_modified.to_rfc3339()),
            ..Default::default()
        };

        // Compute each score independently
        let completeness = self.compute_completeness(&metadata, &mut details);
        let freshness = self
            .compute_freshness(conn, dataset_id, &metadata, &mut details)
            .ok();
        let file_health = self.compute_file_health(&metadata, &mut details);

        // Compute overall (requires at least one score)
        let available_scores: Vec<(f64, f64)> = [
            (completeness, WEIGHT_COMPLETENESS),
            (freshness, WEIGHT_FRESHNESS),
            (file_health, WEIGHT_FILE_HEALTH),
        ]
        .iter()
        .filter_map(|(s, w)| s.map(|score| (score, *w)))
        .collect();

        let overall = if available_scores.is_empty() {
            None
        } else {
            let total_weight: f64 = available_scores.iter().map(|(_, w)| w).sum();
            let weighted_sum: f64 = available_scores.iter().map(|(s, w)| s * w).sum();
            Some(clamp_score(weighted_sum / total_weight))
        };

        let scores = QualityScores {
            completeness_score: completeness,
            freshness_score: freshness,
            file_health_score: file_health,
            overall_score: overall,
            details,
        };

        info!(
            dataset_id,
            overall = ?overall,
            completeness = ?completeness,
            freshness = ?freshness,
            file_health = ?file_health,
            "Quality scores computed"
        );

        Ok(scores)
    }

    /// Compute completeness score from null counts
    ///
    /// Score = 1.0 - (total_nulls / (row_count * column_count))
    /// Empty tables return 1.0 (vacuously complete)
    fn compute_completeness(
        &self,
        metadata: &metafuse_catalog_delta::DeltaMetadata,
        details: &mut QualityDetails,
    ) -> Option<f64> {
        let row_count = metadata.row_count;
        let column_count = metadata.schema.fields.len() as i64;

        if row_count == 0 || column_count == 0 {
            // Empty table is vacuously complete
            return Some(1.0);
        }

        // Sum null counts from column stats
        let total_nulls: i64 = metadata
            .column_stats
            .iter()
            .filter_map(|s| s.null_count)
            .sum();

        details.total_null_count = Some(total_nulls);

        let total_cells = row_count * column_count;
        let null_ratio = total_nulls as f64 / total_cells as f64;
        let score = 1.0 - null_ratio;

        Some(clamp_score(score))
    }

    /// Compute freshness score based on SLA configuration
    ///
    /// Score degrades from 1.0 towards 0.0 as staleness increases beyond SLA.
    fn compute_freshness(
        &self,
        conn: &rusqlite::Connection,
        dataset_id: i64,
        metadata: &metafuse_catalog_delta::DeltaMetadata,
        details: &mut QualityDetails,
    ) -> Result<f64, QualityError> {
        // Get freshness config for this dataset
        let config: Option<(i64, i64)> = conn
            .query_row(
                "SELECT expected_interval_secs, grace_period_secs FROM freshness_config WHERE dataset_id = ?1",
                [dataset_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        let (expected_interval, grace_period) = config.ok_or(QualityError::NoFreshnessConfig)?;

        details.freshness_sla_secs = Some(expected_interval);

        // Calculate staleness
        let last_modified = metadata.last_modified;
        let now = chrono::Utc::now();
        let staleness_secs = (now - last_modified).num_seconds();
        details.staleness_secs = Some(staleness_secs);

        // Calculate score
        // 1.0 if within SLA + grace, degrades towards 0.0 as staleness increases
        let threshold_secs = expected_interval + grace_period;

        let score = if staleness_secs <= threshold_secs {
            1.0
        } else {
            // Degrade linearly: halve the score for each additional SLA period of staleness
            let extra_staleness = staleness_secs - threshold_secs;
            let periods_overdue = extra_staleness as f64 / expected_interval as f64;
            1.0 / (1.0 + periods_overdue)
        };

        Ok(clamp_score(score))
    }

    /// Compute file health score based on small file ratio
    ///
    /// Small files (< 128MB) indicate potential performance issues.
    /// Score = 1.0 - (small_file_ratio * 0.5)
    fn compute_file_health(
        &self,
        metadata: &metafuse_catalog_delta::DeltaMetadata,
        details: &mut QualityDetails,
    ) -> Option<f64> {
        let num_files = metadata.num_files;

        if num_files == 0 {
            return Some(1.0);
        }

        let avg_file_size = metadata.size_bytes / num_files;
        details.avg_file_size = Some(avg_file_size);

        // Count small files
        let small_file_count = if avg_file_size < SMALL_FILE_THRESHOLD_BYTES {
            // If average is below threshold, estimate based on ratio
            // This is an approximation since we don't have individual file sizes
            let estimated_small = (SMALL_FILE_THRESHOLD_BYTES as f64 / avg_file_size as f64)
                .min(num_files as f64) as i64;
            estimated_small.min(num_files)
        } else {
            0
        };

        details.small_file_count = Some(small_file_count);

        let small_file_ratio = small_file_count as f64 / num_files as f64;

        // Score: 1.0 for no small files, down to 0.5 for all small files
        let score = 1.0 - (small_file_ratio * 0.5);

        Some(clamp_score(score))
    }
}

/// Compute quality scores from pre-fetched Delta metadata (synchronous)
///
/// This function takes already-fetched DeltaMetadata and performs synchronous
/// DB operations for freshness config lookup. Use this when the Delta metadata
/// is already available to avoid passing &Connection across async boundaries.
pub fn compute_scores_from_metadata(
    conn: &rusqlite::Connection,
    dataset_id: i64,
    metadata: &metafuse_catalog_delta::DeltaMetadata,
) -> Result<QualityScores, QualityError> {
    tracing::debug!(dataset_id, "Computing quality scores from metadata");

    // Initialize details
    let mut details = QualityDetails {
        row_count: Some(metadata.row_count),
        file_count: Some(metadata.num_files),
        size_bytes: Some(metadata.size_bytes),
        last_modified: Some(metadata.last_modified.to_rfc3339()),
        ..Default::default()
    };

    // Compute each score independently
    let completeness = compute_completeness_sync(metadata, &mut details);
    let freshness = compute_freshness_sync(conn, dataset_id, metadata, &mut details).ok();
    let file_health = compute_file_health_sync(metadata, &mut details);

    // Compute overall (requires at least one score)
    let available_scores: Vec<(f64, f64)> = [
        (completeness, WEIGHT_COMPLETENESS),
        (freshness, WEIGHT_FRESHNESS),
        (file_health, WEIGHT_FILE_HEALTH),
    ]
    .iter()
    .filter_map(|(s, w)| s.map(|score| (score, *w)))
    .collect();

    let overall = if available_scores.is_empty() {
        None
    } else {
        let total_weight: f64 = available_scores.iter().map(|(_, w)| w).sum();
        let weighted_sum: f64 = available_scores.iter().map(|(s, w)| s * w).sum();
        Some(clamp_score(weighted_sum / total_weight))
    };

    tracing::info!(
        dataset_id,
        overall = ?overall,
        completeness = ?completeness,
        freshness = ?freshness,
        file_health = ?file_health,
        "Quality scores computed"
    );

    Ok(QualityScores {
        completeness_score: completeness,
        freshness_score: freshness,
        file_health_score: file_health,
        overall_score: overall,
        details,
    })
}

/// Compute completeness score from null counts (standalone function)
fn compute_completeness_sync(
    metadata: &metafuse_catalog_delta::DeltaMetadata,
    details: &mut QualityDetails,
) -> Option<f64> {
    let row_count = metadata.row_count;
    let column_count = metadata.schema.fields.len() as i64;

    if row_count == 0 || column_count == 0 {
        return Some(1.0);
    }

    let total_nulls: i64 = metadata
        .column_stats
        .iter()
        .filter_map(|s| s.null_count)
        .sum();

    details.total_null_count = Some(total_nulls);

    let total_cells = row_count * column_count;
    let null_ratio = total_nulls as f64 / total_cells as f64;
    Some(clamp_score(1.0 - null_ratio))
}

/// Compute freshness score based on SLA configuration (standalone function)
fn compute_freshness_sync(
    conn: &rusqlite::Connection,
    dataset_id: i64,
    metadata: &metafuse_catalog_delta::DeltaMetadata,
    details: &mut QualityDetails,
) -> Result<f64, QualityError> {
    let config: Option<(i64, i64)> = conn
        .query_row(
            "SELECT expected_interval_secs, grace_period_secs FROM freshness_config WHERE dataset_id = ?1",
            [dataset_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .ok();

    let (expected_interval, grace_period) = config.ok_or(QualityError::NoFreshnessConfig)?;

    details.freshness_sla_secs = Some(expected_interval);

    let last_modified = metadata.last_modified;
    let now = chrono::Utc::now();
    let staleness_secs = (now - last_modified).num_seconds();
    details.staleness_secs = Some(staleness_secs);

    let threshold_secs = expected_interval + grace_period;
    let score = if staleness_secs <= threshold_secs {
        1.0
    } else {
        let extra_staleness = staleness_secs - threshold_secs;
        let periods_overdue = extra_staleness as f64 / expected_interval as f64;
        1.0 / (1.0 + periods_overdue)
    };

    Ok(clamp_score(score))
}

/// Compute file health score based on small file ratio (standalone function)
fn compute_file_health_sync(
    metadata: &metafuse_catalog_delta::DeltaMetadata,
    details: &mut QualityDetails,
) -> Option<f64> {
    let num_files = metadata.num_files;

    if num_files == 0 {
        return Some(1.0);
    }

    let avg_file_size = metadata.size_bytes / num_files;
    details.avg_file_size = Some(avg_file_size);

    let small_file_count = if avg_file_size < SMALL_FILE_THRESHOLD_BYTES {
        let estimated_small =
            (SMALL_FILE_THRESHOLD_BYTES as f64 / avg_file_size as f64).min(num_files as f64) as i64;
        estimated_small.min(num_files)
    } else {
        0
    };

    details.small_file_count = Some(small_file_count);

    let small_file_ratio = small_file_count as f64 / num_files as f64;
    Some(clamp_score(1.0 - (small_file_ratio * 0.5)))
}

/// Clamp a score to valid range [0.0, 1.0]
fn clamp_score(score: f64) -> f64 {
    if score.is_nan() || score.is_infinite() {
        warn!(score, "Invalid score value, clamping to 0.0");
        return 0.0;
    }
    score.clamp(0.0, 1.0)
}

/// Quality computation errors
#[derive(Debug, Clone)]
pub enum QualityError {
    /// Delta table error
    DeltaError(String),
    /// No freshness configuration for dataset
    NoFreshnessConfig,
    /// No last modified timestamp available
    NoLastModified,
    /// Database error
    DatabaseError(String),
    /// All score calculations failed
    AllCalculationsFailed,
}

impl std::fmt::Display for QualityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QualityError::DeltaError(e) => write!(f, "Delta error: {}", e),
            QualityError::NoFreshnessConfig => write!(f, "No freshness configuration"),
            QualityError::NoLastModified => write!(f, "No last modified timestamp"),
            QualityError::DatabaseError(e) => write!(f, "Database error: {}", e),
            QualityError::AllCalculationsFailed => write!(f, "All quality calculations failed"),
        }
    }
}

impl std::error::Error for QualityError {}

// =============================================================================
// Database Operations
// =============================================================================

/// Store quality scores in the database
pub fn store_quality_scores(
    conn: &rusqlite::Connection,
    dataset_id: i64,
    scores: &QualityScores,
) -> Result<i64, rusqlite::Error> {
    let details_json = serde_json::to_string(&scores.details).ok();

    conn.execute(
        r#"
        INSERT INTO quality_metrics (
            dataset_id, computed_at,
            completeness_score, freshness_score, file_health_score, overall_score,
            row_count, file_count, size_bytes, small_file_count, avg_file_size, details
        ) VALUES (
            ?1, datetime('now'),
            ?2, ?3, ?4, ?5,
            ?6, ?7, ?8, ?9, ?10, ?11
        )
        "#,
        rusqlite::params![
            dataset_id,
            scores.completeness_score,
            scores.freshness_score,
            scores.file_health_score,
            scores.overall_score,
            scores.details.row_count,
            scores.details.file_count,
            scores.details.size_bytes,
            scores.details.small_file_count,
            scores.details.avg_file_size,
            details_json,
        ],
    )?;

    Ok(conn.last_insert_rowid())
}

/// Get the latest quality scores for a dataset
pub fn get_latest_quality(
    conn: &rusqlite::Connection,
    dataset_id: i64,
    dataset_name: &str,
) -> Result<Option<QualityResponse>, rusqlite::Error> {
    let result = conn.query_row(
        r#"
        SELECT
            completeness_score, freshness_score, file_health_score, overall_score,
            row_count, file_count, size_bytes, small_file_count, avg_file_size,
            computed_at
        FROM quality_metrics
        WHERE dataset_id = ?1
        ORDER BY computed_at DESC
        LIMIT 1
        "#,
        [dataset_id],
        |row| {
            Ok(QualityResponse {
                dataset_id,
                dataset_name: dataset_name.to_string(),
                computed_at: row.get(9)?,
                scores: QualityScores {
                    completeness_score: row.get(0)?,
                    freshness_score: row.get(1)?,
                    file_health_score: row.get(2)?,
                    overall_score: row.get(3)?,
                    details: QualityDetails {
                        row_count: row.get(4)?,
                        file_count: row.get(5)?,
                        size_bytes: row.get(6)?,
                        small_file_count: row.get(7)?,
                        avg_file_size: row.get(8)?,
                        ..Default::default()
                    },
                },
            })
        },
    );

    match result {
        Ok(r) => Ok(Some(r)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Get datasets with overall quality below threshold
pub fn get_unhealthy_datasets(
    conn: &rusqlite::Connection,
    threshold: f64,
) -> Result<UnhealthyDatasetsResponse, rusqlite::Error> {
    let mut stmt = conn.prepare(
        r#"
        SELECT
            d.id, d.name,
            q.overall_score, q.completeness_score, q.freshness_score, q.file_health_score,
            q.computed_at
        FROM datasets d
        JOIN quality_metrics q ON q.dataset_id = d.id
        WHERE q.id = (
            SELECT id FROM quality_metrics WHERE dataset_id = d.id ORDER BY computed_at DESC LIMIT 1
        )
        AND q.overall_score < ?1
        ORDER BY q.overall_score ASC
        "#,
    )?;

    let datasets: Vec<UnhealthyDatasetEntry> = stmt
        .query_map([threshold], |row| {
            Ok(UnhealthyDatasetEntry {
                dataset_id: row.get(0)?,
                dataset_name: row.get(1)?,
                overall_score: row.get(2)?,
                completeness_score: row.get(3)?,
                freshness_score: row.get(4)?,
                file_health_score: row.get(5)?,
                computed_at: row.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(UnhealthyDatasetsResponse {
        threshold,
        datasets,
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clamp_score_valid() {
        assert_eq!(clamp_score(0.5), 0.5);
        assert_eq!(clamp_score(0.0), 0.0);
        assert_eq!(clamp_score(1.0), 1.0);
    }

    #[test]
    fn test_clamp_score_out_of_range() {
        assert_eq!(clamp_score(1.5), 1.0);
        assert_eq!(clamp_score(-0.5), 0.0);
    }

    #[test]
    fn test_clamp_score_nan_inf() {
        assert_eq!(clamp_score(f64::NAN), 0.0);
        assert_eq!(clamp_score(f64::INFINITY), 0.0);
        assert_eq!(clamp_score(f64::NEG_INFINITY), 0.0);
    }

    #[test]
    fn test_quality_error_display() {
        let err = QualityError::DeltaError("test".to_string());
        assert!(err.to_string().contains("Delta error"));

        let err = QualityError::NoFreshnessConfig;
        assert!(err.to_string().contains("freshness"));
    }

    #[test]
    fn test_quality_details_default() {
        let details = QualityDetails::default();
        assert!(details.row_count.is_none());
        assert!(details.file_count.is_none());
        assert!(details.size_bytes.is_none());
    }

    #[test]
    fn test_store_and_get_quality() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        // Insert a dataset
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

        // Store quality scores
        let scores = QualityScores {
            completeness_score: Some(0.95),
            freshness_score: Some(1.0),
            file_health_score: Some(0.8),
            overall_score: Some(0.92),
            details: QualityDetails {
                row_count: Some(1000),
                file_count: Some(10),
                size_bytes: Some(1024 * 1024 * 1024),
                small_file_count: Some(2),
                avg_file_size: Some(100 * 1024 * 1024),
                ..Default::default()
            },
        };

        let id = store_quality_scores(&conn, dataset_id, &scores).unwrap();
        assert!(id > 0);

        // Get latest quality
        let result = get_latest_quality(&conn, dataset_id, "test_ds").unwrap();
        assert!(result.is_some());

        let quality = result.unwrap();
        assert_eq!(quality.dataset_id, dataset_id);
        assert_eq!(quality.scores.completeness_score, Some(0.95));
        assert_eq!(quality.scores.overall_score, Some(0.92));
    }

    #[test]
    fn test_get_unhealthy_datasets() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        // Insert datasets
        conn.execute(
            "INSERT INTO datasets (name, path, format, created_at, last_updated)
             VALUES ('healthy_ds', '/healthy', 'delta', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO datasets (name, path, format, created_at, last_updated)
             VALUES ('unhealthy_ds', '/unhealthy', 'delta', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();

        let healthy_id: i64 = conn
            .query_row(
                "SELECT id FROM datasets WHERE name = 'healthy_ds'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let unhealthy_id: i64 = conn
            .query_row(
                "SELECT id FROM datasets WHERE name = 'unhealthy_ds'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        // Store quality scores
        let healthy_scores = QualityScores {
            completeness_score: Some(0.95),
            freshness_score: Some(1.0),
            file_health_score: Some(0.9),
            overall_score: Some(0.95),
            details: QualityDetails::default(),
        };

        let unhealthy_scores = QualityScores {
            completeness_score: Some(0.5),
            freshness_score: Some(0.3),
            file_health_score: Some(0.6),
            overall_score: Some(0.45),
            details: QualityDetails::default(),
        };

        store_quality_scores(&conn, healthy_id, &healthy_scores).unwrap();
        store_quality_scores(&conn, unhealthy_id, &unhealthy_scores).unwrap();

        // Get unhealthy datasets (threshold 0.7)
        let result = get_unhealthy_datasets(&conn, 0.7).unwrap();

        assert_eq!(result.threshold, 0.7);
        assert_eq!(result.datasets.len(), 1);
        assert_eq!(result.datasets[0].dataset_name, "unhealthy_ds");
        assert_eq!(result.datasets[0].overall_score, 0.45);
    }

    #[test]
    fn test_get_latest_quality_not_found() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        // Query non-existent dataset
        let result = get_latest_quality(&conn, 9999, "nonexistent").unwrap();
        assert!(result.is_none());
    }
}
