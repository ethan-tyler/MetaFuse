// Infrastructure for usage tracking - some components not yet wired to handlers
#![allow(dead_code)]

//! Usage Analytics Module
//!
//! This module provides usage tracking for MetaFuse datasets, including:
//! - Access counting (reads, searches, API calls)
//! - Unique user tracking (capped at 10K per day to limit memory)
//! - Background periodic flushing to database
//! - Query endpoints for usage analytics
//!
//! # Architecture
//!
//! Uses lock-free DashMap for concurrent counter updates:
//! - Key: (dataset_id, date_str) tuple
//! - Value: UsageCounters with atomic operations
//!
//! A background task periodically flushes counters to the `usage_stats` table.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Maximum unique users tracked per dataset per day (memory protection)
const MAX_UNIQUE_USERS_PER_DAY: usize = 10_000;

/// Default flush interval in seconds
const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 60;

/// Maximum retry attempts for database writes
const MAX_RETRY_ATTEMPTS: u32 = 3;

/// Types of access to track
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessType {
    /// Direct dataset read (GET /datasets/:name)
    Read,
    /// Dataset appeared in search results
    SearchAppearance,
    /// Dataset was part of a lineage query
    LineageQuery,
    /// General API call involving this dataset
    ApiCall,
}

impl AccessType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AccessType::Read => "read",
            AccessType::SearchAppearance => "search_appearance",
            AccessType::LineageQuery => "lineage_query",
            AccessType::ApiCall => "api_call",
        }
    }
}

/// Counters for a single dataset on a single day
pub struct UsageCounters {
    /// Number of read operations
    read_count: AtomicU64,
    /// Number of search appearances
    search_appearances: AtomicU64,
    /// Number of lineage queries
    lineage_queries: AtomicU64,
    /// Number of API calls
    api_calls: AtomicU64,
    /// Unique users who accessed (capped at MAX_UNIQUE_USERS_PER_DAY)
    unique_users: RwLock<HashSet<String>>,
}

impl UsageCounters {
    fn new() -> Self {
        Self {
            read_count: AtomicU64::new(0),
            search_appearances: AtomicU64::new(0),
            lineage_queries: AtomicU64::new(0),
            api_calls: AtomicU64::new(0),
            unique_users: RwLock::new(HashSet::new()),
        }
    }

    /// Increment the appropriate counter based on access type
    fn increment(&self, access_type: AccessType) {
        match access_type {
            AccessType::Read => {
                self.read_count.fetch_add(1, Ordering::Relaxed);
            }
            AccessType::SearchAppearance => {
                self.search_appearances.fetch_add(1, Ordering::Relaxed);
            }
            AccessType::LineageQuery => {
                self.lineage_queries.fetch_add(1, Ordering::Relaxed);
            }
            AccessType::ApiCall => {
                self.api_calls.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Add a user to the unique users set (if not at capacity)
    async fn add_user(&self, user: &str) -> bool {
        let mut users = self.unique_users.write().await;
        if users.len() >= MAX_UNIQUE_USERS_PER_DAY {
            false
        } else {
            users.insert(user.to_string());
            true
        }
    }

    /// Get current counter values
    fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            read_count: self.read_count.load(Ordering::Relaxed),
            search_appearances: self.search_appearances.load(Ordering::Relaxed),
            lineage_queries: self.lineage_queries.load(Ordering::Relaxed),
            api_calls: self.api_calls.load(Ordering::Relaxed),
        }
    }

    /// Reset counters after flush
    fn reset(&self) {
        self.read_count.store(0, Ordering::Relaxed);
        self.search_appearances.store(0, Ordering::Relaxed);
        self.lineage_queries.store(0, Ordering::Relaxed);
        self.api_calls.store(0, Ordering::Relaxed);
    }
}

/// Snapshot of counter values (for database writes)
#[derive(Debug, Clone)]
struct CounterSnapshot {
    read_count: u64,
    search_appearances: u64,
    lineage_queries: u64,
    api_calls: u64,
}

/// Key for the counters map: (dataset_id, date_string)
type CounterKey = (i64, String);

/// Configuration for the usage tracker
#[derive(Debug, Clone)]
pub struct UsageConfig {
    /// How often to flush counters to the database (seconds)
    pub flush_interval_secs: u64,
    /// Maximum unique users to track per dataset per day
    pub max_unique_users: usize,
}

impl Default for UsageConfig {
    fn default() -> Self {
        Self {
            flush_interval_secs: DEFAULT_FLUSH_INTERVAL_SECS,
            max_unique_users: MAX_UNIQUE_USERS_PER_DAY,
        }
    }
}

/// Usage tracker with lock-free counters
pub struct UsageTracker {
    /// Lock-free map: (dataset_id, date) -> UsageCounters
    counters: Arc<DashMap<CounterKey, Arc<UsageCounters>>>,
    /// Configuration
    config: UsageConfig,
}

impl UsageTracker {
    /// Create a new usage tracker
    pub fn new(config: UsageConfig) -> Self {
        Self {
            counters: Arc::new(DashMap::new()),
            config,
        }
    }

    /// Create with default config
    pub fn new_default() -> Self {
        Self::new(UsageConfig::default())
    }

    /// Record an access event
    pub async fn record_access(
        &self,
        dataset_id: i64,
        user: Option<&str>,
        access_type: AccessType,
    ) {
        let date = today_string();
        let key = (dataset_id, date);

        // Get or create counters for this key
        let counters = self
            .counters
            .entry(key)
            .or_insert_with(|| Arc::new(UsageCounters::new()))
            .clone();

        // Increment the appropriate counter
        counters.increment(access_type);

        // Track unique user if provided
        if let Some(u) = user {
            if !counters.add_user(u).await {
                debug!(
                    dataset_id,
                    "Unique user limit reached for dataset, user not tracked"
                );
            }
        }
    }

    /// Record multiple search appearances at once
    pub async fn record_search_appearances(&self, dataset_ids: &[i64], user: Option<&str>) {
        for &dataset_id in dataset_ids {
            self.record_access(dataset_id, user, AccessType::SearchAppearance)
                .await;
        }
    }

    /// Get the number of datasets being tracked
    pub fn tracked_dataset_count(&self) -> usize {
        self.counters.len()
    }

    /// Flush all counters to the database
    ///
    /// Returns the number of records upserted.
    pub async fn flush(
        &self,
        conn: &rusqlite::Connection,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut upserted = 0;

        // Collect keys to process
        let keys: Vec<CounterKey> = self.counters.iter().map(|r| r.key().clone()).collect();

        for key in keys {
            let (dataset_id, stat_date) = &key;

            // Get counters and snapshot
            let counters = match self.counters.get(&key) {
                Some(c) => c.clone(),
                None => continue,
            };

            let snapshot = counters.snapshot();
            let unique_users = counters.unique_users.read().await.len() as i64;

            // Skip if no activity
            if snapshot.read_count == 0
                && snapshot.search_appearances == 0
                && snapshot.lineage_queries == 0
                && snapshot.api_calls == 0
                && unique_users == 0
            {
                continue;
            }

            // Upsert to database with retry
            let result = upsert_with_retry(
                conn,
                *dataset_id,
                stat_date,
                &snapshot,
                unique_users,
                MAX_RETRY_ATTEMPTS,
            );

            match result {
                Ok(_) => {
                    upserted += 1;
                    // Reset counters after successful write
                    counters.reset();
                    // Clear unique users (we've recorded the count)
                    counters.unique_users.write().await.clear();
                }
                Err(e) => {
                    error!(
                        dataset_id,
                        stat_date,
                        error = %e,
                        "Failed to flush usage stats after retries, keeping in memory"
                    );
                }
            }
        }

        // Clean up old entries (dates older than today)
        let today = today_string();
        self.counters.retain(|(_id, date), _| date == &today);

        Ok(upserted)
    }
}

/// Upsert usage stats with retry logic
fn upsert_with_retry(
    conn: &rusqlite::Connection,
    dataset_id: i64,
    stat_date: &str,
    snapshot: &CounterSnapshot,
    unique_users: i64,
    max_attempts: u32,
) -> Result<(), rusqlite::Error> {
    let mut attempts = 0;
    let mut last_error = None;

    while attempts < max_attempts {
        let result = conn.execute(
            r#"
            INSERT INTO usage_stats (dataset_id, stat_date, read_count, unique_users, search_appearances, lineage_queries, api_calls, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, datetime('now'))
            ON CONFLICT(dataset_id, stat_date) DO UPDATE SET
                read_count = read_count + excluded.read_count,
                unique_users = MAX(unique_users, excluded.unique_users),
                search_appearances = search_appearances + excluded.search_appearances,
                lineage_queries = lineage_queries + excluded.lineage_queries,
                api_calls = api_calls + excluded.api_calls,
                updated_at = datetime('now')
            "#,
            rusqlite::params![
                dataset_id,
                stat_date,
                snapshot.read_count as i64,
                unique_users,
                snapshot.search_appearances as i64,
                snapshot.lineage_queries as i64,
                snapshot.api_calls as i64,
            ],
        );

        match result {
            Ok(_) => return Ok(()),
            Err(e) => {
                attempts += 1;
                last_error = Some(e);
                if attempts < max_attempts {
                    // Brief delay before retry
                    std::thread::sleep(Duration::from_millis(10 * attempts as u64));
                }
            }
        }
    }

    Err(last_error.unwrap())
}

/// Background task that periodically flushes usage stats to the database
pub async fn usage_flush_task(
    tracker: Arc<UsageTracker>,
    backend: Arc<metafuse_catalog_storage::DynCatalogBackend>,
) {
    let interval = Duration::from_secs(tracker.config.flush_interval_secs);

    info!(
        interval_secs = tracker.config.flush_interval_secs,
        "Usage flush task started"
    );

    loop {
        tokio::time::sleep(interval).await;

        debug!("Running periodic usage stats flush");

        // Get connection and flush
        match backend.get_connection().await {
            Ok(conn) => {
                let result = tokio::task::spawn_blocking({
                    let tracker = tracker.clone();
                    move || {
                        // Need to block on the async flush
                        tokio::runtime::Handle::current()
                            .block_on(async { tracker.flush(&conn).await })
                    }
                })
                .await;

                match result {
                    Ok(Ok(count)) => {
                        if count > 0 {
                            debug!(count, "Flushed usage stats to database");
                        }
                    }
                    Ok(Err(e)) => {
                        error!(error = %e, "Failed to flush usage stats");
                    }
                    Err(e) => {
                        error!(error = %e, "Usage flush task panicked");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to get connection for usage flush");
            }
        }
    }
}

/// Get today's date as a string (YYYY-MM-DD)
fn today_string() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

// =============================================================================
// Query Types
// =============================================================================

/// Query parameters for usage stats endpoint
#[derive(Debug, Clone, Deserialize)]
pub struct UsageQueryParams {
    /// Time period: 1d, 7d, 30d
    #[serde(default = "default_period")]
    pub period: String,
}

fn default_period() -> String {
    "7d".to_string()
}

/// Single usage stat entry
#[derive(Debug, Clone, Serialize)]
pub struct UsageStatEntry {
    pub stat_date: String,
    pub read_count: i64,
    pub unique_users: i64,
    pub search_appearances: i64,
    pub lineage_queries: i64,
    pub api_calls: i64,
}

/// Response for dataset usage endpoint
#[derive(Debug, Clone, Serialize)]
pub struct DatasetUsageResponse {
    pub dataset_id: i64,
    pub dataset_name: String,
    pub period: String,
    pub total_reads: i64,
    pub total_unique_users: i64,
    pub total_api_calls: i64,
    pub daily_stats: Vec<UsageStatEntry>,
}

/// Popular dataset entry
#[derive(Debug, Clone, Serialize)]
pub struct PopularDatasetEntry {
    pub dataset_id: i64,
    pub dataset_name: String,
    pub total_reads: i64,
    pub unique_users: i64,
    pub api_calls: i64,
}

/// Response for popular datasets endpoint
#[derive(Debug, Clone, Serialize)]
pub struct PopularDatasetsResponse {
    pub period: String,
    pub datasets: Vec<PopularDatasetEntry>,
}

/// Stale dataset entry (no recent access)
#[derive(Debug, Clone, Serialize)]
pub struct StaleDatasetEntry {
    pub dataset_id: i64,
    pub dataset_name: String,
    pub last_accessed_at: Option<String>,
    pub days_since_access: Option<i64>,
}

/// Response for stale datasets endpoint
#[derive(Debug, Clone, Serialize)]
pub struct StaleDatasetsResponse {
    pub stale_threshold_days: i64,
    pub datasets: Vec<StaleDatasetEntry>,
}

// =============================================================================
// Query Functions
// =============================================================================

/// Get usage stats for a specific dataset
pub fn query_dataset_usage(
    conn: &rusqlite::Connection,
    dataset_id: i64,
    dataset_name: &str,
    period: &str,
) -> Result<DatasetUsageResponse, rusqlite::Error> {
    let days = parse_period_days(period);
    let start_date = chrono::Utc::now()
        .checked_sub_signed(chrono::Duration::days(days))
        .unwrap()
        .format("%Y-%m-%d")
        .to_string();

    let mut stmt = conn.prepare(
        r#"
        SELECT stat_date, read_count, unique_users, search_appearances, lineage_queries, api_calls
        FROM usage_stats
        WHERE dataset_id = ?1 AND stat_date >= ?2
        ORDER BY stat_date DESC
        "#,
    )?;

    let daily_stats: Vec<UsageStatEntry> = stmt
        .query_map(rusqlite::params![dataset_id, start_date], |row| {
            Ok(UsageStatEntry {
                stat_date: row.get(0)?,
                read_count: row.get(1)?,
                unique_users: row.get(2)?,
                search_appearances: row.get(3)?,
                lineage_queries: row.get(4)?,
                api_calls: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Calculate totals
    let total_reads: i64 = daily_stats.iter().map(|s| s.read_count).sum();
    let total_unique_users: i64 = daily_stats
        .iter()
        .map(|s| s.unique_users)
        .max()
        .unwrap_or(0);
    let total_api_calls: i64 = daily_stats.iter().map(|s| s.api_calls).sum();

    Ok(DatasetUsageResponse {
        dataset_id,
        dataset_name: dataset_name.to_string(),
        period: period.to_string(),
        total_reads,
        total_unique_users,
        total_api_calls,
        daily_stats,
    })
}

/// Get most popular datasets
pub fn query_popular_datasets(
    conn: &rusqlite::Connection,
    period: &str,
    limit: usize,
) -> Result<PopularDatasetsResponse, rusqlite::Error> {
    let days = parse_period_days(period);
    let start_date = chrono::Utc::now()
        .checked_sub_signed(chrono::Duration::days(days))
        .unwrap()
        .format("%Y-%m-%d")
        .to_string();

    let mut stmt = conn.prepare(
        r#"
        SELECT
            u.dataset_id,
            d.name,
            SUM(u.read_count) as total_reads,
            MAX(u.unique_users) as unique_users,
            SUM(u.api_calls) as api_calls
        FROM usage_stats u
        JOIN datasets d ON d.id = u.dataset_id
        WHERE u.stat_date >= ?1
        GROUP BY u.dataset_id, d.name
        ORDER BY total_reads DESC
        LIMIT ?2
        "#,
    )?;

    let datasets: Vec<PopularDatasetEntry> = stmt
        .query_map(rusqlite::params![start_date, limit as i64], |row| {
            Ok(PopularDatasetEntry {
                dataset_id: row.get(0)?,
                dataset_name: row.get(1)?,
                total_reads: row.get(2)?,
                unique_users: row.get(3)?,
                api_calls: row.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PopularDatasetsResponse {
        period: period.to_string(),
        datasets,
    })
}

/// Get datasets with no recent access
pub fn query_stale_datasets(
    conn: &rusqlite::Connection,
    stale_threshold_days: i64,
) -> Result<StaleDatasetsResponse, rusqlite::Error> {
    let threshold_date = chrono::Utc::now()
        .checked_sub_signed(chrono::Duration::days(stale_threshold_days))
        .unwrap()
        .format("%Y-%m-%d")
        .to_string();

    // Find datasets with no usage stats in the threshold period
    let mut stmt = conn.prepare(
        r#"
        SELECT
            d.id,
            d.name,
            MAX(u.stat_date) as last_accessed
        FROM datasets d
        LEFT JOIN usage_stats u ON d.id = u.dataset_id
        GROUP BY d.id, d.name
        HAVING last_accessed IS NULL OR last_accessed < ?1
        ORDER BY last_accessed ASC NULLS FIRST
        "#,
    )?;

    let today = chrono::Utc::now().date_naive();

    let datasets: Vec<StaleDatasetEntry> = stmt
        .query_map(rusqlite::params![threshold_date], |row| {
            let last_accessed: Option<String> = row.get(2)?;
            let days_since = last_accessed.as_ref().and_then(|d| {
                chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d")
                    .ok()
                    .map(|date| (today - date).num_days())
            });

            Ok(StaleDatasetEntry {
                dataset_id: row.get(0)?,
                dataset_name: row.get(1)?,
                last_accessed_at: last_accessed,
                days_since_access: days_since,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(StaleDatasetsResponse {
        stale_threshold_days,
        datasets,
    })
}

/// Parse period string to days
fn parse_period_days(period: &str) -> i64 {
    match period {
        "1d" => 1,
        "7d" => 7,
        "30d" => 30,
        "90d" => 90,
        _ => 7, // Default to 7 days
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_usage_config_default() {
        let config = UsageConfig::default();
        assert_eq!(config.flush_interval_secs, DEFAULT_FLUSH_INTERVAL_SECS);
        assert_eq!(config.max_unique_users, MAX_UNIQUE_USERS_PER_DAY);
    }

    #[test]
    fn test_access_type_as_str() {
        assert_eq!(AccessType::Read.as_str(), "read");
        assert_eq!(AccessType::SearchAppearance.as_str(), "search_appearance");
        assert_eq!(AccessType::LineageQuery.as_str(), "lineage_query");
        assert_eq!(AccessType::ApiCall.as_str(), "api_call");
    }

    #[tokio::test]
    async fn test_usage_tracker_record_access() {
        let tracker = UsageTracker::new_default();

        // Record some accesses
        tracker
            .record_access(1, Some("alice"), AccessType::Read)
            .await;
        tracker
            .record_access(1, Some("bob"), AccessType::Read)
            .await;
        tracker
            .record_access(1, Some("alice"), AccessType::ApiCall)
            .await;

        assert_eq!(tracker.tracked_dataset_count(), 1);
    }

    #[tokio::test]
    async fn test_usage_tracker_multiple_datasets() {
        let tracker = UsageTracker::new_default();

        tracker
            .record_access(1, Some("alice"), AccessType::Read)
            .await;
        tracker
            .record_access(2, Some("bob"), AccessType::Read)
            .await;
        tracker
            .record_access(3, None, AccessType::SearchAppearance)
            .await;

        // All should be same date, so 3 different keys
        assert_eq!(tracker.tracked_dataset_count(), 3);
    }

    #[tokio::test]
    async fn test_usage_tracker_search_appearances() {
        let tracker = UsageTracker::new_default();

        tracker
            .record_search_appearances(&[1, 2, 3, 4, 5], Some("searcher"))
            .await;

        assert_eq!(tracker.tracked_dataset_count(), 5);
    }

    #[tokio::test]
    async fn test_counter_snapshot() {
        let counters = UsageCounters::new();

        counters.increment(AccessType::Read);
        counters.increment(AccessType::Read);
        counters.increment(AccessType::ApiCall);
        counters.increment(AccessType::SearchAppearance);

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.read_count, 2);
        assert_eq!(snapshot.api_calls, 1);
        assert_eq!(snapshot.search_appearances, 1);
        assert_eq!(snapshot.lineage_queries, 0);
    }

    #[tokio::test]
    async fn test_counter_reset() {
        let counters = UsageCounters::new();

        counters.increment(AccessType::Read);
        counters.increment(AccessType::Read);

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.read_count, 2);

        counters.reset();

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.read_count, 0);
    }

    #[tokio::test]
    async fn test_unique_user_tracking() {
        let counters = UsageCounters::new();

        assert!(counters.add_user("alice").await);
        assert!(counters.add_user("bob").await);
        assert!(counters.add_user("alice").await); // Duplicate, still true

        let users = counters.unique_users.read().await;
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn test_parse_period_days() {
        assert_eq!(parse_period_days("1d"), 1);
        assert_eq!(parse_period_days("7d"), 7);
        assert_eq!(parse_period_days("30d"), 30);
        assert_eq!(parse_period_days("90d"), 90);
        assert_eq!(parse_period_days("invalid"), 7); // Default
    }

    #[test]
    fn test_flush_to_database() {
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

        // Create tracker and record access
        let tracker = UsageTracker::new_default();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            tracker
                .record_access(dataset_id, Some("alice"), AccessType::Read)
                .await;
            tracker
                .record_access(dataset_id, Some("bob"), AccessType::Read)
                .await;
            tracker
                .record_access(dataset_id, None, AccessType::ApiCall)
                .await;

            // Flush to database
            let count = tracker.flush(&conn).await.unwrap();
            assert_eq!(count, 1);
        });

        // Verify in database
        let (read_count, unique_users, api_calls): (i64, i64, i64) = conn
            .query_row(
                "SELECT read_count, unique_users, api_calls FROM usage_stats WHERE dataset_id = ?1",
                [dataset_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(read_count, 2);
        assert_eq!(unique_users, 2);
        assert_eq!(api_calls, 1);
    }

    #[test]
    fn test_query_dataset_usage() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        // Insert a dataset
        conn.execute(
            "INSERT INTO datasets (name, path, format, created_at, last_updated)
             VALUES ('analytics_ds', '/analytics', 'delta', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();

        let dataset_id: i64 = conn
            .query_row(
                "SELECT id FROM datasets WHERE name = 'analytics_ds'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        // Insert some usage stats
        let today = today_string();
        conn.execute(
            "INSERT INTO usage_stats (dataset_id, stat_date, read_count, unique_users, search_appearances, lineage_queries, api_calls)
             VALUES (?1, ?2, 100, 10, 20, 5, 150)",
            rusqlite::params![dataset_id, today],
        )
        .unwrap();

        // Query usage
        let result = query_dataset_usage(&conn, dataset_id, "analytics_ds", "7d").unwrap();

        assert_eq!(result.dataset_id, dataset_id);
        assert_eq!(result.dataset_name, "analytics_ds");
        assert_eq!(result.total_reads, 100);
        assert_eq!(result.total_unique_users, 10);
        assert_eq!(result.total_api_calls, 150);
        assert_eq!(result.daily_stats.len(), 1);
    }

    #[test]
    fn test_query_popular_datasets() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        // Insert datasets
        conn.execute(
            "INSERT INTO datasets (name, path, format, created_at, last_updated)
             VALUES ('popular', '/popular', 'delta', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO datasets (name, path, format, created_at, last_updated)
             VALUES ('less_popular', '/less', 'delta', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();

        let popular_id: i64 = conn
            .query_row(
                "SELECT id FROM datasets WHERE name = 'popular'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let less_popular_id: i64 = conn
            .query_row(
                "SELECT id FROM datasets WHERE name = 'less_popular'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        // Insert usage stats
        let today = today_string();
        conn.execute(
            "INSERT INTO usage_stats (dataset_id, stat_date, read_count, unique_users, api_calls)
             VALUES (?1, ?2, 1000, 50, 2000)",
            rusqlite::params![popular_id, today],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO usage_stats (dataset_id, stat_date, read_count, unique_users, api_calls)
             VALUES (?1, ?2, 100, 5, 200)",
            rusqlite::params![less_popular_id, today],
        )
        .unwrap();

        // Query popular
        let result = query_popular_datasets(&conn, "7d", 10).unwrap();

        assert_eq!(result.datasets.len(), 2);
        assert_eq!(result.datasets[0].dataset_name, "popular");
        assert_eq!(result.datasets[0].total_reads, 1000);
        assert_eq!(result.datasets[1].dataset_name, "less_popular");
    }

    #[test]
    fn test_query_stale_datasets() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        // Insert datasets - one active, one stale, one never accessed
        conn.execute(
            "INSERT INTO datasets (name, path, format, created_at, last_updated)
             VALUES ('active', '/active', 'delta', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO datasets (name, path, format, created_at, last_updated)
             VALUES ('never_accessed', '/never', 'delta', datetime('now'), datetime('now'))",
            [],
        )
        .unwrap();

        let active_id: i64 = conn
            .query_row("SELECT id FROM datasets WHERE name = 'active'", [], |row| {
                row.get(0)
            })
            .unwrap();

        // Insert recent usage for active dataset
        let today = today_string();
        conn.execute(
            "INSERT INTO usage_stats (dataset_id, stat_date, read_count, unique_users, api_calls)
             VALUES (?1, ?2, 10, 1, 20)",
            rusqlite::params![active_id, today],
        )
        .unwrap();

        // Query stale (30 days threshold)
        let result = query_stale_datasets(&conn, 30).unwrap();

        // Only 'never_accessed' should be stale
        assert_eq!(result.datasets.len(), 1);
        assert_eq!(result.datasets[0].dataset_name, "never_accessed");
        assert!(result.datasets[0].last_accessed_at.is_none());
    }
}
