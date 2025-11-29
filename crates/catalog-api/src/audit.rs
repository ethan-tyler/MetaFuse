// Infrastructure for audit logging - some components not yet wired to handlers
#![allow(dead_code)]

//! Audit logging for the MetaFuse catalog API
//!
//! This module provides comprehensive audit logging for compliance and debugging:
//! - Tracks all catalog mutations (create, update, delete)
//! - Captures actor information (API key or IP-based)
//! - Non-blocking async batched writes to database
//! - Graceful degradation on failures (falls back to tracing)
//!
//! ## Database Table
//!
//! Uses the `audit_log` table from v1.0.0 migration:
//! - action: create, update, delete, read, search, export, import
//! - entity_type: dataset, field, tag, lineage, owner, etc.
//! - actor: user identifier or IP
//! - old_values/new_values: JSON snapshots
//!
//! ## Configuration
//!
//! - `METAFUSE_AUDIT_BUFFER_SIZE`: Max events in buffer (default: 1000)
//! - `METAFUSE_AUDIT_FLUSH_INTERVAL_MS`: Flush interval in milliseconds (default: 1000)

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Default buffer size for audit events
const DEFAULT_BUFFER_SIZE: usize = 1000;

/// Default flush interval in milliseconds
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 1000;

/// Audit action types (matches DB CHECK constraint)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuditAction {
    Create,
    Update,
    Delete,
    Read,
    Search,
    Export,
    Import,
}

impl AuditAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditAction::Create => "create",
            AuditAction::Update => "update",
            AuditAction::Delete => "delete",
            AuditAction::Read => "read",
            AuditAction::Search => "search",
            AuditAction::Export => "export",
            AuditAction::Import => "import",
        }
    }
}

/// Actor type (matches DB CHECK constraint)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActorType {
    User,
    Service,
    System,
    Anonymous,
}

impl ActorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ActorType::User => "user",
            ActorType::Service => "service",
            ActorType::System => "system",
            ActorType::Anonymous => "anonymous",
        }
    }
}

/// An audit event to be logged
#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    /// Type of action performed
    pub action: AuditAction,
    /// Type of entity affected (dataset, field, tag, etc.)
    pub entity_type: String,
    /// Identifier of the entity (e.g., dataset name)
    pub entity_id: Option<String>,
    /// Actor who performed the action (user email, service name, or IP)
    pub actor: Option<String>,
    /// Type of actor
    pub actor_type: ActorType,
    /// API key ID if authenticated
    pub api_key_id: Option<i64>,
    /// Request ID for correlation
    pub request_id: String,
    /// Client IP address
    pub client_ip: Option<String>,
    /// Previous values (for updates/deletes)
    pub old_values: Option<serde_json::Value>,
    /// New values (for creates/updates)
    pub new_values: Option<serde_json::Value>,
    /// Additional context
    pub context: Option<serde_json::Value>,
}

impl AuditEvent {
    /// Create a new audit event for a create action
    pub fn create(
        entity_type: impl Into<String>,
        entity_id: impl Into<String>,
        new_values: serde_json::Value,
        request_id: impl Into<String>,
    ) -> Self {
        Self {
            action: AuditAction::Create,
            entity_type: entity_type.into(),
            entity_id: Some(entity_id.into()),
            actor: None,
            actor_type: ActorType::Anonymous,
            api_key_id: None,
            request_id: request_id.into(),
            client_ip: None,
            old_values: None,
            new_values: Some(new_values),
            context: None,
        }
    }

    /// Create a new audit event for an update action
    pub fn update(
        entity_type: impl Into<String>,
        entity_id: impl Into<String>,
        old_values: serde_json::Value,
        new_values: serde_json::Value,
        request_id: impl Into<String>,
    ) -> Self {
        Self {
            action: AuditAction::Update,
            entity_type: entity_type.into(),
            entity_id: Some(entity_id.into()),
            actor: None,
            actor_type: ActorType::Anonymous,
            api_key_id: None,
            request_id: request_id.into(),
            client_ip: None,
            old_values: Some(old_values),
            new_values: Some(new_values),
            context: None,
        }
    }

    /// Create a new audit event for a delete action
    pub fn delete(
        entity_type: impl Into<String>,
        entity_id: impl Into<String>,
        old_values: serde_json::Value,
        request_id: impl Into<String>,
    ) -> Self {
        Self {
            action: AuditAction::Delete,
            entity_type: entity_type.into(),
            entity_id: Some(entity_id.into()),
            actor: None,
            actor_type: ActorType::Anonymous,
            api_key_id: None,
            request_id: request_id.into(),
            client_ip: None,
            old_values: Some(old_values),
            new_values: None,
            context: None,
        }
    }

    /// Set the actor information
    pub fn with_actor(mut self, actor: impl Into<String>, actor_type: ActorType) -> Self {
        self.actor = Some(actor.into());
        self.actor_type = actor_type;
        self
    }

    /// Set the API key ID
    pub fn with_api_key(mut self, api_key_id: i64) -> Self {
        self.api_key_id = Some(api_key_id);
        self
    }

    /// Set the client IP
    pub fn with_client_ip(mut self, ip: impl Into<String>) -> Self {
        self.client_ip = Some(ip.into());
        self
    }

    /// Set additional context
    pub fn with_context(mut self, context: serde_json::Value) -> Self {
        self.context = Some(context);
        self
    }
}

/// Configuration for the audit logger
#[derive(Debug, Clone)]
pub struct AuditConfig {
    /// Maximum number of events in the buffer
    pub buffer_size: usize,
    /// Flush interval in milliseconds
    pub flush_interval_ms: u64,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            buffer_size: std::env::var("METAFUSE_AUDIT_BUFFER_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_BUFFER_SIZE),
            flush_interval_ms: std::env::var("METAFUSE_AUDIT_FLUSH_INTERVAL_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_FLUSH_INTERVAL_MS),
        }
    }
}

/// Handle for logging audit events
///
/// This is a cheap-to-clone handle that can be passed to handlers.
/// Events are sent to a background task for batched database writes.
#[derive(Clone)]
pub struct AuditLogger {
    sender: mpsc::Sender<AuditEvent>,
}

impl AuditLogger {
    /// Create a new audit logger with the given configuration
    ///
    /// Returns the logger handle and a receiver for the background task.
    pub fn new(config: &AuditConfig) -> (Self, mpsc::Receiver<AuditEvent>) {
        let (sender, receiver) = mpsc::channel(config.buffer_size);
        (Self { sender }, receiver)
    }

    /// Log an audit event (non-blocking)
    ///
    /// If the buffer is full, the event is dropped and a warning is logged.
    /// This ensures that audit logging never blocks request processing.
    pub fn log(&self, event: AuditEvent) {
        match self.sender.try_send(event.clone()) {
            Ok(()) => {
                debug!(
                    action = event.action.as_str(),
                    entity_type = %event.entity_type,
                    entity_id = ?event.entity_id,
                    request_id = %event.request_id,
                    "Audit event queued"
                );
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(
                    request_id = %event.request_id,
                    action = event.action.as_str(),
                    entity_type = %event.entity_type,
                    "Audit buffer full, event dropped"
                );
                // Fallback: log to tracing for visibility
                info!(
                    target: "audit_fallback",
                    action = event.action.as_str(),
                    entity_type = %event.entity_type,
                    entity_id = ?event.entity_id,
                    actor = ?event.actor,
                    request_id = %event.request_id,
                    "Audit event (buffer overflow fallback)"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!(
                    request_id = %event.request_id,
                    "Audit channel closed, logging to tracing"
                );
                // Fallback: log to tracing
                info!(
                    target: "audit_fallback",
                    action = event.action.as_str(),
                    entity_type = %event.entity_type,
                    entity_id = ?event.entity_id,
                    actor = ?event.actor,
                    request_id = %event.request_id,
                    "Audit event (channel closed fallback)"
                );
            }
        }
    }

    /// Log an audit event asynchronously (will wait if buffer is full)
    ///
    /// Use this when you can afford to wait for the buffer to have space.
    pub async fn log_async(&self, event: AuditEvent) {
        if let Err(e) = self.sender.send(event.clone()).await {
            error!(
                request_id = %event.request_id,
                error = %e,
                "Failed to send audit event"
            );
        }
    }
}

/// Background task that writes audit events to the database
///
/// This task:
/// 1. Batches events from the channel
/// 2. Writes them to the audit_log table
/// 3. Handles failures gracefully (logs to tracing as fallback)
pub async fn audit_writer_task(
    mut receiver: mpsc::Receiver<AuditEvent>,
    backend: Arc<metafuse_catalog_storage::DynCatalogBackend>,
    config: AuditConfig,
) {
    let flush_interval = std::time::Duration::from_millis(config.flush_interval_ms);
    let mut batch: Vec<AuditEvent> = Vec::with_capacity(100);
    let mut interval = tokio::time::interval(flush_interval);

    info!(
        buffer_size = config.buffer_size,
        flush_interval_ms = config.flush_interval_ms,
        "Audit writer task started"
    );

    loop {
        tokio::select! {
            // Receive events from the channel
            event = receiver.recv() => {
                match event {
                    Some(e) => {
                        batch.push(e);
                        // Flush immediately if batch is getting large
                        if batch.len() >= 100 {
                            flush_batch(&mut batch, &backend).await;
                        }
                    }
                    None => {
                        // Channel closed, flush remaining and exit
                        if !batch.is_empty() {
                            flush_batch(&mut batch, &backend).await;
                        }
                        info!("Audit writer task shutting down");
                        break;
                    }
                }
            }
            // Periodic flush
            _ = interval.tick() => {
                if !batch.is_empty() {
                    flush_batch(&mut batch, &backend).await;
                }
            }
        }
    }
}

/// Flush a batch of audit events to the database
async fn flush_batch(
    batch: &mut Vec<AuditEvent>,
    backend: &Arc<metafuse_catalog_storage::DynCatalogBackend>,
) {
    if batch.is_empty() {
        return;
    }

    let events: Vec<AuditEvent> = std::mem::take(batch);
    let count = events.len();

    debug!(count, "Flushing audit batch");

    // Get connection and write events
    match backend.get_connection().await {
        Ok(conn) => {
            // Write events in a single transaction
            // Return events on error so we can log them as fallback
            let result =
                tokio::task::spawn_blocking(move || match write_events_to_db(&conn, &events) {
                    Ok(written) => Ok(written),
                    Err(e) => Err((e, events)),
                })
                .await;

            match result {
                Ok(Ok(written)) => {
                    debug!(written, "Audit batch written to database");
                }
                Ok(Err((e, events))) => {
                    error!(error = %e, count, "Failed to write audit batch to database");
                    // Log events to tracing as fallback
                    log_events_as_fallback(&events, "DB write failure");
                }
                Err(e) => {
                    error!(error = %e, count, "Audit write task panicked");
                }
            }
        }
        Err(e) => {
            error!(error = %e, count, "Failed to get connection for audit batch");
            // Log events to tracing as fallback
            log_events_as_fallback(&events, "connection failure");
        }
    }
}

/// Log audit events to tracing as a fallback when DB write fails
fn log_events_as_fallback(events: &[AuditEvent], reason: &str) {
    for event in events {
        warn!(
            target: "audit_fallback",
            action = event.action.as_str(),
            entity_type = %event.entity_type,
            entity_id = ?event.entity_id,
            actor = ?event.actor,
            request_id = %event.request_id,
            reason = reason,
            "Audit event (fallback)"
        );
    }
}

/// Write audit events to the database
fn write_events_to_db(
    conn: &rusqlite::Connection,
    events: &[AuditEvent],
) -> Result<usize, rusqlite::Error> {
    let tx = conn.unchecked_transaction()?;

    let mut stmt = tx.prepare_cached(
        r#"
        INSERT INTO audit_log (
            action, entity_type, entity_id, actor, actor_type,
            api_key_id, request_id, client_ip, old_values, new_values, context
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11
        )
        "#,
    )?;

    let mut count = 0;
    for event in events {
        let old_values = event
            .old_values
            .as_ref()
            .and_then(|v| serde_json::to_string(v).ok());
        let new_values = event
            .new_values
            .as_ref()
            .and_then(|v| serde_json::to_string(v).ok());
        let context = event
            .context
            .as_ref()
            .and_then(|v| serde_json::to_string(v).ok());

        stmt.execute(rusqlite::params![
            event.action.as_str(),
            event.entity_type,
            event.entity_id,
            event.actor,
            event.actor_type.as_str(),
            event.api_key_id,
            event.request_id,
            event.client_ip,
            old_values,
            new_values,
            context,
        ])?;
        count += 1;
    }

    drop(stmt);
    tx.commit()?;

    Ok(count)
}

/// Query parameters for listing audit logs
#[derive(Debug, Deserialize)]
pub struct AuditQueryParams {
    /// Filter by entity type
    pub entity_type: Option<String>,
    /// Filter by entity ID
    pub entity_id: Option<String>,
    /// Filter by action
    pub action: Option<String>,
    /// Filter by actor
    pub actor: Option<String>,
    /// Filter by request ID
    pub request_id: Option<String>,
    /// Maximum number of results (default: 100)
    pub limit: Option<i64>,
    /// Offset for pagination (default: 0)
    pub offset: Option<i64>,
}

/// Response for a single audit log entry
#[derive(Debug, Serialize)]
pub struct AuditLogEntry {
    pub id: i64,
    pub timestamp: String,
    pub action: String,
    pub entity_type: String,
    pub entity_id: Option<String>,
    pub actor: Option<String>,
    pub actor_type: Option<String>,
    pub api_key_id: Option<i64>,
    pub request_id: Option<String>,
    pub client_ip: Option<String>,
    pub old_values: Option<serde_json::Value>,
    pub new_values: Option<serde_json::Value>,
    pub context: Option<serde_json::Value>,
}

/// Response for listing audit logs
#[derive(Debug, Serialize)]
pub struct AuditLogResponse {
    pub entries: Vec<AuditLogEntry>,
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
}

/// Query audit logs from the database
pub fn query_audit_logs(
    conn: &rusqlite::Connection,
    params: &AuditQueryParams,
) -> Result<AuditLogResponse, rusqlite::Error> {
    let limit = params.limit.unwrap_or(100).min(1000);
    let offset = params.offset.unwrap_or(0);

    // Build WHERE clause dynamically
    let mut conditions: Vec<String> = Vec::new();
    let mut values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    if let Some(ref entity_type) = params.entity_type {
        conditions.push("entity_type = ?".to_string());
        values.push(Box::new(entity_type.clone()));
    }
    if let Some(ref entity_id) = params.entity_id {
        conditions.push("entity_id = ?".to_string());
        values.push(Box::new(entity_id.clone()));
    }
    if let Some(ref action) = params.action {
        conditions.push("action = ?".to_string());
        values.push(Box::new(action.clone()));
    }
    if let Some(ref actor) = params.actor {
        conditions.push("actor = ?".to_string());
        values.push(Box::new(actor.clone()));
    }
    if let Some(ref request_id) = params.request_id {
        conditions.push("request_id = ?".to_string());
        values.push(Box::new(request_id.clone()));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    // Get total count
    let count_sql = format!("SELECT COUNT(*) FROM audit_log {}", where_clause);
    let total: i64 = {
        let mut stmt = conn.prepare(&count_sql)?;
        let params_ref: Vec<&dyn rusqlite::ToSql> = values.iter().map(|b| b.as_ref()).collect();
        stmt.query_row(params_ref.as_slice(), |row| row.get(0))?
    };

    // Get entries
    let query_sql = format!(
        r#"
        SELECT id, timestamp, action, entity_type, entity_id, actor, actor_type,
               api_key_id, request_id, client_ip, old_values, new_values, context
        FROM audit_log
        {}
        ORDER BY timestamp DESC
        LIMIT ? OFFSET ?
        "#,
        where_clause
    );

    let mut stmt = conn.prepare(&query_sql)?;

    // Add limit and offset to values
    values.push(Box::new(limit));
    values.push(Box::new(offset));
    let params_ref: Vec<&dyn rusqlite::ToSql> = values.iter().map(|b| b.as_ref()).collect();

    let entries: Vec<AuditLogEntry> = stmt
        .query_map(params_ref.as_slice(), |row| {
            let old_values: Option<String> = row.get(10)?;
            let new_values: Option<String> = row.get(11)?;
            let context: Option<String> = row.get(12)?;

            Ok(AuditLogEntry {
                id: row.get(0)?,
                timestamp: row.get(1)?,
                action: row.get(2)?,
                entity_type: row.get(3)?,
                entity_id: row.get(4)?,
                actor: row.get(5)?,
                actor_type: row.get(6)?,
                api_key_id: row.get(7)?,
                request_id: row.get(8)?,
                client_ip: row.get(9)?,
                old_values: old_values.and_then(|s| serde_json::from_str(&s).ok()),
                new_values: new_values.and_then(|s| serde_json::from_str(&s).ok()),
                context: context.and_then(|s| serde_json::from_str(&s).ok()),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(AuditLogResponse {
        entries,
        total,
        limit,
        offset,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_action_as_str() {
        assert_eq!(AuditAction::Create.as_str(), "create");
        assert_eq!(AuditAction::Update.as_str(), "update");
        assert_eq!(AuditAction::Delete.as_str(), "delete");
        assert_eq!(AuditAction::Read.as_str(), "read");
        assert_eq!(AuditAction::Search.as_str(), "search");
        assert_eq!(AuditAction::Export.as_str(), "export");
        assert_eq!(AuditAction::Import.as_str(), "import");
    }

    #[test]
    fn test_actor_type_as_str() {
        assert_eq!(ActorType::User.as_str(), "user");
        assert_eq!(ActorType::Service.as_str(), "service");
        assert_eq!(ActorType::System.as_str(), "system");
        assert_eq!(ActorType::Anonymous.as_str(), "anonymous");
    }

    #[test]
    fn test_audit_event_create() {
        let event = AuditEvent::create(
            "dataset",
            "my_dataset",
            serde_json::json!({"name": "my_dataset"}),
            "req-123",
        );

        assert_eq!(event.action, AuditAction::Create);
        assert_eq!(event.entity_type, "dataset");
        assert_eq!(event.entity_id, Some("my_dataset".to_string()));
        assert_eq!(event.request_id, "req-123");
        assert!(event.old_values.is_none());
        assert!(event.new_values.is_some());
    }

    #[test]
    fn test_audit_event_update() {
        let event = AuditEvent::update(
            "dataset",
            "my_dataset",
            serde_json::json!({"path": "/old"}),
            serde_json::json!({"path": "/new"}),
            "req-456",
        );

        assert_eq!(event.action, AuditAction::Update);
        assert!(event.old_values.is_some());
        assert!(event.new_values.is_some());
    }

    #[test]
    fn test_audit_event_delete() {
        let event = AuditEvent::delete(
            "dataset",
            "my_dataset",
            serde_json::json!({"name": "my_dataset"}),
            "req-789",
        );

        assert_eq!(event.action, AuditAction::Delete);
        assert!(event.old_values.is_some());
        assert!(event.new_values.is_none());
    }

    #[test]
    fn test_audit_event_with_actor() {
        let event = AuditEvent::create("dataset", "my_dataset", serde_json::json!({}), "req-123")
            .with_actor("alice@example.com", ActorType::User);

        assert_eq!(event.actor, Some("alice@example.com".to_string()));
        assert_eq!(event.actor_type, ActorType::User);
    }

    #[test]
    fn test_audit_event_with_api_key() {
        let event = AuditEvent::create("dataset", "my_dataset", serde_json::json!({}), "req-123")
            .with_api_key(42);

        assert_eq!(event.api_key_id, Some(42));
    }

    #[test]
    fn test_audit_event_with_client_ip() {
        let event = AuditEvent::create("dataset", "my_dataset", serde_json::json!({}), "req-123")
            .with_client_ip("192.168.1.1");

        assert_eq!(event.client_ip, Some("192.168.1.1".to_string()));
    }

    #[test]
    fn test_audit_config_defaults() {
        let config = AuditConfig::default();
        assert_eq!(config.buffer_size, DEFAULT_BUFFER_SIZE);
        assert_eq!(config.flush_interval_ms, DEFAULT_FLUSH_INTERVAL_MS);
    }

    #[tokio::test]
    async fn test_audit_logger_send() {
        let config = AuditConfig::default();
        let (logger, mut receiver) = AuditLogger::new(&config);

        let event = AuditEvent::create("dataset", "test", serde_json::json!({}), "req-1");

        logger.log(event.clone());

        // Should receive the event
        let received = receiver.recv().await.unwrap();
        assert_eq!(received.entity_type, "dataset");
        assert_eq!(received.entity_id, Some("test".to_string()));
    }

    #[test]
    fn test_write_events_to_db() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        let events = vec![
            AuditEvent::create(
                "dataset",
                "ds1",
                serde_json::json!({"name": "ds1"}),
                "req-1",
            ),
            AuditEvent::update(
                "dataset",
                "ds2",
                serde_json::json!({"path": "/old"}),
                serde_json::json!({"path": "/new"}),
                "req-2",
            ),
            AuditEvent::delete(
                "dataset",
                "ds3",
                serde_json::json!({"name": "ds3"}),
                "req-3",
            ),
        ];

        let count = write_events_to_db(&conn, &events).unwrap();
        assert_eq!(count, 3);

        // Verify entries were written
        let result: i64 = conn
            .query_row("SELECT COUNT(*) FROM audit_log", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result, 3);
    }

    #[test]
    fn test_query_audit_logs() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        // Initialize schema
        metafuse_catalog_core::init_sqlite_schema(&conn).unwrap();
        metafuse_catalog_core::migrations::run_migrations(&conn).unwrap();

        // Insert test events
        let events = vec![
            AuditEvent::create("dataset", "ds1", serde_json::json!({}), "req-1")
                .with_actor("alice", ActorType::User),
            AuditEvent::update(
                "dataset",
                "ds2",
                serde_json::json!({}),
                serde_json::json!({}),
                "req-2",
            )
            .with_actor("bob", ActorType::User),
            AuditEvent::delete("owner", "owner1", serde_json::json!({}), "req-3")
                .with_actor("alice", ActorType::User),
        ];
        write_events_to_db(&conn, &events).unwrap();

        // Query all
        let params = AuditQueryParams {
            entity_type: None,
            entity_id: None,
            action: None,
            actor: None,
            request_id: None,
            limit: None,
            offset: None,
        };
        let result = query_audit_logs(&conn, &params).unwrap();
        assert_eq!(result.total, 3);
        assert_eq!(result.entries.len(), 3);

        // Query by entity_type
        let params = AuditQueryParams {
            entity_type: Some("dataset".to_string()),
            entity_id: None,
            action: None,
            actor: None,
            request_id: None,
            limit: None,
            offset: None,
        };
        let result = query_audit_logs(&conn, &params).unwrap();
        assert_eq!(result.total, 2);

        // Query by actor
        let params = AuditQueryParams {
            entity_type: None,
            entity_id: None,
            action: None,
            actor: Some("alice".to_string()),
            request_id: None,
            limit: None,
            offset: None,
        };
        let result = query_audit_logs(&conn, &params).unwrap();
        assert_eq!(result.total, 2);

        // Query with pagination
        let params = AuditQueryParams {
            entity_type: None,
            entity_id: None,
            action: None,
            actor: None,
            request_id: None,
            limit: Some(1),
            offset: Some(0),
        };
        let result = query_audit_logs(&conn, &params).unwrap();
        assert_eq!(result.total, 3);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.limit, 1);
        assert_eq!(result.offset, 0);
    }
}
