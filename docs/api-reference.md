# MetaFuse REST API Reference

> **Note:** This documentation is hand-maintained. Please ensure it stays in sync with code changes.
> **Future:** Consider generating this from OpenAPI/Swagger spec.

## Base URL

```
http://localhost:8080
```

Configure via environment variable:
```bash
METAFUSE_PORT=3000 metafuse-api
```

## Authentication

**Current (v0.1.x):** No authentication required. The API is open.

**Future:** API key authentication and RBAC planned for Phase 4.

## Endpoints

### Health Check

**GET /health**

Check if the API server is running.

**Response:**
```json
{
  "status": "ok"
}
```

**Status Codes:**
- `200 OK`: Server is healthy

---

### List Datasets

**GET /api/v1/datasets**

Retrieve a list of all datasets in the catalog.

**Query Parameters:**
- `tenant` (optional): Filter by tenant (e.g., `?tenant=prod`)
- `domain` (optional): Filter by domain (e.g., `?domain=analytics`)

**Example Request:**
```bash
curl http://localhost:8080/api/v1/datasets
curl http://localhost:8080/api/v1/datasets?tenant=prod
curl http://localhost:8080/api/v1/datasets?domain=analytics
```

**Response:**
```json
{
  "datasets": [
    {
      "name": "sales_data",
      "path": "s3://my-bucket/data/sales.parquet",
      "format": "parquet",
      "tenant": "prod",
      "domain": "analytics",
      "owner": "data-team@example.com",
      "description": "Daily sales transactions",
      "fields": 12,
      "created_at": "2025-11-15T10:00:00Z",
      "updated_at": "2025-11-20T08:30:00Z"
    },
    {
      "name": "user_profiles",
      "path": "s3://my-bucket/data/users.parquet",
      "format": "parquet",
      "tenant": "prod",
      "domain": "user-data",
      "owner": "user-team@example.com",
      "description": "User profile information",
      "fields": 8,
      "created_at": "2025-11-10T14:20:00Z",
      "updated_at": "2025-11-19T16:45:00Z"
    }
  ]
}
```

**Status Codes:**
- `200 OK`: Success
- `500 Internal Server Error`: Database error

---

### Get Dataset Details

**GET /api/v1/datasets/:name**

Retrieve detailed information about a specific dataset, including schema and lineage.

**Path Parameters:**
- `name` (required): Dataset name

**Query Parameters:**

- `include` (optional): Comma-separated list of additional data to include. Options: `delta`, `quality`, `lineage`

**Example Requests:**
```bash
# Basic request
curl http://localhost:8080/api/v1/datasets/sales_data

# With Delta Lake metadata
curl "http://localhost:8080/api/v1/datasets/sales_data?include=delta"

# With quality metrics
curl "http://localhost:8080/api/v1/datasets/sales_data?include=quality"

# With all optional data
curl "http://localhost:8080/api/v1/datasets/sales_data?include=delta,quality,lineage"
```

**Response:**
```json
{
  "name": "sales_data",
  "path": "s3://my-bucket/data/sales.parquet",
  "format": "parquet",
  "delta_location": "s3://my-bucket/delta/sales",
  "tenant": "prod",
  "domain": "analytics",
  "owner": "data-team@example.com",
  "description": "Daily sales transactions",
  "row_count": 1500000,
  "size_bytes": 45000000,
  "fields": [
    {
      "name": "transaction_id",
      "data_type": "Int64",
      "nullable": false
    },
    {
      "name": "customer_id",
      "data_type": "Int64",
      "nullable": false
    },
    {
      "name": "amount",
      "data_type": "Float64",
      "nullable": true
    },
    {
      "name": "timestamp",
      "data_type": "Timestamp(Microsecond, None)",
      "nullable": false
    }
  ],
  "upstream_datasets": [
    "raw_transactions",
    "customer_master"
  ],
  "downstream_datasets": [
    "daily_sales_summary",
    "customer_revenue_report"
  ],
  "tags": [
    "sales",
    "transactions",
    "prod"
  ],
  "created_at": "2025-11-15T10:00:00Z",
  "updated_at": "2025-11-20T08:30:00Z"
}
```

**Optional Include Fields:**

When `?include=delta` is specified:
```json
{
  "delta": {
    "version": 10,
    "row_count": 1500000,
    "size_bytes": 45000000,
    "num_files": 24,
    "partition_columns": ["date"],
    "last_modified": "2025-11-20T08:30:00Z"
  }
}
```

When `?include=quality` is specified:
```json
{
  "quality": {
    "overall_score": 0.95,
    "completeness_score": 0.98,
    "freshness_score": 1.0,
    "file_health_score": 0.87,
    "last_computed": "2025-11-20T08:00:00Z"
  }
}
```

When `?include=lineage` is specified:
```json
{
  "lineage": {
    "upstream": ["raw_transactions", "customer_master"],
    "downstream": ["daily_sales_summary", "customer_revenue_report"]
  }
}
```

**Field Types:**

The `data_type` field uses Arrow type notation:
- Primitives: `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`
- Floats: `Float32`, `Float64`
- Strings: `Utf8`, `LargeUtf8`
- Temporal: `Date32`, `Date64`, `Timestamp(unit, tz)`, `Time32`, `Time64`, `Duration`, `Interval`
- Binary: `Binary`, `LargeBinary`
- Complex: `List`, `Struct`, `Map`

**Status Codes:**
- `200 OK`: Success
- `404 Not Found`: Dataset does not exist
- `500 Internal Server Error`: Database error

---

### Search Datasets

**GET /api/v1/search**

Full-text search across dataset names, paths, domains, and field names using SQLite FTS5.

**Query Parameters:**
- `q` (required): Search query

**Example Request:**
```bash
curl "http://localhost:8080/api/v1/search?q=sales"
curl "http://localhost:8080/api/v1/search?q=analytics+revenue"
```

**Response:**
```json
{
  "results": [
    {
      "name": "sales_data",
      "path": "s3://my-bucket/data/sales.parquet",
      "domain": "analytics",
      "owner": "data-team@example.com"
    },
    {
      "name": "daily_sales_summary",
      "path": "s3://my-bucket/aggregates/daily_sales.parquet",
      "domain": "analytics",
      "owner": "data-team@example.com"
    }
  ]
}
```

**Search Features:**
- Searches dataset name, path, domain, and field names
- Supports boolean operators: `sales AND transactions`, `sales OR revenue`
- Supports phrase queries: `"daily sales"`
- Supports prefix matching: `trans*`

**Status Codes:**
- `200 OK`: Success (empty results if no matches)
- `400 Bad Request`: Missing or invalid `q` parameter
- `500 Internal Server Error`: Database error

---

### Create Dataset

**POST /api/v1/datasets**

Create a new dataset in the catalog.

**Request Body:**
```json
{
  "name": "sales_data",
  "path": "s3://my-bucket/data/sales.parquet",
  "format": "parquet",
  "delta_location": "s3://my-bucket/delta/sales",
  "description": "Daily sales transactions",
  "tenant": "prod",
  "domain": "analytics",
  "owner": "data-team@example.com",
  "tags": ["sales", "production"],
  "upstream_datasets": ["raw_transactions"]
}
```

**Required Fields:**
- `name`: Dataset name (alphanumeric, underscore, hyphen, dot)
- `path`: Storage path
- `format`: Data format (parquet, delta, csv, etc.)

**Optional Fields:**
- `delta_location`: Path to Delta table for live metadata queries
- `description`, `tenant`, `domain`, `owner`: Metadata fields
- `tags`: List of tags to attach
- `upstream_datasets`: List of upstream dataset names for lineage

**Status Codes:**
- `201 Created`: Dataset created successfully
- `400 Bad Request`: Invalid input or dataset already exists
- `500 Internal Server Error`: Database error

---

### Update Dataset

**PUT /api/v1/datasets/:name**

Update an existing dataset's metadata.

**Request Body:**
```json
{
  "path": "s3://new-bucket/data/sales.parquet",
  "description": "Updated description",
  "owner": "new-team@example.com"
}
```

All fields are optional. Only provided fields will be updated.

**Status Codes:**
- `200 OK`: Dataset updated successfully
- `400 Bad Request`: Invalid input
- `404 Not Found`: Dataset does not exist
- `500 Internal Server Error`: Database error

---

### Delete Dataset

**DELETE /api/v1/datasets/:name**

Remove a dataset from the catalog.

**Status Codes:**
- `204 No Content`: Dataset deleted successfully
- `404 Not Found`: Dataset does not exist
- `500 Internal Server Error`: Database error

---

### Add Tags

**POST /api/v1/datasets/:name/tags**

Add tags to a dataset.

**Request Body:**
```json
{
  "tags": ["production", "validated"]
}
```

**Response:** Returns the updated list of all tags on the dataset.

**Status Codes:**
- `200 OK`: Tags added successfully
- `404 Not Found`: Dataset does not exist
- `500 Internal Server Error`: Database error

---

### Remove Tags

**POST /api/v1/datasets/:name/tags/remove**

Remove tags from a dataset.

> **Note:** This endpoint uses POST instead of DELETE because DELETE requests with request bodies
> are not universally supported by all HTTP clients and proxies. The `/remove` suffix clearly
> indicates the destructive nature of the operation.

**Request Body:**
```json
{
  "tags": ["deprecated", "test"]
}
```

**Response:** Returns the remaining tags on the dataset after removal.

**Status Codes:**
- `200 OK`: Tags removed successfully
- `404 Not Found`: Dataset does not exist
- `500 Internal Server Error`: Database error

---

### Delta-Delegated Endpoints

These endpoints query live metadata directly from Delta Lake tables. The dataset must have a `delta_location` configured.

#### Get Schema

**GET /api/v1/datasets/:name/schema**

Get the schema directly from the Delta table.

**Query Parameters:**
- `version` (optional): Specific Delta version to query

#### Get Stats

**GET /api/v1/datasets/:name/stats**

Get statistics (row count, size, file count) from the Delta table.

#### Get History

**GET /api/v1/datasets/:name/history**

Get version history from the Delta table.

**Query Parameters:**
- `limit` (optional): Max versions to return (default: 10)

---

## Error Responses

All error responses follow this format:

```json
{
  "error": "Dataset 'unknown_dataset' not found",
  "request_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

The `request_id` is a UUID that can be used to correlate errors with server logs for debugging.

**Common Status Codes:**
- `400 Bad Request`: Invalid request parameters
- `404 Not Found`: Resource does not exist
- `500 Internal Server Error`: Server or database error

---

## CORS Configuration

The API enables CORS for all origins to support web-based UIs:

```rust
// CORS policy
.layer(
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
)
```

This can be restricted in production environments.

---

## Configuration

### Environment Variables

- `METAFUSE_CATALOG_PATH` (or `METAFUSE_CATALOG`): Path to the catalog database file (default: `metafuse_catalog.db`)
- `METAFUSE_PORT` (fallback `PORT`): API server port (default: `8080`)

**Example:**
```bash
METAFUSE_CATALOG=/data/catalog.db METAFUSE_PORT=3000 metafuse-api
```

---

## Usage Examples

### List all prod datasets

```bash
curl "http://localhost:8080/api/v1/datasets?tenant=prod" | jq '.datasets[] | {name, domain, owner}'
```

### Get schema for a dataset

```bash
curl "http://localhost:8080/api/v1/datasets/sales_data" | jq '.fields'
```

### Find datasets with "revenue" in any field

```bash
curl "http://localhost:8080/api/v1/search?q=revenue" | jq '.results'
```

### Check lineage for a dataset

```bash
curl "http://localhost:8080/api/v1/datasets/sales_data" | jq '{upstream, downstream}'
```

---

## Future Endpoints (Planned)

### Glossary Management

**GET /api/v1/glossary**

List all business glossary terms.

**POST /api/v1/glossary**

Create a new glossary term.

### Lineage Visualization

**GET /api/v1/lineage/:name**

Get lineage graph in a format suitable for visualization (e.g., graph JSON).

### Usage Analytics

**GET /api/v1/analytics/popular**

Get most queried/accessed datasets.

**GET /api/v1/analytics/freshness**

Identify stale datasets.

---

## Performance Considerations

- **Catalog caching:** The API opens a connection to the SQLite catalog on each request. Future versions will implement connection pooling.
- **Search performance:** FTS5 queries are fast (<5ms) for 10k datasets. For larger catalogs, consider pagination.
- **Lineage queries:** Lineage traversal uses indexed joins and is typically <10ms. Deep lineage graphs (>5 levels) may be slower.

---

## Client Libraries (Future)

We plan to provide client libraries for:
- Python (metafuse-py)
- JavaScript/TypeScript (metafuse-js)
- Rust (metafuse-client)

Example (Python):

```python
from metafuse import MetaFuseClient

client = MetaFuseClient("http://localhost:8080")
datasets = client.list_datasets(tenant="prod")
details = client.get_dataset("sales_data")
results = client.search("analytics")
```

---

## OpenAPI Specification (Future)

We plan to generate an OpenAPI 3.0 specification for this API, enabling:
- Interactive API documentation (Swagger UI)
- Automated client library generation
- API contract testing

---

## Versioning

The API uses URL versioning: `/api/v1/...`

Breaking changes will increment the version (e.g., `/api/v2/...`), and we will support multiple versions simultaneously during transition periods.

---

## Support

- **Issues:** [https://github.com/ethan-tyler/MetaFuse/issues](https://github.com/ethan-tyler/MetaFuse/issues)
- **Discussions:** [https://github.com/ethan-tyler/MetaFuse/discussions](https://github.com/ethan-tyler/MetaFuse/discussions)
