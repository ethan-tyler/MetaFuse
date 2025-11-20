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

**Example Request:**
```bash
curl http://localhost:8080/api/v1/datasets/sales_data
```

**Response:**
```json
{
  "name": "sales_data",
  "path": "s3://my-bucket/data/sales.parquet",
  "format": "parquet",
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
  "upstream": [
    "raw_transactions",
    "customer_master"
  ],
  "downstream": [
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

## Error Responses

All error responses follow this format:

```json
{
  "error": "Dataset not found",
  "detail": "No dataset with name 'unknown_dataset' exists in the catalog"
}
```

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

### Create/Update Dataset

**POST /api/v1/datasets**

Manually create or update dataset metadata.

### Delete Dataset

**DELETE /api/v1/datasets/:name**

Remove a dataset from the catalog.

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
