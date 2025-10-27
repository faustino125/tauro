# Tauro API

A comprehensive REST API for managing data pipeline orchestration, execution, and monitoring in the Tauro framework. Provides endpoints for pipeline management, run execution, scheduling, streaming operations, and real-time monitoring.

The API is organized around core concepts:
- **Projects**: Workspace containers with pipelines and configurations
- **Pipelines**: Pipeline definitions with node dependencies
- **Runs**: Individual pipeline executions with state tracking
- **Schedules**: Automated execution triggers (CRON or INTERVAL)
- **Streaming**: Real-time data processing pipelines
- **Monitoring**: Health checks, metrics, and statistics

---

## Table of Contents

- [Quick Start](#quick-start)
- [Installation](#installation)
- [Architecture](#architecture)
- [Core Concepts](#core-concepts)
- [API Reference](#api-reference)
- [Project Management](#project-management)
- [Pipeline Execution](#pipeline-execution)
- [Schedule Management](#schedule-management)
- [Streaming Operations](#streaming-operations)
- [Monitoring and Health](#monitoring-and-health)
- [Authentication](#authentication)
- [Error Handling](#error-handling)
- [Examples](#examples)
- [Development](#development)

---

## Quick Start

### Start the API Server

```bash
# Using development server
python -m tauro.api.main

# Or with Uvicorn directly
uvicorn tauro.api.main:app --host 0.0.0.0 --port 8000 --reload
```

### Create a Project

```bash
curl -X POST http://localhost:8000/api/v1/projects \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my_project",
    "description": "My first pipeline project"
  }'
```

### Create and Execute a Pipeline

```bash
# 1. Create a run
curl -X POST http://localhost:8000/api/v1/runs \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "proj_123",
    "pipeline_id": "pipeline_etl",
    "params": {"date": "2025-01-26"}
  }'

# 2. Start the execution
curl -X POST http://localhost:8000/api/v1/runs/run_xyz/start

# 3. Monitor progress
curl http://localhost:8000/api/v1/runs/run_xyz
```

### Create a Schedule

```bash
curl -X POST http://localhost:8000/api/v1/schedules \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "proj_123",
    "pipeline_id": "pipeline_etl",
    "kind": "CRON",
    "expression": "0 2 * * *",
    "enabled": true
  }'
```

---

## Installation

### Requirements

- Python 3.9+
- FastAPI
- Motor (async MongoDB driver)
- Pydantic v2
- APScheduler (for scheduling)
- Croniter (for CRON expressions)

### Installation Methods

**From source:**
```bash
cd core
pip install -e .
```

**With development dependencies:**
```bash
pip install -e ".[dev]"
```

---

## Architecture

### Component Structure

```
tauro/api/
├── main.py              # FastAPI application entry point
├── adapters/            # External system adapters
│   └── orchestration_adapter.py
├── core/                # Core utilities
│   ├── config.py        # Configuration
│   ├── deps.py          # Dependency injection
│   ├── middleware.py    # Request/response middleware
│   ├── responses.py     # Response models
│   └── validators.py    # Input validation
├── db/                  # Database layer
│   ├── connection.py    # MongoDB connection management
│   ├── migrations.py    # Schema migrations
│   └── models.py        # Data models
├── routes/              # API endpoints
│   ├── projects.py
│   ├── pipelines.py
│   ├── runs.py
│   ├── scheduling.py
│   ├── monitoring.py
│   └── config_versions.py
├── schemas/             # Request/response schemas
│   ├── models.py        # Pydantic models
│   ├── requests.py      # Request DTOs
│   ├── responses.py     # Response DTOs
│   └── validators.py    # Schema validators
└── services/            # Business logic
    ├── base.py          # Base service
    ├── project_service.py
    ├── run_service.py
    ├── execution_service.py
    ├── schedule_service.py
    └── config_service.py
```

### Request/Response Flow

```
HTTP Request
    ↓
Middleware (logging, timing)
    ↓
Route Handler (validation)
    ↓
Service Layer (business logic)
    ↓
Database Layer (MongoDB)
    ↓
Response Model (serialization)
    ↓
HTTP Response
```

### Service Layer Pattern

Each service implements:
- Data validation
- Business logic enforcement
- Database interaction
- Error handling
- Logging

Base services provide:
- Pagination helpers
- Query builders
- Transaction support
- Caching

---

## Core Concepts

### Project

A workspace container that groups pipelines, configurations, and executions.

```json
{
  "id": "proj_abc123",
  "name": "analytics_pipeline",
  "description": "Daily ETL pipeline",
  "owner": "user@company.com",
  "created_at": "2025-01-26T10:00:00Z",
  "pipelines": [
    {
      "id": "pipe_etl",
      "name": "bronze_to_silver",
      "nodes": ["extract", "transform", "load"]
    }
  ]
}
```

### Pipeline

Defines the computational graph of data transformations.

```json
{
  "id": "pipe_etl",
  "name": "daily_etl",
  "nodes": [
    {"id": "extract", "type": "source"},
    {"id": "transform", "type": "processor"},
    {"id": "load", "type": "sink"}
  ],
  "edges": [
    {"from": "extract", "to": "transform"},
    {"from": "transform", "to": "load"}
  ]
}
```

### Run

An execution instance of a pipeline.

```json
{
  "id": "run_xyz",
  "project_id": "proj_abc123",
  "pipeline_id": "pipe_etl",
  "state": "RUNNING",
  "params": {"date": "2025-01-26"},
  "started_at": "2025-01-26T10:00:00Z",
  "progress": {
    "total_tasks": 3,
    "completed_tasks": 2,
    "failed_tasks": 0
  }
}
```

### Schedule

Automated triggers for pipeline execution.

Supported types:
- **CRON**: Standard cron expressions (e.g., "0 2 * * *" for 2 AM daily)
- **INTERVAL**: Fixed intervals (e.g., "1d", "2h", "30m")

---

## API Reference

### Base URL

```
http://localhost:8000/api/v1
```

### Common Query Parameters

- `limit`: Number of results (default: 50, max: 100)
- `offset`: Number of results to skip (default: 0)
- `sort`: Sort field (default: "-created_at")

### Response Format

All responses follow this format:

**Success (2xx):**
```json
{
  "data": {...},
  "meta": {
    "timestamp": "2025-01-26T10:00:00Z",
    "request_id": "req_123"
  }
}
```

**List Response (2xx):**
```json
{
  "data": [...],
  "meta": {
    "total": 100,
    "limit": 50,
    "offset": 0,
    "timestamp": "2025-01-26T10:00:00Z"
  }
}
```

**Error (4xx/5xx):**
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid project ID",
    "details": [...]
  },
  "meta": {
    "request_id": "req_123",
    "timestamp": "2025-01-26T10:00:00Z"
  }
}
```

---

## Project Management

### List Projects

```
GET /projects
```

Query Parameters:
- `limit`: Max results (default: 50)
- `offset`: Skip results (default: 0)
- `owner`: Filter by owner

Example:
```bash
curl "http://localhost:8000/api/v1/projects?limit=20&offset=0"
```

### Get Project

```
GET /projects/{project_id}
```

### Create Project

```
POST /projects
```

Request Body:
```json
{
  "name": "my_project",
  "description": "Project description",
  "owner": "user@company.com"
}
```

### Update Project

```
PUT /projects/{project_id}
```

Request Body:
```json
{
  "description": "Updated description"
}
```

### Delete Project

```
DELETE /projects/{project_id}
```

### Get Project Statistics

```
GET /projects/{project_id}/statistics
```

Returns:
- Total pipelines
- Total runs
- Run success rate
- Average execution time

---

## Pipeline Execution

### Create a Run

```
POST /runs
```

Request Body:
```json
{
  "project_id": "proj_123",
  "pipeline_id": "pipe_etl",
  "params": {
    "start_date": "2025-01-01",
    "end_date": "2025-01-31"
  },
  "tags": {
    "env": "production",
    "owner": "user@company.com"
  }
}
```

Response:
```json
{
  "id": "run_xyz",
  "state": "PENDING",
  "created_at": "2025-01-26T10:00:00Z"
}
```

### List Runs

```
GET /runs
```

Query Parameters:
- `project_id`: Filter by project
- `pipeline_id`: Filter by pipeline
- `state`: Filter by state (PENDING, RUNNING, SUCCESS, FAILED)
- `limit`: Max results
- `offset`: Skip results

### Get Run Details

```
GET /runs/{run_id}
```

Returns full run information including progress, timing, and state.

### Start Run Execution

```
POST /runs/{run_id}/start
```

Request Body (optional):
```json
{
  "timeout_seconds": 3600
}
```

Response:
```json
{
  "status": "accepted",
  "run_id": "run_xyz",
  "status_url": "/api/v1/runs/run_xyz"
}
```

### Cancel Run

```
POST /runs/{run_id}/cancel
```

Request Body (optional):
```json
{
  "reason": "User requested cancellation"
}
```

### Get Run Tasks

```
GET /runs/{run_id}/tasks
```

Lists all tasks/nodes executed in the run.

### Get Run Logs

```
GET /runs/{run_id}/logs
```

Query Parameters:
- `level`: Filter by log level (DEBUG, INFO, WARNING, ERROR)
- `limit`: Max log lines
- `offset`: Skip lines

---

## Schedule Management

### Create Schedule

```
POST /schedules
```

Request Body:
```json
{
  "project_id": "proj_123",
  "pipeline_id": "pipe_etl",
  "kind": "CRON",
  "expression": "0 2 * * *",
  "enabled": true,
  "max_concurrency": 1,
  "timeout_seconds": 3600
}
```

CRON Examples:
- `0 0 * * *`: Daily at midnight
- `0 * * * *`: Every hour
- `0 0 * * 0`: Weekly on Sunday
- `0 0 1 * *`: Monthly on 1st day

INTERVAL Examples:
- `1d`: Every 1 day
- `2h`: Every 2 hours
- `30m`: Every 30 minutes
- `15s`: Every 15 seconds

### List Schedules

```
GET /schedules
```

Query Parameters:
- `project_id`: Filter by project
- `pipeline_id`: Filter by pipeline
- `enabled`: Filter by state

### Get Schedule

```
GET /schedules/{schedule_id}
```

### Update Schedule

```
PUT /schedules/{schedule_id}
```

### Enable Schedule

```
POST /schedules/{schedule_id}/enable
```

### Disable Schedule

```
POST /schedules/{schedule_id}/disable
```

### Delete Schedule

```
DELETE /schedules/{schedule_id}
```

### Backfill Schedule

Create historical runs for a schedule (useful for recovering from failures):

```
POST /schedules/{schedule_id}/backfill
```

Request Body:
```json
{
  "count": 10
}
```

Returns historical run dates that would have been executed.

---

## Streaming Operations

### Run Streaming Pipeline

```
POST /stream/run
```

Request Body:
```json
{
  "config": "config/streaming.yaml",
  "pipeline": "kafka_events",
  "mode": "async"
}
```

Modes:
- `async`: Returns immediately, pipeline runs in background
- `sync`: Waits for completion

### Get Streaming Status

```
GET /stream/status
```

Query Parameters:
- `execution_id`: Filter by execution (optional)

### Stop Streaming Pipeline

```
POST /stream/stop
```

Request Body:
```json
{
  "execution_id": "exec_123",
  "timeout": 60
}
```

---

## Monitoring and Health

### Health Check

```
GET /health
```

Returns:
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "timestamp": "2025-01-26T10:00:00Z"
}
```

### Get API Information

```
GET /info
```

Returns API metadata, version, and configuration info.

### Get Statistics

```
GET /statistics
```

Returns:
- Total projects
- Total pipelines
- Total runs (by state)
- Average execution time
- Success rate

### Metrics (Prometheus)

```
GET /metrics
```

Exports metrics in Prometheus format for monitoring integrations.

---

## Authentication

Currently, the API is designed for internal use without authentication. For production use, implement:

- OAuth 2.0 bearer tokens
- API key validation middleware
- Role-based access control (RBAC)

Example middleware pattern:
```python
@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    token = request.headers.get("Authorization")
    if not token:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    # Validate token...
    response = await call_next(request)
    return response
```

---

## Error Handling

### Error Codes

| Code | Status | Meaning |
|------|--------|---------|
| `VALIDATION_ERROR` | 422 | Invalid input data |
| `NOT_FOUND` | 404 | Resource not found |
| `CONFLICT` | 409 | Resource conflict (e.g., duplicate name) |
| `INTERNAL_ERROR` | 500 | Server error |
| `TIMEOUT` | 504 | Operation timeout |

### Error Response Example

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid run parameters",
    "details": [
      {
        "field": "project_id",
        "message": "Project not found"
      }
    ]
  },
  "meta": {
    "request_id": "req_123",
    "timestamp": "2025-01-26T10:00:00Z"
  }
}
```

### Handling Errors in Code

```python
try:
    response = requests.post(
        "http://localhost:8000/api/v1/runs",
        json={"project_id": "proj_123", ...}
    )
    response.raise_for_status()
except requests.exceptions.HTTPError as e:
    error_data = e.response.json()
    print(f"Error: {error_data['error']['code']}")
    print(f"Details: {error_data['error']['details']}")
```

---

## Examples

### Complete ETL Pipeline

```bash
# 1. Create project
PROJECT=$(curl -s -X POST http://localhost:8000/api/v1/projects \
  -H "Content-Type: application/json" \
  -d '{"name": "etl_project"}' | jq -r '.data.id')

# 2. Create run
RUN=$(curl -s -X POST http://localhost:8000/api/v1/runs \
  -H "Content-Type: application/json" \
  -d "{
    \"project_id\": \"$PROJECT\",
    \"pipeline_id\": \"daily_etl\",
    \"params\": {\"date\": \"2025-01-26\"}
  }" | jq -r '.data.id')

# 3. Start execution
curl -s -X POST "http://localhost:8000/api/v1/runs/$RUN/start"

# 4. Monitor progress
curl "http://localhost:8000/api/v1/runs/$RUN" | jq '.data.progress'

# 5. Get logs
curl "http://localhost:8000/api/v1/runs/$RUN/logs?level=ERROR"
```

### Automated Daily Scheduling

```bash
# Create daily 2 AM schedule
curl -X POST http://localhost:8000/api/v1/schedules \
  -H "Content-Type: application/json" \
  -d "{
    \"project_id\": \"$PROJECT\",
    \"pipeline_id\": \"daily_etl\",
    \"kind\": \"CRON\",
    \"expression\": \"0 2 * * *\",
    \"enabled\": true,
    \"timeout_seconds\": 7200
  }"

# Backfill 30 days of historical data
curl -X POST http://localhost:8000/api/v1/schedules/sched_xyz/backfill \
  -H "Content-Type: application/json" \
  -d '{"count": 30}'
```

### Streaming Pipeline

```bash
# Start streaming pipeline
curl -X POST http://localhost:8000/api/v1/stream/run \
  -H "Content-Type: application/json" \
  -d '{
    "config": "config/kafka.yaml",
    "pipeline": "events_processor",
    "mode": "async"
  }'

# Check status
curl http://localhost:8000/api/v1/stream/status

# Stop pipeline
curl -X POST http://localhost:8000/api/v1/stream/stop \
  -H "Content-Type: application/json" \
  -d '{"execution_id": "exec_123", "timeout": 30}'
```

---

## Development

### Run Tests

```bash
pytest tauro/api/tests -v
```

### Run with Hot Reload

```bash
uvicorn tauro.api.main:app --reload
```

### Generate API Documentation

Swagger UI is automatically available at:
```
http://localhost:8000/docs
```

ReDoc documentation:
```
http://localhost:8000/redoc
```

### Database Migrations

```bash
# Create migration
python -m tauro.api.db.migrations create

# Apply migrations
python -m tauro.api.db.migrations up

# Rollback
python -m tauro.api.db.migrations down
```

### Logging Configuration

Configure logging level:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Log files are written to `logs/api.log` by default.

---

## Performance Considerations

### Pagination

Always paginate large result sets:
```bash
curl "http://localhost:8000/api/v1/runs?limit=50&offset=0"
```

### Caching

Statistics are cached for 60 seconds. For real-time data, query specific resources.

### Connection Pooling

MongoDB connections are pooled automatically. Configure pool size in environment variables:
```bash
export MONGO_POOL_SIZE=10
export MONGO_MAX_IDLE_TIME=300
```

### Timeouts

Default timeout for long-running operations: 3600 seconds (1 hour)

---

## Troubleshooting

### Port Already in Use

```bash
# On Windows
netstat -ano | findstr :8000
taskkill /PID <PID> /F

# On macOS/Linux
lsof -i :8000
kill -9 <PID>
```

### MongoDB Connection Issues

Verify connection string:
```python
import motor.motor_asyncio
client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
```

### Slow Queries

Check MongoDB indexes:
```bash
db.pipeline_runs.getIndexes()
```

Add indexes for common queries:
```bash
db.pipeline_runs.createIndex({"project_id": 1, "state": 1})
db.schedules.createIndex({"project_id": 1, "enabled": 1})
```

---

## License

Copyright (c) 2025 Faustino Lopez Ramos. For licensing information, see the LICENSE file in the project root.

---
