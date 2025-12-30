# Tauro Data Virtualization Module

**Enterprise-grade data virtualization layer for unified, secure access to heterogeneous data sources.**

> 🎯 Zero-copy architecture | 🔒 Enterprise security | ⚡ Intelligent optimization | 📊 Full observability

---

## 📚 Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Architecture](#architecture)
4. [Features](#features)
5. [Installation](#installation)
6. [Usage Guide](#usage-guide)
7. [Configuration](#configuration)
8. [Security](#security)
9. [Performance](#performance)
10. [Monitoring](#monitoring)
11. [Troubleshooting](#troubleshooting)
12. [API Reference](#api-reference)

---

## 🎯 Overview

The Data Virtualization Module provides **unified access to heterogeneous data sources** without physically moving data:

- **Single Interface**: Query PostgreSQL, Snowflake, BigQuery, S3, Kafka, etc. uniformly
- **In-Place Access**: Data stays where it is—no ETL, no copies, no latency
- **Enterprise Security**: Field encryption, row-level security, role-based access, audit trails
- **Intelligent Optimization**: Automatic predicate pushdown, cost-based planning, smart caching
- **Compliance Ready**: GDPR, HIPAA, SOC2 support with built-in audit logging

### Key Use Cases

| Use Case | Benefit |
|----------|---------|
| **Data Mesh Federations** | Query across multiple data domains with governance |
| **Privacy-First Analytics** | Apply PII masking and encryption automatically |
| **Cost Reduction** | Eliminate data duplication; access source directly |
| **Multi-Tenant Isolation** | Row-level security for automatic data filtering |
| **Legacy Integration** | Access old systems alongside modern data warehouses |

---

## ⚡ Quick Start

### 1. Import and Create Virtual Data Layer

```python
# Native import from Tauro (now a first-class feature!)
from src.core import VirtualDataLayer, VirtualTable, SourceType, VirtualContext

# Option A: Direct component usage
virtual_layer = VirtualDataLayer()

# Option B: Use VirtualContext for unified interface (recommended)
ctx = VirtualContext()

# Register a PostgreSQL table
customers = VirtualTable(
    name="customers",
    source_type=SourceType.DATABASE,
    connector_type="postgresql",
    connection_id="prod_db",
    table_name="customers",
    schema={
        "id": "INTEGER",
        "name": "VARCHAR",
        "email": "VARCHAR",
        "department": "VARCHAR"
    }
)
virtual_layer.schema_registry.register_table(customers)
```

### 2. Apply Security Policy

```python
# Native imports - all security components available from src.core
from src.core import (
    SecurityEnforcer, TableSecurityPolicy, FieldSecurityPolicy, AccessLevel
)

enforcer = SecurityEnforcer()

# Define security policy
policy = TableSecurityPolicy(
    table_name="customers",
    field_policies={
        "email": FieldSecurityPolicy(
            field_name="email",
            masking_enabled=True,
            allowed_roles=["admin", "analyst"]
        )
    },
    row_level_filters={
        "analyst": "department = 'Sales'",  # Analysts only see Sales dept
        "admin": ""  # Admins see everything
    }
)
enforcer.register_policy(policy)
```

### 3. Query with Optimization

```python
# All federation components available natively
from src.core import (
    FederationEngine, Predicate, PredicateOperator, VirtualReaderFactory
)

federation = FederationEngine()

# Define query predicates (automatically protected from SQL injection)
predicates = [
    Predicate(field="department", operator=PredicateOperator.EQ, value="Sales"),
    Predicate(field="created_date", operator=PredicateOperator.GTE, value="2024-01-01")
]

# Get optimized query plan
plan = federation.plan_query("customers", predicates, projection=["id", "name", "email"])

print(f"Strategy: {plan.execution_strategy.value}")  # 'direct', 'cache', or 'materialized'
print(f"Estimated cost: {plan.estimated_cost:.2f}")
print(f"Estimated rows: {plan.estimated_rows}")

# Execute query
def executor(plan):
    factory = VirtualReaderFactory(context=None)
    reader = factory.create_reader(customers)
    return reader.read()

results, stats = federation.execute_query(plan, executor)

# Apply security policies
rows = list(results)
filtered_rows = enforcer.apply_row_level_security("customers", "analyst", rows)
masked_rows = enforcer.apply_field_masking("customers", "analyst", filtered_rows)

print(f"Execution: {stats.execution_time_ms:.1f}ms, {stats.rows_returned} rows")
```

---

## 🏗️ Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                    Virtual Data Layer API                         │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐ │
│  │ Schema Registry  │  │ Query Federation │  │   Security       │ │
│  │ ─────────────    │  │   Optimizer      │  │  Enforcer        │ │
│  │ • Catalog        │  │ ─────────────    │  │ ─────────────    │ │
│  │ • Lineage        │  │ • Predicate      │  │ • RLS            │ │
│  │ • Statistics     │  │   Pushdown       │  │ • Encryption     │ │
│  │ • Validation     │  │ • Cost Model     │  │ • Masking        │ │
│  │                  │  │ • Caching        │  │ • Audit Trail    │ │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘ │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
                              ↓ ↓ ↓
┌───────────────────────────────────────────────────────────────────┐
│            Virtual Data Source Readers                             │
├───────────────────────────────────────────────────────────────────┤
│  • FilesystemReader (Parquet, CSV, Delta)                         │
│  • DatabaseReader (PostgreSQL, MySQL, Oracle, SQL Server)         │
│  • DataWarehouseReader (Snowflake, BigQuery, Redshift)           │
│  • APIReader (REST endpoints, webhooks)                           │
│  • StreamReader (Kafka, Kinesis)                                 │
└───────────────────────────────────────────────────────────────────┘
                              ↓ ↓ ↓
┌───────────────────────────────────────────────────────────────────┐
│            Physical Data Sources (No Movement)                     │
├───────────────────────────────────────────────────────────────────┤
│  PostgreSQL │ Snowflake │ BigQuery │ S3 │ Kafka │ APIs │ Files   │
└───────────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

### 1. **Virtual Schema Registry**

Centralized metadata catalog without storage overhead:

- **Dynamic Schema**: Register tables from any source with flexible schema definition
- **Schema Evolution**: Track schema versions and breaking changes
- **Data Lineage**: Automatic upstream/downstream dependency tracking
- **Statistics**: Table row count, size, partition info, cache hit rates
- **Validation**: Automatic compatibility checks for joins

**Example:**
```python
# Register table
registry = virtual_layer.schema_registry
registry.register_table(customers)

# Query catalog
sales_tables = registry.list_tables(tag="financial")

# Get lineage
lineage = registry.get_lineage("transactions")
print(f"Upstream: {lineage['upstream']}")
print(f"Downstream: {lineage['downstream']}")

# Export catalog
catalog = registry.export_catalog(format="json")
```

### 2. **Query Federation Engine**

Intelligent distributed query optimization:

- **Predicate Pushdown**: Filters applied at source, not in memory
- **Cost-Based Planning**: Optimizes joins and filter order
- **Multiple Strategies**: Direct execution, caching, materialized views
- **Cross-Source Joins**: Efficiently join data from different systems
- **Statistics Collection**: Real-time query metrics for continuous optimization

**Example:**
```python
from core.virtualization.federation_engine import Predicate, PredicateOperator

# Define predicates (automatically protected from SQL injection)
predicates = [
    Predicate("status", PredicateOperator.EQ, "active"),
    Predicate("created_date", PredicateOperator.GTE, "2024-01-01")
]

# Create optimized plan
plan = federation.plan_query(
    table_name="customers",
    predicates=predicates,
    projection=["id", "name", "email"],
    limit=10000,
    supports_pushdown=True
)

# View plan details
print(f"Estimated cost: {plan.estimated_cost}")
print(f"Estimated rows: {plan.estimated_rows}")
print(f"Strategy: {plan.execution_strategy.value}")
print(f"Pushdown predicates: {[p.to_sql() for p in plan.pushdown_predicates]}")
```

### 3. **Enterprise Security**

Multi-layered security with compliance support:

**Field-Level Encryption**
```python
from core.virtualization.security import FieldSecurityPolicy, AccessLevel

policy.field_policies["ssn"] = FieldSecurityPolicy(
    field_name="ssn",
    access_level=AccessLevel.ENCRYPTED,
    encryption_enabled=True,
    allowed_roles=["hr", "admin"]
)
```

**Row-Level Security (RLS)**
```python
# Automatic filtering based on user/role
policy.row_level_filters = {
    "analyst": "department = 'Sales'",      # SQL WHERE clause
    "manager": "manager_id = {principal}",  # {principal} = current user
    "admin": ""                             # Empty = no filtering
}
```

**PII Masking**
```python
# Automatic masking strategies
from core.virtualization.security import EmailMasker, PhoneMasker, SSNMasker

# Masking applied automatically
masking_strategies = {
    "email": EmailMasker(),    # Shows: a***n@example.com
    "phone": PhoneMasker(),    # Shows: ****1234
    "ssn": SSNMasker()         # Shows: ***-**-5678
}
```

**Audit Logging**
```python
# Complete access trail for compliance
enforcer.audit_access(
    principal="john.doe",
    table_name="customers",
    field_names=["email", "name"],
    row_count=1500,
    status="SUCCESS"
)

# Export for compliance reporting
logs = enforcer.get_audit_logs(
    table_name="customers",
    start_time=datetime(2024, 1, 1),
    end_time=datetime(2024, 12, 31)
)

# Export as JSON or CSV
json_trail = enforcer.export_audit_trail(format="json")
csv_trail = enforcer.export_audit_trail(format="csv")
```

### 4. **Performance Optimization**

Intelligent caching and query planning:

- **Smart Caching**: Automatic decision on when/how to cache
- **Cache Strategies**: Never, Always, Smart, Periodic
- **Cache Invalidation**: TTL-based with manual triggers
- **Connection Pooling**: Reuse connections across queries
- **Materialized Views**: Cache expensive aggregations

**Example:**
```python
from core.virtualization import CacheStrategy

# Smart caching - engine decides
table = VirtualTable(
    name="sales_summary",
    cache_strategy=CacheStrategy.SMART,  # Auto-optimize
    cache_ttl_seconds=3600
)

# Or explicit strategy
table = VirtualTable(
    name="real_time_events",
    cache_strategy=CacheStrategy.NEVER,  # No caching
)

# View optimization metrics
metrics = federation.query_optimizer.get_optimization_metrics()
print(f"Cache hit rate: {metrics['cache_hit_rate']:.1%}")
print(f"Avg execution time: {metrics['avg_execution_time_ms']:.1f}ms")
```

---

## 📦 Installation & Quick Verification

The virtualization module is now a **native first-class feature** of Tauro with full framework integration.

### Simple Verification

```python
# Native imports - no submodule paths needed!
from src.core import (
    VirtualDataLayer,
    SecurityEnforcer,
    FederationEngine,
    VirtualContext
)

print("✅ Virtualization fully integrated as native Tauro module")

# Access as module attribute
import src.core as tauro
virt_module = tauro.virtualization  # Module available directly
```

### Optional Database Connectors

Install specific connectors as needed:

```bash
pip install psycopg2-binary          # PostgreSQL
pip install pymysql                   # MySQL
pip install snowflake-connector-python # Snowflake
pip install google-cloud-bigquery    # BigQuery
pip install databricks-sql-connector  # Databricks
```

---

## 📖 Usage Guide

### Recommended: Use VirtualContext (Simplest)

```python
from src.core import VirtualContext

# Configuration dictionary
config = {
    "virtual_tables": {
        "customers": {
            "source_type": "database",
            "connector_type": "postgresql",
            "connection": "prod_db",
            "table": "customers",
            "schema": {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"}
        }
    },
    "security_policies": {
        "customers": {
            "allowed_roles": ["admin", "analyst"],
            "audit_all_access": True
        }
    }
}

# One-liner initialization
ctx = VirtualContext(config=config)

# Direct access
table = ctx.get_table("customers")
rows = ctx.read_table("customers", principal="analyst", apply_security=True)
```

### Alternative: Direct Component Usage

### Basic Workflow

**Step 1: Create Virtual Data Layer**
```python
from src.core import VirtualDataLayer, VirtualTable, SourceType

virtual_layer = VirtualDataLayer()
```

**Step 2: Register Data Sources**
```python
from src.core import VirtualTable, SourceType, CacheStrategy

# Register PostgreSQL table
pg_table = VirtualTable(
    name="customers",
    source_type=SourceType.DATABASE,
    connector_type="postgresql",
    connection_id="prod_db",
    table_name="customers",
    schema={"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"}
)
virtual_layer.schema_registry.register_table(pg_table)

# Register Snowflake table
sf_table = VirtualTable(
    name="transactions",
    source_type=SourceType.DATA_WAREHOUSE,
    connector_type="snowflake",
    connection_id="warehouse",
    query="SELECT * FROM raw_transactions WHERE date >= '{{ start_date }}'",
    schema={"id": "INTEGER", "amount": "DECIMAL", "date": "DATE"},
    cache_strategy=CacheStrategy.MATERIALIZED
)
virtual_layer.schema_registry.register_table(sf_table)

# Register S3 Parquet files
s3_table = VirtualTable(
    name="events",
    source_type=SourceType.FILESYSTEM,
    connector_type="s3",
    path="s3://my-bucket/events/",
    schema={"event_id": "STRING", "timestamp": "TIMESTAMP", "user_id": "STRING"},
    partitions=["date"]
)
virtual_layer.schema_registry.register_table(s3_table)
```

**Step 3: Configure Security**
```python
from src.core import SecurityEnforcer, TableSecurityPolicy

enforcer = SecurityEnforcer()

policy = TableSecurityPolicy(
    table_name="customers",
    allowed_roles=["admin", "analyst", "engineer"],
    audit_all_access=True
)
enforcer.register_policy(policy)
```

**Step 4: Query Data**
```python
from src.core import VirtualReaderFactory

factory = VirtualReaderFactory(context=None)
reader = factory.create_reader(pg_table)
data = reader.read()

for row in data:
    print(row)
```

### Advanced Patterns

#### Multi-Source Join

```python
from src.core import FederationEngine

federation = FederationEngine()

# Plan join across PostgreSQL and Snowflake
join_plan = federation.plan_federated_join(
    tables=["customers", "transactions"],
    join_predicates={
        "customers_id": "customers.id = transactions.customer_id"
    }
)

print(f"Join cost estimate: {join_plan.estimated_cost}")
```

#### Dynamic Row-Level Security

```python
from src.core import SecurityEnforcer, TableSecurityPolicy

# Apply RLS based on user context
current_user = "john.doe"
user_department = "Sales"

policy = TableSecurityPolicy(
    table_name="customers",
    row_level_filters={current_user: f"department = '{user_department}'"}
)
enforcer = SecurityEnforcer()
enforcer.register_policy(policy)

# Apply to results
rows = list(results)
filtered = enforcer.apply_row_level_security(
    "customers", 
    current_user,  # Automatically applies filter
    rows
)
```

#### Materialized View Creation

```python
from src.core import VirtualDataLayer, VirtualTable, SourceType, CacheStrategy

# Cache expensive aggregation
summary_table = VirtualTable(
    name="sales_summary_daily",
    source_type=SourceType.DATABASE,
    connector_type="postgresql",
    query="""
        SELECT 
            DATE(order_date) as date,
            SUM(amount) as total_sales,
            COUNT(*) as num_orders
        FROM orders
        GROUP BY DATE(order_date)
    """,
    cache_strategy=CacheStrategy.MATERIALIZED,
    cache_ttl_seconds=86400  # Refresh daily
)
virtual_layer.schema_registry.register_table(summary_table)
```

---

## ⚙️ Configuration

### Load from YAML

```python
from src.core import VirtualDataLayer

# Load config file
config = """
virtualization:
  enabled: true
  virtual_tables:
    customers:
      source_type: database
      connector_type: postgresql
      connection: prod_db
      table_name: customers
      cache_strategy: smart
      tags:
        - pii
        - production
    
    transactions:
      source_type: data_warehouse
      connector_type: snowflake
      connection: warehouse
      query: "SELECT * FROM transactions"
      cache_strategy: materialized
      cache_ttl_seconds: 3600
    
    events:
      source_type: stream
      connector_type: kafka
      table_name: events_topic
      cache_strategy: never
"""

virtual_layer = VirtualDataLayer.from_config(yaml.safe_load(config))
```

### VirtualContext Configuration (Recommended)

```python
from src.core import VirtualContext

config = {
    "virtual_tables": {
        "customers": {
            "source_type": "database",
            "connector_type": "postgresql",
            "connection": "prod_db",
            "table_name": "customers",
            "cache_strategy": "smart",
            "cache_ttl_seconds": 3600,
            "description": "Customer master data",
            "tags": ["pii", "production"],
            "schema": {
                "id": "INTEGER",
                "name": "VARCHAR",
                "email": "VARCHAR"
            }
        }
    }
}

virtual_layer = VirtualDataLayer.from_config(config)
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source_type` | Enum | - | DATABASE, FILESYSTEM, API, STREAM, DATA_WAREHOUSE |
| `connector_type` | String | - | postgresql, mysql, snowflake, bigquery, s3, kafka, etc. |
| `connection_id` | String | - | Connection pool identifier |
| `cache_strategy` | Enum | SMART | NEVER, ALWAYS, SMART, PERIODIC |
| `cache_ttl_seconds` | Integer | 3600 | Cache time-to-live in seconds |
| `partitions` | List | [] | Partition column names for pruning |
| `tags` | List | [] | Metadata tags for filtering |

---

## 🔐 Security

### Data Classification

Tag sensitive tables for automatic protection:

```python
from src.core import VirtualTable, SourceType

customers = VirtualTable(
    name="customers",
    source_type=SourceType.DATABASE,
    connector_type="postgresql",
    connection_id="prod_db",
    table_name="customers",
    tags=["pii", "confidential", "financial"]  # ← Tags for classification
)
```

### Field-Level Encryption

```python
from src.core import FieldSecurityPolicy, AccessLevel

# PII fields encrypted at rest
ssn_policy = FieldSecurityPolicy(
    field_name="ssn",
    access_level=AccessLevel.ENCRYPTED,
    encryption_enabled=True,
    allowed_roles=["hr_admin", "compliance"]  # Only these roles see plaintext
)

policy.field_policies["ssn"] = ssn_policy
```

### Row-Level Security (RLS) Examples

```python
# Department-based filtering
policy.row_level_filters["analyst"] = "department = 'Sales'"

# User-based filtering
policy.row_level_filters["manager"] = "manager_id = {principal}"

# Complex expressions
policy.row_level_filters["regional_lead"] = "region IN ('North', 'South')"

# Numeric comparisons
policy.row_level_filters["junior_analyst"] = "salary < 100000"

# Date-based (cohort analysis)
policy.row_level_filters["retention_analyst"] = "signup_date >= DATE_SUB(NOW(), INTERVAL 1 YEAR)"
```

### Compliance Checklist

- [ ] **GDPR**: Enable audit logging, implement RLS for user data, encrypt PII
- [ ] **HIPAA**: Use field-level encryption for health data, maintain audit trail
- [ ] **SOC2**: Configure comprehensive audit logging, enable access controls
- [ ] **PCI-DSS**: Encrypt payment data fields, restrict access by role
- [ ] **CCPA**: Implement data masking, provide audit trail for consumer requests

### Security Hardening

```python
# Maximum security configuration
from src.core import (
    SecurityEnforcer, TableSecurityPolicy, FieldSecurityPolicy, AccessLevel
)

enforcer = SecurityEnforcer()

# Restrictive policy
strict_policy = TableSecurityPolicy(
    table_name="financial_data",
    allowed_roles=["finance_admin", "auditor"],  # Whitelist only
    audit_all_access=True,  # Log every access
    requires_encryption=True,  # All fields encrypted
    field_policies={
        "account_number": FieldSecurityPolicy(
            field_name="account_number",
            access_level=AccessLevel.ENCRYPTED,
            encryption_enabled=True,
            allowed_roles=["finance_admin"]
        ),
        "balance": FieldSecurityPolicy(
            field_name="balance",
            access_level=AccessLevel.ENCRYPTED,
            encryption_enabled=True,
            allowed_roles=["finance_admin", "auditor"]
        )
    }
)

enforcer.register_policy(strict_policy)
```

---

## ⚡ Performance

### Optimization Strategies

| Strategy | When to Use | Benefit |
|----------|-------------|---------|
| **Predicate Pushdown** | Filtering large tables | 90% I/O reduction |
| **Projection** | Selecting few columns | Network bandwidth savings |
| **Caching** | Repeated queries | Sub-second response time |
| **Materialized Views** | Complex aggregations | 10-100x speedup |
| **Partitioning** | Date/region fields | Partition pruning |

### Cache Strategies

```python
from src.core import CacheStrategy, VirtualTable

# NEVER: Always fetch from source (for real-time data)
events = VirtualTable(
    name="user_events",
    cache_strategy=CacheStrategy.NEVER
)

# ALWAYS: Cache results for 1 hour (for stable data)
product_catalog = VirtualTable(
    name="products",
    cache_strategy=CacheStrategy.ALWAYS,
    cache_ttl_seconds=3600
)

# SMART: Let engine decide (balanced default)
customers = VirtualTable(
    name="customers",
    cache_strategy=CacheStrategy.SMART,
    cache_ttl_seconds=3600
)

# PERIODIC: Refresh on schedule (for aggregations)
daily_summary = VirtualTable(
    name="sales_summary",
    cache_strategy=CacheStrategy.PERIODIC,
    cache_ttl_seconds=86400  # Refresh daily
)
```

### Query Plan Analysis

```python
from src.core import FederationEngine

federation = FederationEngine()

predicates = [...]
plan = federation.plan_query("customers", predicates)

# Analyze plan
print(f"Execution Strategy: {plan.execution_strategy.value}")
print(f"Estimated Cost: {plan.estimated_cost:.2f}")
print(f"Estimated Rows: {plan.estimated_rows:,}")
print(f"Estimated Bytes: {plan.estimated_bytes:,}")

# View optimization metrics
metrics = federation.query_optimizer.get_optimization_metrics()
print(f"Cache Hit Rate: {metrics['cache_hit_rate']:.1%}")
print(f"Avg Time: {metrics['avg_execution_time_ms']:.1f}ms")
```

### Performance Tuning Tips

1. **Enable Predicate Pushdown**: Set `supports_pushdown=True`
2. **Limit Result Sets**: Always use `limit` parameter
3. **Project Early**: Specify only needed columns
4. **Use Partitions**: Define partition columns for large tables
5. **Monitor Cache**: Track hit rates and adjust TTL
6. **Profile Queries**: Use execution statistics to identify bottlenecks

---

## 📊 Monitoring

### Health Metrics

```python
# Schema registry status
catalog = virtual_layer.schema_registry.export_catalog()
print(f"Registered tables: {len(catalog['tables'])}")
print(f"Total lineage edges: {len(catalog['lineage'])}")

# Query optimization metrics
metrics = federation.query_optimizer.get_optimization_metrics()
print(f"Total queries: {metrics['total_queries']}")
print(f"Cache hit rate: {metrics['cache_hit_rate']:.1%}")
print(f"Avg execution time: {metrics['avg_execution_time_ms']:.1f}ms")

# Security audit status
logs = enforcer.get_audit_logs(table_name="customers")
print(f"Access events: {len(logs)}")

# Denied access attempts
denied = [log for log in logs if log.status == "DENIED"]
print(f"Denied accesses: {len(denied)}")
for log in denied[:5]:
    print(f"  - {log.principal} → {log.table_name}: {log.denial_reason}")
```

### Monitoring Dashboard Queries

```python
# Query performance distribution
stats_by_table = {}
for qid, stat in federation.query_optimizer._statistics.items():
    table = stat.table_name
    if table not in stats_by_table:
        stats_by_table[table] = []
    stats_by_table[table].append(stat.execution_time_ms)

for table, times in stats_by_table.items():
    print(f"{table}:")
    print(f"  Min: {min(times):.1f}ms")
    print(f"  Avg: {sum(times)/len(times):.1f}ms")
    print(f"  Max: {max(times):.1f}ms")

# Security audit summary
from datetime import datetime, timedelta

last_24h = datetime.utcnow() - timedelta(hours=24)
recent_logs = enforcer.get_audit_logs(start_time=last_24h)

by_operation = {}
for log in recent_logs:
    op = log.operation.value
    by_operation[op] = by_operation.get(op, 0) + 1

print("Operations (last 24h):")
for op, count in sorted(by_operation.items()):
    print(f"  {op}: {count}")
```

---

## 🔧 Troubleshooting

### Common Issues

#### Issue: Slow Queries

**Symptoms**: Queries taking longer than expected

**Diagnosis**:
```python
from src.core import FederationEngine

federation = FederationEngine()

# Check query plan
plan = federation.plan_query("customers", predicates)
print(f"Estimated cost: {plan.estimated_cost}")
print(f"Strategy: {plan.execution_strategy.value}")

# Check cache hit rate
metrics = federation.query_optimizer.get_optimization_metrics()
print(f"Cache hit rate: {metrics['cache_hit_rate']:.1%}")
```

**Solutions**:
- ✅ Enable predicate pushdown: `supports_pushdown=True`
- ✅ Check indexes on source tables
- ✅ Consider materialized view for repeated queries
- ✅ Increase cache TTL for stable data

#### Issue: Query Execution Errors

**Symptoms**: "Query execution failed"

**Diagnosis**:
```python
try:
    results, stats = federation.execute_query(plan, executor)
    for row in results:
        print(row)
except Exception as e:
    logger.error(f"Query failed: {e}")
    # Check source connectivity
    # Verify table/column names
    # Check predicate syntax
```

**Solutions**:
- ✅ Verify source connectivity
- ✅ Check virtual table configuration
- ✅ Validate predicate syntax
- ✅ Review source error logs

#### Issue: Security Policy Not Applied

**Symptoms**: Data accessible without expected filtering

**Diagnosis**:
```python
from src.core import SecurityEnforcer

enforcer = SecurityEnforcer()

# Verify policy is registered
policy = enforcer.get_policy("customers")
print(f"Policy exists: {policy is not None}")

# Check RLS filter
print(f"RLS filters: {policy.row_level_filters}")

# Verify enforcement
rows = list(results)
print(f"Before RLS: {len(rows)} rows")

filtered = enforcer.apply_row_level_security("customers", "analyst", rows)
print(f"After RLS: {len(filtered)} rows")
```

**Solutions**:
- ✅ Confirm policy is registered with `register_policy()`
- ✅ Verify RLS expression syntax
- ✅ Check user/role assignments
- ✅ Review audit logs for enforcement

#### Issue: Out of Memory

**Symptoms**: Process crashes with large datasets

**Solutions**:
- ✅ Use pagination with `limit` parameter
- ✅ Stream results instead of loading all at once
- ✅ Apply filters before caching
- ✅ Use partition pruning for large tables

---

## 📚 API Reference

### Core Classes

#### `VirtualTable`
Represents a virtual table abstraction over a data source.

```python
VirtualTable(
    name: str,                          # Unique table identifier
    source_type: SourceType,            # DATABASE, FILESYSTEM, API, STREAM, DATA_WAREHOUSE
    connector_type: str,                # "postgresql", "snowflake", "s3", etc.
    connection_id: str,                 # Connection pool ID
    query: Optional[str] = None,        # SQL query (for databases)
    table_name: Optional[str] = None,   # Table name (if not query-based)
    path: Optional[str] = None,         # File path (for filesystem)
    schema: Dict[str, str] = {},        # {column: type}
    partitions: List[str] = [],         # Partition columns
    source_tables: List[str] = [],      # Upstream dependencies
    transformation: Optional[str] = None,  # Transformation description
    cache_strategy: CacheStrategy = SMART,  # Caching strategy
    cache_ttl_seconds: int = 3600,      # Cache TTL
    encryption_config: Optional[EncryptionConfig] = None,
    access_control: Dict[str, List[str]] = {},
    description: str = "",
    tags: List[str] = []
)
```

#### `SchemaRegistry`
Central metadata catalog.

```python
registry = SchemaRegistry()

# Register table
registry.register_table(table: VirtualTable) -> None

# Retrieve table
table = registry.get_table(name: str) -> Optional[VirtualTable]

# List tables with filters
tables = registry.list_tables(
    source_type: Optional[SourceType] = None,
    connector_type: Optional[str] = None,
    tag: Optional[str] = None
) -> List[VirtualTable]

# Get data lineage
lineage = registry.get_lineage(table_name: str) -> Dict[str, List[str]]

# Export catalog
catalog = registry.export_catalog(format: str = "json") -> Dict
```

#### `FederationEngine`
Query planning and execution.

```python
engine = FederationEngine()

# Plan query
plan = engine.plan_query(
    table_name: str,
    predicates: List[Predicate],
    projection: Optional[List[str]] = None,
    limit: Optional[int] = None,
    supports_pushdown: bool = True
) -> QueryPlan

# Execute query
results, stats = engine.execute_query(
    plan: QueryPlan,
    executor_func,  # Function to fetch data
    query_id: Optional[str] = None
) -> Tuple[Iterator, QueryStatistics]

# Plan join
join_plan = engine.plan_federated_join(
    tables: List[str],
    join_predicates: Dict[str, str]
) -> JoinPlan
```

#### `SecurityEnforcer`
Access control and encryption.

```python
enforcer = SecurityEnforcer()

# Register policy
enforcer.register_policy(policy: TableSecurityPolicy) -> None

# Validate access
can_access = enforcer.validate_access(
    principal: str,
    table_name: str,
    field_names: Optional[List[str]] = None
) -> bool

# Apply RLS
filtered = enforcer.apply_row_level_security(
    table_name: str,
    principal: str,
    rows: List[Dict]
) -> List[Dict]

# Apply masking
masked = enforcer.apply_field_masking(
    table_name: str,
    principal: str,
    rows: List[Dict]
) -> List[Dict]

# Audit access
log = enforcer.audit_access(
    principal: str,
    table_name: str,
    field_names: Optional[List[str]] = None,
    row_count: int = 0,
    status: str = "SUCCESS",
    denial_reason: str = ""
) -> AuditLog

# Export audit trail
trail = enforcer.export_audit_trail(format: str = "json") -> str
```

### Enums

- **SourceType**: DATABASE, FILESYSTEM, API, STREAM, DATA_WAREHOUSE
- **CacheStrategy**: NEVER, ALWAYS, SMART, PERIODIC
- **ExecutionStrategy**: DIRECT, CACHE, MATERIALIZED, STREAM, DISTRIBUTED
- **PredicateOperator**: EQ, NEQ, GT, GTE, LT, LTE, IN, NOT_IN, LIKE, IS_NULL, IS_NOT_NULL
- **AccessLevel**: UNRESTRICTED, MASKED, ENCRYPTED, RESTRICTED
- **Operation**: SELECT, INSERT, UPDATE, DELETE, EXPORT

---

---

## 🚀 Integration with Tauro Pipeline

VirtualContext integrates seamlessly with Tauro's execution engine:

```python
from src.core import VirtualContext, StreamingContext, PipelineExecutor

# Create contexts
virtual_ctx = VirtualContext(config=virtual_config)
streaming_ctx = StreamingContext(topics=["kafka:events"])

# Use in pipeline
executor = PipelineExecutor()
result = executor.execute(
    "pipeline.yaml",
    contexts={
        "virtual": virtual_ctx,
        "streaming": streaming_ctx,
    }
)
```

## 🤝 Contributing

See [CONTRIBUTING.md](../../docs/contributing.rst) for guidelines.

## 📄 License

MIT License. See [LICENSE](../../LICENSE).

## 📞 Support

- **Documentation**: [VIRTUALIZATION_NATIVE_INTEGRATION.md](../../VIRTUALIZATION_NATIVE_INTEGRATION.md)
- **Native Integration**: ✅ First-class Tauro module
- **Tests**: `python test_native_integration.py`
- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions
