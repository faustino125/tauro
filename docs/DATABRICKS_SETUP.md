# Databricks Setup Guide for Tauro

## 🎯 Overview

This guide explains how to configure Databricks infrastructure for use with Tauro pipelines.

**IMPORTANT**: Tauro is a **pipeline execution framework**. It does NOT provision or configure Databricks infrastructure. All Databricks setup is the **end user's responsibility**.

---

## 📋 Responsibility Model

| Component | Responsible Party | Description |
|-----------|------------------|-------------|
| **Databricks Workspace** | ✅ End User | Create and configure workspace |
| **Authentication & Credentials** | ✅ End User | Generate and manage tokens/service principals |
| **Unity Catalog Infrastructure** | ✅ End User | Create catalogs, schemas, volumes |
| **Permissions & Access Control** | ✅ End User | Grant appropriate permissions |
| **Spark Configuration** | ✅ End User | Configure cluster and Unity Catalog settings |
| **Network & Security** | ✅ End User | Configure VPC, firewall, IAM roles |
| **Pipeline Execution** | ⚙️ Tauro | Execute pipelines using provided infrastructure |
| **Data Read/Write** | ⚙️ Tauro | Read/write data to Unity Catalog tables |

---

## 🔧 Step-by-Step Setup

### 1. Databricks Workspace Setup

**User Responsibility**: Create Databricks workspace in your cloud provider.

```bash
# AWS
aws databricks create-workspace --workspace-name "production-workspace"

# Azure
az databricks workspace create --name "production-workspace" --resource-group "mygroup"

# GCP
gcloud databricks workspaces create "production-workspace"
```

### 2. Authentication Configuration

**User Responsibility**: Generate access token and configure environment.

```bash
# Generate token in Databricks UI:
# Settings → Developer → Access Tokens → Generate New Token

# Set environment variables (REQUIRED before running Tauro)
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890..."

# For production, use secrets manager:
export DATABRICKS_TOKEN=$(aws secretsmanager get-secret-value \
  --secret-id databricks/prod-token --query SecretString --output text)
```

### 3. Unity Catalog Setup

**User Responsibility**: Create catalogs, schemas, and volumes.

#### Option A: Using Databricks SQL CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Create Unity Catalog infrastructure
databricks sql --execute "
  CREATE CATALOG IF NOT EXISTS production;
  CREATE SCHEMA IF NOT EXISTS production.analytics;
  CREATE SCHEMA IF NOT EXISTS production.ml_models;
  CREATE VOLUME IF NOT EXISTS production.analytics.artifacts;
"
```

#### Option B: Using Databricks UI

1. Navigate to **Data** → **Catalogs**
2. Click **Create Catalog**
3. Enter catalog name: `production`
4. Click **Create**
5. Select catalog → **Create Schema**
6. Enter schema name: `analytics`
7. Click **Create**

#### Option C: Using Terraform (Infrastructure as Code)

```hcl
# databricks.tf
resource "databricks_catalog" "production" {
  name    = "production"
  comment = "Production data catalog"
}

resource "databricks_schema" "analytics" {
  catalog_name = databricks_catalog.production.name
  name         = "analytics"
  comment      = "Analytics tables and views"
}

resource "databricks_volume" "artifacts" {
  catalog_name = databricks_catalog.production.name
  schema_name  = databricks_schema.analytics.name
  name         = "artifacts"
  volume_type  = "MANAGED"
}
```

### 4. Permissions Configuration

**User Responsibility**: Grant appropriate permissions to users/service principals.

```sql
-- Grant catalog access
GRANT USE CATALOG ON CATALOG production TO `user@company.com`;
GRANT USE CATALOG ON CATALOG production TO `service-principal-id`;

-- Grant schema permissions
GRANT ALL PRIVILEGES ON SCHEMA production.analytics TO `user@company.com`;
GRANT SELECT, MODIFY ON SCHEMA production.analytics TO `service-principal-id`;

-- Grant table/volume permissions
GRANT ALL PRIVILEGES ON VOLUME production.analytics.artifacts TO `user@company.com`;
```

### 5. Spark Cluster Configuration

**User Responsibility**: Configure cluster with Unity Catalog enabled.

```json
{
  "cluster_name": "tauro-pipeline-cluster",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "num_workers": 2,
  "spark_conf": {
    "spark.databricks.unityCatalog.enabled": "true",
    "spark.databricks.cluster.profile": "singleNode"
  },
  "data_security_mode": "SINGLE_USER"
}
```

### 6. Verify Configuration

**User Responsibility**: Test connectivity and permissions before running Tauro.

```python
# test_databricks_setup.py
import os
from databricks import sql

# Verify environment variables
assert os.getenv("DATABRICKS_HOST"), "DATABRICKS_HOST not set"
assert os.getenv("DATABRICKS_TOKEN"), "DATABRICKS_TOKEN not set"

# Test connectivity
with sql.connect(
    server_hostname=os.getenv("DATABRICKS_HOST"),
    http_path="/sql/1.0/warehouses/xxx",
    access_token=os.getenv("DATABRICKS_TOKEN")
) as connection:
    with connection.cursor() as cursor:
        # Verify catalog exists
        cursor.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in cursor.fetchall()]
        assert "production" in catalogs, "Catalog 'production' not found"
        
        # Verify schema exists
        cursor.execute("SHOW SCHEMAS IN production")
        schemas = [row[0] for row in cursor.fetchall()]
        assert "analytics" in schemas, "Schema 'analytics' not found"
        
        print("✅ Databricks configuration verified successfully")
```

---

## 🚀 Running Tauro with Databricks

Once infrastructure is configured, Tauro can execute pipelines:

### Configuration File (config.yaml)

```yaml
# config.yaml
execution_mode: databricks  # Use Databricks execution

global_settings:
  fail_on_error: true
  databricks_catalog: production  # Catalog created by user
  databricks_schema: analytics    # Schema created by user

# Output configuration
output_config:
  sales_aggregated:
    format: unity_catalog
    catalog_name: production
    schema: analytics
    table_name: sales_aggregated
    write_mode: overwrite
    uc_table_mode: external  # or "managed"
    output_path: "s3://my-bucket/data/production/"  # For external tables
```

### Running the Pipeline

```bash
# Ensure environment is configured
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890..."

# Verify configuration (optional but recommended)
python test_databricks_setup.py

# Run Tauro pipeline
python -m core.cli.cli run_pipeline \
  --config config.yaml \
  --env production \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

---

## ⚠️ Common Issues and Solutions

### Issue 1: "DATABRICKS_HOST not set"

**Cause**: Environment variables not configured.

**Solution**:
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890..."
```

### Issue 2: "Catalog 'production' does not exist"

**Cause**: Unity Catalog not created.

**Solution**:
```sql
CREATE CATALOG IF NOT EXISTS production;
```

### Issue 3: "Permission denied: CREATE CATALOG"

**Cause**: User lacks CREATE CATALOG permission.

**Solution**: Grant permission or pre-create catalog:
```sql
-- As admin
GRANT CREATE CATALOG ON METASTORE TO `user@company.com`;
```

### Issue 4: "Unity Catalog not enabled"

**Cause**: Spark cluster not configured for Unity Catalog.

**Solution**: Add to cluster configuration:
```json
{
  "spark_conf": {
    "spark.databricks.unityCatalog.enabled": "true"
  }
}
```

---

## 🔒 Security Best Practices

### 1. Use Service Principals for Production

```bash
# Create service principal
databricks service-principals create --application-id "12345-67890"

# Generate token for service principal
databricks tokens create-obo \
  --application-id "12345-67890" \
  --lifetime-seconds 7776000  # 90 days
```

### 2. Store Credentials in Secrets Manager

```bash
# AWS Secrets Manager
aws secretsmanager create-secret \
  --name "databricks/prod-token" \
  --secret-string "dapi1234567890..."

# Retrieve in pipeline
export DATABRICKS_TOKEN=$(aws secretsmanager get-secret-value \
  --secret-id databricks/prod-token \
  --query SecretString --output text)
```

### 3. Use IAM Roles for Cloud Storage

```json
{
  "spark_conf": {
    "spark.hadoop.fs.s3a.aws.credentials.provider": 
      "com.amazonaws.auth.InstanceProfileCredentialsProvider"
  }
}
```

### 4. Rotate Tokens Regularly

```bash
# Automated token rotation script
databricks tokens delete --token-id OLD_TOKEN_ID
databricks tokens create --lifetime-seconds 7776000 > new_token.txt
```

---

## 📚 Additional Resources

- [Databricks Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Databricks Authentication Guide](https://docs.databricks.com/dev-tools/auth.html)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
- [Tauro IO Module Documentation](../src/core/io/README.md)
- [Tauro MLOps Module Documentation](../src/core/mlops/README.md)

---

## ✅ Pre-Flight Checklist

Before running Tauro pipelines with Databricks, verify:

- [ ] Databricks workspace created
- [ ] `DATABRICKS_HOST` environment variable set
- [ ] `DATABRICKS_TOKEN` environment variable set
- [ ] Unity Catalog enabled on cluster
- [ ] Catalogs created (or CREATE CATALOG permission granted)
- [ ] Schemas created (or CREATE SCHEMA permission granted)
- [ ] Volumes created for artifacts (if using MLOps)
- [ ] Appropriate permissions granted (USE CATALOG, SELECT, MODIFY, CREATE TABLE)
- [ ] Network connectivity verified
- [ ] Cloud storage credentials configured (for external tables)
- [ ] Test script executed successfully

Once all items are checked, Tauro can execute pipelines using your Databricks infrastructure.
