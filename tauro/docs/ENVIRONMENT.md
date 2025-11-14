# Environment Management & Promotion Guide

## Overview

Tauro manages environments across two distinct subsystems:

1. **CLI Layer** (`tauro/cli/`): Local development and execution
2. **API Layer** (`tauro/api/`): Configuration management, versionning, and deployments

Understanding the differences and integration points is critical for effective workflow.

---

## Table of Contents

1. [Canonical Environments](#canonical-environments)
2. [CLI Environment Workflow](#cli-environment-workflow)
3. [API Environment Workflow](#api-environment-workflow)
4. [Integration Between CLI and API](#integration-between-cli-and-api)
5. [Configuration Promotion Patterns](#configuration-promotion-patterns)
6. [Best Practices](#best-practices)

---

## Canonical Environments

Tauro defines **canonical environments** that are the single source of truth:

### Environment Definitions

| Environment | Category | Purpose | Storage | Audience |
|---|---|---|---|---|
| **base** | Local | Default/foundation config | Files | All systems |
| **dev** | Local | Development/testing | Files | Developers |
| **sandbox** | Local | Isolated testing | Files | Data scientists |
| **sandbox_<dev>** | Local | Personal sandbox | Files | Individual developer |
| **staging** | Remote | Pre-production validation | External | QA/Ops |
| **prod** | Remote | Production | External | Operations |

### Environment Categories

```python
from tauro.common import CanonicalEnvironment, EnvironmentCategory

# Local (file-based, safe)
EnvironmentCategory.LOCAL
→ ["base", "dev", "sandbox"]

# Remote (requires deployment)
EnvironmentCategory.REMOTE
→ ["staging", "prod"]

# Testing (allow experiments)
EnvironmentCategory.TESTING
→ ["dev", "sandbox"]

# Safe (non-production)
EnvironmentCategory.SAFE
→ ["base", "dev", "sandbox", "staging"]

# Dangerous (production)
EnvironmentCategory.DANGEROUS
→ ["prod"]
```

---

## CLI Environment Workflow

### Overview

CLI is used for **local development** and **pipeline execution** with environment-specific configurations.

### Directory Structure

```
my_project/
├── config/
│   ├── global_settings.yaml        # Base: required
│   ├── pipelines.yaml              # Base: required
│   ├── nodes.yaml                  # Base: required
│   ├── input.yaml                  # Base: defaults
│   ├── output.yaml                 # Base: defaults
│   ├── dev/
│   │   ├── global_settings.yaml    # Dev: optional
│   │   ├── input.yaml              # Dev: overrides base
│   │   └── output.yaml             # Dev: overrides base
│   ├── sandbox/
│   │   └── input.yaml              # Sandbox: test data
│   ├── sandbox_alice/              # Alice's personal sandbox
│   │   └── input.yaml              # Alice's test data
│   └── prod/                       # Production (rarely edited locally)
│       └── input.yaml
├── pipelines/
├── tests/
├── settings_yml.json               # Environment index
└── ...
```

### Environment Normalization

CLI automatically normalizes environment names:

```bash
# All these refer to the same environment (dev):
tauro run --env dev
tauro run --env DEV
tauro run --env development
tauro run --env Development

# Sandbox variants:
tauro run --env sandbox
tauro run --env SANDBOX_ALICE    # → sandbox_alice (normalized)
tauro run --env sandbox_@bob!    # → sandbox_bob (sanitized)
```

### Configuration Merging

When loading config for an environment:

1. **Load base** - Foundation configuration (required)
2. **Load environment-specific** - Overrides for the target environment
3. **Merge** - Environment values take precedence
4. **Result** - Complete configuration for execution

Example:

```yaml
# base config
# global_settings.yaml
start_date: "2025-01-01"
end_date: "2025-12-31"
mode: "local"

# dev override
# config/dev/global_settings.yaml
end_date: "2025-01-15"  # Shorter test period

# Merged result for --env dev:
start_date: "2025-01-01"    # From base
end_date: "2025-01-15"      # Overridden by dev
mode: "local"               # From base
```

### Fallback Chain

If a config file is missing, Tauro follows a fallback chain:

```
sandbox_alice
  ↓ (if config/sandbox_alice/input.yaml missing)
sandbox
  ↓ (if config/sandbox/input.yaml missing)
base
  ↓
Error: Missing required configuration
```

For sandbox variants:
```
sandbox_bob → sandbox → base
```

For other environments:
```
dev → base
prod → base
```

### Command Examples

#### Running in Development
```bash
# Load config from config/dev/ (or base if missing)
tauro run --env dev --pipeline load

# Dry-run to validate config
tauro run --env dev --pipeline load --dry-run

# Verbose logging for debugging
tauro run --env dev --pipeline load --log-level DEBUG
```

#### Using Personal Sandbox
```bash
# Create personal sandbox directory
mkdir -p config/sandbox_alice

# Create Alice-specific input config
cat > config/sandbox_alice/input.yaml << EOF
data_path: "/tmp/alice_test_data"
sample_rows: 100
EOF

# Run in Alice's sandbox
tauro run --env sandbox_alice --pipeline transform

# Falls back to sandbox/base if Alice's config missing
```

#### Testing Before Production
```bash
# Test in dev environment
tauro run --env dev --pipeline full_pipeline

# Test in sandbox (isolated)
tauro run --env sandbox --pipeline full_pipeline

# Preview prod config (without executing)
tauro run --env prod --pipeline full_pipeline --dry-run
```

---

## API Environment Workflow

### Overview

API is used for **configuration management, versionning, and deployment** across environments.

### Environment Types

The API recognizes different environments than CLI:

```python
from tauro.api.services.config_version_service import EnvironmentType

EnvironmentType.DEV          # Development
EnvironmentType.STAGING      # Pre-production
EnvironmentType.PRODUCTION   # Production
```

⚠️ **Note**: API does NOT have "sandbox" or "base" environments.

### Configuration Versioning

API provides version control for configurations:

```python
from tauro.api.services.config_version_service import (
    ConfigVersionService,
    PromotionStatus,
    ChangeType,
)

service = ConfigVersionService()

# 1. Create version in dev
version = service.create_version(
    project_id=project_uuid,
    pipeline_id=None,
    snapshot=config_snapshot,
    changes=[
        ConfigChange(
            field_path="global_settings.start_date",
            old_value="2025-01-01",
            new_value="2025-01-15",
            change_type=ChangeType.UPDATED
        )
    ],
    change_reason="Adjusted test period for Q1",
    created_by="alice@example.com",
    tags={"team": "data-eng", "feature": "seasonal-analysis"}
)

# 2. Request approval for promotion
service.request_promotion(
    version_id=version.id,
    target_environment=EnvironmentType.STAGING,
    requested_by="alice@example.com",
    reason="Ready for QA validation"
)

# 3. Approve and promote (typically by Ops)
service.promote_version(
    version_id=version.id,
    target_environment=EnvironmentType.STAGING,
    status=PromotionStatus.APPROVED,
    approved_by="ops@example.com"
)

# 4. Monitor in staging
versions = service.list_versions(
    environment=EnvironmentType.STAGING,
    status=PromotionStatus.PROMOTED
)

# 5. Promote to production
service.promote_version(
    version_id=version.id,
    target_environment=EnvironmentType.PRODUCTION,
    status=PromotionStatus.APPROVED,
    approved_by="ops@example.com"
)
```

### Change Tracking

All configuration changes are tracked:

```python
class ChangeType(Enum):
    CREATED = "created"      # New configuration
    UPDATED = "updated"      # Modified value
    DELETED = "deleted"      # Removed field
    PROMOTED = "promoted"    # Promoted to new env
    ROLLED_BACK = "rolled_back"  # Reverted change
```

---

## Integration Between CLI and API

### Two-Way Integration Pattern

```
┌─────────────────────┐
│   Developer/CLI     │
│  (Local Machine)    │
├─────────────────────┤
│ config/dev/*.yaml   │
│ Run: tauro run      │
└──────────┬──────────┘
           │ (push config)
           ↓
┌─────────────────────┐
│   Tauro API         │
│  (Central Server)   │
├─────────────────────┤
│ Version Control     │
│ Promotion Pipeline  │
│ Audit Trail         │
└──────────┬──────────┘
           │ (deploy config)
           ↓
┌─────────────────────┐
│   Staging/Prod      │
│  (Remote Infra)     │
├─────────────────────┤
│ Deployed Config     │
│ Audit Logs          │
└─────────────────────┘
```

### Development to Production Flow

#### 1. Local Development (CLI)

```bash
# Modify config locally
vim config/dev/input.yaml

# Test locally
tauro run --env dev --pipeline load

# Validate config
tauro validate --env dev

# Push to API
tauro push-config --env dev --message "Fix input format"
```

#### 2. API Management

```python
# API receives and versions the config
# Shows in ConfigVersionService

# Team reviews changes
version = service.get_version(version_id)
print(version.changes)
# [
#   ConfigChange(field: input.format, old: csv, new: json)
# ]

# Request promotion
service.request_promotion(version_id, target=STAGING)
```

#### 3. Promotion to Staging

```python
# QA approves promotion
service.promote_version(
    version_id,
    target=EnvironmentType.STAGING,
    status=PromotionStatus.APPROVED
)

# Execute on staging infra
# (happens automatically or via deployment pipeline)
```

#### 4. Production Deployment

```python
# After staging validation, promote to prod
service.promote_version(
    version_id,
    target=EnvironmentType.PRODUCTION,
    status=PromotionStatus.APPROVED
)

# Production now using new config
# Audit trail captures:
# - Who approved
# - When approved
# - What changed
# - All versions available for rollback
```

---

## Configuration Promotion Patterns

### Pattern 1: Feature Development

**Flow**: Local → Dev → Staging → Prod

```bash
# 1. Work locally in sandbox
tauro run --env sandbox_alice --pipeline feature_x

# 2. Promote sandbox config to dev
tauro push-config --env sandbox_alice --target dev

# 3. API versions and promotes through stages
api.promote(dev → staging → prod)
```

### Pattern 2: Hotfix in Production

**Flow**: Prod → Local → Dev → Prod

```bash
# 1. Download current prod config
tauro pull-config --env prod

# 2. Modify locally and test
tauro run --env dev --pipeline critical_task

# 3. Create emergency promotion request
api.create_hotfix_promotion(
    source_version=prod_v123,
    changes=modified_fields,
    justification="Critical bug fix"
)

# 4. Fast-track approval and deploy
api.promote(dev → prod, bypass_staging=True)
```

### Pattern 3: Configuration Review

**Flow**: Feature branch-like workflow

```bash
# 1. Create configuration proposal
api.create_version(
    project_id=project,
    changes=proposed_changes,
    reason="Quarterly parameter tuning"
)

# 2. Team reviews via API
versions = api.list_proposed_changes()
comments = api.get_comments(version_id)

# 3. Either approve or request changes
if approved:
    api.promote(dev → staging → prod)
else:
    api.request_changes(version_id, feedback)
    # Cycle back to step 1
```

---

## Best Practices

### 1. Keep Base Config Current

The base config is the foundation - keep it up-to-date:

```yaml
# ✓ GOOD: Base has all required sections
# config/global_settings.yaml
project_name: "my_project"
version: "1.0.0"
mode: "local"
layers: [bronze, silver, gold]
start_date: "2025-01-01"
end_date: "2025-12-31"

# ✓ GOOD: Only overrides in environment configs
# config/dev/global_settings.yaml
end_date: "2025-01-15"  # Shorter test period only

# ✗ BAD: Incomplete base config
# config/global_settings.yaml
project_name: "my_project"
# Missing: version, mode, layers, dates

# ✗ BAD: Duplicating unchanged values
# config/dev/global_settings.yaml
project_name: "my_project"  # Same as base!
version: "1.0.0"            # Same as base!
mode: "local"               # Same as base!
```

### 2. Use Meaningful Sandbox Names

```bash
# ✓ GOOD: Clear purpose
mkdir config/sandbox_feature_xyz
mkdir config/sandbox_data_validation
mkdir config/sandbox_performance_test

# ✗ BAD: Unclear
mkdir config/sandbox_test
mkdir config/sandbox_temp
mkdir config/sandbox_new
```

### 3. Document Environment Differences

```yaml
# config/base/global_settings.yaml
# Base configuration for all environments
# Environment-specific overrides:
#   - dev: Shorter date range, verbose logging
#   - sandbox: Sample data, limited parallelism
#   - prod: Full data, optimized parallelism

project_name: "data_pipeline"
mode: "local"  # Override to "databricks" in prod
max_parallel_nodes: 4  # Override to 16 in prod
```

### 4. Validate Before Promotion

```bash
# Always validate before pushing to API
tauro validate --env dev

# Check specific settings
tauro config-check --env dev --section global_settings

# Dry-run to catch execution errors
tauro run --env dev --pipeline load --dry-run
```

### 5. Use Tags for Tracking

```python
# Tag configurations for easy filtering
service.create_version(
    ...,
    tags={
        "team": "data-eng",
        "feature": "seasonal-analysis",
        "priority": "high",
        "reviewed_by": "ops-team",
    }
)

# Later, filter by tags
versions = service.list_versions(
    tags={"team": "data-eng", "feature": "seasonal-analysis"}
)
```

### 6. Implement Promotion Gates

```python
# Require approvals for production
@app.post("/api/v1/promotions/approve")
def approve_promotion(promotion_id: UUID, approved_by: str):
    promotion = service.get_promotion(promotion_id)
    
    # Check if prod promotion requires multiple approvals
    if promotion.target == EnvironmentType.PRODUCTION:
        required_approvers = get_required_approvers()
        if len(promotion.approvals) < len(required_approvers):
            raise ApprovalRequired()
    
    service.approve_promotion(promotion_id, approved_by)
```

### 7. Maintain Audit Trail

Always log who, what, when, why:

```python
service.create_version(
    ...,
    created_by="alice@example.com",  # Who made it
    change_reason="Fix quarterly reporting format",  # Why
    # When: automatically timestamp
    # What: tracked in ConfigChange objects
)
```

---

## Troubleshooting

### Configuration Not Loading

```bash
# 1. Check environment normalization
tauro --env invalid_env
# Error: Invalid environment

# 2. Check what config is loaded
tauro run --env dev --log-level DEBUG --dry-run
# Logs show: "Loaded config from: config/dev/input.yaml"

# 3. Verify fallback chain
tauro run --env sandbox_alice --log-level DEBUG --dry-run
# Logs show: "Environment 'sandbox_alice' not found, falling back to 'sandbox'"
```

### Merge Issues

```bash
# 1. Check merged config
tauro config-show --env dev

# 2. Understand override precedence
# dev config takes precedence over base

# 3. Check for missing files
tauro validate --env dev
# Shows which files are missing from fallback
```

### Promotion Failures

```python
# Check promotion status
version = service.get_version(version_id)
print(version.promotion_status)  # pending, approved, promoted, failed

# Get error details
if version.promotion_status == "failed":
    print(version.error_details)
    # Shows what went wrong during promotion
```

---

## See Also

- [CLI Configuration Guide](../cli/README.md#configuration-model)
- [API Services Documentation](../api/services/)
- [Environment Constants](../common/constants.py)
- [Configuration Validator](../cli/validators.py)
