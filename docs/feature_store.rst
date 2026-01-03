Feature Store Integration
==========================

Tauro provides flexible feature store capabilities that work with your data pipeline architecture.

Feature Store Modes
-------------------

.. warning::

   Feature Store is in **BETA**. Only **Materialized** mode is production-ready.
   Virtualized and Hybrid modes are planned for Q1 2025. See roadmap below.

Tauro supports three modes of operation (with production readiness shown):

**Materialized** (Default) ✅ PRODUCTION READY
   - Pre-computed features stored in dedicated storage
   - Best for: High-speed reads, stable features
   - Trade-off: Requires storage, may become stale
   - Use when: You need fast access to computed features
   - Status: Fully implemented and tested

**Virtualized** (Coming Q1 2025) 🔄 PLANNED
   - Features computed on-demand from source layers (Silver/Gold)
   - Best for: Fresh data, low storage requirements
   - Trade-off: Higher latency, load on source
   - Use when: Data freshness is critical
   - Status: Under development

**Hybrid** (Coming Q2 2025) 🔄 PLANNED
   - Would automatically switch between materialized and virtualized
   - Best for: Optimal balance of performance and freshness
   - Trade-off: More complex configuration
   - Use when: You want automatic optimization
   - Status: Design phase

Configuration
--------------

Set your mode in ``config/global.yaml``:

.. code-block:: yaml

   # Currently only materialized is available
   feature_store:
     mode: materialized
     storage_path: /data/feature_store
     storage_format: parquet  # parquet, delta, or orc
     cache_ttl_hours: 24     # How long to keep cache in memory

**Important:** Virtualized and Hybrid modes are not yet available. Please use Materialized mode for production deployments.

Using Features in Pipelines
----------------------------

**Load features as input**

.. code-block:: yaml

   inputs:
     customer_features:
       feature_store: true
       feature_set: customer_demographics
       time_range: 30d  # Optional: recent data

**Compute and store features**

.. code-block:: python

   def compute_features(df):
       """Compute customer features."""
       features = df.groupby('customer_id').agg({
           'amount': ['sum', 'mean', 'count'],
           'date': 'max'
       })
       return features

Current Recommendations
------------------------

.. code-block:: text

   Use Materialized Mode When:
   ├─ You need sub-second response times
   ├─ Features are expensive to compute
   ├─ Data freshness of hours/days is acceptable
   └─ You want production-ready feature storage

   Planned Modes (Q1-Q2 2025):
   ├─ Virtualized: On-demand computation from source layers
   └─ Hybrid: Automatic optimization between modes

**Current Status:** Only Materialized mode is available in production.
For roadmap updates, see the main README.md

Next Steps
----------

- :doc:`configuration` - Learn all configuration options
- :doc:`best_practices` - Feature store best practices
- :doc:`tutorials/batch_etl` - See feature usage in example
