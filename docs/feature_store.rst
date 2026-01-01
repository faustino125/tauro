Feature Store Integration
==========================

Tauro provides flexible feature store capabilities that work with your data pipeline architecture.

Feature Store Modes
-------------------

Tauro supports three modes of operation:

**Materialized** (Default)
   - Pre-computed features stored in dedicated storage
   - Best for: High-speed reads, stable features
   - Trade-off: Requires storage, may become stale
   - Use when: You need fast access to computed features

**Virtualized**
   - Features computed on-demand from source layers (Silver/Gold)
   - Best for: Fresh data, low storage requirements
   - Trade-off: Higher latency, load on source
   - Use when: Data freshness is critical

**Hybrid** (Recommended)
   - Automatically switches between materialized and virtualized
   - Best for: Optimal balance of performance and freshness
   - Trade-off: More complex configuration
   - Use when: You want automatic optimization

Configuration
--------------

Set your mode in ``config/global.yaml``:

.. code-block:: yaml

   # Choose mode: materialized, virtualized, or hybrid
   feature_store:
     mode: hybrid

For materialized mode, specify storage:

.. code-block:: yaml

   feature_store:
     mode: materialized
     storage_path: /data/feature_store
     storage_format: parquet  # parquet, delta, orc

For virtualized mode, specify source:

.. code-block:: yaml

   feature_store:
     mode: virtualized
     source_schema: silver  # or gold

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

When to Use Each Mode
---------------------

.. code-block:: text

   Use Materialized If:
   ├─ You need sub-second response times
   ├─ Features are expensive to compute
   └─ Data freshness of hours/days is acceptable

   Use Virtualized If:
   ├─ You need always-fresh data
   ├─ Storage is limited
   └─ You can afford latency

   Use Hybrid If:
   ├─ You want automatic optimization
   ├─ You have variable workload patterns
   └─ You want production reliability

Next Steps
----------

- :doc:`configuration` - Learn all configuration options
- :doc:`best_practices` - Feature store best practices
- :doc:`tutorials/batch_etl` - See feature usage in example
