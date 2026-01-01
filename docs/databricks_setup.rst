Databricks Integration Guide
=============================

This guide explains how to use Tauro with Databricks infrastructure for large-scale pipeline execution.

**Important**: Tauro executes pipelines on your infrastructure. Databricks setup and infrastructure provisioning is your responsibility.

Prerequisites
-------------

You need:
- A Databricks workspace configured and running
- Databricks access token or service principal credentials
- Appropriate permissions in Databricks to read/write data
- Unity Catalog enabled (recommended for production)

Setting Up Tauro for Databricks
---------------------------------

**Step 1: Configure credentials**

Set environment variables:

.. code-block:: bash

   export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   export DATABRICKS_TOKEN=dapi1234567890abcdefg

Or add to your ``.env`` file:

.. code-block:: bash

   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your_token_here

**Step 2: Configure Tauro for Databricks**

Update ``config/global.yaml``:

.. code-block:: yaml

   mode: databricks
   
   databricks:
     host: ${DATABRICKS_HOST}
     token: ${DATABRICKS_TOKEN}
     cluster_id: cluster-123456  # or use Databricks SQL warehouse
   
   # Use Unity Catalog paths
   input_path: /Volumes/catalog/schema/input
   output_path: /Volumes/catalog/schema/output

**Step 3: Run your pipeline**

.. code-block:: bash

   tauro --env prod --pipeline my_pipeline

Tauro will execute the pipeline on your Databricks cluster.

Common Patterns
---------------

**Using Delta Lake tables as input/output**

.. code-block:: yaml

   inputs:
     customer_data:
       path: delta_catalog.main.customers
       format: delta

   outputs:
     processed:
       path: delta_catalog.main.customers_processed
       format: delta

**Running on a specific cluster**

.. code-block:: yaml

   databricks:
     cluster_id: cluster-abc123

**Using Databricks SQL warehouse (for smaller queries)**

.. code-block:: yaml

   databricks:
     sql_warehouse_id: warehouse-abc123

Troubleshooting
---------------

**"Authentication failed"**

- Verify your token is valid
- Check DATABRICKS_HOST format
- Ensure token has appropriate permissions

**"Cluster not found"**

- Verify cluster_id is correct
- Ensure cluster is running

**"Path not found in Unity Catalog"**

- Use full path: `catalog.schema.table`
- Verify you have read/write permissions

Next Steps
----------

- :doc:`configuration` - Configure other data sources
- :doc:`best_practices` - Learn production best practices
- :doc:`tutorials/batch_etl` - See a real example
