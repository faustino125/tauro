How Tauro Works (User Perspective)
===================================

This page explains what happens when you run a Tauro pipeline, so you understand the basics.

The Big Picture
---------------

When you execute a pipeline, Tauro:

1. **Loads configuration files** - Reads your YAML files to learn what to do
2. **Validates everything** - Checks that all steps exist and are properly configured
3. **Executes each step** - Runs your Python functions in order
4. **Handles errors** - Stops and reports if anything goes wrong
5. **Saves results** - Writes output to your specified location

.. code-block:: text

   You run:
   $ tauro --env prod --pipeline daily_etl

         ↓
         
   Tauro does the following:
   
   1. Load Configuration
      ✓ Read all YAML files
      ✓ Validate everything exists
      ✓ Prepare execution environment
         
   2. Execute Pipeline
      ✓ Run step 1 (extract data)
      ✓ Run step 2 (transform data)
      ✓ Run step 3 (load results)
      ✓ Handle any errors
         
   3. Complete
      ✓ Save results
      ✓ Report summary

A Simple Example
----------------

Let's walk through a real example with three steps:

**Your Configuration Files**

.. code-block:: yaml

   # config/pipelines.yaml
   pipelines:
     daily_sales:
       nodes: [extract, transform, load]

.. code-block:: yaml

   # config/nodes.yaml
   nodes:
     extract:
       function: "src.nodes.extract"
     transform:
       function: "src.nodes.transform"
     load:
       function: "src.nodes.load"

**Your Python Code**

.. code-block:: python

   # src/nodes/__init__.py
   
   def extract():
       """Load data from CSV."""
       import pandas as pd
       return pd.read_csv("data/input/sales.csv")
   
   def transform(df):
       """Clean the data."""
       return df[df['amount'] > 0]  # Keep only positive amounts
   
   def load(df):
       """Save results."""
       df.to_parquet("data/output/results.parquet")

**Execution Flow**

When you run: ``tauro --env dev --pipeline daily_sales``

.. code-block:: text

   extract()
   ├─ Reads: data/input/sales.csv
   ├─ Returns: DataFrame with 1,000 rows
   │
   └─→ transform(df)
       ├─ Receives: 1,000 rows
       ├─ Filters: Keeps only rows where amount > 0 (950 rows)
       │
       └─→ load(df)
           ├─ Receives: 950 rows
           ├─ Saves: data/output/results.parquet
           └─ Complete!

**Result**

.. code-block:: text

   ✓ Pipeline completed
   ├─ Steps executed: 3
   ├─ Time: 32 seconds
   ├─ Rows processed: 1,000
   ├─ Rows output: 950
   └─ File saved: data/output/results.parquet

How Steps Connect
-----------------

Each step's output becomes the next step's input:

- **Step 1 (extract)**: No input → Returns DataFrame
- **Step 2 (transform)**: Receives DataFrame → Returns filtered DataFrame
- **Step 3 (load)**: Receives DataFrame → Saves to file

This is why step order matters. Each step depends on the previous one.

Multiple Pipelines
-------------------

You can define multiple pipelines that reuse the same functions:

.. code-block:: yaml

   pipelines:
     daily_etl:
       nodes: [extract, transform, load]
     
     weekly_report:
       nodes: [extract, aggregate, report]

Both pipelines can use the same ``extract()`` function. You write it once, use it everywhere.

Environment-Specific Configuration
-----------------------------------

You can have different settings per environment (dev, staging, prod):

.. code-block:: text

   config/
   ├── global.yaml             ← Default settings
   ├── dev/
   │   └── global.yaml         ← Dev overrides
   ├── staging/
   │   └── global.yaml         ← Staging overrides
   └── prod/
       └── global.yaml         ← Production overrides

Example:

.. code-block:: bash

   # Development (uses test data)
   tauro --env dev --pipeline daily_sales
   
   # Production (uses real data)
   tauro --env prod --pipeline daily_sales

Error Handling
--------------

If a step fails, the pipeline stops immediately:

.. code-block:: text

   Step 1: Extract
   ├─ ✓ Success
   └─→ Step 2: Transform
       ├─ ✗ ERROR: Column 'amount' not found!
       └─ Tauro stops here
       
   Step 3: Load (not executed)

Tauro will:
   - **Stop immediately** to prevent bad data
   - **Show the error** in logs and terminal
   - **Let you fix and retry** the pipeline

Next Steps
----------

Now that you understand the basics:

- **Start building**: See :doc:`getting_started`
- **Learn configuration**: Read :doc:`configuration`
- **See examples**: Check out :doc:`tutorials/batch_etl`
- **Get help**: Visit :doc:`advanced/troubleshooting`
