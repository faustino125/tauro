How Tauro Works (User Perspective)
===================================

This page explains what happens when you run a Tauro pipeline, so you understand how everything fits together.

The Bird's Eye View
-------------------

When you execute a pipeline, here's what happens:

.. code-block:: text

   You run:
   $ tauro --env prod --pipeline daily_etl

         ↓
         
   Tauro loads your config:
   ✓ Read global.yaml (general settings)
   ✓ Read pipelines.yaml (what pipeline to run)
   ✓ Read nodes.yaml (what each step does)
   ✓ Read inputs.yaml (where data comes from)
   ✓ Read outputs.yaml (where results go)

         ↓
         
   Tauro builds the pipeline:
   ✓ Find all the steps (nodes)
   ✓ Figure out what order to run them in
   ✓ Check for errors in configuration

         ↓
         
   Tauro runs each step:
   ✓ Load input data
   ✓ Run your Python code
   ✓ Save the result
   ✓ Move to next step

         ↓
         
   Pipeline completes:
   ✓ All steps ran successfully
   ✓ Results are saved
   ✓ You get a summary


Step-by-Step Example
--------------------

Let's trace through a real example. You have this configuration:

**config/pipelines.yaml**

.. code-block:: yaml

   pipelines:
     daily_sales:
       nodes: [extract, transform, load]

**config/nodes.yaml**

.. code-block:: yaml

   nodes:
     extract:
       function: "src.nodes.extract"
     transform:
       function: "src.nodes.transform"
     load:
       function: "src.nodes.load"

**config/inputs.yaml**

.. code-block:: yaml

   inputs:
     raw_sales:
       path: data/input/sales.csv
       format: csv

**config/outputs.yaml**

.. code-block:: yaml

   outputs:
     processed:
       path: data/output/results.parquet
       format: parquet

And your Python code:

**src/nodes/extract.py**

.. code-block:: python

   import pandas as pd

   def extract():
       """Read data from CSV."""
       return pd.read_csv("data/input/sales.csv")

**src/nodes/transform.py**

.. code-block:: python

   def transform(df):
       """Clean the data."""
       return df[df['amount'] > 0]

**src/nodes/load.py**

.. code-block:: python

   def load(df):
       """Save results."""
       df.to_parquet("data/output/results.parquet")

Now when you run:

.. code-block:: bash

   tauro --env dev --pipeline daily_sales

Here's what happens:

**1. Configuration Loading**

.. code-block:: text

   Tauro reads your config files:
   
   [global.yaml] → [pipelines.yaml]
                    ↓
                [nodes.yaml]
                    ↓
                [inputs.yaml]
                    ↓
                [outputs.yaml]

   Tauro validates:
   ✓ All files are valid YAML
   ✓ All references exist
   ✓ Required fields are present

**2. Pipeline Analysis**

.. code-block:: text

   From pipelines.yaml, Tauro learns:
   - Pipeline name: "daily_sales"
   - Steps: extract → transform → load
   
   From nodes.yaml, Tauro learns:
   - extract: runs src.nodes.extract()
   - transform: runs src.nodes.transform()
   - load: runs src.nodes.load()
   
   From inputs.yaml, Tauro learns:
   - Where to get data: data/input/sales.csv
   
   From outputs.yaml, Tauro learns:
   - Where to save: data/output/results.parquet

**3. Step 1: Extract**

.. code-block:: text

   extract() function runs:
   
   Input:
   ├─ raw_sales from config/inputs.yaml
   │  └─ data/input/sales.csv (CSV file)
   │
   Processing:
   ├─ pd.read_csv("data/input/sales.csv")
   │
   Output:
   └─ DataFrame with sales data
      ├─ id, name, amount, date
      ├─ 1000 rows
      └─ Passed to next step

**4. Step 2: Transform**

.. code-block:: text

   transform() function runs:
   
   Input:
   ├─ DataFrame from extract()
   │  └─ 1000 rows with all data
   │
   Processing:
   ├─ Filter: keep only rows where amount > 0
   │
   Output:
   └─ Filtered DataFrame
      ├─ 950 rows (50 had amount <= 0)
      └─ Passed to next step

**5. Step 3: Load**

.. code-block:: text

   load() function runs:
   
   Input:
   ├─ DataFrame from transform()
   │  └─ 950 cleaned rows
   │
   Processing:
   ├─ Save to Parquet format
   │
   Output:
   └─ File written
      └─ data/output/results.parquet
         ├─ 950 rows of clean sales data
         └─ Stored efficiently in Parquet format

**6. Completion**

.. code-block:: text

   ✅ Pipeline complete!
   
   Summary:
   ├─ Started: 14:32:15
   ├─ Completed: 14:32:47
   ├─ Duration: 32 seconds
   ├─ Rows processed: 1000
   ├─ Rows output: 950
   └─ Results saved to: data/output/results.parquet


What Gets Passed Between Steps?
--------------------------------

Each step outputs something, which becomes the input to the next step:

.. code-block:: text

   extract()
       │
       ├─ Returns: pandas DataFrame
       │           (1000 rows, 5 columns)
       │
       ▼
   transform(df)
       │
       ├─ Receives: df (the DataFrame from extract)
       ├─ Returns: filtered DataFrame
       │           (950 rows, 5 columns)
       │
       ▼
   load(df)
       │
       ├─ Receives: df (the DataFrame from transform)
       ├─ Returns: nothing (just saves to file)
       │
       ▼
   Done!

This is why step order matters! Each step depends on the previous one.

Understanding Node Functions
-----------------------------

A node function is just a Python function:

.. code-block:: python

   def my_node(df):
       """A node function.
       
       Args:
           df: Input data from previous node
           
       Returns:
           processed_data: Output to pass to next node
       """
       # Your logic here
       return processed_data

**Input**: Data from previous step (or from inputs.yaml for first step)  
**Processing**: Your Python code  
**Output**: Data to pass to next step (or save if last step)

Common Node Patterns
~~~~~~~~~~~~~~~~~~~~~

**1. Read from Source (First Step)**

.. code-block:: python

   def extract():
       """Get data from somewhere."""
       # No input, just read from file/database
       df = pd.read_csv("data/input.csv")
       return df

**2. Transform (Middle Steps)**

.. code-block:: python

   def transform(df):
       """Process data."""
       # Input: df from previous step
       df_clean = df.dropna()
       df_agg = df_clean.groupby('date').sum()
       # Output: transformed data
       return df_agg

**3. Save Result (Last Step)**

.. code-block:: python

   def load(df):
       """Save data."""
       # Input: df from previous step
       df.to_csv("data/output.csv", index=False)
       # Usually no output (or return True to indicate success)

Configuration vs Code
----------------------

Your configuration tells Tauro **what to do**, but your code **does it**:

.. code-block:: text

   Config (YAML):              Code (Python):
   ─────────────────           ──────────────
   What nodes exist    ───→    What each node does
   What order          ───→    How it processes data
   Where to get data   ───→    How to read files
   Where to save       ───→    How to write results

Configuration is about **orchestration** (what and when).  
Code is about **logic** (how).

Together they create a complete pipeline.

Multiple Pipelines
-------------------

You can have multiple pipelines that share code:

.. code-block:: yaml

   # config/pipelines.yaml
   pipelines:
     daily_etl:
       nodes: [extract, transform, load]
     
     weekly_report:
       nodes: [extract, aggregate, report]
     
     data_quality:
       nodes: [validate, check_rules]

Each pipeline is independent, but they can reuse the same node functions:

.. code-block:: text

   extract()          → used by: daily_etl, weekly_report, data_quality
   transform()        → used by: daily_etl
   aggregate()        → used by: weekly_report
   report()           → used by: weekly_report
   validate()         → used by: data_quality
   check_rules()      → used by: data_quality
   load()             → used by: daily_etl

This is powerful because you write the function once and reuse it.

Environment Overrides
---------------------

You can have different configurations per environment:

.. code-block:: text

   config/
   ├── pipelines.yaml          ← Same for all environments
   ├── nodes.yaml              ← Same for all environments
   ├── global.yaml             ← Base (default)
   ├── dev/
   │   └── global.yaml         ← Dev overrides
   ├── staging/
   │   └── global.yaml         ← Staging overrides
   └── prod/
       └── global.yaml         ← Production overrides

When you run:

.. code-block:: bash

   # Development
   tauro --env dev --pipeline my_pipeline
   # Loads: config/global.yaml + config/dev/global.yaml

   # Production
   tauro --env prod --pipeline my_pipeline
   # Loads: config/global.yaml + config/prod/global.yaml

This lets you:
   - Use test data in dev
   - Use production data in prod
   - Have different timeouts in staging
   - Run with different logging levels

Error Handling
--------------

What happens if something goes wrong?

.. code-block:: text

   Step 1: Extract
   ├─ Runs extract()
   ├─ ✓ Success
   └─ Passes data to Step 2
   
   Step 2: Transform
   ├─ Runs transform()
   ├─ ✗ ERROR: Column 'amount' not found!
   ├─ Tauro catches the error
   ├─ Pipeline stops
   └─ Error logged and displayed
   
   Next steps not run:
   Step 3: Load (skipped because of error in Step 2)

Tauro's error handling:
   - **Stops immediately**: Prevents cascading failures
   - **Logs the error**: Shows you what went wrong
   - **Offers recovery**: You can fix and re-run
   - **Optional retry**: Automatically retry failed steps

Performance & Parallelism
--------------------------

Tauro can run independent steps in parallel:

.. code-block:: yaml

   # If nodes are independent:
   pipelines:
     my_pipeline:
       nodes: [load_customers, load_products, load_sales]
       # All three could run at the same time!

   # If nodes depend on each other:
   pipelines:
     my_pipeline:
       nodes: [extract, transform, load]
       # Must run in order: extract → transform → load

Tauro automatically figures out what can run in parallel.

Logging & Monitoring
--------------------

Tauro keeps track of execution:

.. code-block:: text

   Pipeline Execution Timeline:
   
   14:32:15 - Started pipeline 'daily_sales'
   14:32:16 - Starting node 'extract'
   14:32:20 - Completed node 'extract' (4.2s)
   14:32:20 - Starting node 'transform'
   14:32:35 - Completed node 'transform' (15.1s)
   14:32:35 - Starting node 'load'
   14:32:47 - Completed node 'load' (12.3s)
   14:32:47 - Pipeline completed successfully
   
   Summary:
   Total time: 32.6 seconds
   Nodes executed: 3
   Status: SUCCESS

You can increase logging detail:

.. code-block:: bash

   tauro --env dev --pipeline my_pipeline --log-level DEBUG

This shows you exactly what's happening at each step.

Key Takeaways
-------------

1. **Configuration tells Tauro what to do** (pipelines.yaml, nodes.yaml)
2. **Your code does the actual work** (Python functions)
3. **Steps run in sequence** (unless independent, then parallel)
4. **Each step's output is the next step's input**
5. **Errors stop the pipeline** (to prevent bad data)
6. **You can have multiple pipelines** using shared code
7. **Different environments use different configs**
8. **Tauro logs everything** for debugging

Next Steps
----------

Now that you understand the flow:

- :doc:`getting_started` - Create your first pipeline
- :doc:`guides/batch_etl` - Build a realistic example
- :doc:`guides/configuration` - Master configuration
- :doc:`guides/troubleshooting` - Debug when things fail
